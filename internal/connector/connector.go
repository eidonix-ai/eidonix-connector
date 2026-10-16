// Package connector serves gateway streams against the locally configured
// services. Each service type accepts exactly its fixed list of requests;
// every outbound connection goes through a dialer pinned to the hosts the
// configuration names.
package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/models"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
	"github.com/eidonix-ai/eidonix-connector/internal/storage"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

type upstream struct {
	base   *url.URL
	client *http.Client
	policy *network.Policy
	flight int
	eject  time.Time
}
type service struct {
	cfg       config.Service
	typ       servicetype.Type
	storage   *storage.Handler
	upstreams []*upstream
	flight    int
	tokens    float64
	last      time.Time
	models    []string       // advertised names
	allow     *models.Policy // nil for models: any and types without models
	// Size limits are internal defaults by type.
	maxRequest, maxResponse int64
}

// healthInterval is servicetype.HealthInterval; tests shorten it.
var healthInterval = servicetype.HealthInterval

type Connector struct {
	mu          sync.Mutex
	services    map[string]*service
	order       []string
	audit       *os.File
	auditPath   string
	auditMu     sync.Mutex
	cancel      context.CancelFunc
	storageInit sync.WaitGroup
}

// New builds the services of a validated configuration and opens the access log.
func New(c *config.Config) (*Connector, error) {
	ctx, cancel := context.WithCancel(context.Background())
	x := &Connector{services: map[string]*service{}, cancel: cancel, auditPath: c.AccessLog}
	if c.AccessLog != "" {
		f, e := os.OpenFile(c.AccessLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			cancel()
			return nil, e
		}
		x.audit = f
	}
	for _, cfg := range c.Services {
		s := &service{cfg: cfg, typ: cfg.Contract(), last: time.Now(), tokens: math.Max(1, float64(cfg.MaxRequestsPerMinute)), maxResponse: servicetype.MaxResponseBytes}
		s.maxRequest = s.typ.MaxRequestBytes
		if cfg.Models.Allowlist() {
			s.allow = models.New(cfg.Models.Names, cfg.Models.Target)
			s.models = s.allow.Names()
		}
		x.services[cfg.Name] = s
		x.order = append(x.order, cfg.Name)
		if cfg.Type == protocol.TypeStorage {
			x.storageInit.Add(1)
			go func() { defer x.storageInit.Done(); x.startStorage(ctx, s) }()
			continue
		}
		for _, raw := range cfg.URLs {
			p, e := network.New(ctx, []string{raw})
			if e != nil {
				x.Close()
				return nil, fmt.Errorf("%s service %q: %w", cfg.Type, cfg.Name, e)
			}
			u, _ := url.Parse(raw)
			t := &http.Transport{Proxy: nil, DialContext: p.DialContext, DisableCompression: true, MaxResponseHeaderBytes: 64 << 10, ResponseHeaderTimeout: max(120*time.Second, cfg.IdleTimeout)}
			s.upstreams = append(s.upstreams, &upstream{base: u, policy: p, client: &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}})
		}
		go x.health(ctx, s, healthInterval)
		for _, u := range s.upstreams {
			go x.refreshDNS(ctx, u)
		}
	}
	return x, nil
}
func (x *Connector) Close() error {
	x.cancel()
	x.storageInit.Wait()
	x.mu.Lock()
	for _, s := range x.services {
		if s.storage != nil {
			s.storage.Close()
		}
		for _, u := range s.upstreams {
			u.client.CloseIdleConnections()
		}
	}
	x.mu.Unlock()
	x.auditMu.Lock()
	defer x.auditMu.Unlock()
	if x.audit != nil {
		return x.audit.Close()
	}
	return nil
}
func (x *Connector) healthy(s *service) bool {
	if s.cfg.Type == protocol.TypeStorage {
		return s.storage != nil && s.storage.Healthy()
	}
	for _, u := range s.upstreams {
		if time.Now().After(u.eject) {
			return true
		}
	}
	return false
}

// HelloServices advertises every configured service.
func (x *Connector) HelloServices() []protocol.Service {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := []protocol.Service{}
	for _, name := range x.order {
		s := x.services[name]
		a := protocol.Service{Name: name, Type: s.cfg.Type, Models: append([]string{}, s.models...), ModelsAny: s.cfg.Models.Any, MaxConcurrentRequests: s.cfg.MaxConcurrentRequests, Healthy: x.healthy(s)}
		if st := s.cfg.Storage; st != nil {
			a.Storage = &protocol.StorageService{MaxKeyBytes: st.MaxKeyBytes(), MaxObjectBytes: st.MaxFileSize, Encryption: st.Encryption}
		}
		out = append(out, a)
	}
	return out
}

// StatusServices reports health, in-flight requests and the current model list.
func (x *Connector) StatusServices() []protocol.ServiceStatus {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := []protocol.ServiceStatus{}
	for _, name := range x.order {
		s := x.services[name]
		flight := s.flight
		if s.storage != nil {
			flight = s.storage.InFlight()
		}
		out = append(out, protocol.ServiceStatus{Name: name, Healthy: x.healthy(s), Models: append([]string{}, s.models...), InFlight: flight})
	}
	return out
}

// health probes GET <url>/health on every upstream: any response below 500
// counts as healthy; dial errors, timeouts and 5xx eject the address for 30s.
// Services with models: any also refresh their model list.
func (x *Connector) health(ctx context.Context, s *service, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, u := range s.upstreams {
				c, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := u.policy.Refresh(c)
				if err == nil {
					req, _ := http.NewRequestWithContext(c, "GET", u.base.String()+servicetype.HealthPath, nil)
					x.authorize(req.Header, s)
					resp, e := u.client.Do(req)
					err = e
					if e == nil {
						io.Copy(io.Discard, io.LimitReader(resp.Body, 65536))
						resp.Body.Close()
						if resp.StatusCode >= 500 {
							err = fmt.Errorf("health status %d", resp.StatusCode)
						}
					}
				}
				if err == nil && s.cfg.Models.Any {
					x.discover(c, s, u)
				}
				cancel()
				x.mu.Lock()
				if err == nil {
					u.eject = time.Time{}
				} else {
					u.eject = time.Now().Add(servicetype.EjectFor)
				}
				x.mu.Unlock()
			}
		}
	}
}

// acquire reserves a request slot: busy at max_concurrent_requests,
// rate_limited past max_requests_per_minute, upstream_unavailable when every
// address is ejected. The least-loaded healthy address is chosen.
func (x *Connector) acquire(s *service) (*upstream, string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if s.flight >= s.cfg.MaxConcurrentRequests {
		return nil, "busy"
	}
	now := time.Now()
	if rate := float64(s.cfg.MaxRequestsPerMinute); rate > 0 {
		s.tokens = math.Min(rate, s.tokens+now.Sub(s.last).Minutes()*rate)
		s.last = now
		if s.tokens < 1 {
			return nil, "rate_limited"
		}
		s.tokens--
	}
	var chosen *upstream
	for _, u := range s.upstreams {
		if now.Before(u.eject) {
			continue
		}
		if chosen == nil || u.flight < chosen.flight {
			chosen = u
		}
	}
	if chosen == nil {
		return nil, "upstream_unavailable"
	}
	s.flight++
	chosen.flight++
	return chosen, ""
}
func (x *Connector) release(s *service, u *upstream, bad bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	s.flight--
	u.flight--
	if bad {
		for _, other := range s.upstreams {
			if other != u && !time.Now().Before(other.eject) {
				u.eject = time.Now().Add(servicetype.EjectFor)
				break
			}
		}
	}
}

// errorResponse writes a connector-generated HTTP error carrying a closed error code.
func errorResponse(c net.Conn, code string) int {
	status := protocol.HTTPStatus(code)
	body := code + "\n"
	fmt.Fprintf(c, "HTTP/1.1 %d %s\r\n%s: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", status, http.StatusText(status), protocol.HeaderError, code, len(body), body)
	return status
}

type idleConn struct {
	net.Conn
	idle time.Duration
	mu   sync.Mutex
}

func (c *idleConn) touch() { c.mu.Lock(); c.Conn.SetDeadline(time.Now().Add(c.idle)); c.mu.Unlock() }
func (c *idleConn) Read(b []byte) (int, error) {
	c.touch()
	n, e := c.Conn.Read(b)
	if n > 0 {
		c.touch()
	}
	return n, e
}
func (c *idleConn) Write(b []byte) (int, error) {
	c.touch()
	n, e := c.Conn.Write(b)
	if n > 0 {
		c.touch()
	}
	return n, e
}

type counted struct {
	readMu   sync.Mutex
	source   io.Closer
	r        io.Reader
	n        atomic.Int64
	max      int64
	touch    func()
	overflow atomic.Bool
}

func (c *counted) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.n.Load() >= c.max {
		var b [1]byte
		n, e := c.r.Read(b[:])
		if n > 0 {
			c.overflow.Store(true)
			return 0, fmt.Errorf("size limit")
		}
		return 0, e
	}
	if int64(len(p)) > c.max-c.n.Load() {
		p = p[:c.max-c.n.Load()]
	}
	n, e := c.r.Read(p)
	c.n.Add(int64(n))
	if n > 0 {
		c.touch()
	}
	return n, e
}

func (c *counted) Close() error {
	if c.source == nil {
		return nil
	}
	err := c.source.Close()
	c.readMu.Lock()
	c.readMu.Unlock()
	return err
}
func timedOut(err error) bool {
	var n net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &n) && n.Timeout())
}
func connectFailed(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// ReopenLogs changes the access log file without interrupting active streams.
func (x *Connector) ReopenLogs() error {
	if x.auditPath == "" {
		return nil
	}
	f, err := os.OpenFile(x.auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	x.auditMu.Lock()
	defer x.auditMu.Unlock()
	old := x.audit
	x.audit = f
	if old != nil {
		return old.Close()
	}
	return nil
}

// forwarded request headers; each type may add its own (e.g. Anthropic-Version).
var forwarded = []string{"Content-Type", "Accept", "X-Request-Id"}

func cleanHeaders(in http.Header, extra []string) http.Header {
	h := make(http.Header)
	for _, k := range append(forwarded, extra...) {
		if v := in.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return h
}

// authorize adds the service's api_key, the only credential the connector sends upstream.
func (x *Connector) authorize(h http.Header, s *service) {
	if s.cfg.APIKey != "" {
		h.Set("Authorization", "Bearer "+s.cfg.APIKey)
	}
}

// allowed reports whether the service accepts this request (method WEBSOCKET for upgrades).
func (s *service) allowed(method, path string) bool {
	if s.cfg.Type == protocol.TypeCustom {
		for _, r := range s.cfg.Paths {
			if r.Method == method && r.Path == path {
				return true
			}
		}
		return false
	}
	return s.typ.Allows(method, path)
}

// upstreamPath maps the gateway-facing path through api_path.
func (s *service) upstreamPath(path string) string {
	if s.cfg.Type == protocol.TypeCustom {
		return path
	}
	return s.typ.UpstreamPath(s.cfg.APIPath, path)
}

func (x *Connector) ServeStream(parent context.Context, raw net.Conn) {
	defer raw.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() { <-ctx.Done(); raw.Close() }()
	raw.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReaderSize(raw, 4096)
	entry := &accessEntry{Status: 403}
	storageAudit := false
	start := time.Now()
	var h protocol.StreamHeader
	defer func() {
		if !storageAudit {
			entry.Time, entry.Session, entry.DurationMS = time.Now().UTC(), protocol.SessionID(ctx), time.Since(start).Milliseconds()
			x.writeAccess(entry)
		}
	}()
	var e error
	h, e = protocol.ReadStreamHeader(br)
	if e != nil {
		entry.Status, entry.Error = 400, "invalid_stream_header"
		return
	}
	entry.Service, entry.RequestID, entry.OrgID, entry.OrgSlug, entry.UserKeyID = h.Service, h.RequestID, h.OrgID, h.OrgSlug, h.UserKeyID
	x.mu.Lock()
	s := x.services[h.Service]
	var storageHandler *storage.Handler
	if s != nil {
		storageHandler = s.storage
		entry.Type = s.cfg.Type
	}
	x.mu.Unlock()
	if h.Kind != "http" && h.Kind != "websocket" && h.Kind != "storage" {
		entry.Status, entry.Error = 400, "unsupported_kind"
		protocol.WriteStorageResult(raw, protocol.StorageErrorResult("unsupported_kind"), "")
		return
	}
	if h.Kind == "storage" {
		switch {
		case s == nil:
			entry.Error = "route_not_found"
		case s.cfg.Type != protocol.TypeStorage:
			entry.Error = "wrong_route_type"
		case storageHandler == nil:
			entry.Error = "backend_unavailable"
		default:
			storageAudit = true
			storageHandler.Serve(ctx, raw, br, h)
			return
		}
		protocol.WriteStorageResult(raw, protocol.StorageErrorResult(entry.Error), h.Op)
		return
	}
	if s == nil {
		entry.Error = "route_not_found"
		entry.Status = errorResponse(raw, entry.Error)
		return
	}
	if s.cfg.Type == protocol.TypeStorage {
		entry.Error = "path_not_allowed"
		entry.Status = errorResponse(raw, entry.Error)
		return
	}
	c := &idleConn{Conn: raw, idle: s.cfg.IdleTimeout}
	c.touch()
	fail := func(code string) { entry.Error = code; entry.Status = errorResponse(c, code) }

	header := []byte{}
	for {
		line, err := br.ReadSlice('\n')
		header = append(header, line...)
		if err != nil && err != bufio.ErrBufferFull || len(header) > 64<<10 {
			fail("invalid_request")
			return
		}
		if string(line) == "\r\n" {
			break
		}
	}
	br = bufio.NewReader(io.MultiReader(bytes.NewReader(header), br))
	req, e := http.ReadRequest(br)
	if e != nil {
		fail("invalid_request")
		return
	}
	defer func() { raw.Close(); req.Body.Close() }()
	entry.Method, entry.Path = req.Method, req.URL.Path
	path := req.RequestURI
	// Queries are never forwarded; they are tolerated (and dropped) only on GET.
	if req.URL.IsAbs() || req.URL.Host != "" || servicetype.ValidatePath(strings.Split(path, "?")[0]) != nil || strings.Contains(path, "?") && req.Method != "GET" {
		fail("path_not_allowed")
		return
	}
	method := req.Method
	if h.Kind == "websocket" {
		if req.Method != "GET" {
			fail("path_not_allowed")
			return
		}
		method = servicetype.WebSocket
	} else if req.Method != "GET" && req.Method != "POST" && req.Method != "PUT" {
		fail("path_not_allowed")
		return
	}
	if !s.allowed(method, req.URL.Path) {
		fail("path_not_allowed")
		return
	}
	if h.Kind == "http" && req.Method != "GET" {
		ct, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
		ok := false
		for _, v := range s.typ.ContentTypes {
			ok = ok || ct == v
		}
		if err != nil || !ok {
			fail("content_type_not_allowed")
			return
		}
	}
	maxBody := s.maxRequest
	if req.ContentLength > maxBody {
		fail("request_too_large")
		return
	}
	// An allowlisted service answers its own model list.
	if s.allow != nil && req.Method == "GET" && req.URL.Path == "/v1/models" {
		list := s.allow.ModelList()
		fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(list), list)
		entry.Status, entry.BytesOut = 200, int64(len(list))
		return
	}
	u, code := x.acquire(s)
	if code != "" {
		fail(code)
		return
	}
	bad := false
	defer func() { x.release(s, u, bad) }()
	entry.UpstreamHost = u.base.Host
	if h.Kind == "websocket" {
		entry.Status, entry.BytesIn, entry.BytesOut, bad = x.websocket(ctx, c, br, s, u, req)
		return
	}
	body := &counted{r: req.Body, source: req.Body, max: maxBody, touch: c.touch}
	defer func() { raw.Close(); body.Close(); entry.BytesIn = body.n.Load() }()
	body.r = &endReader{Reader: req.Body, remaining: req.ContentLength, end: func() {
		go func() {
			var b [1]byte
			_, err := raw.Read(b[:])
			if !timedOut(err) {
				cancel()
			}
		}()
	}}
	var upstreamBody io.Reader = body
	upstreamLength := req.ContentLength
	var startRewrite func() error
	var rewriteInvalid atomic.Bool
	setModel := func(r models.Result) {
		entry.Model = r.Model
		if r.Renamed() {
			entry.UpstreamModel = r.Target
		}
	}
	modelError := func(err error) {
		switch {
		case body.overflow.Load():
			fail("request_too_large")
		case errors.Is(err, models.ErrNotAllowed):
			fail("model_not_allowed")
		case errors.Is(err, models.ErrRequired):
			fail("model_required")
		default:
			fail("invalid_request")
		}
	}
	if s.allow != nil && req.Method != "GET" {
		ct, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
		switch ct {
		case "application/json":
			// Bounded by the type's request limit (and the service's concurrency) like any body.
			data, err := io.ReadAll(body)
			if err != nil {
				modelError(err)
				return
			}
			rewritten, r, err := s.allow.RewriteJSON(data)
			if r.Model != "" {
				entry.Model = r.Model
			}
			if err != nil {
				modelError(err)
				return
			}
			setModel(r)
			upstreamBody, upstreamLength = bytes.NewReader(rewritten), int64(len(rewritten))
		case "multipart/form-data":
			// Streamed part by part; nothing is sent upstream until the model is accepted.
			pr, pw := io.Pipe()
			decision := make(chan error, 1)
			finished := make(chan struct{})
			started := false
			startRewrite = func() error {
				started = true
				go func() {
					defer close(finished)
					err := s.allow.RewriteMultipart(pw, body, params["boundary"], func(r models.Result, err error) {
						if r.Model != "" {
							entry.Model = r.Model
						}
						if err == nil {
							setModel(r)
						}
						decision <- err
					})
					if errors.Is(err, models.ErrInvalid) {
						rewriteInvalid.Store(true)
					}
					pw.CloseWithError(err)
				}()
				return <-decision
			}
			defer func() {
				if !started {
					return
				}
				// Unblock the rewriter (client read or pipe write), then wait for it.
				raw.Close()
				pr.Close()
				<-finished
			}()
			upstreamBody, upstreamLength = pr, -1
		}
	}
	target := *u.base
	target.Path = s.upstreamPath(req.URL.Path)
	target.RawQuery = ""
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	up, e := http.NewRequestWithContext(requestCtx, req.Method, target.String(), upstreamBody)
	if e != nil {
		return
	}
	up.ContentLength = upstreamLength
	up.Header = cleanHeaders(req.Header, s.typ.Headers)
	x.authorize(up.Header, s)
	idle := s.cfg.IdleTimeout
	var idleExpired atomic.Bool
	timer := time.AfterFunc(idle, func() { idleExpired.Store(true); cancelRequest() })
	defer timer.Stop()
	body.touch = func() { c.touch(); timer.Reset(idle) }
	if startRewrite != nil {
		if err := startRewrite(); err != nil {
			modelError(err)
			return
		}
	}
	resp, e := u.client.Do(up)
	if e != nil {
		bad = connectFailed(e)
		code := "upstream_error"
		if timedOut(e) || idleExpired.Load() {
			code = "upstream_timeout"
		}
		if body.overflow.Load() {
			bad, code = false, "request_too_large"
		} else if rewriteInvalid.Load() {
			bad, code = false, "invalid_request"
		}
		fail(code)
		return
	}
	defer resp.Body.Close()
	c.touch()
	timer.Reset(idle)
	entry.Status = resp.StatusCode
	bad = resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504
	resp.Trailer = nil
	resp.Header = cleanResponse(resp.Header)
	limit := s.maxResponse
	if resp.ContentLength > limit {
		fail("response_too_large")
		return
	}
	counter := &counted{r: resp.Body, max: limit, touch: func() { c.touch(); timer.Reset(idle) }}
	resp.Body = io.NopCloser(counter)
	resp.ContentLength = -1
	resp.TransferEncoding = []string{"chunked"}
	resp.Close = true
	writeErr := resp.Write(c)
	entry.BytesOut = counter.n.Load()
	if counter.overflow.Load() {
		entry.Status, entry.Error = 502, "response_too_large"
	} else if idleExpired.Load() || timedOut(writeErr) {
		entry.Status, entry.Error = 504, "upstream_timeout"
	} else if writeErr != nil {
		entry.Error = "response_interrupted"
	}
}
func cleanResponse(in http.Header) http.Header {
	h := make(http.Header)
	for _, k := range []string{"Content-Type", "Cache-Control", "Retry-After", "X-Request-Id"} {
		if v := in.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return h
}
func (x *Connector) websocket(ctx context.Context, c *idleConn, br *bufio.Reader, s *service, u *upstream, req *http.Request) (int, int64, int64, bool) {
	target := *u.base
	if target.Scheme == "http" {
		target.Scheme = "ws"
	} else {
		target.Scheme = "wss"
	}
	target.Path = s.upstreamPath(req.URL.Path)
	d := websocket.Dialer{NetDialContext: u.policy.DialContext, HandshakeTimeout: c.idle, Proxy: nil}
	headers := cleanHeaders(req.Header, s.typ.Headers)
	x.authorize(headers, s)
	ws, _, e := d.DialContext(ctx, target.String(), headers)
	if e != nil {
		errorResponse(c, "upstream_error")
		return 502, 0, 0, true
	}
	defer ws.Close()
	fmt.Fprint(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	max := servicetype.MaxWebSocketMessageBytes
	ws.SetReadLimit(int64(max))
	ws.UnderlyingConn().SetReadDeadline(time.Now().Add(c.idle))
	done := make(chan error, 2)
	var in, out int64
	go func() {
		for {
			frame, e := protocol.ReadFrame(br, max)
			typ, p := frame.Type, frame.Payload
			if e != nil {
				done <- e
				return
			}
			ws.SetWriteDeadline(time.Now().Add(c.idle))
			if typ == 8 {
				done <- ws.WriteControl(websocket.CloseMessage, p, time.Now().Add(time.Second))
				return
			}
			atomic.AddInt64(&in, int64(len(p)))
			if e = ws.WriteMessage(int(typ), p); e != nil {
				done <- e
				return
			}
			c.touch()
			ws.UnderlyingConn().SetReadDeadline(time.Now().Add(c.idle))
		}
	}()
	go func() {
		for {
			typ, p, e := ws.ReadMessage()
			if e != nil {
				done <- e
				return
			}
			atomic.AddInt64(&out, int64(len(p)))
			if e = protocol.WriteFrame(c, protocol.Frame{Type: byte(typ), Payload: p}, max); e != nil {
				done <- e
				return
			}
			ws.UnderlyingConn().SetReadDeadline(time.Now().Add(c.idle))
		}
	}()
	select {
	case <-ctx.Done():
		c.Close()
		ws.Close()
		<-done
	case <-done:
	}
	c.Close()
	ws.Close()
	<-done
	return 101, atomic.LoadInt64(&in), atomic.LoadInt64(&out), false
}

type endReader struct {
	io.Reader
	once      sync.Once
	remaining int64
	end       func()
}

func (r *endReader) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	if r.remaining >= 0 {
		r.remaining -= int64(n)
	}
	if e == io.EOF || r.remaining == 0 {
		r.once.Do(r.end)
	}
	return n, e
}

// discover refreshes the advertised model list of a models: any service from
// the server's own model list.
func (x *Connector) discover(ctx context.Context, s *service, u *upstream) {
	if !s.typ.Allows("GET", "/v1/models") {
		return
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", u.base.String()+s.upstreamPath("/v1/models"), nil)
	x.authorize(req.Header, s)
	resp, e := u.client.Do(req)
	if e != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}
	var data struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&data) != nil {
		return
	}
	ids := []string{}
	for _, m := range data.Data {
		if len(ids) >= 256 {
			break
		}
		if len(m.ID) > 0 && len(m.ID) <= 256 {
			ids = append(ids, m.ID)
		}
	}
	x.mu.Lock()
	s.models = ids
	x.mu.Unlock()
}

func (x *Connector) refreshDNS(ctx context.Context, u *upstream) {
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			e := u.policy.Refresh(c)
			cancel()
			if e != nil {
				x.mu.Lock()
				u.eject = time.Now().Add(servicetype.EjectFor)
				x.mu.Unlock()
				log.Printf("upstream %s marked unhealthy: %v", u.base.Host, e)
			}
		}
	}
}

func (x *Connector) auditStorage(entry storage.AuditEntry) {
	x.auditMu.Lock()
	defer x.auditMu.Unlock()
	if x.audit != nil {
		json.NewEncoder(x.audit).Encode(entry)
	}
}

func (x *Connector) startStorage(ctx context.Context, s *service) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		handler, err := storage.New(attempt, s.cfg, x.auditStorage)
		cancel()
		if err == nil {
			if ctx.Err() != nil {
				handler.Close()
				return
			}
			x.mu.Lock()
			s.storage = handler
			handler.Start(ctx)
			x.mu.Unlock()
			return
		}
		x.auditStorage(storage.AuditEntry{Time: time.Now().UTC(), Origin: "local", Service: s.cfg.Name, Type: protocol.TypeStorage, Op: "initialize", Status: "backend_unavailable"})
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
