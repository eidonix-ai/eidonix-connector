package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/modelalias"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
	"github.com/eidonix-ai/eidonix-connector/internal/profiles"
	"github.com/eidonix-ai/eidonix-connector/internal/storage"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
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
)

type upstream struct {
	base   *url.URL
	client *http.Client
	policy *network.Policy
	flight int
	eject  time.Time
}
type route struct {
	cfg       config.Route
	storage   *storage.Handler
	upstreams []*upstream
	flight    int
	next      int
	tokens    float64
	last      time.Time
	models    []string
	aliases   *modelalias.Table
}
type Connector struct {
	mu            sync.Mutex
	routes        map[string]*route
	order         []string
	audit         *os.File
	auditPath     string
	auditMu       sync.Mutex
	cancel        context.CancelFunc
	storageInit   sync.WaitGroup
	listener      net.Listener
	healthAddress string
	healthTarget  *atomic.Pointer[Connector]
}

func New(c *config.Config) (*Connector, error) {
	normalized, err := c.Normalized()
	if err != nil {
		return nil, err
	}
	c = normalized
	ctx, cancel := context.WithCancel(context.Background())
	x := &Connector{routes: map[string]*route{}, cancel: cancel, auditPath: c.AuditLog}
	if c.AuditLog != "" {
		f, e := os.OpenFile(c.AuditLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			cancel()
			return nil, e
		}
		x.audit = f
	}
	for _, cfg := range c.Routes {
		r := &route{cfg: cfg, tokens: math.Max(1, cfg.RateLimit), last: time.Now(), aliases: modelalias.New(cfg.Models)}
		// Until discovery succeeds (or when the upstream has no model list), report the aliases.
		r.models = r.aliases.Names()
		if cfg.Type == "object_storage" {
			x.routes[cfg.ID] = r
			x.order = append(x.order, cfg.ID)
			x.storageInit.Add(1)
			go func() { defer x.storageInit.Done(); x.startStorage(ctx, r) }()
			continue
		}
		for _, s := range cfg.Upstreams {
			p, e := network.New(ctx, []string{s}, cfg.AllowCIDRs)
			if e != nil {
				x.Close()
				return nil, e
			}
			u, _ := url.Parse(s)
			t := &http.Transport{Proxy: nil, DialContext: p.DialContext, DisableCompression: true, MaxResponseHeaderBytes: 64 << 10, ResponseHeaderTimeout: max(120*time.Second, routeIdle(cfg))}
			r.upstreams = append(r.upstreams, &upstream{base: u, policy: p, client: &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}})
		}
		x.routes[cfg.ID] = r
		x.order = append(x.order, cfg.ID)
		go x.health(ctx, r)
		for _, u := range r.upstreams {
			go x.refreshDNS(ctx, u)
		}
	}
	if c.HealthListen != "" {
		l, e := net.Listen("tcp", c.HealthListen)
		if e != nil {
			x.Close()
			return nil, e
		}
		x.listener = l
		x.healthAddress = c.HealthListen
		x.healthTarget = &atomic.Pointer[Connector]{}
		x.healthTarget.Store(x)
		target := x.healthTarget
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" || r.URL.Path != "/health" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(target.Load().StatusServices())
		}), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
		go server.Serve(l)
	}
	return x, nil
}
func (x *Connector) Close() error {
	x.cancel()
	x.storageInit.Wait()
	x.mu.Lock()
	if x.listener != nil {
		x.listener.Close()
	}
	for _, r := range x.routes {
		if r.storage != nil {
			r.storage.Close()
		}
		for _, u := range r.upstreams {
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
func (x *Connector) healthy(r *route) bool {
	if r.storage != nil {
		return r.storage.Healthy()
	}
	for _, u := range r.upstreams {
		if time.Now().After(u.eject) {
			return true
		}
	}
	return false
}
func (x *Connector) HelloServices() []protocol.Service {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := []protocol.Service{}
	for _, id := range x.order {
		r := x.routes[id]
		advertisement := protocol.Service{Name: id, Type: r.cfg.Type, MaxConcurrentRequests: r.cfg.MaxConcurrency, Healthy: x.healthy(r), Models: append([]string{}, r.models...)}
		if r.cfg.Type == "object_storage" {
			maxObject, _ := config.Bytes(r.cfg.MaxObjectBytes)
			advertisement.Type = protocol.TypeStorage
			advertisement.Storage = &protocol.StorageService{MaxKeyBytes: 1024 - len(r.cfg.Prefix), MaxObjectBytes: maxObject, Encryption: r.cfg.Encryption.Mode}
		}
		out = append(out, advertisement)
	}
	return out
}
func (x *Connector) StatusServices() []protocol.ServiceStatus {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := []protocol.ServiceStatus{}
	for _, id := range x.order {
		r := x.routes[id]
		flight := r.flight
		if r.storage != nil {
			flight = r.storage.InFlight()
		}
		out = append(out, protocol.ServiceStatus{Name: id, Healthy: x.healthy(r), Models: append([]string{}, r.models...), InFlight: flight})
	}
	return out
}
func (x *Connector) health(ctx context.Context, r *route) {
	d, _ := time.ParseDuration(r.cfg.Health.Interval)
	tick := time.NewTicker(d)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, u := range r.upstreams {
				c, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := u.policy.Refresh(c)
				if err == nil {
					req, _ := http.NewRequestWithContext(c, "GET", u.base.String()+r.cfg.Health.Path, nil)
					for k, v := range r.cfg.UpstreamHeaders {
						req.Header.Set(k, v)
					}
					resp, e := u.client.Do(req)
					if e == nil && resp.StatusCode == 404 && r.cfg.Flavor == "go-llm-proxy" {
						resp.Body.Close()
						req.URL.Path = "/v1/models"
						resp, e = u.client.Do(req)
					}
					err = e
					if e == nil {
						io.Copy(io.Discard, io.LimitReader(resp.Body, 65536))
						resp.Body.Close()
						if resp.StatusCode >= 400 {
							err = fmt.Errorf("health status")
						}
					}
				}
				if err == nil {
					x.discover(c, r, u)
				}
				cancel()
				x.mu.Lock()
				if err == nil {
					u.eject = time.Time{}
				}
				if err != nil {
					u.eject = time.Now().Add(30 * time.Second)
				}
				x.mu.Unlock()
			}
		}
	}
}
func (x *Connector) acquire(r *route) (*upstream, int) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if r.flight >= r.cfg.MaxConcurrency {
		return nil, 429
	}
	now := time.Now()
	if r.cfg.RateLimit > 0 {
		r.tokens += now.Sub(r.last).Seconds() * r.cfg.RateLimit
		if r.tokens > math.Max(1, r.cfg.RateLimit) {
			r.tokens = math.Max(1, r.cfg.RateLimit)
		}
		r.last = now
		if r.tokens < 1 {
			return nil, 429
		}
		r.tokens--
	}
	var chosen *upstream
	for i := 0; i < len(r.upstreams); i++ {
		u := r.upstreams[(r.next+i)%len(r.upstreams)]
		if now.Before(u.eject) {
			continue
		}
		if chosen == nil || r.cfg.Balance == "least_inflight" && u.flight < chosen.flight {
			chosen = u
		}
		if r.cfg.Balance == "round_robin" {
			r.next = (r.next + i + 1) % len(r.upstreams)
			break
		}
	}
	if chosen == nil {
		return nil, 502
	}
	r.flight++
	chosen.flight++
	return chosen, 0
}
func (x *Connector) release(r *route, u *upstream, bad bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	r.flight--
	u.flight--
	if bad {
		for _, other := range r.upstreams {
			if other != u && !time.Now().Before(other.eject) {
				u.eject = time.Now().Add(30 * time.Second)
				break
			}
		}
	}
}
func errorResponse(c net.Conn, status int, code string) {
	body := code + "\n"
	fmt.Fprintf(c, "HTTP/1.1 %d %s\r\n"+protocol.HeaderError+": %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", status, http.StatusText(status), code, len(body), body)
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
func routeIdle(r config.Route) time.Duration { d, _ := time.ParseDuration(r.IdleTimeout); return d }
func timedOut(err error) bool {
	var n net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &n) && n.Timeout())
}
func connectFailed(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// ReopenLogs changes the audit file without interrupting active streams.
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

var forwarded = []string{"Content-Type", "Accept", "X-Request-Id"}

func cleanHeaders(in http.Header) http.Header {
	h := make(http.Header)
	for _, k := range forwarded {
		if v := in.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return h
}
func (x *Connector) ServeStream(parent context.Context, raw net.Conn) {
	defer raw.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() { <-ctx.Done(); raw.Close() }()
	raw.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReaderSize(raw, 4096)
	var h protocol.StreamHeader
	storageAudit := false
	method, auditPath := "", ""
	status := 403
	var in, out int64
	upname, auditError := "", ""
	var modelRewrite *modelalias.Rewrite
	start := time.Now()
	defer func() {
		if storageAudit {
			return
		}
		x.auditMu.Lock()
		defer x.auditMu.Unlock()
		if x.audit != nil {
			entry := map[string]any{"timestamp": time.Now().UTC(), "session": protocol.SessionID(ctx), "route": h.Service, "method": method, "path": auditPath, "status": status, "bytes_in": in, "bytes_out": out, "duration_ms": time.Since(start).Milliseconds(), "upstream": upname, "request_id": h.RequestID, "error": auditError}
			if modelRewrite != nil {
				// Model ids are metadata; request and response bodies are never logged.
				entry["model"] = modelRewrite.Alias
				entry["upstream_model"] = modelRewrite.Target
			}
			json.NewEncoder(x.audit).Encode(entry)
		}
	}()
	var e error
	h, e = protocol.ReadStreamHeader(br)
	if e != nil {
		h = protocol.StreamHeader{}
		status = 400
		auditError = "invalid_stream_header"
		return
	}
	x.mu.Lock()
	r := x.routes[h.Service]
	var storageHandler *storage.Handler
	if r != nil {
		storageHandler = r.storage
	}
	x.mu.Unlock()
	if h.Kind != "http" && h.Kind != "websocket" && h.Kind != "storage" {
		status = 400
		auditError = "unsupported_kind"
		protocol.WriteStorageResult(raw, protocol.StorageErrorResult("unsupported_kind"), "")
		return
	}
	if h.Kind == "storage" {
		if r == nil {
			auditError = "route_not_found"
			protocol.WriteStorageResult(raw, protocol.StorageErrorResult(auditError), h.Op)
			return
		}
		if r.cfg.Type != "object_storage" {
			auditError = "wrong_route_type"
			protocol.WriteStorageResult(raw, protocol.StorageErrorResult(auditError), h.Op)
			return
		}
		if storageHandler == nil {
			auditError = "backend_unavailable"
			protocol.WriteStorageResult(raw, protocol.StorageErrorResult(auditError), h.Op)
			return
		}
		storageAudit = true
		storageHandler.Serve(ctx, raw, br, h)
		return
	}
	if r != nil && r.cfg.Type == "object_storage" {
		auditError = "path_not_allowed"
		errorResponse(raw, 403, auditError)
		return
	}
	if r == nil {
		status = 404
		errorResponse(raw, 404, "route_not_found")
		return
	}
	idle, _ := time.ParseDuration(r.cfg.IdleTimeout)
	c := &idleConn{Conn: raw, idle: idle}
	c.touch()

	header := []byte{}
	for {
		line, err := br.ReadSlice('\n')
		header = append(header, line...)
		if err != nil && err != bufio.ErrBufferFull || len(header) > 64<<10 {
			errorResponse(c, 403, "invalid_headers")
			return
		}
		if string(line) == "\r\n" {
			break
		}
	}
	br = bufio.NewReader(io.MultiReader(strings.NewReader(string(header)), br))
	req, e := http.ReadRequest(br)
	if e != nil {
		errorResponse(c, 403, "invalid_request")
		return
	}
	defer func() { raw.Close(); req.Body.Close() }()
	method, auditPath = req.Method, req.URL.Path
	path := req.RequestURI
	if req.URL.IsAbs() || req.URL.Host != "" || profiles.ValidatePath(strings.Split(path, "?")[0]) != nil || strings.Contains(path, "?") && !(req.Method == "GET" && req.URL.Path == "/v1/models") {
		errorResponse(c, 403, "path_not_allowed")
		return
	}
	allowed := false
	if h.Kind == "http" {
		for _, p := range r.cfg.MethodsPaths {
			allowed = allowed || p[0] == req.Method && p[1] == req.URL.Path
		}
	} else if h.Kind == "websocket" && req.Method == "GET" {
		for _, p := range r.cfg.WebsocketPaths {
			allowed = allowed || p == req.URL.Path
		}
	}
	if !allowed {
		errorResponse(c, 403, "path_not_allowed")
		return
	}
	if h.Kind == "http" && req.Method != "GET" {
		ct, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
		types := r.cfg.ContentTypes
		if len(types) == 0 {
			types = profiles.Table[r.cfg.Type].ContentTypes
		}
		ok := false
		for _, v := range types {
			ok = ok || ct == v
		}
		if err != nil || !ok {
			errorResponse(c, 403, "content_type_not_allowed")
			return
		}
	}
	max, _ := config.Bytes(r.cfg.MaxRequestBytes)
	if req.ContentLength > max {
		status = 413
		errorResponse(c, 413, "request_too_large")
		return
	}
	u, fail := x.acquire(r)
	if fail != 0 {
		status = fail
		code := "upstream_unavailable"
		if fail == 429 {
			code = "busy"
		}
		auditError = code
		errorResponse(c, fail, code)
		return
	}
	bad := false
	defer func() { x.release(r, u, bad) }()
	upname = u.base.Host
	if h.Kind == "websocket" {
		status, in, out, bad = x.websocket(ctx, c, br, r, u, req)
		return
	}
	body := &counted{r: req.Body, source: req.Body, max: max, touch: c.touch}
	defer func() { raw.Close(); body.Close(); in = body.n.Load() }()
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
	var rewriteInvalid atomic.Bool
	var startRewrite func()
	rewriteStarted := false
	if r.aliases != nil && req.Method != "GET" {
		ct, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
		switch ct {
		case "application/json":
			// Bounded by max_request_bytes (and the route's concurrency) like any request body.
			data, err := io.ReadAll(body)
			if err != nil {
				status, auditError = 400, "invalid_request"
				if body.overflow.Load() {
					status, auditError = 413, "request_too_large"
				}
				errorResponse(c, status, auditError)
				return
			}
			rewritten, rw, err := r.aliases.RewriteJSON(data)
			if err != nil {
				status, auditError = 400, "invalid_request"
				errorResponse(c, status, auditError)
				return
			}
			modelRewrite = rw
			upstreamBody, upstreamLength = bytes.NewReader(rewritten), int64(len(rewritten))
		case "multipart/form-data":
			// Streamed part by part; the rewritten length is unknown, so the upstream request is chunked.
			pr, pw := io.Pipe()
			finished := make(chan *modelalias.Rewrite, 1)
			startRewrite = func() {
				go func() {
					rw, err := r.aliases.RewriteMultipart(pw, body, params["boundary"])
					if errors.Is(err, modelalias.ErrInvalid) {
						rewriteInvalid.Store(true)
					}
					pw.CloseWithError(err)
					finished <- rw
				}()
			}
			defer func() {
				if !rewriteStarted {
					return
				}
				// Unblock the rewriter (client read or pipe write), then collect its result.
				raw.Close()
				pr.Close()
				if rw := <-finished; rw != nil && !rewriteInvalid.Load() {
					modelRewrite = rw
				}
			}()
			upstreamBody, upstreamLength = pr, -1
		}
	}
	target := *u.base
	target.Path = req.URL.Path
	target.RawQuery = "" // Model-list queries are intentionally not forwarded.
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	up, e := http.NewRequestWithContext(requestCtx, req.Method, target.String(), upstreamBody)
	if e != nil {
		return
	}
	up.ContentLength = upstreamLength
	up.Header = cleanHeaders(req.Header)
	for k, v := range r.cfg.UpstreamHeaders {
		up.Header.Set(k, v)
	}
	var idleExpired atomic.Bool
	timer := time.AfterFunc(idle, func() { idleExpired.Store(true); cancelRequest() })
	defer timer.Stop()
	body.touch = func() { c.touch(); timer.Reset(idle) }
	if startRewrite != nil {
		rewriteStarted = true
		startRewrite()
	}
	resp, e := u.client.Do(up)
	if e != nil {
		bad = connectFailed(e)
		status = 502
		auditError = "upstream_error"
		if timedOut(e) || idleExpired.Load() {
			status = 504
			auditError = "upstream_timeout"
		}
		if rewriteInvalid.Load() {
			status = 400
			bad = false
			auditError = "invalid_request"
		}
		if body.overflow.Load() {
			status = 413
			bad = false
			auditError = "request_too_large"
		}
		errorResponse(c, status, auditError)
		return
	}
	defer resp.Body.Close()
	c.touch()
	timer.Reset(idle)
	status = resp.StatusCode
	bad = status == 502 || status == 503 || status == 504
	resp.Trailer = nil
	resp.Header = cleanResponse(resp.Header)
	limit, _ := config.Bytes(r.cfg.MaxResponseBytes)
	if resp.ContentLength > limit {
		status = 502
		auditError = "response_too_large"
		errorResponse(c, 502, "response_too_large")
		return
	}
	responseBody := io.Reader(resp.Body)
	if r.aliases != nil && req.Method == "GET" && req.URL.Path == "/v1/models" && resp.StatusCode == 200 {
		// Present aliases instead of upstream ids. Only this small list is rewritten;
		// other responses (including streams) pass through and may carry upstream ids.
		const modelListLimit = 4 << 20
		listed, err := io.ReadAll(io.LimitReader(resp.Body, modelListLimit+1))
		if err != nil {
			status = 502
			auditError = "upstream_error"
			if idleExpired.Load() || timedOut(err) {
				status, auditError = 504, "upstream_timeout"
			}
			errorResponse(c, status, auditError)
			return
		}
		if len(listed) <= modelListLimit {
			listed, _ = r.aliases.RewriteModelList(listed)
			responseBody = bytes.NewReader(listed)
		} else {
			responseBody = io.MultiReader(bytes.NewReader(listed), resp.Body)
		}
	}
	counter := &counted{r: responseBody, max: limit, touch: func() { c.touch(); timer.Reset(idle) }}
	resp.Body = io.NopCloser(counter)
	resp.ContentLength = -1
	resp.TransferEncoding = []string{"chunked"}
	resp.Close = true
	writeErr := resp.Write(c)
	out = counter.n.Load()
	if counter.overflow.Load() {
		status = 502
		auditError = "response_too_large"
	} else if idleExpired.Load() || timedOut(writeErr) {
		status = 504
		auditError = "upstream_timeout"
	} else if writeErr != nil {
		auditError = "response_interrupted"
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
func (x *Connector) websocket(ctx context.Context, c *idleConn, br *bufio.Reader, r *route, u *upstream, req *http.Request) (int, int64, int64, bool) {
	target := *u.base
	if target.Scheme == "http" {
		target.Scheme = "ws"
	} else {
		target.Scheme = "wss"
	}
	target.Path = req.URL.Path
	d := websocket.Dialer{NetDialContext: u.policy.DialContext, HandshakeTimeout: c.idle}
	headers := cleanHeaders(req.Header)
	for k, v := range r.cfg.UpstreamHeaders {
		headers.Set(k, v)
	}
	ws, _, e := d.DialContext(ctx, target.String(), headers)
	if e != nil {
		errorResponse(c, 502, "upstream_error")
		return 502, 0, 0, true
	}
	defer ws.Close()
	fmt.Fprint(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	max, _ := config.Bytes(r.cfg.MaxWebsocketMessageBytes)
	ws.SetReadLimit(max)
	ws.UnderlyingConn().SetReadDeadline(time.Now().Add(c.idle))
	done := make(chan error, 2)
	var in, out int64
	go func() {
		for {
			frame, e := protocol.ReadFrame(br, int(max))
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
			in += int64(len(p))
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
			out += int64(len(p))
			if e = protocol.WriteFrame(c, protocol.Frame{Type: byte(typ), Payload: p}, int(max)); e != nil {
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
	return 101, in, out, false
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

func (x *Connector) discover(ctx context.Context, r *route, u *upstream) {
	allowed := false
	for _, p := range r.cfg.MethodsPaths {
		if p == [2]string{"GET", "/v1/models"} {
			allowed = true
		}
	}
	if !allowed {
		return
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", u.base.String()+"/v1/models", nil)
	for k, v := range r.cfg.UpstreamHeaders {
		req.Header.Set(k, v)
	}
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
	r.models = r.aliases.Present(ids)
	x.mu.Unlock()
}

// NewReplacement reuses an unchanged health listener after construction succeeds.
// The caller must serialize replacement and closing the prior connector.
func NewReplacement(c *config.Config, old *Connector) (*Connector, error) {
	old.mu.Lock()
	reuse := c.HealthListen != "" && c.HealthListen == old.healthAddress
	old.mu.Unlock()
	if !reuse {
		return New(c)
	}
	copyConfig := *c
	copyConfig.HealthListen = ""
	next, e := New(&copyConfig)
	if e != nil {
		return nil, e
	}
	old.mu.Lock()
	next.listener = old.listener
	next.healthAddress = old.healthAddress
	next.healthTarget = old.healthTarget
	old.listener = nil
	old.healthTarget.Store(next)
	old.mu.Unlock()
	return next, nil
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
				u.eject = time.Now().Add(30 * time.Second)
				x.mu.Unlock()
				log.Printf("upstream %s DNS refresh rejected: %v", u.base.Host, e)
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

func (x *Connector) startStorage(ctx context.Context, r *route) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		handler, err := storage.New(attempt, r.cfg, x.auditStorage)
		cancel()
		if err == nil {
			if ctx.Err() != nil {
				handler.Close()
				return
			}
			x.mu.Lock()
			r.storage = handler
			handler.Start(ctx)
			x.mu.Unlock()
			return
		}
		x.auditStorage(storage.AuditEntry{Timestamp: time.Now().UTC(), Origin: "local", Route: r.cfg.ID, Op: "initialize", Status: "backend_unavailable"})
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
