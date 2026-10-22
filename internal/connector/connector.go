// Package connector serves gateway streams against the locally configured
// services. Each http service accepts exactly the requests its
// allowed-endpoints lists; every outbound connection goes through a dialer
// pinned to the address the configuration names.
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
	"github.com/eidonix-ai/eidonix-connector/internal/storage"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

// Internal limits shared by every http service.
const (
	MaxRequestBytes          int64 = 100 << 20
	MaxResponseBytes         int64 = 256 << 20
	MaxWebSocketMessageBytes       = 4 << 20
	HealthInterval                 = 15 * time.Second
	EjectFor                       = 30 * time.Second
)

type service struct {
	cfg     config.Service
	storage *storage.Handler
	base    *url.URL
	client  *http.Client
	policy  *network.Policy
	flight  int
	tokens  float64
	last    time.Time
	eject   time.Time      // unhealthy until
	allow   *models.Policy // nil when the service does not check models
	// ports are a machine's services; a request to the machine is routed
	// to one of them (route).
	ports []*service
	// Size limits (MaxRequestBytes, MaxResponseBytes; tests lower them).
	maxRequest, maxResponse int64
	stats                   *rolling
}

// healthInterval is HealthInterval; tests shorten it.
var healthInterval = HealthInterval

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
		s := newService(cfg)
		x.services[cfg.Name] = s
		x.order = append(x.order, cfg.Name)
		if cfg.Kind == config.KindS3 {
			x.storageInit.Add(1)
			go func() { defer x.storageInit.Done(); x.startStorage(ctx, s) }()
			continue
		}
		targets := []*service{s}
		if cfg.Machine() {
			targets = nil
			for _, pc := range cfg.Ports {
				p := newService(pc)
				s.ports = append(s.ports, p)
				targets = append(targets, p)
			}
		}
		for _, t := range targets {
			if err := x.startHTTP(ctx, t); err != nil {
				x.Close()
				return nil, fmt.Errorf("service %q: %w", t.cfg.Name, err)
			}
		}
	}
	return x, nil
}

func newService(cfg config.Service) *service {
	s := &service{cfg: cfg, last: time.Now(), tokens: math.Max(1, float64(cfg.MaxRequestsPerMinute)), maxRequest: MaxRequestBytes, maxResponse: MaxResponseBytes, stats: &rolling{}}
	if cfg.Models.Set() {
		s.allow = models.New(cfg.Models.Names, cfg.Models.Target)
	}
	return s
}

// startHTTP pins an http service's address and starts its health checks.
func (x *Connector) startHTTP(ctx context.Context, s *service) error {
	p, err := network.New(ctx, []string{s.cfg.URL})
	if err != nil {
		return err
	}
	s.base, _ = url.Parse(s.cfg.URL)
	s.policy = p
	t := &http.Transport{Proxy: nil, DialContext: p.DialContext, DisableCompression: true, MaxResponseHeaderBytes: 64 << 10, ResponseHeaderTimeout: max(120*time.Second, s.cfg.IdleTimeout)}
	s.client = &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	go x.health(ctx, s, healthInterval)
	go x.refreshDNS(ctx, s)
	return nil
}
func (x *Connector) Close() error {
	x.cancel()
	x.storageInit.Wait()
	x.mu.Lock()
	for _, s := range x.services {
		if s.storage != nil {
			s.storage.Close()
		}
		for _, t := range append([]*service{s}, s.ports...) {
			if t.client != nil {
				t.client.CloseIdleConnections()
			}
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
	if s.cfg.Kind == config.KindS3 {
		return s.storage != nil && s.storage.Healthy()
	}
	if s.ports != nil {
		// A machine takes requests while any port can serve them; its parts
		// say which are down (degraded).
		for _, p := range s.ports {
			if x.healthy(p) {
				return true
			}
		}
		return false
	}
	return !time.Now().Before(s.eject)
}

// parts are a machine's ports as a gateway sees them (nil otherwise).
func (x *Connector) parts(s *service) []protocol.ServicePart {
	var out []protocol.ServicePart
	for _, p := range s.ports {
		part := protocol.ServicePart{Models: append([]string{}, p.cfg.Models.Names...), Healthy: x.healthy(p)}
		for _, e := range p.cfg.Endpoints {
			part.Endpoints = append(part.Endpoints, e.String())
		}
		out = append(out, part)
	}
	return out
}

// advertised are what a service shares: its models and endpoints, a
// machine's those of all its ports (each once, in file order).
func advertised(s *service) (models, endpoints []string) {
	models = []string{}
	seen := map[string]bool{}
	for _, t := range append([]*service{s}, s.ports...) {
		for _, m := range t.cfg.Models.Names {
			models = append(models, m)
		}
		for _, e := range t.cfg.Endpoints {
			if !seen[e.String()] {
				seen[e.String()] = true
				endpoints = append(endpoints, e.String())
			}
		}
	}
	return models, endpoints
}

// inFlight is a service's running requests (a machine's: its ports').
func inFlight(s *service) int {
	n := s.flight
	if s.storage != nil {
		n = s.storage.InFlight()
	}
	for _, p := range s.ports {
		n += p.flight
	}
	return n
}

// HelloServices advertises every configured service.
func (x *Connector) HelloServices() []protocol.Service {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := []protocol.Service{}
	for _, name := range x.order {
		s := x.services[name]
		models, endpoints := advertised(s)
		a := protocol.Service{Name: name, Kind: s.cfg.Kind, Models: models, Endpoints: endpoints, MaxConcurrentRequests: s.cfg.MaxConcurrentRequests, Healthy: x.healthy(s), Parts: x.parts(s)}
		if st := s.cfg.Storage; st != nil {
			a.Storage = &protocol.StorageService{MaxKeyBytes: st.MaxKeyBytes(), MaxObjectBytes: st.MaxFileSize, Encryption: st.Encryption, ReadOnly: st.ReadOnly}
		}
		out = append(out, a)
	}
	return out
}

// StatusServices reports health, in-flight requests and the model list.
func (x *Connector) StatusServices() []protocol.ServiceStatus {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := []protocol.ServiceStatus{}
	for _, name := range x.order {
		s := x.services[name]
		models, _ := advertised(s)
		out = append(out, protocol.ServiceStatus{Name: name, Healthy: x.healthy(s), Models: models, InFlight: inFlight(s), Parts: x.parts(s)})
	}
	return out
}

// probe checks a service once: GET <health> when the service names a health
// path (any response below 500 counts as healthy), otherwise a TCP connection
// to its address. The pinned addresses are re-resolved first.
func (x *Connector) probe(ctx context.Context, s *service) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.policy.Refresh(ctx); err != nil {
		return err
	}
	if s.cfg.Health == "" {
		c, err := s.policy.DialContext(ctx, "tcp", config.HostPort(s.cfg.URL))
		if err == nil {
			c.Close()
		}
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", s.base.String()+s.cfg.Health, nil)
	x.authorize(req.Header, s)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 65536))
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("health status %d", resp.StatusCode)
	}
	return nil
}

// health probes the service every interval; a failed probe marks it
// unhealthy (requests answer upstream_unavailable) until a probe succeeds.
func (x *Connector) health(ctx context.Context, s *service, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			err := x.probe(ctx, s)
			x.mu.Lock()
			if err == nil {
				s.eject = time.Time{}
			} else {
				s.eject = time.Now().Add(EjectFor)
			}
			x.mu.Unlock()
		}
	}
}

// acquire reserves a request slot: busy at max-concurrent-requests,
// rate_limited past max-requests-per-minute, upstream_unavailable while the
// service is unhealthy.
func (x *Connector) acquire(s *service) string {
	x.mu.Lock()
	defer x.mu.Unlock()
	if s.flight >= s.cfg.MaxConcurrentRequests {
		return "busy"
	}
	now := time.Now()
	if now.Before(s.eject) {
		return "upstream_unavailable"
	}
	if rate := float64(s.cfg.MaxRequestsPerMinute); rate > 0 {
		s.tokens = math.Min(rate, s.tokens+now.Sub(s.last).Minutes()*rate)
		s.last = now
		if s.tokens < 1 {
			return "rate_limited"
		}
		s.tokens--
	}
	s.flight++
	return ""
}
func (x *Connector) release(s *service) {
	x.mu.Lock()
	defer x.mu.Unlock()
	s.flight--
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

// forwarded are the only request headers sent upstream. Anthropic-Version and
// Anthropic-Beta select the Anthropic Messages API version and features; they
// carry no credential or routing, and other servers ignore them.
// X-Affinity-Key is an opaque conversation hash a load balancer in front of
// several replicas may route on (haproxy: balance hdr(X-Affinity-Key)), so a
// conversation keeps landing on the replica that holds its prefix cache.
var forwarded = []string{"Content-Type", "Accept", "X-Request-Id", "Anthropic-Version", "Anthropic-Beta", "X-Affinity-Key"}

func cleanHeaders(in http.Header) http.Header {
	h := make(http.Header)
	for _, k := range forwarded {
		if v := in.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return h
}

// authorize adds the service's api-key, the only credential the connector sends upstream.
func (x *Connector) authorize(h http.Header, s *service) {
	if s.cfg.APIKey != "" {
		h.Set("Authorization", "Bearer "+s.cfg.APIKey)
	}
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
			if entry.Kind != "" {
				x.count(entry.Service, entry.Error != "" || entry.Status >= 500)
			}
		}
	}()
	var e error
	h, e = protocol.ReadStreamHeader(br)
	if e != nil {
		entry.Status, entry.Error = 400, "invalid_stream_header"
		return
	}
	entry.Service, entry.RequestID, entry.OrgID, entry.OrgSlug, entry.UserKeyID = h.Service, h.RequestID, h.OrgID, h.OrgSlug, h.UserKeyID
	entry.Application, entry.For = h.Application, h.For
	x.mu.Lock()
	s := x.services[h.Service]
	var storageHandler *storage.Handler
	if s != nil {
		storageHandler = s.storage
		entry.Kind = s.cfg.Kind
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
		case s.cfg.Kind != config.KindS3:
			entry.Error = "wrong_route_type"
		case storageHandler == nil:
			entry.Error = "backend_unavailable"
		default:
			storageAudit = true
			storageHandler.Serve(ctx, raw, br, h)
			return
		}
		entry.Status = protocol.HTTPStatus(entry.Error)
		protocol.WriteStorageResult(raw, protocol.StorageErrorResult(entry.Error), h.Op)
		return
	}
	if s == nil {
		entry.Error = "route_not_found"
		entry.Status = errorResponse(raw, entry.Error)
		return
	}
	if s.cfg.Kind == config.KindS3 {
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
	if req.URL.IsAbs() || req.URL.Host != "" || config.ValidatePath(strings.Split(path, "?")[0]) != nil || strings.Contains(path, "?") && req.Method != "GET" {
		fail("path_not_allowed")
		return
	}
	method := req.Method
	if h.Kind == "websocket" {
		if req.Method != "GET" {
			fail("path_not_allowed")
			return
		}
		method = config.WebSocket
	} else if req.Method != "GET" && req.Method != "POST" && req.Method != "PUT" && req.Method != "DELETE" {
		fail("path_not_allowed")
		return
	}
	if s.ports != nil {
		p, code := x.route(s, req, method, h.Kind)
		switch code {
		case "":
			s = p
			c.idle = s.cfg.IdleTimeout
			c.touch()
		case listModels:
			entry.Status = writeModelList(c, s, method, req.URL.Path)
			return
		default:
			fail(code)
			return
		}
	}
	if !s.cfg.Allows(method, req.URL.Path) {
		fail("path_not_allowed")
		return
	}
	maxBody := s.maxRequest
	if req.ContentLength > maxBody {
		fail("request_too_large")
		return
	}
	// A service that checks models inspects every request body: POST and PUT
	// bodies must be JSON or multipart (so the model can be read), and GET
	// and DELETE requests may not carry one.
	inspect := ""
	if s.allow != nil && h.Kind == "http" {
		ct, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
		switch {
		case req.Method == "GET" || req.Method == "DELETE":
			if req.ContentLength != 0 {
				fail("invalid_request")
				return
			}
		case err == nil && (ct == "application/json" || ct == "multipart/form-data"):
			inspect = ct
		default:
			fail("content_type_not_allowed")
			return
		}
	}
	if code := x.acquire(s); code != "" {
		fail(code)
		return
	}
	defer x.release(s)
	entry.UpstreamHost = s.base.Host
	if h.Kind == "websocket" {
		entry.Status, entry.BytesIn, entry.BytesOut = x.websocket(ctx, c, br, s, req)
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
	if inspect != "" {
		_, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
		switch inspect {
		case "application/json":
			// Bounded by the request limit (and the service's concurrency) like any body.
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
	target := *s.base
	target.Path = req.URL.Path
	target.RawQuery = ""
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	up, e := http.NewRequestWithContext(requestCtx, req.Method, target.String(), upstreamBody)
	if e != nil {
		return
	}
	up.ContentLength = upstreamLength
	up.Header = cleanHeaders(req.Header)
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
	resp, e := s.client.Do(up)
	if e != nil {
		code := "upstream_error"
		if timedOut(e) || idleExpired.Load() {
			code = "upstream_timeout"
		}
		if body.overflow.Load() {
			code = "request_too_large"
		} else if rewriteInvalid.Load() {
			code = "invalid_request"
		}
		fail(code)
		return
	}
	defer resp.Body.Close()
	c.touch()
	timer.Reset(idle)
	entry.Status = resp.StatusCode
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
func (x *Connector) websocket(ctx context.Context, c *idleConn, br *bufio.Reader, s *service, req *http.Request) (int, int64, int64) {
	target := *s.base
	if target.Scheme == "http" {
		target.Scheme = "ws"
	} else {
		target.Scheme = "wss"
	}
	target.Path = req.URL.Path
	d := websocket.Dialer{NetDialContext: s.policy.DialContext, HandshakeTimeout: c.idle, Proxy: nil}
	headers := cleanHeaders(req.Header)
	x.authorize(headers, s)
	ws, _, e := d.DialContext(ctx, target.String(), headers)
	if e != nil {
		errorResponse(c, "upstream_error")
		return 502, 0, 0
	}
	defer ws.Close()
	fmt.Fprint(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	max := MaxWebSocketMessageBytes
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
	return 101, atomic.LoadInt64(&in), atomic.LoadInt64(&out)
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

// refreshDNS re-resolves the service's pinned host every minute; a changed
// answer marks it unhealthy (DNS changes never widen what may be dialed).
func (x *Connector) refreshDNS(ctx context.Context, s *service) {
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			e := s.policy.Refresh(c)
			cancel()
			if e != nil {
				x.mu.Lock()
				s.eject = time.Now().Add(EjectFor)
				x.mu.Unlock()
				log.Printf("service %s marked unhealthy: %v", s.cfg.Name, e)
			}
		}
	}
}

func (x *Connector) auditStorage(entry storage.AuditEntry) {
	if entry.Origin == "upstream" {
		x.count(entry.Service, entry.Status != "ok")
	}
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
		x.auditStorage(storage.AuditEntry{Time: time.Now().UTC(), Origin: "local", Service: s.cfg.Name, Kind: config.KindS3, Op: "initialize", Status: "backend_unavailable"})
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// listModels is route's answer for GET /v1/models on a machine whose ports
// share it: the connector lists their models itself.
const listModels = "list_models"

// route picks the port of machine m that serves a request: the only port
// that allows it, else the port listing the model its JSON body names. That
// body is read here, within the request limit, and replayed to the port.
// Configuration checks make the choice unique (config.checkMachine).
func (x *Connector) route(m *service, req *http.Request, method, kind string) (*service, string) {
	var allow []*service
	for _, p := range m.ports {
		if p.cfg.Allows(method, req.URL.Path) {
			allow = append(allow, p)
		}
	}
	switch {
	case len(allow) == 0:
		return nil, "path_not_allowed"
	case len(allow) == 1:
		return allow[0], ""
	case kind == "http" && req.Method == http.MethodGet && req.URL.Path == "/v1/models":
		return nil, listModels
	}
	ct, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if kind != "http" || (req.Method != http.MethodPost && req.Method != http.MethodPut) || ct != "application/json" {
		return nil, "model_required"
	}
	if req.ContentLength > m.maxRequest {
		return nil, "request_too_large"
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, m.maxRequest+1))
	switch {
	case err != nil:
		return nil, "invalid_request"
	case int64(len(data)) > m.maxRequest:
		return nil, "request_too_large"
	}
	var body struct {
		Model *string `json:"model"`
	}
	if json.Unmarshal(data, &body) != nil {
		return nil, "invalid_request"
	}
	if body.Model == nil {
		return nil, "model_required"
	}
	req.Body, req.ContentLength = io.NopCloser(bytes.NewReader(data)), int64(len(data))
	for _, p := range allow {
		if p.allow != nil && p.allow.Allows(*body.Model) {
			return p, ""
		}
	}
	return nil, "model_not_allowed"
}

// writeModelList answers GET /v1/models on a machine with the models of
// the ports that allow it.
func writeModelList(w io.Writer, m *service, method, path string) int {
	type entry struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	data := []entry{}
	for _, p := range m.ports {
		if p.cfg.Allows(method, path) {
			for _, n := range p.cfg.Models.Names {
				data = append(data, entry{n, "model", m.cfg.Name})
			}
		}
	}
	body, _ := json.Marshal(map[string]any{"object": "list", "data": data})
	resp := &http.Response{StatusCode: http.StatusOK, ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)), Close: true}
	_ = resp.Write(w)
	return http.StatusOK
}
