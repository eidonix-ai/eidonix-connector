// Package testgateway provides a development peer (protocol v2) for connector integration tests.
// It is deliberately not a production gateway or a tenant authorization service.
package testgateway

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

const maxBody = 256 << 20

type Gateway struct {
	token    string
	key      ed25519.PrivateKey
	mu       sync.Mutex
	sessions map[*peer]bool
	closed   bool
	// AuthAttempts counts auth messages received on the control stream.
	AuthAttempts atomic.Int64
	// Caller metadata stamped on every stream header this gateway opens.
	OrgID, OrgSlug, UserKeyID string
}
type peer struct {
	mux          *yamux.Session
	services     map[string]protocol.Service
	slots        chan struct{}
	capabilities map[string]bool
	lastStatus   atomic.Pointer[protocol.Status]
}

func New(token string, key ed25519.PrivateKey) (*Gateway, error) {
	if _, _, err := protocol.ParseToken(token); err != nil {
		return nil, err
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid signing key")
	}
	return &Gateway{token: token, key: key, sessions: make(map[*peer]bool), OrgID: "1", OrgSlug: "local-test", UserKeyID: "1"}, nil
}
func (g *Gateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	for p := range g.sessions {
		p.mux.Close()
	}
	return nil
}

// ServeHTTP serves the connector session endpoint (the token arrives on the
// control stream, after the gateway has proved its identity) and, behind the
// local bearer token, /status and /services/<name>/<path> for development use.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == protocol.SessionPath {
		g.connect(w, r)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+g.token)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	switch {
	case r.URL.Path == "/status":
		g.mu.Lock()
		n := len(g.sessions)
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"sessions": n})
	case strings.HasPrefix(r.URL.Path, "/services/"):
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/services/"), "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			http.Error(w, "service path required", 400)
			return
		}
		// Preserve escaped paths: the connector, not this harness, validates them.
		req := r.Clone(r.Context())
		req.URL.Path = "/" + parts[1]
		req.URL.RawPath = ""
		if r.URL.RawPath != "" {
			raw := strings.SplitN(strings.TrimPrefix(r.URL.EscapedPath(), "/services/"), "/", 2)
			if len(raw) == 2 {
				req.URL.RawPath = "/" + raw[1]
			}
		}
		req.RequestURI = ""
		req.Header = req.Header.Clone()
		req.Header.Del("Authorization")
		if websocket.IsWebSocketUpgrade(r) {
			g.websocket(w, r, req, parts[0])
			return
		}
		resp, err := g.RoundTrip(req, parts[0])
		if err != nil {
			http.Error(w, "connector offline or stream failed", 502)
			return
		}
		defer resp.Body.Close()
		for _, h := range []string{"Content-Type", "Content-Length", "X-Request-Id", "Retry-After", protocol.HeaderError} {
			if v := resp.Header.Values(h); len(v) > 0 {
				w.Header()[h] = v
			}
		}
		w.WriteHeader(resp.StatusCode)
		b := make([]byte, 32<<10)
		for {
			n, e := resp.Body.Read(b)
			if n > 0 {
				if _, err = w.Write(b[:n]); err != nil {
					return
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			if e != nil {
				if e != io.EOF {
					panic(http.ErrAbortHandler)
				}
				return
			}
		}
	default:
		http.NotFound(w, r)
	}
}
func (g *Gateway) connect(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}
	offered := false
	for _, s := range websocket.Subprotocols(r) {
		offered = offered || s == protocol.Subprotocol
	}
	if !offered {
		http.Error(w, "unsupported protocol", 426)
		return
	}
	nonce := r.Header.Get(protocol.HeaderNonce)
	b, err := base64.StdEncoding.DecodeString(nonce)
	instance := r.Header.Get(protocol.HeaderInstance)
	if err != nil || len(b) != 32 || len(instance) != 36 || r.Header.Get("Authorization") != "" {
		http.Error(w, "invalid handshake", 400)
		return
	}
	up := websocket.Upgrader{Subprotocols: []string{protocol.Subprotocol}, HandshakeTimeout: 10 * time.Second}
	ws, err := up.Upgrade(w, r, protocol.IdentityHeaders(g.key, nonce, instance, time.Now()))
	if err != nil {
		return
	}
	cfg := yamux.DefaultConfig()
	cfg.MaxStreamWindowSize = 256 << 10
	cfg.KeepAliveInterval = 15 * time.Second
	cfg.ConnectionWriteTimeout = 10 * time.Second
	cfg.AcceptBacklog = 32
	cfg.LogOutput = io.Discard
	cfg.StreamCloseTimeout = 0
	mux, err := yamux.Server(protocol.NewWebSocketConn(ws), cfg)
	if err != nil {
		ws.Close()
		return
	}
	defer mux.Close()
	timer := time.AfterFunc(10*time.Second, func() { mux.Close() })
	control, err := mux.AcceptStream()
	if err != nil {
		timer.Stop()
		return
	}
	timer.Stop()
	if control.StreamID() != 1 {
		return
	}
	control.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(control)
	env, err := protocol.ReadMessage(br)
	var auth protocol.Auth
	if err != nil || env.Type != protocol.MessageAuth || env.Decode(&auth) != nil {
		return
	}
	g.AuthAttempts.Add(1)
	if subtle.ConstantTimeCompare([]byte(auth.Token), []byte(g.token)) != 1 {
		protocol.WriteMessage(control, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthFailed, Reason: "invalid_token"})
		return
	}
	id, _ := protocol.NewInstanceID()
	if protocol.WriteMessage(control, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthOK, SessionID: id, Org: "local-test"}) != nil {
		return
	}
	var hello protocol.Hello
	if env, err = protocol.ReadMessage(br); err != nil || env.Type != protocol.MessageHello || env.Decode(&hello) != nil || hello.InstanceID != instance || len(hello.Services) > 128 {
		return
	}
	p := &peer{mux: mux, services: make(map[string]protocol.Service), slots: make(chan struct{}, 64), capabilities: make(map[string]bool)}
	for _, capability := range hello.Capabilities {
		p.capabilities[capability] = true
	}
	rejected := []protocol.Rejected{}
	names := make([]string, 0, len(hello.Services))
	for _, service := range hello.Services {
		if !protocol.ValidServiceName(service.Name) {
			return
		}
		if _, ok := p.services[service.Name]; ok {
			return
		}
		if service.Type == protocol.TypeStorage && !p.capabilities[protocol.CapabilityStorage] {
			rejected = append(rejected, protocol.Rejected{Name: service.Name, Reason: "storage_capability_missing"})
			continue
		}
		p.services[service.Name] = service
		names = append(names, service.Name)
	}
	if protocol.WriteMessage(control, protocol.HelloAck{V: protocol.Version, Type: protocol.MessageHelloAck, Accepted: names, Rejected: rejected, Limits: protocol.Limits{MaxStreams: 64}}) != nil {
		return
	}
	control.SetDeadline(time.Time{})
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.sessions[p] = true
	g.mu.Unlock()
	defer func() { g.mu.Lock(); delete(g.sessions, p); g.mu.Unlock() }()
	// A connector must never open a second stream, even in this development peer.
	go func() {
		s, e := mux.AcceptStream()
		if e == nil {
			s.Close()
			mux.Close()
		}
	}()
	done := make(chan struct{})
	defer close(done)
	var lastPong atomic.Int64
	lastPong.Store(time.Now().UnixNano())
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if time.Since(time.Unix(0, lastPong.Load())) > 45*time.Second {
					mux.Close()
					return
				}
				control.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if protocol.WriteMessage(control, protocol.Control{V: protocol.Version, Type: protocol.MessagePing}) != nil {
					mux.Close()
					return
				}
			}
		}
	}()
	for {
		control.SetReadDeadline(time.Now().Add(45 * time.Second))
		m, err := protocol.ReadMessage(br)
		if err != nil {
			return
		}
		switch m.Type {
		case protocol.MessagePong:
			lastPong.Store(time.Now().UnixNano())
		case protocol.MessageStatus:
			var status protocol.Status
			if m.Decode(&status) != nil {
				return
			}
			p.lastStatus.Store(&status)
		default:
			return
		}
	}
}
func (g *Gateway) open(ctx context.Context, service, kind string) (net.Conn, func(), error) {
	id, _ := protocol.NewInstanceID()
	return g.openHeader(ctx, protocol.StreamHeader{V: protocol.Version, Service: service, Kind: kind, RequestID: id})
}
func (g *Gateway) openHeader(ctx context.Context, header protocol.StreamHeader) (net.Conn, func(), error) {
	g.mu.Lock()
	header.OrgID, header.OrgSlug, header.UserKeyID = g.OrgID, g.OrgSlug, g.UserKeyID
	var chosen *peer
	for p := range g.sessions {
		if r, ok := p.services[header.Service]; ok && !p.mux.IsClosed() {
			if header.Kind == "storage" && (r.Type != protocol.TypeStorage || !p.capabilities[protocol.CapabilityStorage]) {
				continue
			}
			if header.Kind != "storage" && r.Type == protocol.TypeStorage {
				continue
			}
			if chosen == nil || len(p.slots) < len(chosen.slots) {
				chosen = p
			}
		}
	}
	if chosen == nil {
		g.mu.Unlock()
		return nil, nil, errors.New("connector offline")
	}
	select {
	case chosen.slots <- struct{}{}:
	default:
		g.mu.Unlock()
		return nil, nil, errors.New("connector busy")
	}
	g.mu.Unlock()
	s, err := chosen.mux.OpenStream()
	if err != nil {
		<-chosen.slots
		return nil, nil, err
	}
	c := &idleConn{Conn: s, idle: 120 * time.Second}
	stop := context.AfterFunc(ctx, func() { s.SetDeadline(time.Now()); s.Close() })
	var once sync.Once
	closeFn := func() { once.Do(func() { stop(); s.Close(); <-chosen.slots }) }
	if err = protocol.WriteStreamHeader(c, header); err != nil {
		closeFn()
		return nil, nil, err
	}
	return c, closeFn, nil
}

// RoundTrip sends one request to a locally advertised service. Close the response body.
func (g *Gateway) RoundTrip(req *http.Request, service string) (*http.Response, error) {
	c, closeFn, err := g.open(req.Context(), service, "http")
	if err != nil {
		return nil, err
	}
	finish := closeFn
	var closeOnce sync.Once
	closeRequest := func() {
		closeOnce.Do(func() {
			if req.Body != nil {
				req.Body.Close()
			}
		})
	}
	stopBody := context.AfterFunc(req.Context(), closeRequest)
	closeFn = func() { finish(); stopBody(); closeRequest() }
	r := req.Clone(req.Context())
	r.RequestURI = ""
	r.URL.Scheme = ""
	r.URL.Host = ""
	r.Host = "connector"
	r.Close = true
	// Read concurrently so early rejections can stop an upload.
	go func() {
		defer closeRequest()
		if e := r.Write(c); e != nil {
			closeFn()
		}
	}()
	hr := &headerReader{r: c, left: 64 << 10}
	br := bufio.NewReader(hr)
	resp, err := http.ReadResponse(br, r)
	hr.unlimited = true
	if err != nil {
		closeFn()
		return nil, err
	}
	if resp.ContentLength > maxBody {
		closeFn()
		return nil, errors.New("response too large")
	}
	resp.Body = &body{ReadCloser: resp.Body, left: maxBody, closeFn: closeFn}
	return resp, nil
}

type headerReader struct {
	r         io.Reader
	left      int
	unlimited bool
}

func (r *headerReader) Read(p []byte) (int, error) {
	if !r.unlimited {
		if r.left == 0 {
			return 0, errors.New("response headers too large")
		}
		if len(p) > r.left {
			p = p[:r.left]
		}
	}
	n, e := r.r.Read(p)
	r.left -= n
	return n, e
}

type body struct {
	io.ReadCloser
	left    int64
	closeFn func()
}

func (b *body) Read(p []byte) (int, error) {
	if b.left == 0 {
		var probe [1]byte
		n, e := b.ReadCloser.Read(probe[:])
		if n > 0 {
			b.closeFn()
			return 0, errors.New("response too large")
		}
		return 0, e
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, e := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, e
}
func (b *body) Close() error { b.closeFn(); return b.ReadCloser.Close() }

type idleConn struct {
	net.Conn
	idle              time.Duration
	mu                sync.Mutex
	readCap, writeCap time.Time
}

func earliest(a, b time.Time) time.Time {
	if !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}
func (c *idleConn) touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := time.Now().Add(c.idle)
	c.Conn.SetReadDeadline(earliest(next, c.readCap))
	c.Conn.SetWriteDeadline(earliest(next, c.writeCap))
}
func (c *idleConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readCap = t
	return c.Conn.SetReadDeadline(earliest(time.Now().Add(c.idle), t))
}
func (c *idleConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCap = t
	return c.Conn.SetWriteDeadline(earliest(time.Now().Add(c.idle), t))
}
func (c *idleConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readCap = t
	c.writeCap = t
	return c.Conn.SetDeadline(earliest(time.Now().Add(c.idle), t))
}
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
func (g *Gateway) websocket(w http.ResponseWriter, original, req *http.Request, service string) {
	c, finish, err := g.open(req.Context(), service, "websocket")
	if err != nil {
		http.Error(w, "connector offline", 502)
		return
	}
	defer finish()
	req.Host = "connector"
	req.URL.Scheme = ""
	req.URL.Host = ""
	if req.Write(c) != nil {
		http.Error(w, "stream failed", 502)
		return
	}
	hr := &headerReader{r: c, left: 64 << 10}
	br := bufio.NewReader(hr)
	resp, err := http.ReadResponse(br, req)
	hr.unlimited = true
	if err != nil {
		http.Error(w, "stream failed", 502)
		return
	}
	if resp.StatusCode != 101 {
		resp.Body.Close()
		http.Error(w, fmt.Sprintf("connector returned %d", resp.StatusCode), 502)
		return
	}
	up := websocket.Upgrader{HandshakeTimeout: 10 * time.Second}
	ws, err := up.Upgrade(w, original, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	ws.SetReadLimit(4 << 20)
	done := make(chan error, 1)
	go func() {
		for {
			typ, p, e := ws.ReadMessage()
			if e != nil {
				done <- e
				finish()
				return
			}
			if e = protocol.WriteFrame(c, protocol.Frame{Type: byte(typ), Payload: p}, 4<<20); e != nil {
				done <- e
				ws.Close()
				return
			}
		}
	}()
	for {
		f, e := protocol.ReadFrame(br, 4<<20)
		if e != nil {
			break
		}
		ws.SetWriteDeadline(time.Now().Add(120 * time.Second))
		if e = ws.WriteMessage(int(f.Type), f.Payload); e != nil || f.Type == 8 {
			break
		}
	}
	ws.Close()
	finish()
	<-done
}

// Services returns the services the newest live session advertised in hello.
func (g *Gateway) Services() []protocol.Service {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := []protocol.Service{}
	for p := range g.sessions {
		out = out[:0]
		for _, s := range p.services {
			out = append(out, s)
		}
	}
	return out
}

// LastStatus returns the most recent status message of any live session.
func (g *Gateway) LastStatus() *protocol.Status {
	g.mu.Lock()
	defer g.mu.Unlock()
	for p := range g.sessions {
		if s := p.lastStatus.Load(); s != nil {
			return s
		}
	}
	return nil
}
