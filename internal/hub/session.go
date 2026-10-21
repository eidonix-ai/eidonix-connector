package hub

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// hubName is what auth_ok's org field carries: connectors log it as the
// name of the server they are connected to.
const hubName = "eidonix-connect"

// session is one live connector session.
type session struct {
	id          string
	site        string
	token       string // the token it authenticated with
	instance    string
	version     string
	os, arch    string
	remoteIP    string
	connectedAt time.Time
	mux         *yamux.Session
	slots       chan struct{} // the session's stream cap
	caps        []string
	draining    atomic.Bool
	// send writes one control message (serialised with the ping loop).
	send func(typ, reason string) error

	mu     sync.Mutex
	routes map[string]*route
}

// route is one service a session offers.
type route struct {
	name      string
	kind      string
	endpoints []string
	storage   *protocol.StorageService // s3 services
	// maxConcurrent is the advertised max_concurrent_requests, which slots
	// enforces at the hub.
	maxConcurrent int
	slots         chan struct{}
	inFlight      atomic.Int64

	// healthy, models and parts change with status messages (session.mu).
	healthy bool
	models  []string
	// parts are a machine's ports: what each serves and whether it is
	// healthy (protocol.ServicePart); nil for other services.
	parts []protocol.ServicePart
}

func (s *session) has(c string) bool {
	for _, x := range s.caps {
		if x == c {
			return true
		}
	}
	return false
}

// close tells the connector why and ends the session; every active stream
// fails at once.
func (s *session) close(reason string) {
	if reason != "" && s.send != nil {
		_ = s.send(protocol.MessageClose, reason)
	}
	s.mux.Close()
}

func muxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.MaxStreamWindowSize = 256 << 10
	cfg.KeepAliveInterval = pingInterval
	cfg.ConnectionWriteTimeout = 10 * time.Second
	cfg.AcceptBacklog = 8
	cfg.LogOutput = io.Discard
	return cfg
}

func validHandshake(nonce, instance string) bool {
	b, err := base64.StdEncoding.DecodeString(nonce)
	return err == nil && len(b) == 32 && len(instance) == 36
}

// ServeSession is GET /v1/connectors/session on the public port: one
// connector's outbound WebSocket. The upgrade carries no credential; the hub
// signs the connector's nonce and instance, and only a connector that
// verified the signature against its pinned keys sends its token, on the
// control stream.
func (h *Hub) ServeSession(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != protocol.SessionPath {
		http.NotFound(w, r)
		return
	}
	ip := clientIP(r)
	if !h.limiter.allow("ip:" + ip) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	if r.Method != http.MethodGet || !websocket.IsWebSocketUpgrade(r) {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return
	}
	supported := false
	for _, p := range websocket.Subprotocols(r) {
		supported = supported || p == protocol.Subprotocol
	}
	if !supported {
		http.Error(w, "unsupported connector protocol: this hub speaks "+protocol.Subprotocol, http.StatusUpgradeRequired)
		return
	}
	nonce, instance := r.Header.Get(protocol.HeaderNonce), r.Header.Get(protocol.HeaderInstance)
	// A connector never sends a credential before it has checked who we
	// are; one that does is refused without reading it.
	if r.Header.Get("Authorization") != "" || !validHandshake(nonce, instance) {
		http.Error(w, "invalid handshake", http.StatusBadRequest)
		return
	}
	cfg := h.config()
	up := websocket.Upgrader{Subprotocols: []string{protocol.Subprotocol}, HandshakeTimeout: 10 * time.Second, CheckOrigin: func(*http.Request) bool { return true }}
	ws, err := up.Upgrade(w, r, protocol.IdentityHeaders(cfg.ServerKey, nonce, instance, time.Now()))
	if err != nil {
		return // Upgrade has answered
	}
	mux, err := yamux.Server(protocol.NewWebSocketConn(ws), muxConfig())
	if err != nil {
		ws.Close()
		return
	}
	defer mux.Close()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.wg.Add(1) // under mu, so never concurrent with Close's Wait
	h.mu.Unlock()
	defer h.wg.Done()

	timer := time.AfterFunc(helloTimeout, func() { mux.Close() })
	control, err := mux.AcceptStream()
	timer.Stop()
	if err != nil || control.StreamID() != 1 {
		return
	}
	_ = control.SetDeadline(time.Now().Add(helloTimeout))
	br := bufio.NewReader(control)
	log := h.log.With("remote_ip", ip, "instance", instance)
	fail := func(reason string) {
		_ = protocol.WriteMessage(control, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthFailed, Reason: reason})
	}

	// auth: the token, only now.
	env, err := protocol.ReadMessage(br)
	var auth protocol.Auth
	if err != nil || env.Type != protocol.MessageAuth || env.Decode(&auth) != nil {
		return
	}
	h.authAttempts.Add(1)
	tokenID, _, err := protocol.ParseToken(auth.Token)
	if err != nil {
		log.Warn("session refused", "reason", "invalid token")
		fail("invalid token")
		return
	}
	site := cfg.siteForToken(auth.Token)
	if site == nil {
		log.Warn("session refused", "reason", "invalid token", "token_id", tokenID)
		fail("invalid token")
		return
	}
	// Per token after authentication, so guesses at a known token id can't
	// use up the real connector's reconnects (those are limited per IP).
	if !h.limiter.allow("token:" + tokenID) {
		fail("too many connection attempts; try again later")
		return
	}
	if site.Disabled {
		log.Warn("session refused", "reason", "site disabled", "site", site.Name)
		fail("site disabled")
		return
	}
	sessID, _ := protocol.NewInstanceID()
	if protocol.WriteMessage(control, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthOK, SessionID: sessID, Org: hubName}) != nil {
		return
	}
	s := &session{id: sessID, site: site.Name, token: auth.Token, instance: instance, remoteIP: ip,
		connectedAt: time.Now().UTC(), mux: mux, routes: map[string]*route{}}
	log = log.With("site", site.Name, "session", sessID)

	// hello: the site and its services.
	env, err = protocol.ReadMessage(br)
	var hello protocol.Hello
	if err != nil || env.Type != protocol.MessageHello || env.Decode(&hello) != nil || hello.InstanceID != instance || len(hello.Services) > maxServices {
		log.Warn("session refused", "reason", "bad hello")
		return
	}
	s.version, s.os, s.arch = printableClip(hello.Version, 64), printableClip(hello.OS, 32), printableClip(hello.Arch, 32)
	// The connector's file names the site it serves: a token pasted into
	// the wrong machine's file is refused before it can serve anything.
	if hello.Site != site.Name {
		log.Warn("session refused", "reason", "site_mismatch", "hello_site", printableClip(hello.Site, 40))
		_ = protocol.WriteMessage(control, protocol.Control{V: protocol.Version, Type: protocol.MessageClose, Reason: "site_mismatch"})
		return
	}
	for _, c := range hello.Capabilities {
		if len(s.caps) < maxCapabilities {
			s.caps = append(s.caps, printableClip(c, 32))
		}
	}
	accepted, rejected, total := []string{}, []protocol.Rejected{}, 0
	for _, svc := range hello.Services {
		if !protocol.ValidServiceName(svc.Name) {
			rejected = append(rejected, protocol.Rejected{Name: printableClip(svc.Name, 40), Reason: "invalid service name"})
			continue
		}
		if reason := s.admit(svc); reason != "" {
			rejected = append(rejected, protocol.Rejected{Name: svc.Name, Reason: reason})
			continue
		}
		limit := min(max(svc.MaxConcurrentRequests, 1), maxStreamsPerSession)
		rt := &route{name: svc.Name, kind: svc.Kind, endpoints: clipEndpoints(svc.Endpoints), maxConcurrent: limit,
			slots: make(chan struct{}, limit), healthy: svc.Healthy, models: clipModels(svc.Models), parts: clipParts(svc.Parts)}
		if st := svc.Storage; st != nil && svc.Kind == protocol.KindS3 {
			rt.storage = &protocol.StorageService{MaxKeyBytes: st.MaxKeyBytes, MaxObjectBytes: st.MaxObjectBytes, Encryption: printableClip(st.Encryption, 16), ReadOnly: st.ReadOnly}
		}
		s.routes[svc.Name] = rt
		accepted = append(accepted, svc.Name)
		total += limit
	}
	s.slots = make(chan struct{}, min(max(total, 1), maxStreamsPerSession))
	ack := protocol.HelloAck{V: protocol.Version, Type: protocol.MessageHelloAck, Accepted: accepted, Rejected: rejected, Limits: protocol.Limits{MaxStreams: cap(s.slots)}}
	if protocol.WriteMessage(control, ack) != nil {
		return
	}
	_ = control.SetDeadline(time.Time{})
	var writeMu sync.Mutex
	s.send = func(typ, reason string) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = control.SetWriteDeadline(time.Now().Add(controlWrite))
		return protocol.WriteMessage(control, protocol.Control{V: protocol.Version, Type: typ, Reason: reason})
	}
	if reason := h.register(s); reason != "" {
		log.Warn("session refused", "reason", reason)
		s.close(reason)
		return
	}
	defer h.unregister(s)
	for name := range site.Services {
		if s.routes[name] == nil {
			log.Warn("the file has settings for a service this session does not offer", "service", name)
		}
	}
	log.Info("session up", "services", len(accepted), "rejected", len(rejected), "version", s.version)
	defer log.Info("session down")
	h.run(s, control, br, log)
}

// admit is a service's admission; "" accepts it. An s3 service needs
// storage/1 and an http service inference/1; any other kind is refused.
func (s *session) admit(svc protocol.Service) string {
	switch {
	case s.routes[svc.Name] != nil:
		return "duplicate service name"
	case svc.Kind == protocol.KindS3:
		if !s.has(protocol.CapabilityStorage) {
			return "storage_capability_missing"
		}
	case svc.Kind == protocol.KindHTTP:
		if !s.has(protocol.CapabilityInference) {
			return "inference_capability_missing"
		}
	default:
		return "unknown_kind"
	}
	return ""
}

// run drives a registered session: heartbeats, status, draining. It returns
// when the session ends.
func (h *Hub) run(s *session, control *yamux.Stream, br *bufio.Reader, log *slog.Logger) {
	// A connector never opens streams after the control stream: any
	// attempt is a protocol violation that ends the session.
	go func() {
		if st, err := s.mux.AcceptStream(); err == nil {
			st.Close()
			log.Warn("connector opened a stream (protocol violation); closing")
			s.mux.Close()
		}
	}()
	done := make(chan struct{})
	defer close(done)
	var lastPong atomic.Int64
	lastPong.Store(time.Now().UnixNano())
	go func() {
		ping := time.NewTicker(pingInterval)
		defer ping.Stop()
		for {
			select {
			case <-done:
				return
			case <-ping.C:
				if time.Since(time.Unix(0, lastPong.Load())) > pongTimeout || s.send(protocol.MessagePing, "") != nil {
					s.mux.Close()
					return
				}
			}
		}
	}()
	for {
		_ = control.SetReadDeadline(time.Now().Add(pongTimeout))
		env, err := protocol.ReadMessage(br)
		if err != nil {
			return
		}
		switch env.Type {
		case protocol.MessagePong:
			lastPong.Store(time.Now().UnixNano())
		case protocol.MessageStatus:
			if s.applyStatus(env.Raw) != nil {
				return
			}
		case protocol.MessageDraining:
			if !s.draining.Swap(true) {
				log.Info("session draining: no new requests")
			}
		default:
			// Unknown control messages are ignored.
		}
	}
}

// applyStatus applies a status message. Models is always present; an empty
// list clears ours.
func (s *session) applyStatus(raw json.RawMessage) error {
	var m protocol.Status
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range m.Services {
		if rt := s.routes[st.Name]; rt != nil {
			rt.healthy = st.Healthy
			rt.models = clipModels(st.Models)
			rt.parts = clipParts(st.Parts)
		}
	}
	return nil
}

// register adds a session unless the file no longer lets it in (the file
// may have changed since authentication) or its site has too many.
func (h *Hub) register(s *session) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return "hub shutting down"
	}
	if reason := h.cfg.Load().refuses(s); reason != "" {
		return reason
	}
	if len(h.sessions[s.site]) >= maxSessionsPerSite {
		return "too many sessions for this site"
	}
	h.sessions[s.site] = append(h.sessions[s.site], s)
	return ""
}

func (h *Hub) unregister(s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	list := h.sessions[s.site]
	for i, x := range list {
		if x == s {
			list = append(list[:i:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(h.sessions, s.site)
	} else {
		h.sessions[s.site] = list
	}
}

// refuses reports why the file no longer admits a session ("" if it does).
func (c *Config) refuses(s *session) string {
	site := c.Sites[s.site]
	switch {
	case site == nil:
		return "site removed"
	case site.Disabled:
		return "site disabled"
	case !site.Accepts(s.token):
		return "token rotated"
	}
	return ""
}

// siteForToken returns the site one of whose tokens is token. Every token in
// the file is compared, in constant time, so the time taken does not say
// which site (if any) matched.
func (c *Config) siteForToken(token string) *Site {
	var found *Site
	for _, name := range c.Order {
		if s := c.Sites[name]; s.Accepts(token) && found == nil {
			found = s
		}
	}
	return found
}

// sessionsOf returns a site's live sessions, oldest first.
func (h *Hub) sessionsOf(site string) []*session {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*session(nil), h.sessions[site]...)
}

func printableClip(s string, n int) string {
	if len(s) > n {
		s = s[:n]
	}
	b := []byte(s)
	for i := range b {
		if b[i] < 32 || b[i] > 126 {
			b[i] = '?'
		}
	}
	return string(b)
}

// clipEndpoints bounds a service's advertised endpoints: at most
// maxEndpointsPerService, each printable and at most 256 bytes (others are
// dropped). They are shown, never routed on: the connector enforces them.
func clipEndpoints(eps []string) []string {
	out := []string{}
	for _, e := range eps {
		if len(out) == maxEndpointsPerService {
			break
		}
		if e != "" && len(e) <= 256 && printableClip(e, 256) == e {
			out = append(out, e)
		}
	}
	return out
}

func clipModels(m []string) []string {
	if len(m) > maxModelsPerService {
		m = m[:maxModelsPerService]
	}
	out := make([]string, 0, len(m))
	for _, id := range m {
		out = append(out, printableClip(id, maxModelBytes))
	}
	return out
}

// maxParts bounds a service's parts (a connector's machine has at most 64
// ports).
const maxParts = 64

// clipParts bounds what a connector reports of a machine's ports.
func clipParts(in []protocol.ServicePart) []protocol.ServicePart {
	if len(in) == 0 {
		return nil
	}
	out := make([]protocol.ServicePart, 0, min(len(in), maxParts))
	for _, p := range in[:min(len(in), maxParts)] {
		out = append(out, protocol.ServicePart{Models: clipModels(p.Models), Endpoints: clipEndpoints(p.Endpoints), Healthy: p.Healthy})
	}
	return out
}
