// Package session maintains the authenticated outbound upstream session.
package session

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Handler interface {
	ServeStream(context.Context, net.Conn)
	HelloServices() []protocol.Service
	StatusServices() []protocol.ServiceStatus
}

// Refuser is an optional Handler capability. A draining session answers each
// stream that still arrives by reading its header and replying with the closed
// error code (see DrainRefusal) instead of serving it; a handler without it
// has such streams closed unanswered.
type Refuser interface {
	RefuseStream(ctx context.Context, c net.Conn, code string)
}

// DrainRefusal is the closed error code a draining session answers streams
// with. It is "busy": the request was never forwarded to a service, and a
// gateway already treats busy as "try another eligible session".
const DrainRefusal = "busy"

// State is a snapshot of one session's connection, passed to Options.OnState
// whenever it changes.
type State struct {
	Connected   bool
	SessionID   string
	Hub         string // the gateway's name for this connector (auth_ok org)
	Since       time.Time
	Draining    bool
	DrainingAt  time.Time
	LastError   string
	LastErrorAt time.Time
}

// errDrained ends connect once a requested drain has finished.
var errDrained = errors.New("session drained")

type Options struct {
	Server, Token, Version string
	// Site is the connector's name in Setup, sent in hello.
	Site        string
	Keys        []ed25519.PublicKey
	TLSConfig   *tls.Config
	DialContext func(context.Context, string, string) (net.Conn, error)
	MaxStreams  int
	started     time.Time
	refresh     func(context.Context) error
	Proxy       func(*http.Request) (*url.URL, error)
	// Drain, when closed, drains the session: the connector tells the
	// gateway (a draining control message), refuses streams that still arrive
	// and returns nil once the streams already running have finished. Without
	// a connection it just stops reconnecting. Cancel ctx to end stragglers.
	Drain <-chan struct{}
	// OnState, if set, is called (never concurrently) when the connection
	// is established, ends, starts draining or fails.
	OnState func(State)
	// onEstablished and onDraining are called by connect.
	onEstablished func(sessionID, hub string)
	onDraining    func()
}

var processInstance = sync.OnceValues(protocol.NewInstanceID)
var processStarted = time.Now()

func RunOptions(ctx context.Context, o Options, h Handler) error {
	u, e := url.Parse(o.Server)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("upstream url must be an HTTPS origin without a path")
	}
	if _, _, e := protocol.ParseToken(o.Token); e != nil {
		return e
	}
	if len(o.Keys) == 0 {
		return errors.New("no upstream public keys")
	}
	instance, e := processInstance()
	if e != nil {
		return e
	}
	u.Scheme = "wss"
	u.Path = protocol.SessionPath
	if o.MaxStreams <= 0 {
		o.MaxStreams = 128
	}
	o.started = processStarted
	var mu sync.Mutex
	var state State
	update := func(change func(*State)) {
		mu.Lock()
		defer mu.Unlock()
		change(&state)
		if o.OnState != nil {
			o.OnState(state)
		}
	}
	o.onEstablished = func(id, hub string) {
		update(func(s *State) { s.Connected, s.SessionID, s.Hub, s.Since = true, id, hub, time.Now() })
	}
	o.onDraining = func() {
		update(func(s *State) { s.Draining, s.DrainingAt = true, time.Now() })
	}
	drained := func() bool {
		select {
		case <-o.Drain:
			return true
		default:
			return false
		}
	}
	delay := time.Second
	for ctx.Err() == nil && !drained() {
		started := time.Now()
		e = connect(ctx, u.String(), instance, o, h)
		if ctx.Err() != nil {
			update(func(s *State) { s.Connected = false })
			break
		}
		if errors.Is(e, errDrained) || drained() {
			update(func(s *State) { s.Connected = false })
			return nil
		}
		slog.Warn("upstream session ended", "error", e)
		update(func(s *State) {
			s.Connected, s.Draining, s.LastError, s.LastErrorAt = false, false, e.Error(), time.Now()
		})
		if time.Since(started) > time.Minute {
			delay = time.Second
		}
		wait := delay/2 + time.Duration(rand.Int63n(int64(delay/2)+1))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-o.Drain:
			timer.Stop()
		case <-timer.C:
		}
		if delay < 60*time.Second {
			delay *= 2
			if delay > 60*time.Second {
				delay = 60 * time.Second
			}
		}
	}
	return ctx.Err()
}

// ErrAuthFailed reports that the upstream refused the connection token.
var ErrAuthFailed = errors.New("upstream refused the connection token")

// ErrSiteMismatch reports that the upstream refused the configured site name.
var ErrSiteMismatch = errors.New("upstream refused the site name")

// connect runs one session. The upgrade request carries no credential: the
// token is sent on the control stream only after the server's signature over
// this connection's nonce and instance has been verified.
func connect(ctx context.Context, endpoint, instance string, o Options, h Handler) error {
	nonce, e := protocol.NewNonce()
	if e != nil {
		return e
	}
	headers := http.Header{}
	headers.Set(protocol.HeaderNonce, nonce)
	headers.Set(protocol.HeaderInstance, instance)
	d := websocket.Dialer{Proxy: o.Proxy, TLSClientConfig: o.TLSConfig, NetDialContext: o.DialContext, HandshakeTimeout: 10 * time.Second, Subprotocols: []string{protocol.Subprotocol}}
	ws, response, e := d.DialContext(ctx, endpoint, headers)
	if e != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return fmt.Errorf("upstream upgrade: %w", e)
	}
	defer ws.Close()
	if ws.Subprotocol() != protocol.Subprotocol {
		return errors.New("upstream did not accept the " + protocol.Subprotocol + " subprotocol")
	}
	if e = protocol.VerifyIdentity(response.Header, nonce, instance, o.Keys, time.Now()); e != nil {
		return fmt.Errorf("upstream identity: %w", e)
	}
	yc := yamux.DefaultConfig()
	yc.MaxStreamWindowSize = 256 << 10
	yc.KeepAliveInterval = 15 * time.Second
	yc.ConnectionWriteTimeout = 10 * time.Second
	yc.AcceptBacklog = o.MaxStreams
	yc.LogOutput = io.Discard
	yc.StreamCloseTimeout = 0
	mux, e := yamux.Client(protocol.NewWebSocketConn(ws), yc)
	if e != nil {
		return e
	}
	defer mux.Close()
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	var established atomic.Bool
	go func() {
		select {
		case <-ctx.Done():
			mux.Close()
		case <-o.Drain:
			// A drain asked for before the session is up just abandons it;
			// an established session drains in the loop below.
			if !established.Load() {
				mux.Close()
			}
		case <-done:
		}
	}()
	control, e := mux.OpenStream()
	if e != nil {
		return e
	}
	defer control.Close()
	control.SetDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(control)
	if e = protocol.WriteMessage(control, protocol.Auth{V: protocol.Version, Type: protocol.MessageAuth, Token: o.Token}); e != nil {
		return e
	}
	env, e := protocol.ReadMessage(reader)
	if e != nil {
		return e
	}
	var auth protocol.AuthResult
	if e = env.Decode(&auth); e != nil {
		return e
	}
	if env.Type == protocol.MessageAuthFailed {
		return fmt.Errorf("%w: %s", ErrAuthFailed, auth.Reason)
	}
	if env.Type != protocol.MessageAuthOK || auth.SessionID == "" {
		return errors.New("expected auth_ok")
	}
	if e = protocol.WriteMessage(control, protocol.Hello{V: protocol.Version, Type: protocol.MessageHello, Site: o.Site, InstanceID: instance, Version: o.Version, OS: runtime.GOOS, Arch: runtime.GOARCH, Capabilities: []string{protocol.CapabilityInference, protocol.CapabilityStorage}, Services: h.HelloServices()}); e != nil {
		return e
	}
	env, e = protocol.ReadMessage(reader)
	if e != nil {
		return e
	}
	var ack protocol.HelloAck
	if env.Type == protocol.MessageClose || env.Type == protocol.MessageAuthFailed {
		var c protocol.Control
		env.Decode(&c)
		if c.Reason == "site_mismatch" {
			return fmt.Errorf("%w: site %q is not this connector's name in Eidonix Setup; set site in the configuration to the name Setup shows", ErrSiteMismatch, o.Site)
		}
		return fmt.Errorf("upstream closed session: %s", c.Reason)
	}
	if env.Type != protocol.MessageHelloAck || env.Decode(&ack) != nil {
		return errors.New("expected hello_ack")
	}
	control.SetDeadline(time.Time{})
	sessionCtx = protocol.WithSessionID(sessionCtx, auth.SessionID)
	established.Store(true)
	if o.onEstablished != nil {
		o.onEstablished(auth.SessionID, auth.Org)
	}
	slog.Info("upstream session established", "session", auth.SessionID, "hub", auth.Org, "accepted", len(ack.Accepted))
	for _, r := range ack.Rejected {
		slog.Warn("upstream rejected service", "service", r.Name, "reason", r.Reason)
	}
	limit := o.MaxStreams
	if ack.Limits.MaxStreams > 0 && ack.Limits.MaxStreams < limit {
		limit = ack.Limits.MaxStreams
	}
	failures := make(chan error, 2)
	draining := make(chan struct{})
	go func() { failures <- controlLoop(sessionCtx, control, reader, h, o.started, draining) }()
	acceptDone := make(chan struct{})
	var workers sync.WaitGroup
	defer func() {
		cancel()
		mux.Close()
		<-acceptDone
		workers.Wait()
	}()
	// serving counts streams being served, refusing the ones answered while
	// draining; both change under mu, and once draining is set no stream is
	// served any more.
	var mu sync.Mutex
	var serving, refusing int
	isDraining := false
	idle := make(chan struct{})
	slots := make(chan struct{}, limit)
	refuser, _ := h.(Refuser)
	go func() {
		defer close(acceptDone)
		for {
			stream, err := mux.AcceptStream()
			if err != nil {
				failures <- err
				return
			}
			mu.Lock()
			if isDraining {
				// Not served: the gateway should have stopped sending, so this
				// one raced the draining message. Answer it with DrainRefusal.
				if refuser == nil || refusing >= maxRefusals {
					mu.Unlock()
					stream.Close()
					continue
				}
				refusing++
				mu.Unlock()
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer func() { mu.Lock(); refusing--; mu.Unlock() }()
					defer stream.Close()
					refuser.RefuseStream(sessionCtx, stream, DrainRefusal)
				}()
				continue
			}
			select {
			case slots <- struct{}{}:
				serving++
				mu.Unlock()
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer func() { <-slots }()
					defer stream.Close()
					defer func() {
						mu.Lock()
						serving--
						if isDraining && serving == 0 {
							close(idle)
						}
						mu.Unlock()
					}()
					h.ServeStream(sessionCtx, stream)
				}()
			default:
				mu.Unlock()
				stream.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-failures:
		return e
	case <-o.Drain:
	}
	// Drain: tell the gateway, stop serving new streams, and wait for the ones
	// running (ctx ends them if the caller's bound passes first).
	if o.onDraining != nil {
		o.onDraining()
	}
	slog.Info("upstream session draining", "session", auth.SessionID)
	mu.Lock()
	isDraining = true
	if serving == 0 {
		close(idle)
	}
	mu.Unlock()
	close(draining)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-failures:
		return e
	case <-idle:
	}
	// Let refusals already under way finish before the connection closes.
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		mu.Lock()
		n := refusing
		mu.Unlock()
		if n == 0 {
			break
		}
	}
	return errDrained
}

// maxRefusals bounds the streams a draining session answers at once; further
// ones are closed unanswered.
const maxRefusals = 16

func controlLoop(ctx context.Context, c net.Conn, r *bufio.Reader, h Handler, started time.Time, draining <-chan struct{}) error {
	var writeMu sync.Mutex
	write := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		c.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return protocol.WriteJSONLine(c, protocol.MaxControlBytes, v)
	}
	finished := make(chan struct{})
	defer close(finished)
	if started.IsZero() {
		started = time.Now()
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		lastSent := time.Time{}
		var lastStatus []byte
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case <-draining:
				draining = nil
				if e := write(protocol.Control{V: protocol.Version, Type: protocol.MessageDraining}); e != nil {
					c.Close()
					return
				}
			case <-ticker.C:
				services := h.StatusServices()
				encoded, _ := json.Marshal(services)
				if bytes.Equal(encoded, lastStatus) && time.Since(lastSent) < 15*time.Second {
					continue
				}
				lastStatus = encoded
				lastSent = time.Now()
				if e := write(protocol.Status{V: protocol.Version, Type: protocol.MessageStatus, Services: services, UptimeS: int64(time.Since(started).Seconds())}); e != nil {
					c.Close()
					return
				}
			}
		}
	}()
	for {
		c.SetReadDeadline(time.Now().Add(50 * time.Second))
		m, e := protocol.ReadMessage(r)
		if e != nil {
			return e
		}
		switch m.Type {
		case protocol.MessagePing:
			if e := write(protocol.Control{V: protocol.Version, Type: protocol.MessagePong}); e != nil {
				return e
			}
		case protocol.MessageClose:
			var c protocol.Control
			m.Decode(&c)
			return fmt.Errorf("upstream closed session: %s", c.Reason)
		default:
			slog.Warn("unknown control message", "type", m.Type)
		}
	}
}
