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
	"time"
)

type Handler interface {
	ServeStream(context.Context, net.Conn)
	HelloServices() []protocol.Service
	StatusServices() []protocol.ServiceStatus
}
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
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		e = connect(ctx, u.String(), instance, o, h)
		if ctx.Err() != nil {
			break
		}
		slog.Warn("upstream session ended", "error", e)
		if time.Since(started) > time.Minute {
			delay = time.Second
		}
		wait := delay/2 + time.Duration(rand.Int63n(int64(delay/2)+1))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
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
	go func() {
		select {
		case <-ctx.Done():
			mux.Close()
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
	slog.Info("upstream session established", "session", auth.SessionID, "org", auth.Org, "accepted", len(ack.Accepted))
	for _, r := range ack.Rejected {
		slog.Warn("upstream rejected service", "service", r.Name, "reason", r.Reason)
	}
	limit := o.MaxStreams
	if ack.Limits.MaxStreams > 0 && ack.Limits.MaxStreams < limit {
		limit = ack.Limits.MaxStreams
	}
	failures := make(chan error, 2)
	go func() { failures <- controlLoop(sessionCtx, control, reader, h, o.started) }()
	acceptDone := make(chan struct{})
	var workers sync.WaitGroup
	defer func() {
		cancel()
		mux.Close()
		<-acceptDone
		workers.Wait()
	}()
	slots := make(chan struct{}, limit)
	go func() {
		defer close(acceptDone)
		for {
			stream, err := mux.AcceptStream()
			if err != nil {
				failures <- err
				return
			}
			select {
			case slots <- struct{}{}:
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer func() { <-slots }()
					defer stream.Close()
					h.ServeStream(sessionCtx, stream)
				}()
			default:
				stream.Close()
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-failures:
		return e
	}
}
func controlLoop(ctx context.Context, c net.Conn, r *bufio.Reader, h Handler, started time.Time) error {
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
