// Package session maintains the authenticated outbound cloud session.
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
	"github.com/eidonix-ai/cletus-connector/protocol"
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
	HelloRoutes() []protocol.Route
	StatusRoutes() []protocol.RouteStatus
}
type Options struct {
	Server, Token, Version string
	Keys                   map[string]ed25519.PublicKey
	TLSConfig              *tls.Config
	DialContext            func(context.Context, string, string) (net.Conn, error)
	MaxStreams             int
	started                time.Time
	refresh                func(context.Context) error
	Proxy                  func(*http.Request) (*url.URL, error)
}

func RunOptions(ctx context.Context, o Options, h Handler) error {
	u, e := url.Parse(o.Server)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || (u.Port() != "" && u.Port() != "443") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("cloud server must be an HTTPS origin on port 443")
	}
	id, _, e := protocol.ParseToken(o.Token)
	if e != nil {
		return e
	}
	if len(o.Keys) == 0 {
		return errors.New("no pinned server keys")
	}
	instance, e := protocol.NewInstanceID()
	if e != nil {
		return e
	}
	u.Scheme = "wss"
	u.Path = "/v1/connectors/session"
	if o.MaxStreams <= 0 {
		o.MaxStreams = 128
	}
	o.started = time.Now()
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		e = connect(ctx, u.String(), id, instance, o, h)
		if ctx.Err() != nil {
			break
		}
		slog.Warn("cloud session ended", "error", e)
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
func connect(ctx context.Context, endpoint, id, instance string, o Options, h Handler) error {
	nonce, e := protocol.NewNonce()
	if e != nil {
		return e
	}
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+o.Token)
	headers.Set("X-Cletus-Connector-Nonce", nonce)
	headers.Set("X-Cletus-Connector-Version", o.Version)
	headers.Set("X-Cletus-Connector-Instance", instance)
	proxy := o.Proxy
	if proxy == nil {
		proxy = http.ProxyFromEnvironment
	}
	d := websocket.Dialer{Proxy: proxy, TLSClientConfig: o.TLSConfig, NetDialContext: o.DialContext, HandshakeTimeout: 10 * time.Second, Subprotocols: []string{protocol.Subprotocol}}
	ws, response, e := d.DialContext(ctx, endpoint, headers)
	if e != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return fmt.Errorf("cloud upgrade: %w", e)
	}
	defer ws.Close()
	if ws.Subprotocol() != protocol.Subprotocol {
		return errors.New("incorrect cloud subprotocol")
	}
	if e = protocol.VerifyIdentity(response.Header, nonce, id, instance, o.Keys, time.Now()); e != nil {
		return e
	}
	yc := yamux.DefaultConfig()
	yc.MaxStreamWindowSize = 256 << 10
	yc.KeepAliveInterval = 15 * time.Second
	yc.ConnectionWriteTimeout = 10 * time.Second
	yc.AcceptBacklog = o.MaxStreams
	yc.LogOutput = io.Discard
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
	if e = protocol.WriteJSONLine(control, protocol.MaxControlBytes, protocol.Message{V: 1, Type: "hello", InstanceID: instance, Version: o.Version, OS: runtime.GOOS, Arch: runtime.GOARCH, Routes: h.HelloRoutes()}); e != nil {
		return e
	}
	reader := bufio.NewReader(control)
	var ack protocol.Message
	if e = protocol.ReadJSONLine(reader, protocol.MaxControlBytes, &ack); e != nil {
		return e
	}
	if ack.V != 1 || ack.Type != "hello_ack" || ack.SessionID == "" {
		return errors.New("expected hello_ack")
	}
	control.SetDeadline(time.Time{})
	sessionCtx = protocol.WithSessionID(sessionCtx, ack.SessionID)
	for _, r := range ack.RejectedRoutes {
		slog.Warn("cloud rejected route", "route", r.ID, "reason", r.Reason)
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
				routes := h.StatusRoutes()
				encoded, _ := json.Marshal(routes)
				if bytes.Equal(encoded, lastStatus) && time.Since(lastSent) < 15*time.Second {
					continue
				}
				lastStatus = encoded
				lastSent = time.Now()
				if e := write(protocol.StatusMessage{V: 1, Type: "status", Routes: routes, UptimeS: int64(time.Since(started).Seconds())}); e != nil {
					c.Close()
					return
				}
			}
		}
	}()
	for {
		c.SetReadDeadline(time.Now().Add(50 * time.Second))
		var m protocol.Message
		if e := protocol.ReadJSONLine(r, protocol.MaxControlBytes, &m); e != nil {
			return e
		}
		if m.V != 1 {
			return errors.New("invalid control version")
		}
		switch m.Type {
		case "ping":
			if e := write(protocol.Message{V: 1, Type: "pong"}); e != nil {
				return e
			}
		case "close":
			return errors.New("server closed session")
		default:
			slog.Warn("unknown control message", "type", m.Type)
		}
	}
}
