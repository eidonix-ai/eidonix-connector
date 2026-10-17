package session

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/internal/testgateway"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeHandler struct{}

func (fakeHandler) ServeStream(context.Context, net.Conn)    {}
func (fakeHandler) HelloServices() []protocol.Service        { return nil }
func (fakeHandler) StatusServices() []protocol.ServiceStatus { return nil }

var testToken = "cxn_ABCDEFGHIJKLMNOP_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))

// upstreamResult is what a fake upstream observed during one connection.
type upstreamResult struct {
	sawToken bool   // the token appeared in the upgrade request or any later byte
	auth     string // token received in an auth message
	hello    bool
}

// fakeUpstream signs the upgrade with key (mangling the nonce when !valid), then
// either runs the control handshake or, when the signature is bad, records
// every byte the connector sends until it disconnects.
func fakeUpstream(key ed25519.PrivateKey, valid bool, authOK bool, results chan<- upstreamResult) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res upstreamResult
		defer func() { results <- res }()
		var dump bytes.Buffer
		r.Header.Write(&dump)
		if strings.Contains(dump.String(), testToken) || strings.Contains(dump.String(), "Bearer") {
			res.sawToken = true
		}
		nonce := r.Header.Get(protocol.HeaderNonce)
		if !valid {
			nonce = "replayed"
		}
		hdr := protocol.IdentityHeaders(key, nonce, r.Header.Get(protocol.HeaderInstance), time.Now())
		ws, e := (&websocket.Upgrader{Subprotocols: []string{protocol.Subprotocol}}).Upgrade(w, r, hdr)
		if e != nil {
			return
		}
		defer ws.Close()
		if !valid {
			ws.SetReadDeadline(time.Now().Add(time.Second))
			for {
				_, b, e := ws.ReadMessage()
				if e != nil {
					return
				}
				if bytes.Contains(b, []byte(testToken)) {
					res.sawToken = true
				}
			}
		}
		cfg := yamux.DefaultConfig()
		cfg.LogOutput = io.Discard
		mux, e := yamux.Server(protocol.NewWebSocketConn(ws), cfg)
		if e != nil {
			return
		}
		defer mux.Close()
		c, e := mux.AcceptStream()
		if e != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Second))
		br := bufio.NewReader(c)
		env, e := protocol.ReadMessage(br)
		if e != nil || env.Type != protocol.MessageAuth {
			return
		}
		var auth protocol.Auth
		env.Decode(&auth)
		res.auth = auth.Token
		if !authOK {
			protocol.WriteMessage(c, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthFailed, Reason: "invalid_token"})
			return
		}
		protocol.WriteMessage(c, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthOK, SessionID: "s-1", Org: "Example"})
		if env, e = protocol.ReadMessage(br); e != nil || env.Type != protocol.MessageHello {
			return
		}
		res.hello = true
		protocol.WriteMessage(c, protocol.HelloAck{V: protocol.Version, Type: protocol.MessageHelloAck, Limits: protocol.Limits{MaxStreams: 4}})
		protocol.WriteMessage(c, protocol.Control{V: protocol.Version, Type: protocol.MessageClose, Reason: "test done"})
	})
}
func dialTo(addr string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
}

// TestUnlistedServerKeyNeverReceivesToken: a server that cannot prove it holds
// a configured key never sees the token, in the upgrade or afterwards.
func TestUnlistedServerKeyNeverReceivesToken(t *testing.T) {
	configured, _, _ := ed25519.GenerateKey(rand.Reader)
	_, impostor, _ := ed25519.GenerateKey(rand.Reader)
	for name, key := range map[string]ed25519.PrivateKey{"unlisted-key": impostor} {
		t.Run(name, func(t *testing.T) {
			results := make(chan upstreamResult, 1)
			srv := httptest.NewTLSServer(fakeUpstream(key, true, true, results))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o := Options{Token: testToken, Keys: []ed25519.PublicKey{configured}, TLSConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: dialTo(strings.TrimPrefix(srv.URL, "https://")), MaxStreams: 4}
			e := connect(ctx, "wss://upstream.example/v1/connectors/session", "instance", o, fakeHandler{})
			if e == nil || !strings.Contains(e.Error(), "not one of the configured public keys") {
				t.Fatalf("expected identity failure, got %v", e)
			}
			res := <-results
			if res.sawToken || res.auth != "" {
				t.Fatalf("token reached an unverified server: %+v", res)
			}
		})
	}
}
func TestVerifyBeforeAuth(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "bad-signature", true: "valid"}[valid], func(t *testing.T) {
			results := make(chan upstreamResult, 1)
			srv := httptest.NewTLSServer(fakeUpstream(key, valid, true, results))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o := Options{Token: testToken, Keys: []ed25519.PublicKey{pub}, TLSConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: dialTo(strings.TrimPrefix(srv.URL, "https://")), MaxStreams: 4}
			e := connect(ctx, "wss://upstream.example/v1/connectors/session", "instance", o, fakeHandler{})
			if e == nil {
				t.Fatal("expected session end")
			}
			res := <-results
			if res.sawToken {
				t.Fatal("token sent in the upgrade request")
			}
			if valid != (res.auth == testToken && res.hello) {
				t.Fatalf("valid=%v result=%+v err=%v", valid, res, e)
			}
		})
	}
}
func TestAuthFailedEndsSession(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	results := make(chan upstreamResult, 1)
	srv := httptest.NewTLSServer(fakeUpstream(key, true, false, results))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	o := Options{Token: testToken, Keys: []ed25519.PublicKey{pub}, TLSConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: dialTo(strings.TrimPrefix(srv.URL, "https://")), MaxStreams: 4}
	e := connect(ctx, "wss://upstream.example/v1/connectors/session", "instance", o, fakeHandler{})
	if !errors.Is(e, ErrAuthFailed) {
		t.Fatal(e)
	}
	if res := <-results; res.hello {
		t.Fatal("hello sent after auth_failed")
	}
}

// singleConnListener lets the inspection proxy terminate TLS inside CONNECT.
type singleConnListener struct {
	c    net.Conn
	used bool
	done chan struct{}
	once sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if !l.used {
		l.used = true
		return l.c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *singleConnListener) Close() error   { l.once.Do(func() { close(l.done) }); return l.c.Close() }
func (l *singleConnListener) Addr() net.Addr { return l.c.LocalAddr() }

// A TLS-inspecting proxy the host trusts can relay the challenge, but cannot
// forge a signature for a fresh nonce: the replayed case must not get the token.
func TestTLSInspectionProxyKeepsIdentityPin(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	for _, valid := range []bool{true, false} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			results := make(chan upstreamResult, 1)
			var connects atomic.Int64
			signer := fakeUpstream(key, valid, true, results)
			certServer := httptest.NewTLSServer(signer)
			defer certServer.Close()
			tlsTrust := certServer.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "CONNECT" || r.Host != "example.com:443" {
					http.Error(w, "denied", 403)
					return
				}
				connects.Add(1)
				c, _, e := w.(http.Hijacker).Hijack()
				if e != nil {
					return
				}
				io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
				listener := &singleConnListener{c: c, done: make(chan struct{})}
				server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer listener.Close(); signer.ServeHTTP(w, r) })}
				defer server.Close()
				server.Serve(tls.NewListener(listener, &tls.Config{Certificates: certServer.TLS.Certificates, MinVersion: tls.VersionTLS12}))
			}))
			defer proxy.Close()
			proxyURL, _ := url.Parse(proxy.URL)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o := Options{Token: testToken, Keys: []ed25519.PublicKey{pub}, TLSConfig: tlsTrust, Proxy: http.ProxyURL(proxyURL), MaxStreams: 4}
			e := connect(ctx, "wss://example.com/v1/connectors/session", "instance", o, fakeHandler{})
			if e == nil {
				t.Fatal("expected session end")
			}
			if connects.Load() != 1 {
				t.Fatal("proxy was not used")
			}
			res := <-results
			if valid != (res.auth == testToken) || res.sawToken {
				t.Fatalf("valid=%v result=%+v", valid, res)
			}
		})
	}
}

// TestSiteMismatchEndsSession: a gateway that knows the connector by another
// name closes the session after hello with site_mismatch, and the connector
// says so.
func TestSiteMismatchEndsSession(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	g, err := testgateway.New(testToken, key)
	if err != nil {
		t.Fatal(err)
	}
	g.Site = "example-site"
	defer g.Close()
	srv := httptest.NewTLSServer(g)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	o := Options{Token: testToken, Site: "other-site", Keys: []ed25519.PublicKey{pub}, TLSConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: dialTo(strings.TrimPrefix(srv.URL, "https://")), MaxStreams: 4}
	instance := "00000000-0000-4000-8000-000000000001"
	e := connect(ctx, "wss://upstream.example/v1/connectors/session", instance, o, fakeHandler{})
	if !errors.Is(e, ErrSiteMismatch) || !strings.Contains(e.Error(), `site "other-site"`) {
		t.Fatal(e)
	}
}
