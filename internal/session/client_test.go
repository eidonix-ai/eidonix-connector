package session

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/eidonix-ai/cletus-connector/protocol"
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

func (fakeHandler) ServeStream(context.Context, net.Conn) {}
func (fakeHandler) HelloRoutes() []protocol.Route         { return nil }
func (fakeHandler) StatusRoutes() []protocol.RouteStatus  { return nil }
func TestVerifyBeforeMultiplex(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "bad", true: "valid"}[valid], func(t *testing.T) {
			received := make(chan bool, 1)
			upgrader := websocket.Upgrader{Subprotocols: []string{protocol.Subprotocol}}
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ts := time.Now().UTC().Format(time.RFC3339)
				hdr := http.Header{}
				hdr.Set("X-Cletus-Server-Key-Id", "k")
				hdr.Set("X-Cletus-Server-Time", ts)
				nonce := r.Header.Get("X-Cletus-Connector-Nonce")
				if !valid {
					nonce = "wrong"
				}
				hdr.Set("X-Cletus-Server-Signature", protocol.SignIdentity(key, nonce, "ABCDEFGHIJKLMNOP", r.Header.Get("X-Cletus-Connector-Instance"), ts))
				ws, e := upgrader.Upgrade(w, r, hdr)
				if e != nil {
					return
				}
				defer ws.Close()
				cfg := yamux.DefaultConfig()
				mux, e := yamux.Server(protocol.NewWebSocketConn(ws), cfg)
				if e != nil {
					return
				}
				defer mux.Close()
				c, e := mux.AcceptStream()
				received <- e == nil
				if e == nil {
					c.Close()
				}
			}))
			defer srv.Close()
			addr := strings.TrimPrefix(srv.URL, "https://")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			o := Options{Server: "https://cloud.example", Token: "cxn_ABCDEFGHIJKLMNOP_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)), Keys: map[string]ed25519.PublicKey{"k": pub}, TLSConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}, MaxStreams: 4}
			e := connect(ctx, "wss://cloud.example/v1/connectors/session", "ABCDEFGHIJKLMNOP", "instance", o, fakeHandler{})
			if e == nil {
				t.Fatal("expected session end")
			}
			select {
			case got := <-received:
				if got != valid {
					t.Fatalf("stream received=%v", got)
				}
			case <-ctx.Done():
				t.Fatal("server did not finish")
			}
		})
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
func TestTLSInspectionProxyKeepsIdentityPin(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	for _, valid := range []bool{true, false} {
		t.Run(fmt.Sprint(valid), func(t *testing.T) {
			received := make(chan bool, 1)
			var connects atomic.Int64
			signer := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ts := time.Now().UTC().Format(time.RFC3339)
				hdr := http.Header{}
				hdr.Set("X-Cletus-Server-Key-Id", "k")
				hdr.Set("X-Cletus-Server-Time", ts)
				nonce := r.Header.Get("X-Cletus-Connector-Nonce")
				if !valid {
					nonce = "replayed"
				}
				hdr.Set("X-Cletus-Server-Signature", protocol.SignIdentity(key, nonce, "ABCDEFGHIJKLMNOP", r.Header.Get("X-Cletus-Connector-Instance"), ts))
				ws, e := (&websocket.Upgrader{Subprotocols: []string{protocol.Subprotocol}}).Upgrade(w, r, hdr)
				if e != nil {
					return
				}
				defer ws.Close()
				mux, e := yamux.Server(protocol.NewWebSocketConn(ws), yamux.DefaultConfig())
				if e != nil {
					return
				}
				defer mux.Close()
				c, e := mux.AcceptStream()
				received <- e == nil
				if c != nil {
					c.Close()
				}
			})
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			o := Options{Token: "cxn_ABCDEFGHIJKLMNOP_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)), Keys: map[string]ed25519.PublicKey{"k": pub}, TLSConfig: tlsTrust, Proxy: http.ProxyURL(proxyURL), MaxStreams: 4}
			e := connect(ctx, "wss://example.com/v1/connectors/session", "ABCDEFGHIJKLMNOP", "instance", o, fakeHandler{})
			if e == nil {
				t.Fatal("expected session end")
			}
			if connects.Load() != 1 {
				t.Fatal("proxy was not used")
			}
			select {
			case got := <-received:
				if got != valid {
					t.Fatalf("accepted stream%v", got)
				}
			case <-ctx.Done():
				t.Fatal("inspection handshake did not complete")
			}
		})
	}
}
