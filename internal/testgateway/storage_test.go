package testgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

func TestStorageCapabilityNegotiation(t *testing.T) {
	for _, capable := range []bool{false, true} {
		name := "missing"
		if capable {
			name = "present"
		}
		t.Run(name, func(t *testing.T) {
			_, key, _ := ed25519.GenerateKey(rand.Reader)
			g, e := New(testToken, key)
			if e != nil {
				t.Fatal(e)
			}
			defer g.Close()
			server := httptest.NewTLSServer(g)
			defer server.Close()
			instance, _ := protocol.NewInstanceID()
			nonce, _ := protocol.NewNonce()
			dialer := websocket.Dialer{TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig.Clone(), Subprotocols: []string{protocol.Subprotocol}}
			ws, _, e := dialer.Dial("wss"+strings.TrimPrefix(server.URL, "https")+"/v1/connectors/session", http.Header{protocol.HeaderNonce: {nonce}, protocol.HeaderInstance: {instance}})
			if e != nil {
				t.Fatal(e)
			}
			defer ws.Close()
			cfg := yamux.DefaultConfig()
			cfg.LogOutput = io.Discard
			mux, e := yamux.Client(protocol.NewWebSocketConn(ws), cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer mux.Close()
			control, e := mux.OpenStream()
			if e != nil {
				t.Fatal(e)
			}
			control.SetDeadline(time.Now().Add(time.Second))
			br := bufio.NewReader(control)
			if e := protocol.WriteMessage(control, protocol.Auth{V: protocol.Version, Type: protocol.MessageAuth, Token: testToken}); e != nil {
				t.Fatal(e)
			}
			if env, e := protocol.ReadMessage(br); e != nil || env.Type != protocol.MessageAuthOK {
				t.Fatal(env, e)
			}
			hello := protocol.Hello{V: protocol.Version, Type: protocol.MessageHello, InstanceID: instance, Site: "local-test", Services: []protocol.Service{{Name: "files", Kind: protocol.KindS3, Storage: &protocol.StorageService{MaxKeyBytes: 1023, MaxObjectBytes: 100, Encryption: "sse-s3"}}}}
			if capable {
				hello.Capabilities = []string{protocol.CapabilityStorage}
			}
			if e := protocol.WriteMessage(control, hello); e != nil {
				t.Fatal(e)
			}
			env, e := protocol.ReadMessage(br)
			var ack protocol.HelloAck
			if e != nil || env.Decode(&ack) != nil {
				t.Fatal(e)
			}
			if capable {
				if len(ack.Accepted) != 1 || len(ack.Rejected) != 0 {
					t.Fatal(ack)
				}
			} else {
				if len(ack.Accepted) != 0 || len(ack.Rejected) != 1 || ack.Rejected[0].Reason != "storage_capability_missing" {
					t.Fatal(ack)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				_, _, e = g.open(ctx, "files", "storage")
				if e == nil {
					t.Fatal("opened storage without capability")
				}
			}
		})
	}
}
func fakeStoragePeer(t *testing.T, g *Gateway, handler func(net.Conn)) {
	t.Helper()
	a, b := net.Pipe()
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	cfg.StreamCloseTimeout = 0
	cloud, e := yamux.Server(a, cfg)
	if e != nil {
		t.Fatal(e)
	}
	connector, e := yamux.Client(b, cfg)
	if e != nil {
		t.Fatal(e)
	}
	p := &peer{mux: cloud, services: map[string]protocol.Service{"files": {Name: "files", Kind: protocol.KindS3}}, capabilities: map[string]bool{protocol.CapabilityStorage: true}, slots: make(chan struct{}, 4)}
	g.mu.Lock()
	g.sessions[p] = true
	g.mu.Unlock()
	t.Cleanup(func() { connector.Close(); cloud.Close() })
	go func() {
		s, e := connector.AcceptStream()
		if e != nil {
			return
		}
		defer s.Close()
		handler(s)
	}()
}
func TestStorageGatewayGetIntegrity(t *testing.T) {
	for _, mode := range []string{"valid", "missing_marker", "missing_trailer", "wrong_hash", "trailing_byte", "long_body"} {
		t.Run(mode, func(t *testing.T) {
			_, key, _ := ed25519.GenerateKey(rand.Reader)
			g, _ := New(testToken, key)
			defer g.Close()
			fakeStoragePeer(t, g, func(c net.Conn) {
				br := bufio.NewReader(c)
				if _, e := protocol.ReadStreamHeader(br); e != nil {
					return
				}
				size := int64(3)
				result := protocol.StorageResult{V: protocol.Version, OK: true, Size: &size, ETag: "etag", ContentType: "text/plain", LastModified: "2026-10-11T00:00:00Z"}
				if e := protocol.WriteStorageResult(c, result, "get"); e != nil {
					return
				}
				data := []byte("abc")
				if mode == "long_body" {
					data = []byte("abcd")
				}
				if e := protocol.WriteStorageFrame(c, data); e != nil {
					return
				}
				if mode == "missing_marker" {
					return
				}
				if e := protocol.WriteStorageFrame(c, nil); e != nil {
					return
				}
				if mode == "missing_trailer" {
					return
				}
				var wire bytes.Buffer
				w := protocol.NewStorageBodyWriter(&wire, 100)
				w.Write(data)
				w.Finish()
				_, _ = protocol.ReadStorageFrame(&wire)
				_, _ = protocol.ReadStorageFrame(&wire)
				trailer, _ := protocol.ReadStorageTrailer(bufio.NewReader(&wire))
				if mode == "wrong_hash" {
					trailer.SHA256 = hex.EncodeToString(make([]byte, 32))
				}
				protocol.WriteStorageTrailer(c, trailer)
				if mode == "trailing_byte" {
					c.Write([]byte("x"))
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, body, e := g.Storage(ctx, protocol.StreamHeader{Service: "files", Op: "get", Key: protocol.EncodeStorageKey("a")}, nil)
			if e != nil {
				t.Fatal(e)
			}
			defer body.Close()
			b, e := io.ReadAll(body)
			if mode == "valid" {
				if e != nil || string(b) != "abc" {
					t.Fatal(string(b), e)
				}
			} else if e == nil {
				t.Fatalf("%s ended cleanly with%q", mode, b)
			}
		})
	}
}
func TestStorageGatewayPutHalfClose(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	g, _ := New(testToken, key)
	defer g.Close()
	verified := make(chan bool, 1)
	fakeStoragePeer(t, g, func(c net.Conn) {
		br := bufio.NewReader(c)
		h, e := protocol.ReadStreamHeader(br)
		if e != nil {
			verified <- false
			return
		}
		reader := protocol.NewStorageBodyReader(br, *h.Size, 100, c.SetReadDeadline)
		data, e := io.ReadAll(reader)
		valid := e == nil && reader.Verified() && string(data) == "abc"
		verified <- valid
		if !valid {
			return
		}
		n := int64(len(data))
		protocol.WriteStorageResult(c, protocol.StorageResult{V: protocol.Version, OK: true, Size: &n, ETag: "etag"}, "put")
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	size := int64(3)
	result, body, e := g.Storage(ctx, protocol.StreamHeader{Service: "files", Op: "put", Key: protocol.EncodeStorageKey("a"), Size: &size}, strings.NewReader("abc"))
	if e != nil || !result.OK || body != nil {
		t.Fatal(result, e)
	}
	if !<-verified {
		t.Fatal("put returned before verified FIN")
	}
}

func TestStorageFINDeadlineSurvivesIdleWrapper(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &idleConn{Conn: a, idle: time.Hour}
	c.SetReadDeadline(time.Now().Add(15 * time.Millisecond))
	start := time.Now()
	var buf [1]byte
	_, e := c.Read(buf[:])
	if e == nil || time.Since(start) > time.Second {
		t.Fatalf("explicit FIN deadline overwritten:%v", e)
	}
}
