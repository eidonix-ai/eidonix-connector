package connector

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeniedRequestsBeforeDial(t *testing.T) {
	cfg := &config.Config{Server: "https://example.com", TokenFile: "/tmp/token", ServerKeys: map[string]string{"test": base64.StdEncoding.EncodeToString(make([]byte, 32))}, Routes: []config.Route{{ID: "chat", Type: "openai_chat", Upstreams: []string{"http://127.0.0.1:1"}, MaxRequestBytes: "4"}}}
	x, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer x.Close()
	for _, tc := range []struct {
		path, ct string
		size     int
		status   int
	}{{"/admin", "application/json", 0, 403}, {"/v1/../admin", "application/json", 0, 403}, {"/v1//chat/completions", "application/json", 0, 403}, {"/v1/chat/completions?fetch=bad", "application/json", 0, 403}, {"/v1/chat/completions", "text/plain", 0, 403}, {"/v1/chat/completions", "application/json", 5, 413}} {
		t.Run(tc.path+tc.ct, func(t *testing.T) {
			a, b := net.Pipe()
			defer b.Close()
			b.SetDeadline(time.Now().Add(time.Second))
			go x.ServeStream(context.Background(), a)
			go func() {
				protocol.WriteStreamHeader(b, protocol.StreamHeader{V: protocol.Version, Service: "chat", Kind: "http"})
				fmt.Fprintf(b, "POST %s HTTP/1.1\r\nHost: evil\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n", tc.path, tc.ct, tc.size)
			}()
			r, e := http.ReadResponse(bufio.NewReader(b), nil)
			if e != nil {
				t.Fatal(e)
			}
			r.Body.Close()
			if r.StatusCode != tc.status {
				t.Fatalf("got %d expected %d", r.StatusCode, tc.status)
			}
		})
	}
}
func TestHeaderSanitizing(t *testing.T) {
	h := cleanHeaders(http.Header{"Authorization": {"secret"}, "Cookie": {"cookie"}, "Host": {"evil"}, "X-Forwarded-For": {"fake"}, "Content-Type": {"application/json"}, "Fileurl": {"http://evil"}})
	if len(h) != 1 || h.Get("Content-Type") != "application/json" {
		t.Fatalf("unsafe forwarded headers %v", h)
	}
}

type trackedListener struct{ closed bool }

func (l *trackedListener) Accept() (net.Conn, error) { return nil, fmt.Errorf("unused") }
func (l *trackedListener) Close() error              { l.closed = true; return nil }
func (l *trackedListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9877}
}
func TestReplacementReusesHealthListener(t *testing.T) {
	cfg := &config.Config{Server: "https://example.com", TokenFile: "/tmp/token", ServerKeys: map[string]string{"test": base64.StdEncoding.EncodeToString(make([]byte, 32))}, Routes: []config.Route{{ID: "chat", Type: "openai_chat", Upstreams: []string{"http://127.0.0.1:1"}}}}
	old, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	l := &trackedListener{}
	old.listener = l
	old.healthAddress = "127.0.0.1:9877"
	old.healthTarget = &atomic.Pointer[Connector]{}
	old.healthTarget.Store(old)
	cfg.HealthListen = old.healthAddress
	next, e := NewReplacement(cfg, old)
	if e != nil {
		old.Close()
		t.Fatal(e)
	}
	old.Close()
	if l.closed || next.healthTarget.Load() != next {
		t.Fatal("listener closed or health target unchanged")
	}
	next.Close()
	if !l.closed {
		t.Fatal("new connector leaked listener")
	}
}

func TestHealthAuthDiscoveryAndRecovery(t *testing.T) {
	var fail atomic.Bool
	var unauthorized atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-secret" {
			unauthorized.Store(true)
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/health" {
			if fail.Load() {
				w.WriteHeader(503)
			}
			return
		}
		if r.URL.Path == "/v1/models" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"data":[{"id":"test-model"}]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	cfg := &config.Config{Server: "https://example.com", TokenFile: "/tmp/token", ServerKeys: map[string]string{"test": base64.StdEncoding.EncodeToString(make([]byte, 32))}, Routes: []config.Route{{ID: "chat", Type: "openai_chat", Upstreams: []string{server.URL}, Health: config.Health{Path: "/health", Interval: "1s"}, UpstreamHeaders: map[string]string{"Authorization": "Bearer local-secret"}}}}
	x, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer x.Close()
	wait := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("health state did not converge")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(func() bool { s := x.StatusServices()[0]; return len(s.Models) == 1 && s.Models[0] == "test-model" })
	fail.Store(true)
	wait(func() bool { return !x.StatusServices()[0].Healthy })
	fail.Store(false)
	wait(func() bool { return x.StatusServices()[0].Healthy })
	if unauthorized.Load() {
		t.Fatal("health or model discovery omitted local auth")
	}
}
