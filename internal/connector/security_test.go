package connector

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func TestDeniedRequestsBeforeDial(t *testing.T) {
	x := newConnector(t, testConfig(t, "  chat:\n    address: %s\n    allowed-endpoints: "+chatEndpoints+"\n    models: [m]\n", "http://127.0.0.1:1"))
	x.services["chat"].maxRequest = 4
	for _, tc := range []struct {
		path, ct string
		size     int
		status   int
	}{{"/admin", "application/json", 0, 403}, {"/v1/../admin", "application/json", 0, 403}, {"/v1//chat/completions", "application/json", 0, 403}, {"/v1/chat/completions?fetch=bad", "application/json", 0, 403}, {"/v1/completions", "application/json", 0, 403}, {"/v1/score", "application/json", 0, 403}, {"/v1/chat/completions", "text/plain", 0, 403}, {"/v1/chat/completions", "application/json", 5, 413}, {"/v1/", "application/json", 0, 403}, {"/v1/chat/completions/", "application/json", 0, 403}, {"/v1/chat%2fcompletions", "application/json", 0, 403}} {
		t.Run(tc.path+tc.ct, func(t *testing.T) {
			a, b := net.Pipe()
			defer b.Close()
			b.SetDeadline(time.Now().Add(time.Second))
			go x.ServeStream(context.Background(), a)
			go func() {
				protocol.WriteStreamHeader(b, protocol.StreamHeader{V: protocol.Version, Service: "chat", Kind: "http", RequestID: "r"})
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
	h := cleanHeaders(http.Header{"Authorization": {"secret"}, "Cookie": {"cookie"}, "Host": {"evil"}, "X-Forwarded-For": {"fake"}, "Content-Type": {"application/json"}, "Fileurl": {"http://evil"}, "Anthropic-Version": {"2023-06-01"}, "Anthropic-Beta": {"x"}, "X-Affinity-Key": {"9f2c"}, "Openai-Organization": {"o"}, "Proxy-Authorization": {"p"}})
	if len(h) != 4 || h.Get("Content-Type") != "application/json" || h.Get("Anthropic-Version") != "2023-06-01" || h.Get("Anthropic-Beta") != "x" || h.Get("X-Affinity-Key") != "9f2c" {
		t.Fatalf("unsafe forwarded headers %v", h)
	}
}

// TestHealthPathAuthAndRecovery: a service with health is probed with GET
// <health> and its api-key; 5xx marks it unhealthy (requests are refused
// without contacting it), any other answer healthy again.
func TestHealthPathAuthAndRecovery(t *testing.T) {
	var fail, unauthorized atomic.Bool
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-secret" {
			unauthorized.Store(true)
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/healthz" {
			probes.Add(1)
			if fail.Load() {
				w.WriteHeader(503)
			} else {
				w.WriteHeader(404) // any status below 500 counts as healthy
			}
			return
		}
		io.WriteString(w, "served")
	}))
	defer server.Close()
	healthInterval = 50 * time.Millisecond
	defer func() { healthInterval = HealthInterval }()
	x := newConnector(t, testConfig(t, "  chat:\n    address: %s\n    allowed-endpoints: "+chatEndpoints+"\n    health: /healthz\n    api-key: local-secret\n", server.URL))
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
	wait(func() bool { return probes.Load() > 0 && x.StatusServices()[0].Healthy })
	fail.Store(true)
	wait(func() bool { return !x.StatusServices()[0].Healthy })
	if r, _ := exchange(t, x, "chat", get("/v1/models")); r.StatusCode != 502 || r.Header.Get(protocol.HeaderError) != "upstream_unavailable" {
		t.Fatal(r.StatusCode, r.Header)
	}
	fail.Store(false)
	wait(func() bool { return x.StatusServices()[0].Healthy })
	if r, body := exchange(t, x, "chat", get("/v1/models")); r.StatusCode != 200 || string(body) != "served" {
		t.Fatal(r.StatusCode, string(body))
	}
	if unauthorized.Load() {
		t.Fatal("health probe omitted local auth")
	}
}

// TestReachabilityWithoutHealth: without health the service is healthy while
// its address accepts connections, and no HTTP request is made.
func TestReachabilityWithoutHealth(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
	healthInterval = 50 * time.Millisecond
	defer func() { healthInterval = HealthInterval }()
	x := newConnector(t, chatConfig(t, server.URL))
	time.Sleep(200 * time.Millisecond)
	if !x.StatusServices()[0].Healthy || requests.Load() != 0 {
		t.Fatal("reachable service unhealthy, or probed over HTTP")
	}
	server.Close()
	deadline := time.Now().Add(3 * time.Second)
	for x.StatusServices()[0].Healthy {
		if time.Now().After(deadline) {
			t.Fatal("unreachable service stayed healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
