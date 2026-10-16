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

	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func TestDeniedRequestsBeforeDial(t *testing.T) {
	x := newConnector(t, chatConfig(t, "http://127.0.0.1:1"))
	x.services["chat"].maxRequest = 4
	for _, tc := range []struct {
		path, ct string
		size     int
		status   int
	}{{"/admin", "application/json", 0, 403}, {"/v1/../admin", "application/json", 0, 403}, {"/v1//chat/completions", "application/json", 0, 403}, {"/v1/chat/completions?fetch=bad", "application/json", 0, 403}, {"/v1/completions", "application/json", 0, 403}, {"/v1/score", "application/json", 0, 403}, {"/v1/chat/completions", "text/plain", 0, 403}, {"/v1/chat/completions", "application/json", 5, 413}} {
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
	h := cleanHeaders(http.Header{"Authorization": {"secret"}, "Cookie": {"cookie"}, "Host": {"evil"}, "X-Forwarded-For": {"fake"}, "Content-Type": {"application/json"}, "Fileurl": {"http://evil"}, "Anthropic-Version": {"2023-06-01"}}, nil)
	if len(h) != 1 || h.Get("Content-Type") != "application/json" {
		t.Fatalf("unsafe forwarded headers %v", h)
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
			} else {
				w.WriteHeader(404) // any status below 500 counts as healthy
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
	healthInterval = 50 * time.Millisecond
	defer func() { healthInterval = servicetype.HealthInterval }()
	cfg := testConfig(t, "chat:\n  - name: chat\n    url: %s\n    models: any\n    api_key: local-secret\n", server.URL)
	x := newConnector(t, cfg)
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
