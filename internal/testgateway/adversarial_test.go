package testgateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

const adversarialChat = "  test:\n    address: %s\n    allowed-endpoints: [POST /v1/chat/completions, POST /v1/responses, GET /v1/models]\n"

func TestAdversarialPathsNeverContactUpstream(t *testing.T) {
	var hits atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(200) }))
	defer up.Close()
	rig := newRig(t, adversarialChat, up.URL)
	for _, path := range []string{"/admin", "/v1/../admin", "/v1//chat/completions", "/v1/%2e%2e/admin", "/v1%2fchat/completions", "/v1/chat/completions?url=http://evil", "/" + strings.Repeat("a", 257)} {
		resp, _ := request(t, rig.g, "POST", path, "application/json", "{}")
		if resp.StatusCode != 403 {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("denied requests contacted upstream %d times", hits.Load())
	}
}
func TestAdversarialHeadersAndRedirect(t *testing.T) {
	seen := make(chan http.Header, 1)
	var redirected atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer other.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Set("Location", other.URL)
		w.WriteHeader(302)
	}))
	defer up.Close()
	rig := newRig(t, adversarialChat+"    api-key: local\n", up.URL)
	req, _ := http.NewRequest("POST", "http://connector/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	for _, h := range []string{"Authorization", "Cookie", "X-Forwarded-For", "FileUrl"} {
		req.Header.Set(h, "malicious")
	}
	req.Header.Set("Connection", "Cookie")
	req.Host = "attacker"
	resp, e := rig.g.RoundTrip(req, "test")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 302 {
		t.Fatal(resp.StatusCode)
	}
	h := <-seen
	if h.Get("Authorization") != "Bearer local" || h.Get("Cookie") != "" || h.Get("X-Forwarded-For") != "" || h.Get("FileUrl") != "" {
		t.Fatal(h)
	}
	if redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
}
func TestAdversarialConcurrencyAndCancelBeforeHeaders(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer up.Close()
	rig := newRig(t, adversarialChat+"    max-concurrent-requests: 1\n    idle-timeout: 2s\n", up.URL)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://connector/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	finished := make(chan error, 1)
	go func() {
		r, e := rig.g.RoundTrip(req, "test")
		if r != nil {
			r.Body.Close()
		}
		finished <- e
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request never began")
	}
	resp, _ := request(t, rig.g, "POST", "/v1/chat/completions", "application/json", "{}")
	if resp.StatusCode != 429 {
		t.Fatal(resp.StatusCode)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(700 * time.Millisecond):
		t.Error("cancel before headers did not stop upstream")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Error("gateway did not finish")
	}
}
func TestAdversarialActiveSSEHasNoTotalCap(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 12; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer up.Close()
	rig := newRig(t, adversarialChat+"    idle-timeout: 120ms\n", up.URL)
	resp, b := request(t, rig.g, "POST", "/v1/chat/completions", "application/json", "{}")
	if resp.StatusCode != 200 || !strings.Contains(string(b), "data: 11") {
		t.Fatalf("stream cut%d %q", resp.StatusCode, b)
	}
}
func TestAdversarialOneWayWebSocketActivity(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer ws.Close()
		for i := 0; i < 12; i++ {
			if ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprint(i))) != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer up.Close()
	rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: [WEBSOCKET /v1/stream, GET /v1/config]\n    idle-timeout: 120ms\n", up.URL)
	d := websocket.Dialer{TLSClientConfig: rig.server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}
	ws, _, e := d.Dial("wss"+strings.TrimPrefix(rig.server.URL, "https")+"/services/test/v1/stream", http.Header{"Authorization": {"Bearer " + testToken}})
	if e != nil {
		t.Fatal(e)
	}
	defer ws.Close()
	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 12; i++ {
		_, b, e := ws.ReadMessage()
		if e != nil || string(b) != fmt.Sprint(i) {
			t.Fatalf("message%d: %q %v", i, b, e)
		}
	}
}
func TestAdversarialSilentResponseHitsIdle(t *testing.T) {
	stopped := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		close(stopped)
	}))
	defer up.Close()
	rig := newRig(t, adversarialChat+"    idle-timeout: 120ms\n", up.URL)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://connector/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	started := time.Now()
	resp, e := rig.g.RoundTrip(req, "test")
	if resp != nil {
		resp.Body.Close()
	}
	if e != nil {
		t.Fatal(e)
	}
	if resp.StatusCode != 504 || resp.Header.Get(protocol.HeaderError) != "upstream_timeout" {
		t.Fatalf("idle response:%d %v", resp.StatusCode, resp.Header)
	}
	if time.Since(started) > 700*time.Millisecond {
		t.Fatal("idle timeout not enforced")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("idle did not cancel upstream")
	}
}
