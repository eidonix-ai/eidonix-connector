package testgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/cletus-connector/internal/config"
	"github.com/eidonix-ai/cletus-connector/internal/connector"
	"github.com/eidonix-ai/cletus-connector/internal/session"
	"github.com/gorilla/websocket"
)

// Synthetic all-zero credential used only by local tests.
const testToken = "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type rig struct {
	g      *Gateway
	server *httptest.Server
	audit  string
}

func newRig(t *testing.T, routes []config.Route) *rig {
	t.Helper()
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	g, e := New(testToken, "test", key)
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewTLSServer(g)
	audit := filepath.Join(t.TempDir(), "audit.jsonl")
	c, e := connector.New(&config.Config{Server: "https://127.0.0.1", TokenFile: "unused", ServerKeys: map[string]string{"test": base64.StdEncoding.EncodeToString(pub)}, AuditLog: audit, Routes: routes})
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	go func() {
		done <- session.RunOptions(ctx, session.Options{Server: "https://127.0.0.1", Token: testToken, Keys: map[string]ed25519.PublicKey{"test": pub}, TLSConfig: tlsConfig, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		}}, c)
	}()
	t.Cleanup(func() {
		cancel()
		g.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("session did not shut down")
		}
		c.Close()
		server.Close()
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		g.mu.Lock()
		n := len(g.sessions)
		g.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connector failed to register")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return &rig{g, server, audit}
}
func request(t *testing.T, g *Gateway, method, path, ct, data string) (*http.Response, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, method, "http://connector"+path, strings.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Content-Type", ct)
	resp, e := g.RoundTrip(req, "test")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(resp.Body)
	if e != nil {
		t.Fatal(e)
	}
	return resp, body
}
func TestEveryProfileOverTLSYamux(t *testing.T) {
	cases := []struct{ profile, flavor, method, path, ct string }{
		{"openai_chat", "", "POST", "/v1/chat/completions", "application/json"},
		{"openai_chat", "vllm", "POST", "/v1/responses", "application/json"},
		{"openai_embeddings", "", "POST", "/v1/embeddings", "application/json"},
		{"anthropic_messages", "", "POST", "/v1/messages", "application/json"},
		{"rerank", "", "POST", "/rerank", "application/json"},
		{"audio_transcription", "", "POST", "/v1/audio/transcriptions", "multipart/form-data; boundary=abc"},
		{"audio_speech", "", "POST", "/v1/audio/speech", "application/json"},
		{"image", "", "POST", "/v1/images/edits", "multipart/form-data; boundary=abc"},
		{"document_ocr", "", "POST", "/layout-parsing", "application/json"},
		{"document_conversion", "gotenberg", "POST", "/forms/libreoffice/convert", "multipart/form-data; boundary=abc"},
		{"document_conversion", "tika", "PUT", "/tika", "application/octet-stream"},
		{"http", "", "POST", "/custom", "text/plain"},
	}
	for _, tc := range cases {
		t.Run(tc.profile+tc.flavor, func(t *testing.T) {
			seen := make(chan string, 1)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				seen <- r.Method + " " + r.URL.Path + " " + string(b)
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write([]byte{0, 1, 2, 255})
			}))
			defer up.Close()
			route := config.Route{ID: "test", Type: tc.profile, Flavor: tc.flavor, Upstreams: []string{up.URL}, Health: config.Health{Interval: "1h"}}
			if tc.profile == "http" {
				route.MethodsPaths = [][2]string{{"POST", "/custom"}}
				route.ContentTypes = []string{"text/plain"}
			}
			rig := newRig(t, []config.Route{route})
			resp, body := request(t, rig.g, tc.method, tc.path, tc.ct, "sentinel-prompt")
			if resp.StatusCode != 200 || !bytes.Equal(body, []byte{0, 1, 2, 255}) {
				t.Fatalf("response %d %q", resp.StatusCode, body)
			}
			if got := <-seen; got != tc.method+" "+tc.path+" sentinel-prompt" {
				t.Fatal(got)
			}
			deadline := time.Now().Add(time.Second)
			for {
				b, _ := os.ReadFile(rig.audit)
				if len(b) > 0 {
					if bytes.Contains(b, []byte("sentinel-prompt")) {
						t.Fatal("audit leaked request body")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no audit entry")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
func TestSSEFlushAndCancellation(t *testing.T) {
	stopped := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(stopped)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	rig := newRig(t, []config.Route{{ID: "test", Type: "openai_chat", Upstreams: []string{up.URL}, IdleTimeout: "2s", Health: config.Health{Interval: "1h"}}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://connector/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	resp, e := rig.g.RoundTrip(req, "test")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	line, e := bufio.NewReader(resp.Body).ReadString('\n')
	if e != nil || line != "data: first\n" {
		t.Fatalf("SSE first event %q: %v", line, e)
	}
	cancel()
	resp.Body.Close()
	select {
	case <-stopped:
	case <-time.After(700 * time.Millisecond):
		t.Fatal("stream cancellation did not cancel upstream promptly")
	}
}
func TestWebSocketRelay(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer ws.Close()
		for {
			typ, b, e := ws.ReadMessage()
			if e != nil {
				return
			}
			if ws.WriteMessage(typ, b) != nil {
				return
			}
		}
	}))
	defer up.Close()
	rig := newRig(t, []config.Route{{ID: "test", Type: "realtime_ws", Upstreams: []string{up.URL}, WebsocketPaths: []string{"/stream"}, Health: config.Health{Interval: "1h"}}})
	d := websocket.Dialer{TLSClientConfig: rig.server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}
	ws, resp, e := d.Dial("wss"+strings.TrimPrefix(rig.server.URL, "https")+"/routes/test/stream", http.Header{"Authorization": {"Bearer " + testToken}})
	if e != nil {
		t.Fatalf("dial %v response %v", e, resp)
	}
	defer ws.Close()
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	for _, typ := range []int{websocket.TextMessage, websocket.BinaryMessage} {
		payload := []byte("voice data")
		if e = ws.WriteMessage(typ, payload); e != nil {
			t.Fatal(e)
		}
		got, b, e := ws.ReadMessage()
		if e != nil || got != typ || !bytes.Equal(b, payload) {
			t.Fatalf("echo %d %q %v", got, b, e)
		}
	}
}
func TestGatewayRequiresToken(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	g, _ := New(testToken, "test", key)
	defer g.Close()
	for _, path := range []string{"/status", "/v1/connectors/session", "/routes/test/v1/models"} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 401 {
			t.Fatalf("%s accepted anonymous access", path)
		}
	}
}

func TestLargeOCRUpload(t *testing.T) {
	const size = 40 << 20
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, e := io.Copy(io.Discard, r.Body)
		if e != nil || n != size {
			t.Errorf("OCR upload bytes=%d error=%v", n, e)
			http.Error(w, "upload", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	}))
	defer up.Close()
	rig := newRig(t, []config.Route{{ID: "test", Type: "document_ocr", Upstreams: []string{up.URL}, Health: config.Health{Interval: "1h"}}})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://connector/layout-parsing", io.LimitReader(zeroReader{}, size))
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/json")
	resp, e := rig.g.RoundTrip(req, "test")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil || resp.StatusCode != 200 {
		t.Fatalf("OCR response %d %q %v", resp.StatusCode, b, e)
	}
}

type zeroReader struct{}

func (zeroReader) Read(b []byte) (int, error) { clear(b); return len(b), nil }

func TestLongSSESoak(t *testing.T) {
	value := os.Getenv("CLETUS_SOAK_DURATION")
	if value == "" {
		t.Skip("set CLETUS_SOAK_DURATION=45m for release soak")
	}
	duration, e := time.ParseDuration(value)
	if e != nil || duration <= 0 {
		t.Fatal("invalid CLETUS_SOAK_DURATION")
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		until := time.NewTimer(duration)
		defer until.Stop()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-until.C:
				io.WriteString(w, "data: [DONE]\n\n")
				return
			case <-tick.C:
				io.WriteString(w, "data: still running\n\n")
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer up.Close()
	rig := newRig(t, []config.Route{{ID: "test", Type: "openai_chat", Upstreams: []string{up.URL}, IdleTimeout: "1s", Health: config.Health{Interval: "1h"}}})
	req, _ := http.NewRequest("POST", "http://connector/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	started := time.Now()
	resp, e := rig.g.RoundTrip(req, "test")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil || !bytes.Contains(b, []byte("[DONE]")) || time.Since(started) < duration {
		t.Fatalf("soak ended early after %s: %v", time.Since(started), e)
	}
}

func TestReconnectAfterNetworkLoss(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	}))
	defer up.Close()
	rig := newRig(t, []config.Route{{ID: "test", Type: "openai_chat", Upstreams: []string{up.URL}, Health: config.Health{Interval: "1h"}}})
	rig.g.mu.Lock()
	var first *peer
	for p := range rig.g.sessions {
		first = p
		break
	}
	rig.g.mu.Unlock()
	if first == nil {
		t.Fatal("missing initial session")
	}
	first.mux.Close()
	deadline := time.Now().Add(4 * time.Second)
	for {
		rig.g.mu.Lock()
		ready := false
		for p := range rig.g.sessions {
			if p != first {
				ready = true
			}
		}
		rig.g.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connector did not reconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	resp, b := request(t, rig.g, "POST", "/v1/chat/completions", "application/json", "{}")
	if resp.StatusCode != 200 || string(b) != `{"ok":true}` {
		t.Fatalf("after reconnect: %d %q", resp.StatusCode, b)
	}
}
