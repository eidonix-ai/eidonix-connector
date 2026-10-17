package testgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/connector"
	"github.com/eidonix-ai/eidonix-connector/internal/session"
	"github.com/gorilla/websocket"
)

// Synthetic all-zero credential used only by local tests.
const testToken = "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type rig struct {
	g      *Gateway
	server *httptest.Server
	audit  string
}

// Synthetic placeholder key text; the rig replaces it with the real test key.
const upstreamTemplate = `site: local-test
upstream:
  url: https://127.0.0.1
  token: ` + testToken + `
  public-keys: [%s]
services:
`

// newRig starts a test gateway and a connector session for services (YAML,
// %s replaced by each origin argument in order; http:// origins become
// host:port addresses).
func newRig(t *testing.T, services string, origins ...any) *rig {
	t.Helper()
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	return newRigWithKeys(t, key, pub, services, origins...)
}
func newRigWithKeys(t *testing.T, key ed25519.PrivateKey, configured ed25519.PublicKey, services string, origins ...any) *rig {
	t.Helper()
	g, e := New(testToken, key)
	if e != nil {
		t.Fatal(e)
	}
	g.Site = "local-test"
	for i, o := range origins {
		if s, ok := o.(string); ok {
			origins[i] = strings.TrimPrefix(s, "http://")
		}
	}
	server := httptest.NewTLSServer(g)
	audit := filepath.Join(t.TempDir(), "access.log")
	cfg, e := config.Parse([]byte(fmt.Sprintf(upstreamTemplate, base64.StdEncoding.EncodeToString(configured)) + fmt.Sprintf(services, origins...)))
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	cfg.AccessLog = audit
	c, e := connector.New(cfg)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	go func() {
		done <- session.RunOptions(ctx, session.Options{Server: "https://127.0.0.1", Token: testToken, Site: cfg.Site, Keys: []ed25519.PublicKey{configured}, TLSConfig: tlsConfig, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
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
	r := &rig{g, server, audit}
	if configured.Equal(key.Public()) {
		r.waitSession(t)
	}
	return r
}
func (r *rig) waitSession(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.g.mu.Lock()
		n := len(r.g.sessions)
		r.g.mu.Unlock()
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("connector failed to register")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func request(t *testing.T, g *Gateway, method, path, ct, data string) (*http.Response, []byte) {
	return requestTo(t, g, "test", method, path, ct, data)
}
func requestTo(t *testing.T, g *Gateway, service, method, path, ct, data string) (*http.Response, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, method, "http://connector"+path, strings.NewReader(data))
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Content-Type", ct)
	resp, e := g.RoundTrip(req, service)
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

// TestEveryServiceShapeOverTLSYamux: the requests Eidonix sends to each kind
// of server pass through a v3 session unchanged (same path, same body).
func TestEveryServiceShapeOverTLSYamux(t *testing.T) {
	cases := []struct{ endpoints, method, path, ct string }{
		{"[POST /v1/*, GET /v1/models]", "POST", "/v1/chat/completions", "application/json"},
		{"[POST /v1/*, GET /v1/models]", "POST", "/v1/responses", "application/json"},
		{"[POST /v1/*, GET /v1/models]", "GET", "/v1/models", ""},
		{"[POST /v1/messages, POST /v1/messages/count_tokens]", "POST", "/v1/messages", "application/json"},
		{"[POST /embeddings]", "POST", "/embeddings", "application/json"},
		{"[POST /v1/rerank]", "POST", "/v1/rerank", "application/json"},
		{"[POST /v1/audio/transcriptions]", "POST", "/v1/audio/transcriptions", "multipart/form-data; boundary=abc"},
		{"[POST /v1/audio/speech, GET /v1/audio/voices]", "POST", "/v1/audio/speech", "application/json"},
		{"[POST /v1/audio/speech, GET /v1/audio/voices]", "GET", "/v1/audio/voices", ""},
		{"[POST /v1/images/*]", "POST", "/v1/images/edits", "multipart/form-data; boundary=abc"},
		{"[POST /layout-parsing]", "POST", "/layout-parsing", "application/json"},
		{"[POST /convert, POST /extract]", "POST", "/convert", "application/octet-stream"},
		{"[POST /convert, POST /extract]", "POST", "/extract", "application/pdf"},
		{"[WEBSOCKET /v1/stream, GET /v1/config]", "GET", "/v1/config", ""},
		{"[POST /custom]", "POST", "/custom", "text/plain"},
		{"[PUT /v1/state]", "PUT", "/v1/state", "application/json"},
		{"[DELETE /v1/jobs/*]", "DELETE", "/v1/jobs/7", ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			seen := make(chan string, 1)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					return
				}
				b, _ := io.ReadAll(r.Body)
				seen <- r.Method + " " + r.URL.Path + " " + string(b)
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write([]byte{0, 1, 2, 255})
			}))
			defer up.Close()
			rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: "+tc.endpoints+"\n", up.URL)
			data := "sentinel-prompt"
			if tc.method == "GET" || tc.method == "DELETE" {
				data = ""
			}
			resp, body := request(t, rig.g, tc.method, tc.path, tc.ct, data)
			if resp.StatusCode != 200 || !bytes.Equal(body, []byte{0, 1, 2, 255}) {
				t.Fatalf("response %d %q", resp.StatusCode, body)
			}
			if got := <-seen; got != tc.method+" "+tc.path+" "+data {
				t.Fatal(got)
			}
			deadline := time.Now().Add(time.Second)
			for {
				b, _ := os.ReadFile(rig.audit)
				if len(b) > 0 {
					if bytes.Contains(b, []byte("sentinel-prompt")) {
						t.Fatal("access log leaked request body")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no access log entry")
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
	rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: [POST /v1/chat/completions, POST /v1/responses, GET /v1/models]\n    idle-timeout: 2s\n", up.URL)
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
	rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: [WEBSOCKET /v1/stream, GET /v1/config]\n", up.URL)
	d := websocket.Dialer{TLSClientConfig: rig.server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}
	ws, resp, e := d.Dial("wss"+strings.TrimPrefix(rig.server.URL, "https")+"/services/test/v1/stream", http.Header{"Authorization": {"Bearer " + testToken}})
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
	g, _ := New(testToken, key)
	defer g.Close()
	for _, path := range []string{"/status", "/services/test/v1/models"} {
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
	rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: [POST /layout-parsing]\n", up.URL)
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
	value := os.Getenv("EIDONIX_SOAK_DURATION")
	if value == "" {
		t.Skip("set EIDONIX_SOAK_DURATION=45m for release soak")
	}
	duration, e := time.ParseDuration(value)
	if e != nil || duration <= 0 {
		t.Fatal("invalid EIDONIX_SOAK_DURATION")
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
	rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: [POST /v1/chat/completions, POST /v1/responses, GET /v1/models]\n    idle-timeout: 1s\n", up.URL)
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
	rig := newRig(t, "  test:\n    address: %s\n    allowed-endpoints: [POST /v1/chat/completions, POST /v1/responses, GET /v1/models]\n", up.URL)
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
