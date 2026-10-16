package testgateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

// fakeS3 is a minimal in-memory S3 double: bucket probes, list, put, head, get.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/files-bucket")
	switch {
	case r.Method == "HEAD" && (path == "" || path == "/"):
		w.WriteHeader(200)
	case r.Method == "GET" && r.URL.Query().Has("uploads"):
		io.WriteString(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
	case r.Method == "GET" && r.URL.Query().Has("list-type"):
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
	case r.Method == "PUT":
		b, _ := io.ReadAll(r.Body)
		f.objects[path] = b
		w.Header().Set("ETag", `"etag-1"`)
	case r.Method == "HEAD" || r.Method == "GET":
		b, ok := f.objects[path]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			if r.Method == "GET" {
				io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			}
			return
		}
		w.Header().Set("ETag", `"etag-1"`)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Last-Modified", time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if r.Method == "GET" {
			w.Write(b)
		}
	default:
		w.WriteHeader(500)
	}
}

// echoServer answers every request with "<METHOD> <path> model=<model>",
// serves /health, echoes WebSockets, and holds requests for model "block" until released.
type echoServer struct {
	release chan struct{}
	mu      sync.Mutex
	seen    []string
}

func (e *echoServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		return
	}
	if websocket.IsWebSocketUpgrade(r) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			typ, b, err := ws.ReadMessage()
			if err != nil || ws.WriteMessage(typ, append([]byte(r.URL.Path+":"), b...)) != nil {
				return
			}
		}
	}
	model := ""
	body, _ := io.ReadAll(r.Body)
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		_, params, _ := strings.Cut(r.Header.Get("Content-Type"), "boundary=")
		if form, err := multipart.NewReader(bytes.NewReader(body), params).ReadForm(1 << 20); err == nil && len(form.Value["model"]) == 1 {
			model = form.Value["model"][0]
		}
		_ = params
	} else {
		var m struct{ Model string }
		json.Unmarshal(body, &m)
		model = m.Model
	}
	line := r.Method + " " + r.URL.Path + " model=" + model
	e.mu.Lock()
	e.seen = append(e.seen, line)
	e.mu.Unlock()
	if model == "block" {
		<-e.release
	}
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, line)
}

const acceptanceServices = `chat:
  - name: chat
    url: %[1]s
    models:
      chat-large: server/chat-large-v2
      chat-small: chat-small
  - name: tiny
    url: %[1]s
    models: any
    max_concurrent_requests: 1
anthropic:
  - name: claude
    url: %[1]s
    models: [claude-local]
embeddings:
  - name: embed
    url: %[1]s
    api_path: /
    models: {nomic-embed: nomic-ai/nomic-embed-text-v1.5}
rerank:
  - name: rerank
    url: %[1]s
    models: [jina-reranker]
transcription:
  - name: stt
    url: %[1]s
    models: {whisper: /models/whisper-large}
speech:
  - name: tts
    url: %[1]s
    models: [kokoro]
image:
  - name: image
    url: %[1]s
    models: [image-gen]
document:
  - name: ocr
    url: %[1]s
conversion:
  - name: convert
    url: %[1]s
realtime:
  - name: voice
    url: %[1]s
storage:
  - name: files
    endpoint: %[2]s
    bucket: files-bucket
    prefix: org/
    access_key: placeholder-access
    secret_key: placeholder-secret
    encryption_key: MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=
    ca_certificate: |
%[3]s
custom:
  - name: tool
    url: %[1]s
    paths: ["POST /v1/run"]
`

func multipartBody(model string) (string, string) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	if model != "" {
		w.WriteField("model", model)
	}
	f, _ := w.CreateFormFile("file", "a.wav")
	f.Write([]byte("RIFF-audio"))
	w.Close()
	return b.String(), w.FormDataContentType()
}

// TestAcceptanceEveryTypeOverV2Session: one service of every type works
// through a v2 session with the test gateway, including storage and a
// realtime WebSocket; refusals and per-service limits behave as specified.
func TestAcceptanceEveryTypeOverV2Session(t *testing.T) {
	echo := &echoServer{release: make(chan struct{})}
	up := httptest.NewServer(echo)
	defer up.Close()
	s3 := httptest.NewTLSServer(&fakeS3{objects: map[string][]byte{}})
	defer s3.Close()
	certPEM := strings.TrimSpace(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s3.Certificate().Raw})))
	indented := "      " + strings.ReplaceAll(certPEM, "\n", "\n      ")
	rig := newRig(t, acceptanceServices, up.URL, s3.URL, indented)

	// hello advertised every service with its type, models and limit.
	byName := map[string]protocol.Service{}
	for _, s := range rig.g.Services() {
		byName[s.Name] = s
	}
	if len(byName) != 13 {
		t.Fatalf("advertised %d services", len(byName))
	}
	if c := byName["chat"]; c.Type != "chat" || strings.Join(c.Models, ",") != "chat-large,chat-small" || c.ModelsAny || c.MaxConcurrentRequests != 32 {
		t.Fatalf("%+v", c)
	}
	if c := byName["tiny"]; !c.ModelsAny || c.MaxConcurrentRequests != 1 {
		t.Fatalf("%+v", c)
	}
	if f := byName["files"]; f.Type != "storage" || f.Storage == nil || f.Storage.Encryption != "sse-c" {
		t.Fatalf("%+v", f)
	}

	audio, audioType := multipartBody("whisper")
	image, imageType := multipartBody("image-gen")
	for _, tc := range []struct{ service, method, path, ct, body, want string }{
		{"chat", "POST", "/v1/chat/completions", "application/json", `{"model":"chat-large"}`, "POST /v1/chat/completions model=server/chat-large-v2"},
		{"chat", "POST", "/v1/responses", "application/json", `{"model":"chat-small"}`, "POST /v1/responses model=chat-small"},
		{"claude", "POST", "/v1/messages", "application/json", `{"max_tokens":1}`, "POST /v1/messages model=claude-local"},
		{"embed", "POST", "/v1/embeddings", "application/json", `{"model":"nomic-embed","input":"x"}`, "POST /embeddings model=nomic-ai/nomic-embed-text-v1.5"},
		{"rerank", "POST", "/v1/rerank", "application/json", `{"model":"jina-reranker"}`, "POST /v1/rerank model=jina-reranker"},
		{"stt", "POST", "/v1/audio/transcriptions", audioType, audio, "POST /v1/audio/transcriptions model=/models/whisper-large"},
		{"tts", "POST", "/v1/audio/speech", "application/json", `{"model":"kokoro"}`, "POST /v1/audio/speech model=kokoro"},
		{"tts", "GET", "/v1/audio/voices", "", "", "GET /v1/audio/voices model="},
		{"image", "POST", "/v1/images/edits", imageType, image, "POST /v1/images/edits model=image-gen"},
		{"ocr", "POST", "/layout-parsing", "application/json", `{"file":"x"}`, "POST /layout-parsing model="},
		{"convert", "POST", "/convert", "application/pdf", "%PDF", "POST /convert model="},
		{"voice", "GET", "/v1/config", "", "", "GET /v1/config model="},
		{"tool", "POST", "/v1/run", "application/json", "{}", "POST /v1/run model="},
	} {
		resp, body := requestTo(t, rig.g, tc.service, tc.method, tc.path, tc.ct, tc.body)
		if resp.StatusCode != 200 || string(body) != tc.want {
			t.Errorf("%s %s %s: %d %q, want %q", tc.service, tc.method, tc.path, resp.StatusCode, body, tc.want)
		}
	}
	// Allowlisted model list answered locally.
	resp, body := requestTo(t, rig.g, "chat", "GET", "/v1/models", "", "")
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"id":"chat-large"`) || strings.Contains(string(body), "server/") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}

	// Refusals: model outside the allowlist, path outside the contract.
	echo.mu.Lock()
	before := len(echo.seen)
	echo.mu.Unlock()
	for _, tc := range []struct{ service, path, body, code string }{
		{"chat", "/v1/chat/completions", `{"model":"server/chat-large-v2"}`, "model_not_allowed"},
		{"chat", "/v1/chat/completions", `{"model":"gpt-unknown"}`, "model_not_allowed"},
		{"chat", "/v1/chat/completions", `{"messages":[]}`, "model_required"},
		{"chat", "/v1/completions", `{"model":"chat-large"}`, "path_not_allowed"},
		{"embed", "/embeddings", `{"model":"nomic-embed"}`, "path_not_allowed"},
		{"tool", "/v1/other", `{}`, "path_not_allowed"},
	} {
		resp, _ := requestTo(t, rig.g, tc.service, "POST", tc.path, "application/json", tc.body)
		if got := resp.Header.Get(protocol.HeaderError); got != tc.code || resp.StatusCode != protocol.HTTPStatus(tc.code) {
			t.Errorf("%s %s %s: %d %s, want %s", tc.service, tc.path, tc.body, resp.StatusCode, got, tc.code)
		}
	}
	echo.mu.Lock()
	if len(echo.seen) != before {
		t.Errorf("refused requests reached the server: %v", echo.seen[before:])
	}
	echo.mu.Unlock()

	// Per-service max_concurrent_requests returns busy.
	held := make(chan struct{})
	go func() {
		defer close(held)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "POST", "http://connector/v1/chat/completions", strings.NewReader(`{"model":"block"}`))
		req.Header.Set("Content-Type", "application/json")
		if resp, err := rig.g.RoundTrip(req, "tiny"); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		echo.mu.Lock()
		blocked := len(echo.seen) > before
		echo.mu.Unlock()
		if blocked || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	resp, _ = requestTo(t, rig.g, "tiny", "POST", "/v1/chat/completions", "application/json", `{"model":"anything"}`)
	if resp.StatusCode != 429 || resp.Header.Get(protocol.HeaderError) != "busy" {
		t.Errorf("second request on a 1-slot service: %d %s", resp.StatusCode, resp.Header.Get(protocol.HeaderError))
	}
	// Another service stays available meanwhile.
	if resp, _ := requestTo(t, rig.g, "chat", "POST", "/v1/chat/completions", "application/json", `{"model":"chat-small"}`); resp.StatusCode != 200 {
		t.Errorf("busy service blocked another: %d", resp.StatusCode)
	}
	close(echo.release)
	<-held

	// Realtime WebSocket through the session.
	d := websocket.Dialer{TLSClientConfig: rig.server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}
	ws, _, err := d.Dial("wss"+strings.TrimPrefix(rig.server.URL, "https")+"/services/voice/v1/stream", http.Header{"Authorization": {"Bearer " + testToken}})
	if err != nil {
		t.Fatal(err)
	}
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	ws.WriteMessage(websocket.BinaryMessage, []byte("pcm"))
	if _, b, err := ws.ReadMessage(); err != nil || string(b) != "/v1/stream:pcm" {
		t.Fatalf("websocket echo %q %v", b, err)
	}
	ws.Close()

	// Storage: put, head and get through the typed storage stream.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	size := int64(len("hello storage"))
	key := protocol.EncodeStorageKey("ws/notes.txt")
	if r, _, err := rig.g.Storage(ctx, protocol.StreamHeader{Service: "files", Op: "put", Key: key, Size: &size, ContentType: "text/plain"}, strings.NewReader("hello storage")); err != nil || !r.OK {
		t.Fatalf("put %+v %v", r, err)
	}
	if r, _, err := rig.g.Storage(ctx, protocol.StreamHeader{Service: "files", Op: "head", Key: key}, nil); err != nil || *r.Size != size {
		t.Fatalf("head %+v %v", r, err)
	}
	r, got, err := rig.g.Storage(ctx, protocol.StreamHeader{Service: "files", Op: "get", Key: key}, nil)
	if err != nil {
		t.Fatal(r, err)
	}
	data, err := io.ReadAll(got)
	got.Close()
	if err != nil || string(data) != "hello storage" {
		t.Fatalf("get %q %v", data, err)
	}
	if _, _, err := rig.g.Storage(ctx, protocol.StreamHeader{Service: "chat", Op: "head", Key: key}, nil); err == nil {
		t.Fatal("storage stream to a chat service accepted")
	}

	// The access log names each request's caller as the gateway reported it.
	logged, _ := os.ReadFile(rig.audit)
	types := map[string]bool{}
	for _, line := range bytes.Split(bytes.TrimSpace(logged), []byte("\n")) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) != nil {
			t.Fatalf("access line %s", line)
		}
		if entry["origin"] == "local" {
			continue // the connector's own storage checks
		}
		if entry["org_slug"] != "local-test" || entry["user_key_id"] != "1" || entry["request_id"] == "" {
			t.Fatalf("access line %s", line)
		}
		types[fmt.Sprint(entry["type"])] = true
	}
	list := []string{}
	for k := range types {
		list = append(list, k)
	}
	sort.Strings(list)
	if len(list) != 12 {
		t.Fatalf("access log covers types %v", list)
	}
}

// TestAcceptanceUnlistedKeyNeverGetsToken: a gateway signing with a key the
// connector does not list never receives an auth message.
func TestAcceptanceUnlistedKeyNeverGetsToken(t *testing.T) {
	_, impostor, _ := ed25519.GenerateKey(rand.Reader)
	configured, _, _ := ed25519.GenerateKey(rand.Reader)
	rig := newRigWithKeys(t, impostor, configured, "chat:\n  - name: c\n    url: http://127.0.0.1:1\n    models: any\n")
	time.Sleep(1500 * time.Millisecond) // at least one connection attempt and retry
	rig.g.mu.Lock()
	sessions := len(rig.g.sessions)
	rig.g.mu.Unlock()
	if n := rig.g.AuthAttempts.Load(); n != 0 || sessions != 0 {
		t.Fatalf("auth attempts %d, sessions %d", n, sessions)
	}
}
