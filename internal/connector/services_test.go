package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// fakeServer records what the server actually received.
type fakeServer struct {
	mu      sync.Mutex
	hits    atomic.Int64
	paths   []string
	body    string
	model   string
	file    []byte
	auth    string
	headers http.Header
	models  string // GET .../models response
	release chan struct{}
}

func (f *fakeServer) start(t *testing.T) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.paths = append(f.paths, r.Method+" "+r.URL.Path)
		f.auth = r.Header.Get("Authorization")
		f.headers = r.Header.Clone()
		f.mu.Unlock()
		if r.URL.Path == "/health" {
			return
		}
		f.hits.Add(1)
		if f.release != nil {
			<-f.release
		}
		switch {
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/models"):
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, f.models)
		case strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data"):
			if err := r.ParseMultipartForm(64 << 20); err != nil {
				w.WriteHeader(400)
				return
			}
			f.mu.Lock()
			f.model = r.FormValue("model")
			if file, _, err := r.FormFile("file"); err == nil {
				f.file, _ = io.ReadAll(file)
			}
			f.mu.Unlock()
			io.WriteString(w, `{"text":"ok"}`)
		default:
			data, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.body = string(data)
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true}`)
		}
	}))
	t.Cleanup(s.Close)
	return s
}
func (f *fakeServer) last() (string, string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := ""
	if len(f.paths) > 0 {
		path = f.paths[len(f.paths)-1]
	}
	return path, f.body, f.model
}

const rerankYAML = `rerank:
  - name: rr
    url: %s
    models:
      jina-reranker: jina-reranker-v3
      bge: bge
`

func TestAllowlistJSON(t *testing.T) {
	f := &fakeServer{}
	cfg := testConfig(t, rerankYAML, f.start(t).URL)
	x := newConnector(t, cfg)
	body := `{"query":"q","model":"jina-reranker","documents":["model: jina-reranker"]}`
	resp, _ := exchange(t, x, "rr", post("/v1/rerank", "application/json", body))
	_, got, _ := f.last()
	if resp.StatusCode != 200 || got != `{"query":"q","model":"jina-reranker-v3","documents":["model: jina-reranker"]}` {
		t.Fatalf("%d %s", resp.StatusCode, got)
	}
	record := lastAccess(t, cfg.AccessLog)
	if record["model"] != "jina-reranker" || record["upstream_model"] != "jina-reranker-v3" || record["status"] != float64(200) || record["org_slug"] != "example-org" || record["user_key_id"] != "42" || record["org_id"] != "7" || record["request_id"] != "req-1" || record["type"] != "rerank" {
		t.Fatalf("access %v", record)
	}
	if strings.Contains(fmt.Sprint(record), "documents") {
		t.Fatal("access log contains body content")
	}
	// Not renamed: forwarded byte for byte, no upstream_model.
	body = `{"model":  "bge" , "query":"q"}`
	exchange(t, x, "rr", post("/v1/rerank", "application/json", body))
	if _, got, _ = f.last(); got != body {
		t.Fatal(got)
	}
	if record = lastAccess(t, cfg.AccessLog); record["model"] != "bge" || record["upstream_model"] != nil {
		t.Fatal(record)
	}
}

// TestModelNotAllowed: a model outside the allowlist is refused and never forwarded.
func TestModelNotAllowed(t *testing.T) {
	f := &fakeServer{}
	cfg := testConfig(t, rerankYAML, f.start(t).URL)
	x := newConnector(t, cfg)
	for _, tc := range []struct{ body, code string }{
		{`{"model":"jina-reranker-v3","query":"q"}`, "model_not_allowed"},
		{`{"model":"secret","query":"q"}`, "model_not_allowed"},
		{`{"query":"q"}`, "model_required"},
		{`{"Model":"secret","model":"bge"}`, "invalid_request"},
		{`{"model":"bge","model":"secret"}`, "invalid_request"},
		{`{"model":"bge"`, "invalid_request"},
	} {
		resp, _ := exchange(t, x, "rr", post("/v1/rerank", "application/json", tc.body))
		if resp.Header.Get(protocol.HeaderError) != tc.code || resp.StatusCode != protocol.HTTPStatus(tc.code) {
			t.Fatalf("%s: %d %s", tc.body, resp.StatusCode, resp.Header.Get(protocol.HeaderError))
		}
		if record := lastAccess(t, cfg.AccessLog); record["error"] != tc.code {
			t.Fatal(record)
		}
	}
	if f.hits.Load() != 0 {
		t.Fatal("refused request reached the server")
	}
}
func TestSingleModelIsAdded(t *testing.T) {
	f := &fakeServer{}
	x := newConnector(t, testConfig(t, "embeddings:\n  - name: e\n    url: %s\n    models: {nomic: nomic-ai/nomic-embed-text-v1.5}\n", f.start(t).URL))
	if resp, _ := exchange(t, x, "e", post("/v1/embeddings", "application/json", `{"input":"x"}`)); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if _, got, _ := f.last(); got != `{"model":"nomic-ai/nomic-embed-text-v1.5","input":"x"}` {
		t.Fatal(got)
	}
}
func TestAllowlistRespectsRequestLimit(t *testing.T) {
	f := &fakeServer{}
	x := newConnector(t, testConfig(t, rerankYAML, f.start(t).URL))
	x.services["rr"].maxRequest = 64
	body := `{"model":"bge","query":"` + strings.Repeat("x", 100) + `"}`
	req := "POST /v1/rerank HTTP/1.1\r\nHost: ignored\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" + fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)
	if resp, _ := exchange(t, x, "rr", req); resp.StatusCode != 413 || f.hits.Load() != 0 {
		t.Fatalf("got %d, server hits %d", resp.StatusCode, f.hits.Load())
	}
}

func transcription(fields [][2]string, file []byte, fileFirst bool) string {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	writeFile := func() { fw, _ := w.CreateFormFile("file", "speech.wav"); fw.Write(file) }
	if fileFirst {
		writeFile()
	}
	for _, f := range fields {
		w.WriteField(f[0], f[1])
	}
	if !fileFirst {
		writeFile()
	}
	w.Close()
	return post("/v1/audio/transcriptions", w.FormDataContentType(), b.String())
}
func TestAllowlistMultipart(t *testing.T) {
	f := &fakeServer{}
	cfg := testConfig(t, "transcription:\n  - name: stt\n    url: %s\n    models:\n      whisper: /models/faster-whisper-large-v3-turbo\n      tiny: tiny\n", f.start(t).URL)
	x := newConnector(t, cfg)
	file := bytes.Repeat([]byte("\x00RIFFwhisper\r\n--"), 20000)
	for _, fileFirst := range []bool{false, true} {
		resp, _ := exchange(t, x, "stt", transcription([][2]string{{"model", "whisper"}, {"language", "en"}}, file, fileFirst))
		_, _, model := f.last()
		if resp.StatusCode != 200 || model != "/models/faster-whisper-large-v3-turbo" || !bytes.Equal(f.file, file) {
			t.Fatalf("status %d model %v", resp.StatusCode, model)
		}
		if record := lastAccess(t, cfg.AccessLog); record["model"] != "whisper" || record["upstream_model"] != "/models/faster-whisper-large-v3-turbo" {
			t.Fatalf("access %v", record)
		}
	}
	hits := f.hits.Load()
	for _, tc := range []struct {
		fields [][2]string
		code   string
	}{
		{[][2]string{{"model", "large-v3"}}, "model_not_allowed"},
		{[][2]string{{"language", "en"}}, "model_required"},
		{[][2]string{{"model", "tiny"}, {"model", "secret"}}, "invalid_request"},
	} {
		resp, _ := exchange(t, x, "stt", transcription(tc.fields, file, true))
		if resp.Header.Get(protocol.HeaderError) != tc.code {
			t.Fatalf("%v: %d %s", tc.fields, resp.StatusCode, resp.Header.Get(protocol.HeaderError))
		}
	}
	// The duplicate case opened the gate at "tiny"; the server saw a broken body
	// at most, never the second model. The other two never reached the server.
	if got := f.hits.Load() - hits; got > 1 {
		t.Fatalf("refused uploads reached the server %d times", got)
	}
	if _, _, model := f.last(); model == "secret" || model == "large-v3" {
		t.Fatal("refused model forwarded")
	}
}

// TestLocalModelList: an allowlisted service answers GET /v1/models itself.
func TestLocalModelList(t *testing.T) {
	f := &fakeServer{models: `{"data":[{"id":"should-not-appear"}]}`}
	x := newConnector(t, testConfig(t, rerankYAML, f.start(t).URL))
	resp, data := exchange(t, x, "rr", get("/v1/models"))
	var list struct {
		Object string
		Data   []struct{ ID, Object string }
	}
	if err := json.Unmarshal(data, &list); err != nil || resp.StatusCode != 200 || list.Object != "list" || len(list.Data) != 2 || list.Data[0].ID != "jina-reranker" || list.Data[1].ID != "bge" {
		t.Fatalf("%d %s", resp.StatusCode, data)
	}
	if f.hits.Load() != 0 {
		t.Fatal("model list fetched from the server")
	}
	if got := x.HelloServices()[0]; !reflect.DeepEqual(got.Models, []string{"jina-reranker", "bge"}) || got.ModelsAny {
		t.Fatal(got)
	}
}

// TestAPIPathAndAnyDiscovery: api_path / maps /v1/x to /x (Infinity), models: any
// passes through, discovery fills the advertised list, and an emptied list is reported as [].
func TestAPIPathAndAnyDiscovery(t *testing.T) {
	healthInterval = 30 * time.Millisecond
	defer func() { healthInterval = servicetype.HealthInterval }()
	f := &fakeServer{models: `{"data":[{"id":"a"},{"id":"b"}]}`}
	s := f.start(t)
	x := newConnector(t, testConfig(t, "embeddings:\n  - name: e\n    url: %s\n    api_path: /\n    models: any\n", s.URL))
	if resp, _ := exchange(t, x, "e", post("/v1/embeddings", "application/json", `{"model":"anything","input":"x"}`)); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if path, body, _ := f.last(); path != "POST /embeddings" || body != `{"model":"anything","input":"x"}` {
		t.Fatal(path, body)
	}
	exchange(t, x, "e", get("/v1/models"))
	if path, _, _ := f.last(); path != "GET /models" {
		t.Fatal(path)
	}
	wait := func(want []string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			got := x.StatusServices()[0].Models
			if reflect.DeepEqual(got, want) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("models %v, want %v", got, want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait([]string{"a", "b"})
	if h := x.HelloServices()[0]; !h.ModelsAny {
		t.Fatal(h)
	}
	f.mu.Lock()
	f.models = `{"data":[]}`
	f.mu.Unlock()
	wait([]string{})
	encoded, _ := json.Marshal(x.StatusServices())
	if !strings.Contains(string(encoded), `"models":[]`) {
		t.Fatal(string(encoded))
	}
	f.mu.Lock()
	sawHealth := false
	for _, p := range f.paths {
		sawHealth = sawHealth || p == "GET /health"
	}
	f.mu.Unlock()
	if !sawHealth {
		t.Fatal("health is not probed at the server root")
	}
}

// TestPerServiceBusy: max_concurrent_requests is enforced per service.
func TestPerServiceBusy(t *testing.T) {
	f := &fakeServer{release: make(chan struct{})}
	s := f.start(t)
	x := newConnector(t, testConfig(t, "chat:\n  - name: one\n    url: %s\n    models: any\n    max_concurrent_requests: 1\n  - name: two\n    url: %[1]s\n    models: any\n", s.URL))
	done := make(chan struct{})
	go func() { defer close(done); exchange(t, x, "one", post("/v1/chat/completions", "application/json", `{}`)) }()
	deadline := time.Now().Add(2 * time.Second)
	for f.hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	resp, _ := exchange(t, x, "one", post("/v1/chat/completions", "application/json", `{}`))
	if resp.StatusCode != 429 || resp.Header.Get(protocol.HeaderError) != "busy" {
		t.Fatal(resp.StatusCode, resp.Header)
	}
	// Another service on the same machine is unaffected by one's limit.
	if s := x.StatusServices(); s[0].InFlight != 1 || s[1].InFlight != 0 {
		t.Fatal(s)
	}
	close(f.release)
	<-done
	if resp, _ := exchange(t, x, "two", post("/v1/chat/completions", "application/json", `{}`)); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
}
func TestRateLimitPerMinute(t *testing.T) {
	f := &fakeServer{}
	x := newConnector(t, testConfig(t, "chat:\n  - name: c\n    url: %s\n    models: any\n    max_requests_per_minute: 2\n", f.start(t).URL))
	for i, want := range []int{200, 200, 429} {
		resp, _ := exchange(t, x, "c", post("/v1/chat/completions", "application/json", `{}`))
		if resp.StatusCode != want || (want == 429 && resp.Header.Get(protocol.HeaderError) != "rate_limited") {
			t.Fatalf("request %d: %d", i, resp.StatusCode)
		}
	}
}

// TestContracts: each type accepts exactly its list; custom exactly its paths.
func TestContracts(t *testing.T) {
	f := &fakeServer{}
	s := f.start(t)
	x := newConnector(t, testConfig(t, `chat:
  - name: chat
    url: %s
    models: any
    api_key: sk-local
anthropic:
  - name: claude
    url: %[1]s
    models: any
realtime:
  - name: voice
    url: %[1]s
custom:
  - name: tool
    url: %[1]s
    paths: ["POST /v1/run"]
`, s.URL))
	for _, tc := range []struct {
		service, request string
		ok               bool
	}{
		{"chat", post("/v1/chat/completions", "application/json", "{}"), true},
		{"chat", post("/v1/responses", "application/json", "{}"), true},
		{"chat", post("/v1/completions", "application/json", "{}"), false},
		{"chat", post("/v1/embeddings", "application/json", "{}"), false},
		{"chat", get("/metrics"), false},
		{"claude", post("/v1/messages", "application/json", "{}"), true},
		{"claude", post("/v1/chat/completions", "application/json", "{}"), false},
		{"voice", get("/v1/config"), true},
		{"voice", get("/v1/stream"), false},
		{"tool", post("/v1/run", "application/json", "{}"), true},
		{"tool", get("/v1/run"), false},
		{"tool", post("/v1/other", "application/json", "{}"), false},
	} {
		resp, _ := exchange(t, x, tc.service, tc.request)
		if tc.ok != (resp.StatusCode == 200) || !tc.ok && resp.Header.Get(protocol.HeaderError) != "path_not_allowed" {
			t.Fatalf("%s %q: %d %s", tc.service, strings.SplitN(tc.request, "\r\n", 2)[0], resp.StatusCode, resp.Header.Get(protocol.HeaderError))
		}
	}
	// The api_key is the only credential sent upstream; the caller's is dropped.
	req := "POST /v1/chat/completions HTTP/1.1\r\nHost: ignored\r\nAuthorization: Bearer caller\r\nAnthropic-Version: 1\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
	exchange(t, x, "chat", req)
	f.mu.Lock()
	auth, anthropic := f.auth, f.headers.Get("Anthropic-Version")
	f.mu.Unlock()
	if auth != "Bearer sk-local" || anthropic != "" {
		t.Fatal(auth, anthropic)
	}
	exchange(t, x, "claude", strings.Replace(req, "/v1/chat/completions", "/v1/messages", 1))
	f.mu.Lock()
	auth, anthropic = f.auth, f.headers.Get("Anthropic-Version")
	f.mu.Unlock()
	if auth != "" || anthropic != "1" {
		t.Fatal(auth, anthropic)
	}
}
func TestStreamMetadataInAccessLogOnRefusal(t *testing.T) {
	cfg := chatConfig(t, "http://127.0.0.1:1")
	x := newConnector(t, cfg)
	exchange(t, x, "chat", get("/admin"))
	r := lastAccess(t, cfg.AccessLog)
	if r["error"] != "path_not_allowed" || r["status"] != float64(403) || r["service"] != "chat" || r["org_slug"] != "example-org" || r["path"] != "/admin" || r["method"] != "GET" {
		t.Fatal(r)
	}
	for _, k := range []string{"time", "session", "service", "type", "method", "path", "status", "bytes_in", "bytes_out", "duration_ms", "model", "org_slug", "org_id", "user_key_id", "request_id", "upstream_host", "error"} {
		if _, ok := r[k]; !ok {
			t.Fatalf("access log lacks %s: %v", k, r)
		}
	}
}
