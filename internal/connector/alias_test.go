package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// aliasUpstream records what the upstream actually received.
type aliasUpstream struct {
	hits  atomic.Int64
	body  atomic.Value // string
	model atomic.Value // string
	file  atomic.Value // []byte
	clen  atomic.Int64
}

func (a *aliasUpstream) server(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.hits.Add(1)
		a.clen.Store(r.ContentLength)
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"object":"list","data":[{"id":"jina-reranker-v3","object":"model"},{"id":"other","object":"model"}]}`)
		case strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data"):
			if err := r.ParseMultipartForm(64 << 20); err != nil {
				w.WriteHeader(400)
				return
			}
			a.model.Store(r.FormValue("model"))
			f, _, err := r.FormFile("file")
			if err == nil {
				data, _ := io.ReadAll(f)
				a.file.Store(data)
			}
			io.WriteString(w, `{"text":"ok"}`)
		default:
			data, _ := io.ReadAll(r.Body)
			a.body.Store(string(data))
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"model":"jina-reranker-v3","results":[]}`)
		}
	}))
}

func aliasConnector(t *testing.T, origin, typ string) (*Connector, string) {
	t.Helper()
	cfg := reviewConfig(origin)
	cfg.AuditLog = filepath.Join(t.TempDir(), "audit.jsonl")
	cfg.Routes[0].Type = typ
	cfg.Routes[0].Models = map[string]string{"jina-reranker": "jina-reranker-v3", "whisper": "/models/faster-whisper-large-v3-turbo"}
	if typ == "openai_embeddings" {
		cfg.Routes[0].Models["nomic-embed"] = "jina-reranker-v3"
	}
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { x.Close() })
	return x, cfg.AuditLog
}

func post(path, ct, body string) string {
	return fmt.Sprintf("POST %s HTTP/1.1\r\nHost: ignored\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s", path, ct, len(body), body)
}

func lastAudit(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	var record map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestAliasJSONRequestRewrite(t *testing.T) {
	up := &aliasUpstream{}
	s := up.server(t)
	defer s.Close()
	x, audit := aliasConnector(t, s.URL, "rerank")

	body := `{"query":"q","model":"jina-reranker","documents":["model: jina-reranker"]}`
	resp, data := reviewRequest(t, x, post("/v1/rerank", "application/json", body))
	want := `{"query":"q","model":"jina-reranker-v3","documents":["model: jina-reranker"]}`
	if resp.StatusCode != 200 || up.body.Load() != want || up.clen.Load() != int64(len(want)) {
		t.Fatalf("status %d upstream got %v (len %d)", resp.StatusCode, up.body.Load(), up.clen.Load())
	}
	if !strings.Contains(string(data), "jina-reranker-v3") {
		t.Fatal("response bodies must not be rewritten")
	}
	record := lastAudit(t, audit)
	if record["model"] != "jina-reranker" || record["upstream_model"] != "jina-reranker-v3" || record["status"] != float64(200) {
		t.Fatalf("audit %v", record)
	}
	if strings.Contains(fmt.Sprint(record), "documents") {
		t.Fatal("audit contains body content")
	}

	// Non-alias names pass through byte for byte.
	body = `{"model":  "bge-reranker" , "query":"q"}`
	if resp, _ = reviewRequest(t, x, post("/v1/rerank", "application/json", body)); resp.StatusCode != 200 || up.body.Load() != body {
		t.Fatalf("passthrough changed: %v", up.body.Load())
	}
	if record = lastAudit(t, audit); record["model"] != nil || record["upstream_model"] != nil {
		t.Fatalf("audit recorded a rewrite that did not happen: %v", record)
	}
}

func TestAliasJSONRejectsAmbiguousBodies(t *testing.T) {
	up := &aliasUpstream{}
	s := up.server(t)
	defer s.Close()
	x, audit := aliasConnector(t, s.URL, "rerank")
	for _, body := range []string{`{"model":"jina-reranker","model":"secret"}`, `{"model":"jina-reranker"`, `{"model":"jina-reranker"}{}`} {
		resp, _ := reviewRequest(t, x, post("/v1/rerank", "application/json", body))
		if resp.StatusCode != 400 || resp.Header.Get(protocol.HeaderError) != "invalid_request" {
			t.Fatalf("%s: got %d", body, resp.StatusCode)
		}
		if record := lastAudit(t, audit); record["error"] != "invalid_request" || record["status"] != float64(400) {
			t.Fatalf("audit %v", record)
		}
	}
	if up.hits.Load() != 0 {
		t.Fatal("ambiguous body reached upstream")
	}
}

func TestAliasJSONRespectsRequestLimit(t *testing.T) {
	up := &aliasUpstream{}
	s := up.server(t)
	defer s.Close()
	cfg := reviewConfig(s.URL)
	cfg.Routes[0].Type = "rerank"
	cfg.Routes[0].MaxRequestBytes = "64"
	cfg.Routes[0].Models = map[string]string{"jina-reranker": "jina-reranker-v3"}
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	body := `{"model":"jina-reranker","query":"` + strings.Repeat("x", 100) + `"}`
	// Unknown length (chunked) so the cap is enforced while buffering, not by Content-Length.
	req := "POST /v1/rerank HTTP/1.1\r\nHost: ignored\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" + fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)
	if resp, _ := reviewRequest(t, x, req); resp.StatusCode != 413 || up.hits.Load() != 0 {
		t.Fatalf("got %d, upstream hits %d", resp.StatusCode, up.hits.Load())
	}
}

func multipartRequest(t *testing.T, fields [][2]string, file []byte) string {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	for _, f := range fields {
		w.WriteField(f[0], f[1])
	}
	fw, _ := w.CreateFormFile("file", "speech.wav")
	fw.Write(file)
	w.Close()
	return post("/v1/audio/transcriptions", w.FormDataContentType(), b.String())
}

func TestAliasMultipartRewrite(t *testing.T) {
	up := &aliasUpstream{}
	s := up.server(t)
	defer s.Close()
	x, audit := aliasConnector(t, s.URL, "audio_transcription")
	file := bytes.Repeat([]byte("\x00RIFFwhisper\r\n--"), 20000)
	resp, _ := reviewRequest(t, x, multipartRequest(t, [][2]string{{"model", "whisper"}, {"language", "en"}}, file))
	if resp.StatusCode != 200 || up.model.Load() != "/models/faster-whisper-large-v3-turbo" || !bytes.Equal(up.file.Load().([]byte), file) {
		t.Fatalf("status %d model %v", resp.StatusCode, up.model.Load())
	}
	if record := lastAudit(t, audit); record["model"] != "whisper" || record["upstream_model"] != "/models/faster-whisper-large-v3-turbo" {
		t.Fatalf("audit %v", record)
	}
	// Non-alias names pass through.
	if resp, _ = reviewRequest(t, x, multipartRequest(t, [][2]string{{"model", "large-v3"}}, file)); resp.StatusCode != 200 || up.model.Load() != "large-v3" {
		t.Fatalf("passthrough model %v", up.model.Load())
	}
	// Duplicate model fields are rejected rather than forwarded.
	up.model.Store("")
	resp, _ = reviewRequest(t, x, multipartRequest(t, [][2]string{{"model", "whisper"}, {"model", "secret"}}, file))
	if resp.StatusCode != 400 || resp.Header.Get(protocol.HeaderError) != "invalid_request" || up.model.Load() != "" {
		t.Fatalf("duplicate model: status %d, upstream model %v", resp.StatusCode, up.model.Load())
	}
}

func TestAliasModelListAndAdvertisement(t *testing.T) {
	up := &aliasUpstream{}
	s := up.server(t)
	defer s.Close()
	x, _ := aliasConnector(t, s.URL, "openai_embeddings")
	_, data := reviewRequest(t, x, "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
	var list struct {
		Object string
		Data   []struct{ ID, Object string }
	}
	if err := json.Unmarshal(data, &list); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, m := range list.Data {
		ids = append(ids, m.ID)
	}
	if list.Object != "list" || !reflect.DeepEqual(ids, []string{"jina-reranker", "nomic-embed", "other"}) {
		t.Fatalf("model list %s", data)
	}
	// Before discovery the advertised models are the aliases.
	if got := x.HelloServices()[0].Models; !reflect.DeepEqual(got, []string{"jina-reranker", "nomic-embed", "whisper"}) {
		t.Fatalf("initial hello models %v", got)
	}
}

func TestAliasDiscoveryPresentsAliases(t *testing.T) {
	up := &aliasUpstream{}
	s := up.server(t)
	defer s.Close()
	cfg := reviewConfig(s.URL)
	cfg.Routes[0].Type = "openai_embeddings"
	cfg.Routes[0].Health = config.Health{Path: "/v1/models", Interval: "1s"}
	cfg.Routes[0].Models = map[string]string{"nomic-embed": "jina-reranker-v3"}
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	want := []string{"nomic-embed", "other"}
	deadline := time.Now().Add(4 * time.Second)
	for !reflect.DeepEqual(x.StatusServices()[0].Models, want) || !reflect.DeepEqual(x.HelloServices()[0].Models, want) {
		if time.Now().After(deadline) {
			t.Fatalf("status models %v", x.StatusServices()[0].Models)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestAliasNamesReportedWithoutModelsEndpoint(t *testing.T) {
	x, _ := aliasConnector(t, "http://127.0.0.1:1", "rerank")
	if got := x.StatusServices()[0].Models; !reflect.DeepEqual(got, []string{"jina-reranker", "whisper"}) {
		t.Fatalf("status models %v", got)
	}
	if got := x.HelloServices()[0].Models; !reflect.DeepEqual(got, []string{"jina-reranker", "whisper"}) {
		t.Fatalf("hello models %v", got)
	}
}
