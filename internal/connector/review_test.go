package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func TestHeaderTimeoutUsesConfiguredIdle(t *testing.T) {
	cfg := chatConfig(t, "http://127.0.0.1:1")
	cfg.Services[0].IdleTimeout = 600 * time.Second
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	tr := x.services["chat"].client.Transport.(*http.Transport)
	if tr.ResponseHeaderTimeout != 600*time.Second {
		t.Fatalf("timeout %v", tr.ResponseHeaderTimeout)
	}
}
func TestIdleBeforeResponseHeadersReturns504(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer s.Close()
	cfg := chatConfig(t, s.URL)
	cfg.Services[0].IdleTimeout = 30 * time.Millisecond
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	resp, _ := exchange(t, x, "chat", "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
	if resp.StatusCode != 504 || resp.Header.Get(protocol.HeaderError) != "upstream_timeout" {
		t.Fatalf("got %d %v", resp.StatusCode, resp.Header)
	}
}
func Test500DoesNotMarkUnhealthy(t *testing.T) {
	var count atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); w.WriteHeader(500) }))
	defer s.Close()
	x, err := New(chatConfig(t, s.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	for i := 0; i < 2; i++ {
		resp, _ := exchange(t, x, "chat", "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
		if resp.StatusCode != 500 {
			t.Fatalf("request %d: %d", i, resp.StatusCode)
		}
	}
	if count.Load() != 2 || !x.StatusServices()[0].Healthy || !x.services["chat"].eject.IsZero() {
		t.Fatal("500 marked the service unhealthy")
	}
}
func TestResponseTrailersAndModelQueriesDropped(t *testing.T) {
	var query atomic.Value
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query.Store(r.URL.RawQuery)
		w.Header().Set("Trailer", "X-Internal-Secret")
		io.WriteString(w, "ok")
		w.Header().Set("X-Internal-Secret", "SENTINEL_SECRET")
	}))
	defer s.Close()
	x, err := New(chatConfig(t, s.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	resp, data := exchange(t, x, "chat", "GET /v1/models?fetch=http%3A%2F%2Fexample.com HTTP/1.1\r\nHost: ignored\r\n\r\n")
	if query.Load() != "" || len(resp.Trailer) != 0 || bytes.Contains(data, []byte("SENTINEL_SECRET")) {
		t.Fatalf("query/trailer leaked: %v %v %s", query.Load(), resp.Trailer, data)
	}
}
func TestMalformedAndResponseOverflowAudited(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush()
		io.WriteString(w, "oversize-response")
	}))
	defer s.Close()
	cfg := chatConfig(t, s.URL)
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	x.services["chat"].maxResponse = 4
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() { x.ServeStream(context.Background(), a); close(done) }()
	io.WriteString(b, "{invalid}\n")
	b.Close()
	<-done
	exchange(t, x, "chat", "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
	data, err := os.ReadFile(cfg.AccessLog)
	if err != nil {
		t.Fatal(err)
	}
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 2 || records[0]["error"] != "invalid_stream_header" || records[1]["error"] != "response_too_large" || records[1]["status"] != float64(502) {
		t.Fatalf("audit records %s", data)
	}
}
func TestReopenLogsDoesNotCancelLiveStream(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		close(started)
		<-finish
		io.WriteString(w, "last")
	}))
	defer s.Close()
	cfg := chatConfig(t, s.URL)
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	a, b := net.Pipe()
	defer b.Close()
	b.SetDeadline(time.Now().Add(2 * time.Second))
	done := make(chan struct{})
	go func() { x.ServeStream(context.Background(), a); close(done) }()
	go func() {
		protocol.WriteStreamHeader(b, protocol.StreamHeader{V: protocol.Version, Service: "chat", Kind: "http", RequestID: "r"})
		io.WriteString(b, "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
	}()
	resp, err := http.ReadResponse(bufio.NewReader(b), nil)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 5)
	if _, err = io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	<-started
	if err = os.Rename(cfg.AccessLog, cfg.AccessLog+".1"); err != nil {
		t.Fatal(err)
	}
	if err = x.ReopenLogs(); err != nil {
		t.Fatal(err)
	}
	close(finish)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(first)+string(rest) != "firstlast" {
		t.Fatalf("stream interrupted: %s %s %v", first, rest, err)
	}
	b.Close()
	<-done
	data, err := os.ReadFile(cfg.AccessLog)
	if err != nil || !bytes.Contains(data, []byte(`"status":200`)) {
		t.Fatalf("audit not reopened: %s %v", data, err)
	}
}
func TestAuditCountsCompletedUpload(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); io.WriteString(w, "ok") }))
	defer s.Close()
	cfg := chatConfig(t, s.URL)
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	body := `{"prompt":"SENTINEL_BODY"}`
	exchange(t, x, "chat", fmt.Sprintf("POST /v1/chat/completions HTTP/1.1\r\nHost: ignored\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body))
	data, _ := os.ReadFile(cfg.AccessLog)
	var record map[string]any
	if err = json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		t.Fatal(err)
	}
	if record["bytes_in"] != float64(len(body)) || strings.Contains(string(data), "SENTINEL_BODY") {
		t.Fatalf("audit %s", data)
	}
}

type parserConn struct {
	reader *bytes.Reader
	mu     sync.Mutex
}

func (c *parserConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reader.Read(p)
}

func (c *parserConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *parserConn) Close() error                     { return nil }
func (c *parserConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *parserConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *parserConn) SetDeadline(time.Time) error      { return nil }
func (c *parserConn) SetReadDeadline(time.Time) error  { return nil }
func (c *parserConn) SetWriteDeadline(time.Time) error { return nil }

type parserTransport struct{}

func (parserTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body != nil { // health probes have no body
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
}
func FuzzHTTPStreamParser(f *testing.F) {
	for _, seed := range []string{"GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n", "POST /v1/../admin HTTP/1.1\r\nHost: ignored\r\n\r\n", "POST /v1/chat/completions HTTP/1.1\r\nHost: ignored\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n0\r\n\r\n"} {
		f.Add([]byte(seed))
	}
	x, err := New(chatConfig(f, "http://127.0.0.1:1"))
	if err != nil {
		f.Fatal(err)
	}
	defer x.Close()
	x.services["chat"].client.Transport = parserTransport{}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65536 {
			t.Skip()
		}
		header := []byte(`{"v":2,"service":"chat","kind":"http"}` + "\n")
		x.ServeStream(context.Background(), &parserConn{reader: bytes.NewReader(append(header, data...))})
	})
}

func TestResponseHeadersResetIdleTimer(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(200 * time.Millisecond)
		io.WriteString(w, "done")
	}))
	defer s.Close()
	cfg := chatConfig(t, s.URL)
	cfg.Services[0].IdleTimeout = 300 * time.Millisecond
	x, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	resp, data := exchange(t, x, "chat", "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
	if resp.StatusCode != 200 || string(data) != "done" {
		t.Fatalf("headers did not reset idle timer: %d %q", resp.StatusCode, data)
	}
}
