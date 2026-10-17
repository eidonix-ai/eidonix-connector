package storage

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testHandler(t *testing.T, serve http.HandlerFunc, audit func(AuditEntry)) (*Handler, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(serve)
	cfg := config.StorageDefaults()
	cfg.Endpoint, cfg.Bucket, cfg.PathStyle, cfg.Encryption = server.URL, "test-bucket", true, "none"
	cfg.MaxFileSize, cfg.PartSize, cfg.IdleTimeout = 10<<20, 5<<20, 2*time.Second
	h, e := newHandler(context.Background(), "files", 2, cfg, "test-access", "test-secret", nil, audit)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Close(); server.Close() })
	return h, server
}
func TestInvalidKeysNeverReachBackend(t *testing.T) {
	var calls atomic.Int32
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }, nil)
	zero := int64(0)
	for _, key := range []string{"../escape", "/absolute", "a//b", "a\\b", "a\x00b", "a\u0085b", ".eidonix-connector/probe/x", strings.Repeat("k", 1024)} {
		for _, header := range []protocol.StreamHeader{{Op: "head", Key: b64(key)}, {Op: "put", Key: b64(key), Size: &zero}, {Op: "delete", Key: b64(key)}, {Op: "get", Key: b64(key)}, {Op: "copy", SrcKey: b64(key), DstKey: b64("safe")}, {Op: "copy", SrcKey: b64("safe"), DstKey: b64(key)}, {Op: "list", Prefix: b64(key), Max: 1}, {Op: "list", Prefix: "", Token: b64(key), Max: 1}} {
			r, _ := storageExchange(t, h, header, nil)
			if r["ok"] == true {
				t.Fatalf("accepted %s %q", header.Op, key)
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid keys caused %d backend calls", calls.Load())
	}
}
func TestSmallBadUploadsNeverCommit(t *testing.T) {
	var calls atomic.Int32
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }, nil)
	size := int64(3)
	valid := putFrames([]byte("abc"), false)
	for _, body := range [][]byte{valid[:len(valid)-1], putFrames([]byte("abc"), true), append(append([]byte{}, valid...), 1), valid[:7]} {
		r, _ := storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64("safe"), Size: &size}, body)
		if r["ok"] == true {
			t.Fatal("bad upload committed")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("bad small uploads made %d backend calls", calls.Load())
	}
}
func TestReadPartPreservesProtocolErrors(t *testing.T) {
	r := io.MultiReader(bytes.NewReader([]byte("abc")), errorReader{})
	_, e := readPart(r, make([]byte, 10))
	if e != io.ErrUnexpectedEOF {
		t.Fatalf("lost source error %v", e)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func TestRateAndAudit(t *testing.T) {
	var mu sync.Mutex
	entries := []AuditEntry{}
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }, func(a AuditEntry) { mu.Lock(); entries = append(entries, a); mu.Unlock() })
	h.cfg.MaxOpsPerSecond = .001
	h.tokens = 1
	h.last = time.Now()
	first, _ := storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64("filename-sensitive"), OrgID: "7", OrgSlug: "example-org", UserKeyID: "42"}, nil)
	if errorCode(first) != "not_found" {
		t.Fatal(first)
	}
	second, _ := storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64("filename-sensitive")}, nil)
	if errorCode(second) != "rate_limited" {
		t.Fatal(second)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(entries) != 2 || entries[1].Status != "rate_limited" || len(entries[1].KeyHash) != 64 || strings.Contains(entries[1].KeyHash, "filename") {
		t.Fatalf("invalid audit %+v", entries)
	}
	// Access log lines name the service, its kind and the caller the gateway reported.
	if e := entries[0]; e.Service != "files" || e.Kind != "s3" || e.OrgSlug != "example-org" || e.OrgID != "7" || e.UserKeyID != "42" || e.RequestID != "docker-test" || e.Time.IsZero() || e.Origin != "upstream" {
		t.Fatalf("access entry %+v", e)
	}
}
// TestReadOnlyRefusesWrites: a read-only service refuses put, delete and copy
// before any backend call, and still serves reads.
func TestReadOnlyRefusesWrites(t *testing.T) {
	var calls atomic.Int32
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" && r.Method != "HEAD" {
			t.Errorf("read-only service issued %s", r.Method)
		}
		w.WriteHeader(404)
	}, nil)
	h.cfg.ReadOnly = true
	size := int64(3)
	for _, header := range []protocol.StreamHeader{{Op: "put", Key: b64("a"), Size: &size}, {Op: "delete", Key: b64("a")}, {Op: "copy", SrcKey: b64("a"), DstKey: b64("b")}} {
		r, _ := storageExchange(t, h, header, putFrames([]byte("abc"), false))
		if errorCode(r) != "forbidden" {
			t.Fatalf("%s: %v", header.Op, r)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("refused writes made %d backend calls", calls.Load())
	}
	r, _ := storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64("a")}, nil)
	if errorCode(r) != "not_found" || calls.Load() == 0 {
		t.Fatalf("read refused: %v", r)
	}
}
func TestRedirectNeverFollowed(t *testing.T) {
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))
	defer target.Close()
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }, nil)
	r, _ := storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64("safe")}, nil)
	if r["ok"] == true || reached.Load() {
		t.Fatal("storage redirect followed")
	}
}

func TestSweepConfinedOldUploadsAndOwnActive(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	aborted := []string{}
	audit := []AuditEntry{}
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Query().Has("uploads") {
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>`)
			for _, u := range []struct {
				key, id string
				age     time.Duration
			}{{"folder/old", "old", 25 * time.Hour}, {"fresh", "fresh", time.Hour}, {"bad//key", "invalid", 25 * time.Hour}, {"own", "own", 25 * time.Hour}, {".eidonix-connector/probe/abandoned", "probe", 25 * time.Hour}} {
				fmt.Fprintf(w, "<Upload><Key>%s</Key><UploadId>%s</UploadId><Initiated>%s</Initiated></Upload>", u.key, u.id, now.Add(-u.age).Format(time.RFC3339))
			}
			io.WriteString(w, `</ListMultipartUploadsResult>`)
			return
		}
		if r.Method == "DELETE" {
			mu.Lock()
			aborted = append(aborted, r.URL.Query().Get("uploadId"))
			mu.Unlock()
			w.WriteHeader(204)
			return
		}
		t.Errorf("unexpected sweep call %s %s", r.Method, r.URL)
		w.WriteHeader(500)
	}, func(a AuditEntry) { mu.Lock(); audit = append(audit, a); mu.Unlock() })
	h.active["own"] = true
	h.sweep(context.Background(), now)
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(aborted, ",") != "old,probe" {
		t.Fatalf("aborted %v", aborted)
	}
	if len(audit) != 2 || audit[0].Origin != "local" || audit[0].UploadAgeSeconds != 25*3600 || len(audit[0].KeyHash) != 64 {
		t.Fatalf("invalid local audit %+v", audit)
	}
}
func TestChecksReadOnlyAndFailedProbeCleaned(t *testing.T) {
	var mu sync.Mutex
	methods := []string{}
	writeKeys := []string{}
	deletes := []string{}
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		if r.Method == "PUT" {
			writeKeys = append(writeKeys, r.URL.Path)
		}
		if r.Method == "DELETE" {
			deletes = append(deletes, r.URL.Path)
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == "HEAD":
			w.WriteHeader(200)
		case r.Method == "GET" && r.URL.Query().Has("list-type"):
			io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
		case r.Method == "GET" && r.URL.Query().Has("uploads"):
			io.WriteString(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
		case r.Method == "GET" && r.URL.Path == "/":
			io.WriteString(w, `<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>`)
		case r.Method == "PUT":
			w.Header().Set("ETag", `"test-etag"`)
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(200)
		case r.Method == "DELETE":
			w.WriteHeader(204)
		default:
			w.WriteHeader(500)
		}
	}, nil)
	if _, e := h.Check(context.Background(), false); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	for _, method := range methods {
		if method != "GET" && method != "HEAD" {
			t.Fatalf("readonly check issued %s", method)
		}
	}
	mu.Unlock()
	if _, e := h.Check(context.Background(), true); e == nil {
		t.Fatal("injected probe get failure was ignored")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(writeKeys) != 1 || len(deletes) < 1 || writeKeys[0] != deletes[len(deletes)-1] || !strings.Contains(writeKeys[0], "/test-bucket/.eidonix-connector/probe/") {
		t.Fatalf("probe writes/deletes %v %v", writeKeys, deletes)
	}
}
func TestBackendMetadataAndXMLLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		serve http.HandlerFunc
	}{{"etag", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", strings.Repeat("x", 257))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", "1")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
	}}, {"huge-list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(3<<20))
		w.WriteHeader(200)
	}}} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := testHandler(t, tc.serve, nil)
			header := protocol.StreamHeader{Op: "head", Key: b64("safe")}
			if tc.name == "huge-list" {
				header = protocol.StreamHeader{Op: "list", Max: 1}
			}
			r, _ := storageExchange(t, h, header, nil)
			if errorCode(r) != "backend_invalid" {
				t.Fatalf("response not bounded: %v", r)
			}
		})
	}
}
func TestUploadLifetimeWhileDataFlows(t *testing.T) {
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("unfinished upload reached backend")
		w.WriteHeader(500)
	}, nil)
	h.lifetime = 80 * time.Millisecond
	serverStream, clientStream := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Serve(context.Background(), serverStream, bufio.NewReader(serverStream), protocol.StreamHeader{V: protocol.Version, Kind: "storage", Service: "files", RequestID: "test", Op: "put", Key: b64("safe"), Size: intPtr(-1)})
	}()
	defer clientStream.Close()
	clientStream.SetDeadline(time.Now().Add(time.Second))
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for {
			if protocol.WriteStorageFrame(clientStream, []byte("flowing")) != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	<-done
	clientStream.Close()
	<-writeDone
}
func intPtr(n int64) *int64 { return &n }

func TestNonPutEOFImmediatelyCancelsBackend(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", "3")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("ETag", `"tag"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(200)
			return
		}
		close(started)
		<-r.Context().Done()
		close(canceled)
	}, nil)
	serverStream, clientStream := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Serve(context.Background(), serverStream, bufio.NewReader(serverStream), protocol.StreamHeader{V: protocol.Version, Kind: "storage", Service: "files", RequestID: "cancel-test", Op: "get", Key: b64("safe")})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		clientStream.Close()
		t.Fatal("backend get did not begin")
	}
	clientStream.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("request EOF did not cancel backend")
	}
	<-done
}

type eofDeadlineConn struct {
	net.Conn
	mu          sync.Mutex
	seen        bool
	overwritten bool
}

func (c *eofDeadlineConn) SetReadDeadline(d time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Until(d) > 20*time.Second {
		c.seen = true
		return c.Conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	}
	if c.seen && time.Until(d) > time.Second {
		c.overwritten = true
	}
	return c.Conn.SetReadDeadline(d)
}
func TestVerifiedTrailerWaitsForBoundedFIN(t *testing.T) {
	var calls atomic.Int32
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }, nil)
	a, b := net.Pipe()
	c := &eofDeadlineConn{Conn: a}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Serve(context.Background(), c, bufio.NewReader(c), protocol.StreamHeader{V: protocol.Version, Kind: "storage", Service: "files", RequestID: "fin-test", Op: "put", Key: b64("safe"), Size: intPtr(3)})
	}()
	defer b.Close()
	b.SetDeadline(time.Now().Add(time.Second))
	if _, e := b.Write(putFrames([]byte("abc"), false)); e != nil {
		t.Fatal(e)
	}
	result, e := protocol.ReadStorageResult(bufio.NewReader(b), "put")
	if e != nil {
		t.Fatal(e)
	}
	if result.Error == nil || result.Error.Code != "timeout" {
		t.Fatalf("missing FIN got %+v", result)
	}
	<-done
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.seen || c.overwritten || calls.Load() != 0 {
		t.Fatalf("FIN bound seen=%v overwritten=%v backendcalls=%d", c.seen, c.overwritten, calls.Load())
	}
}
