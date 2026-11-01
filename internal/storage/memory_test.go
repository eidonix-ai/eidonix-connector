package storage

import (
	"bufio"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/hashicorp/yamux"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// objectStore is a small in-memory S3 double: single-part PUT, GET, HEAD and
// a flat ListObjectsV2.
type objectStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (o *objectStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case r.Method == "PUT":
		b, _ := io.ReadAll(r.Body)
		o.data[key] = b
		w.Header().Set("ETag", `"test-etag"`)
	case r.Method == "GET" && r.URL.Query().Get("list-type") == "2":
		type content struct {
			Key  string
			Size int
			ETag string
		}
		var out struct {
			XMLName  xml.Name `xml:"ListBucketResult"`
			Contents []content
		}
		for k, v := range o.data {
			out.Contents = append(out.Contents, content{k, len(v), `"test-etag"`})
		}
		xml.NewEncoder(w).Encode(out)
	case r.Method == "GET" || r.Method == "HEAD":
		b, ok := o.data[key]
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if r.Method == "GET" {
			w.Write(b)
		}
	default:
		w.WriteHeader(404)
	}
}

// memoryRig serves storage streams over one long-lived yamux session, as the
// connector does, so a measurement sees the handler and not session set-up.
type memoryRig struct {
	h      *Handler
	client *yamux.Session
}

func newMemoryRig(t *testing.T, concurrent int) *memoryRig {
	t.Helper()
	store := &objectStore{data: map[string][]byte{}}
	server := httptest.NewServer(store)
	cfg := config.StorageDefaults() // the default 16 MiB part size
	cfg.Endpoint, cfg.Bucket, cfg.PathStyle, cfg.Encryption = server.URL, "test-bucket", true, "none"
	cfg.MaxOpsPerSecond = 1e9
	h, e := newHandler(context.Background(), "files", concurrent, cfg, "test-access", "test-secret", nil, nil)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	a, b := net.Pipe()
	ycfg := yamux.DefaultConfig()
	ycfg.LogOutput = io.Discard
	client, e := yamux.Client(a, ycfg)
	if e != nil {
		t.Fatal(e)
	}
	srv, e := yamux.Server(b, ycfg)
	if e != nil {
		t.Fatal(e)
	}
	go func() {
		for {
			s, e := srv.AcceptStream()
			if e != nil {
				return
			}
			go func() {
				defer s.Close()
				r := bufio.NewReader(s)
				if header, e := protocol.ReadStreamHeader(r); e == nil {
					h.Serve(context.Background(), s, r, header)
				}
			}()
		}
	}()
	t.Cleanup(func() { client.Close(); srv.Close(); h.Close(); server.Close() })
	return &memoryRig{h: h, client: client}
}

// do runs one storage operation and returns whether it succeeded.
func (m *memoryRig) do(header protocol.StreamHeader, body []byte) bool {
	s, e := m.client.OpenStream()
	if e != nil {
		return false
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(20 * time.Second))
	header.V, header.Service, header.Kind, header.RequestID = protocol.Version, "files", "storage", "memory-test"
	if protocol.WriteStreamHeader(s, header) != nil {
		return false
	}
	if header.Op == "put" {
		s.Write(body)
		s.Close()
	}
	r := bufio.NewReader(s)
	var result map[string]any
	if protocol.ReadJSONLine(r, 1<<20, &result) != nil {
		return false
	}
	io.Copy(io.Discard, r)
	return result["ok"] == true
}

func (m *memoryRig) put(i int, data []byte) bool {
	size := int64(len(data))
	return m.do(protocol.StreamHeader{Op: "put", Key: b64(fmt.Sprintf("objects/%d.json", i)), Size: &size}, putFrames(data, false))
}
func (m *memoryRig) get(i int) bool {
	return m.do(protocol.StreamHeader{Op: "get", Key: b64(fmt.Sprintf("objects/%d.json", i))}, nil)
}

// burst runs n small puts, each followed by a get, over the given number of
// workers, and reports the bytes allocated per put plus the heap afterwards.
func (m *memoryRig) burst(t *testing.T, n, workers int) (perPut uint64, after runtime.MemStats) {
	t.Helper()
	data := []byte(`{"id":"` + strings.Repeat("x", 4000) + `"}`)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var wg sync.WaitGroup
	jobs := make(chan int)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if !m.put(i, data) || !m.get(i) {
					t.Errorf("object %d failed", i)
				}
			}
		}()
	}
	for i := range n {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(n), after
}

// TestSmallPutsDoNotAllocateAPart guards the memory blow-up where every put,
// whatever its size, allocated a whole part buffer (16 MiB by default) and a
// burst of small puts could outrun the garbage collector inside a container's
// memory limit. A few-KB put must allocate far less than a part.
func TestSmallPutsDoNotAllocateAPart(t *testing.T) {
	for _, workers := range []int{1, 16} {
		rig := newMemoryRig(t, 16)
		perPut, after := rig.burst(t, 200, workers)
		t.Logf("workers=%d alloc/put+get=%d KiB heap-inuse=%d MiB sys=%d MiB", workers, perPut>>10, after.HeapInuse>>20, after.Sys>>20)
		if perPut > 2<<20 {
			t.Fatalf("%d workers: a small put and get allocated %d bytes, want under 2 MiB (part size %d)", workers, perPut, rig.h.part)
		}
	}
}

func TestListDoesNotGrowTheHeap(t *testing.T) {
	rig := newMemoryRig(t, 16)
	rig.burst(t, 100, 4)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 300 {
		if !rig.do(protocol.StreamHeader{Op: "list", Prefix: "", Max: 100}, nil) {
			t.Fatal("list failed")
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("heap-inuse before=%d KiB after=%d KiB", before.HeapInuse>>10, after.HeapInuse>>10)
	if after.HeapInuse > before.HeapInuse+8<<20 {
		t.Fatalf("heap grew from %d to %d across 300 lists", before.HeapInuse, after.HeapInuse)
	}
	rig.h.mu.Lock()
	defer rig.h.mu.Unlock()
	if len(rig.h.active) != 0 || rig.h.inflight != 0 {
		t.Fatalf("handler retained state: %d uploads, %d in flight", len(rig.h.active), rig.h.inflight)
	}
}

// TestUploadRefusesBodyLongerThanSize: the buffer for a small known size has
// room for one byte more, so a longer body is refused before any backend call.
func TestUploadRefusesBodyLongerThanSize(t *testing.T) {
	var calls int
	var mu sync.Mutex
	h, _ := testHandler(t, func(w http.ResponseWriter, r *http.Request) { mu.Lock(); calls++; mu.Unlock(); w.WriteHeader(500) }, nil)
	for _, size := range []int64{0, 3} {
		_, e := h.upload(context.Background(), "safe", "", strings.NewReader("abcdefgh"), size)
		var v *protocol.StorageError
		if !errors.As(e, &v) || v.Code != "size_mismatch" {
			t.Fatalf("size %d: %v", size, e)
		}
	}
	if calls != 0 {
		t.Fatalf("%d backend calls", calls)
	}
}
