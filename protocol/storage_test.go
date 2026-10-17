package protocol

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/hashicorp/yamux"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

func int64ptr(n int64) *int64 { return &n }
func storageHeader(op string) StreamHeader {
	return StreamHeader{V: Version, Service: "files", Kind: "storage", RequestID: "r-1", Op: op, Key: EncodeStorageKey("a"), Size: int64ptr(-1), Max: 1000}
}
func TestStorageWorstCaseGolden(t *testing.T) {
	key := EncodeStorageKey(strings.Repeat("x", 1023))
	for _, op := range []string{"copy", "list"} {
		meta := strings.Repeat("m", MaxStreamMetadataBytes)
		h := StreamHeader{V: Version, Service: strings.Repeat("r", 40), Kind: "storage", RequestID: strings.Repeat("i", 64), OrgID: meta, OrgSlug: meta, UserKeyID: meta, Op: op}
		if op == "copy" {
			h.SrcKey = key
			h.DstKey = key
		} else {
			h.Prefix = key
			h.Token = key
			h.Recursive = true
			h.Max = 1000
		}
		var b bytes.Buffer
		if e := WriteStreamHeader(&b, h); e != nil {
			t.Fatal(e)
		}
		if b.Len() > MaxHeaderBytes {
			t.Fatal("header budget exceeded")
		}
		golden := readGolden(t, "storage-"+op+"-header.json", b.Bytes())
		if !bytes.Equal(golden, b.Bytes()) {
			t.Fatalf("%s golden differs", op)
		}
		got, e := ReadStreamHeader(bufio.NewReader(&b))
		if e != nil {
			t.Fatal(e)
		}
		if e := ValidateStorageHeader(got, MaxStorageObjectBytes, 1); e != nil {
			t.Fatal(e)
		}
	}
}
func TestUnknownKindParses(t *testing.T) {
	h, e := ReadStreamHeader(bufio.NewReader(strings.NewReader(`{"v":3,"service":"r","kind":"future/1","request_id":"r","endpoint":"http://evil"}` + "\n")))
	if e != nil || h.Kind != "future/1" {
		t.Fatal(h, e)
	}
	var b bytes.Buffer
	if e := WriteStorageResult(&b, StorageErrorResult("unsupported_kind"), ""); e != nil {
		t.Fatal(e)
	}
	want := `{"v":3,"ok":false,"error":{"code":"unsupported_kind","message":"unsupported stream kind"}}` + "\n"
	if b.String() != want {
		t.Fatal(b.String())
	}
}
func TestStorageKeyValidation(t *testing.T) {
	for _, key := range []string{"../x", "a/../x", "/x", "a//b", "a\\b", "a\x00b", "a\x7fb", "a\u0085b", ".eidonix-connector", ".eidonix-connector/x", string([]byte{255})} {
		if _, e := DecodeStorageKey(EncodeStorageKey(key), 7, false); e == nil {
			t.Fatalf("accepted%q", key)
		}
	}
	if _, e := DecodeStorageKey(EncodeStorageKey(strings.Repeat("x", 1023)), 1, false); e != nil {
		t.Fatal(e)
	}
	if _, e := DecodeStorageKey(EncodeStorageKey(strings.Repeat("x", 1023)), 2, false); e == nil {
		t.Fatal("1025 accepted")
	}
	if _, e := DecodeStorageKey("YQ==", 1, false); e == nil {
		t.Fatal("padded accepted")
	}
	if _, e := DecodeStorageKey("YR", 1, false); e == nil {
		t.Fatal("noncanonical accepted")
	}
	if _, e := DecodeStorageKey("", 1, true); e != nil {
		t.Fatal(e)
	}
}
func TestStorageHeaderFields(t *testing.T) {
	for _, op := range []string{"put", "get", "head", "delete", "list", "copy"} {
		h := storageHeader(op)
		if op == "copy" {
			h.SrcKey = h.Key
			h.DstKey = h.Key
		}
		if e := ValidateStorageHeader(h, 100, 1); e != nil {
			t.Fatalf("%s:%v", op, e)
		}
	}
	cases := []StreamHeader{storageHeader("admin"), storageHeader("put"), storageHeader("get"), storageHeader("list")}
	cases[1].Size = nil
	cases[2].Range = []int64{0, 1, 2}
	cases[3].Token = "bad="
	for _, h := range cases {
		if e := ValidateStorageHeader(h, 100, 1); e == nil {
			t.Fatalf("accepted%+v", h)
		}
	}
}
func storageBody(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := NewStorageBodyWriter(&b, MaxStorageObjectBytes)
	if _, e := w.Write(data); e != nil {
		t.Fatal(e)
	}
	if e := w.Finish(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}
func TestStorageBodyIntegrity(t *testing.T) {
	data := bytes.Repeat([]byte("a"), MaxStorageFrameBytes+123)
	wire := storageBody(t, data)
	b, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(bytes.NewReader(wire)), int64(len(data)), MaxStorageObjectBytes))
	if e != nil || !bytes.Equal(b, data) {
		t.Fatal(len(b), e)
	}
	for _, modify := range []func([]byte) []byte{func(b []byte) []byte { return b[:len(b)-1] }, func(b []byte) []byte { return append(b, 'x') }, func(b []byte) []byte { b[4] ^= 1; return b }} {
		bad := modify(bytes.Clone(wire))
		if _, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(bytes.NewReader(bad)), -1, MaxStorageObjectBytes)); e == nil {
			t.Fatal("bad body accepted")
		}
	}
	if _, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(bytes.NewReader(wire)), int64(len(data)+1), MaxStorageObjectBytes)); e == nil {
		t.Fatal("short size accepted")
	}
	if _, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(bytes.NewReader(wire)), -1, 10)); e == nil {
		t.Fatal("oversize accepted")
	}
	if b, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(bytes.NewReader(storageBody(t, nil))), 0, 100)); e != nil || len(b) != 0 {
		t.Fatal(b, e)
	}
}
func TestStorageTrailerRequiredAndLimits(t *testing.T) {
	for _, line := range []string{`{"bytes":0}`, `{"sha256":"` + strings.Repeat("0", 64) + `"}`, `{"bytes":0,"sha256":"` + strings.Repeat("A", 64) + `"}`, strings.Repeat("x", 257)} {
		if _, e := ReadStorageTrailer(bufio.NewReader(strings.NewReader(line + "\n"))); e == nil {
			t.Fatal("invalid trailer accepted")
		}
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxStorageFrameBytes+1)
	if _, e := ReadStorageFrame(bytes.NewReader(header[:])); !errors.Is(e, ErrStorageProtocol) {
		t.Fatal(e)
	}
}
func TestStorageResultBounds(t *testing.T) {
	r := StorageResult{V: Version, OK: true, Size: int64ptr(1), ETag: "etag", ContentType: "text/plain", LastModified: "2026-10-11T00:00:00Z"}
	var b bytes.Buffer
	if e := WriteStorageResult(&b, r, "head"); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadStorageResult(bufio.NewReader(&b), "head"); e != nil {
		t.Fatal(e)
	}
	r.ETag = strings.Repeat("x", 257)
	if e := WriteStorageResult(&b, r, "head"); e == nil {
		t.Fatal("long etag accepted")
	}
	r = StorageResult{V: Version, OK: true, Objects: make([]StorageObject, 1001)}
	if e := WriteStorageResult(&b, r, "list"); e == nil {
		t.Fatal("1001 entries accepted")
	}
	r = StorageResult{V: Version, OK: true}
	b.Reset()
	if e := WriteStorageResult(&b, r, "list"); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(b.String(), `"objects":[]`) || !strings.Contains(b.String(), `"truncated":false`) {
		t.Fatal(b.String())
	}
	for _, line := range []string{`{"v":3}`, `{"ok":true}`, `{"v":3,"ok":false,"error":{"code":"invented","message":"x"}}`} {
		if _, e := ReadStorageResult(bufio.NewReader(strings.NewReader(line+"\n")), "get"); e == nil {
			t.Fatal("invalid result accepted")
		}
	}
}
func TestStorageEOFWaitBound(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	data := storageBody(t, []byte("abc"))
	go func() { b.Write(data) }()
	hook := func(deadline time.Time) error {
		if until := time.Until(deadline); until < 29*time.Second || until > 30*time.Second {
			t.Errorf("wrong EOF timeout%v", until)
		}
		return a.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	}
	_, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(a), 3, 100, hook))
	var se *StorageError
	if !errors.As(e, &se) || se.Code != "timeout" {
		t.Fatalf("unbounded/misclassified EOF:%v", e)
	}
}
func TestYamuxPutHalfCloseStillReadsResult(t *testing.T) {
	a, b := net.Pipe()
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	cfg.StreamCloseTimeout = 0
	client, e := yamux.Client(a, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	server, e := yamux.Server(b, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	errCh := make(chan error, 1)
	go func() {
		s, e := server.AcceptStream()
		if e != nil {
			errCh <- e
			return
		}
		defer s.Close()
		data, e := io.ReadAll(NewStorageBodyReader(bufio.NewReader(s), 3, 100, s.SetReadDeadline))
		if e != nil {
			errCh <- e
			return
		}
		if string(data) != "abc" {
			errCh <- errors.New("bad body")
			return
		}
		errCh <- WriteStorageResult(s, StorageResult{V: Version, OK: true, Size: int64ptr(3), ETag: "etag"}, "put")
	}()
	s, e := client.OpenStream()
	if e != nil {
		t.Fatal(e)
	}
	s.SetDeadline(time.Now().Add(time.Second))
	w := NewStorageBodyWriter(s, 100)
	w.Write([]byte("abc"))
	if e := w.Finish(); e != nil {
		t.Fatal(e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	r, e := ReadStorageResult(bufio.NewReader(s), "put")
	if e != nil || !r.OK {
		t.Fatal(r, e)
	}
	if e := <-errCh; e != nil {
		t.Fatal(e)
	}
}
func FuzzStorageHeader(f *testing.F) {
	b, _ := json.Marshal(storageHeader("put"))
	f.Add(append(b, '\n'))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, e := ReadStreamHeader(bufio.NewReader(bytes.NewReader(b)))
		if e == nil {
			ValidateStorageHeader(h, MaxStorageObjectBytes, 7)
		}
	})
}
func FuzzStorageFrame(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) { ReadStorageFrame(bytes.NewReader(b)) })
}
func FuzzStorageTrailer(f *testing.F) {
	sum := sha256.Sum256(nil)
	f.Add([]byte(`{"bytes":0,"sha256":"` + hex.EncodeToString(sum[:]) + `"}` + "\n"))
	f.Fuzz(func(t *testing.T, b []byte) { ReadStorageTrailer(bufio.NewReader(bytes.NewReader(b))) })
}
func FuzzStorageResult(f *testing.F) {
	f.Add([]byte(`{"v":3,"ok":true,"objects":[],"truncated":false}`+"\n"), "list")
	f.Fuzz(func(t *testing.T, b []byte, op string) { ReadStorageResult(bufio.NewReader(bytes.NewReader(b)), op) })
}
func FuzzStorageBody(f *testing.F) {
	var b bytes.Buffer
	w := NewStorageBodyWriter(&b, 100)
	w.Finish()
	f.Add(b.Bytes())
	f.Fuzz(func(t *testing.T, b []byte) {
		io.Copy(io.Discard, NewStorageBodyReader(bufio.NewReader(bytes.NewReader(b)), -1, 1<<20))
	})
}
func FuzzStorageKey(f *testing.F) {
	f.Add(EncodeStorageKey("ws:uuid/file"))
	f.Fuzz(func(t *testing.T, s string) { DecodeStorageKey(s, 7, true) })
}

func TestStorageLineSizeCaps(t *testing.T) {
	for _, test := range []struct {
		op  string
		max int
	}{{"get", MaxStorageResultBytes}, {"list", MaxStorageListResultBytes}} {
		line := `{"v":3,"ok":false,"error":{"code":"busy","message":"x"},"ignored":"` + strings.Repeat("x", test.max) + `"}` + "\n"
		if _, e := ReadStorageResult(bufio.NewReader(strings.NewReader(line)), test.op); e == nil {
			t.Fatal("oversized result accepted")
		}
	}
	line := `{"v":3,"ok":true,"truncated":false}` + "\n"
	if _, e := ReadStorageResult(bufio.NewReader(strings.NewReader(line)), "list"); e == nil {
		t.Fatal("missing objects accepted")
	}
}

func TestStorageManyTinyObjectsBounded(t *testing.T) {
	objects := `[` + strings.Repeat(`{},`, 300000) + `{}]`
	for _, field := range []string{`"objects":` + objects, `"objects":` + objects + `,"objects":[]`, `"Objects":` + objects} {
		line := `{"v":3,"ok":false,"error":{"code":"busy","message":"busy"},` + field + `}` + "\n"
		if len(line) > MaxStorageListResultBytes {
			t.Fatal("test exceeds line cap")
		}
		if _, e := ReadStorageResult(bufio.NewReader(strings.NewReader(line)), "list"); e == nil {
			t.Fatal("huge object array accepted")
		}
	}
	// The whole line is bounded, and array decoding must stay bounded too.
	line := `{"v":3,"ok":true,"objects":` + objects + `,"truncated":false}` + "\n"
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	before := stats.TotalAlloc
	if _, e := ReadStorageResult(bufio.NewReader(strings.NewReader(line)), "list"); e == nil {
		t.Fatal("huge object array accepted")
	}
	runtime.ReadMemStats(&stats)
	if allocated := stats.TotalAlloc - before; allocated > 16<<20 {
		t.Fatalf("oversized object array allocated %d bytes", allocated)
	}
}
