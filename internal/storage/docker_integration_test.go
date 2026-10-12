package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/cletus-connector/internal/config"
	"github.com/eidonix-ai/cletus-connector/protocol"
	"github.com/hashicorp/yamux"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func dockerFixture(t *testing.T) (*Handler, *minio.Client, config.Route, []byte) {
	t.Helper()
	endpoint := os.Getenv("CONNECTOR_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("run ci/storage-minio.sh for TLS MinIO tests")
	}
	ca, e := os.ReadFile(os.Getenv("CONNECTOR_TEST_S3_CA"))
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid test CA")
	}
	u, e := url.Parse(endpoint)
	if e != nil {
		t.Fatal(e)
	}
	client, e := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("CONNECTOR_TEST_S3_ACCESS"), os.Getenv("CONNECTOR_TEST_S3_SECRET"), ""), Secure: true, Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}})
	if e != nil {
		t.Fatal(e)
	}
	id := make([]byte, 8)
	rand.Read(id)
	bucket := "test-" + hex.EncodeToString(id)
	ctx := context.Background()
	if e = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); e != nil {
		t.Fatal(e)
	}
	key := make([]byte, 32)
	rand.Read(key)
	cfg := config.Route{ID: "files", Type: "object_storage", Endpoint: endpoint, Region: "us-east-1", Bucket: bucket, Prefix: "objects/", PathStyle: true, TLSCAFile: os.Getenv("CONNECTOR_TEST_S3_CA"), Encryption: config.Encryption{Mode: "sse-c"}, MaxObjectBytes: "5GiB", PartSize: "5MiB", MaxConcurrency: 4, MaxOpsPerSecond: 10000, IdleTimeout: "10s", MaxUploadLifetime: "6h", AbandonedUploadAge: "24h"}
	h, e := newHandler(ctx, cfg, os.Getenv("CONNECTOR_TEST_S3_ACCESS"), os.Getenv("CONNECTOR_TEST_S3_SECRET"), key, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		h.Close()
		for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err == nil {
				client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{})
			}
		}
		client.RemoveBucket(ctx, bucket)
	})
	return h, client, cfg, key
}

func storageExchange(t *testing.T, h *Handler, header protocol.StreamHeader, body []byte) (map[string]any, []byte) {
	t.Helper()
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		s, e := server.AcceptStream()
		if e != nil {
			return
		}
		defer s.Close()
		r := bufio.NewReader(s)
		parsed, e := protocol.ReadStreamHeader(r)
		if e == nil {
			h.Serve(context.Background(), s, r, parsed)
		}
	}()
	s, e := client.OpenStream()
	if e != nil {
		t.Fatal(e)
	}
	s.SetDeadline(time.Now().Add(20 * time.Second))
	header.V = 1
	header.Route = "files"
	header.Kind = "storage"
	header.RequestID = "docker-test"
	if e = protocol.WriteStreamHeader(s, header); e != nil {
		t.Fatal(e)
	}
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		s.Write(body)
		if header.Op == "put" {
			s.Close()
		}
	}()
	r := bufio.NewReader(s)
	var result map[string]any
	e = protocol.ReadJSONLine(r, 1<<20, &result)
	rest, readErr := io.ReadAll(r)
	resultBytes, _ := json.Marshal(result)
	transcript := append(resultBytes, rest...)
	secrets := []string{os.Getenv("CONNECTOR_TEST_S3_ACCESS"), os.Getenv("CONNECTOR_TEST_S3_SECRET"), h.cfg.Bucket, h.cfg.Endpoint}
	if h.sse != nil {
		headers := http.Header{}
		h.sse.Marshal(headers)
		encoded := headers.Get("X-Amz-Server-Side-Encryption-Customer-Key")
		if encoded != "" {
			secrets = append(secrets, encoded)
			raw, _ := base64.StdEncoding.DecodeString(encoded)
			secrets = append(secrets, string(raw))
		}
	}
	for _, secret := range secrets {
		if secret != "" && bytes.Contains(transcript, []byte(secret)) {
			t.Fatal("private storage configuration leaked onto stream")
		}
	}

	<-writeDone
	<-done
	if e != nil {
		t.Fatalf("result failed: %v", e)
	}
	if readErr != nil {
		t.Fatalf("body failed: %v", readErr)
	}
	return result, rest
}
func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
func putFrames(data []byte, wrongHash bool) []byte {
	var b bytes.Buffer
	for offset := 0; offset < len(data); {
		n := min(1<<20, len(data)-offset)
		binary.Write(&b, binary.BigEndian, uint32(n))
		b.Write(data[offset : offset+n])
		offset += n
	}
	binary.Write(&b, binary.BigEndian, uint32(0))
	sum := sha256.Sum256(data)
	if wrongHash {
		sum[0] ^= 1
	}
	json.NewEncoder(&b).Encode(map[string]any{"bytes": len(data), "sha256": hex.EncodeToString(sum[:])})
	return b.Bytes()
}
func resultOK(t *testing.T, r map[string]any) {
	t.Helper()
	if r["ok"] != true {
		t.Fatalf("operation failed: %#v", r)
	}
}
func errorCode(r map[string]any) string {
	e, _ := r["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}
func decodeGet(t *testing.T, b []byte) []byte {
	t.Helper()
	r := bytes.NewReader(b)
	var out bytes.Buffer
	for {
		var n uint32
		if e := binary.Read(r, binary.BigEndian, &n); e != nil {
			t.Fatal(e)
		}
		if n == 0 {
			break
		}
		if n > 1<<20 {
			t.Fatal("oversized frame")
		}
		if _, e := io.CopyN(&out, r, int64(n)); e != nil {
			t.Fatal(e)
		}
	}
	var trailer struct {
		Bytes int64  `json:"bytes"`
		SHA   string `json:"sha256"`
	}
	if e := json.NewDecoder(r).Decode(&trailer); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(out.Bytes())
	if trailer.Bytes != int64(out.Len()) || trailer.SHA != hex.EncodeToString(sum[:]) {
		t.Fatal("get checksum mismatch")
	}
	return out.Bytes()
}
func TestDockerStorageSixOperations(t *testing.T) {
	h, admin, cfg, _ := dockerFixture(t)
	for _, n := range []int{0, 73, (5 << 20) + 37} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), n)
			size := int64(n)
			key := fmt.Sprintf("item-%d", n)
			r, _ := storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64(key), Size: &size, ContentType: "application/octet-stream"}, putFrames(data, false))
			resultOK(t, r)
			r, body := storageExchange(t, h, protocol.StreamHeader{Op: "get", Key: b64(key)}, nil)
			resultOK(t, r)
			if !bytes.Equal(decodeGet(t, body), data) {
				t.Fatal("round trip mismatch")
			}
			r, _ = storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64(key)}, nil)
			resultOK(t, r)
			if r["size"] != float64(n) {
				t.Fatal("head size")
			}
			if n > 1 {
				r, body = storageExchange(t, h, protocol.StreamHeader{Op: "get", Key: b64(key), Range: []int64{0, 1}}, nil)
				resultOK(t, r)
				if !bytes.Equal(decodeGet(t, body), data[:2]) {
					t.Fatal("range")
				}
			}
			r, _ = storageExchange(t, h, protocol.StreamHeader{Op: "copy", SrcKey: b64(key), DstKey: b64(key + "-copy")}, nil)
			resultOK(t, r)
			r, _ = storageExchange(t, h, protocol.StreamHeader{Op: "list", Max: 1000, Recursive: true}, nil)
			resultOK(t, r)
			if n > 0 {
				obj, e := admin.GetObject(context.Background(), cfg.Bucket, cfg.Prefix+key, minio.GetObjectOptions{})
				if e == nil {
					_, e = io.ReadAll(obj)
					obj.Close()
				}
				if e == nil {
					t.Fatal("SSE-C object read without key")
				}
			}
			r, _ = storageExchange(t, h, protocol.StreamHeader{Op: "delete", Key: b64(key + "-copy")}, nil)
			resultOK(t, r)
			r, _ = storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64(key + "-copy")}, nil)
			if errorCode(r) != "not_found" {
				t.Fatal(r)
			}
		})
	}
	checks, e := h.Check(context.Background(), false)
	if e != nil {
		t.Fatalf("readonly checks: %v (%v)", e, checks)
	}
	checks, e = h.Check(context.Background(), true)
	if e != nil {
		t.Fatalf("write probe: %v (%v)", e, checks)
	}
	for object := range admin.ListObjects(context.Background(), cfg.Bucket, minio.ListObjectsOptions{Prefix: cfg.Prefix + ".cletus-connector/", Recursive: true}) {
		if object.Err != nil {
			t.Fatal(object.Err)
		}
		t.Fatal("probe left object behind")
	}
}
func TestDockerCommitAndConfinement(t *testing.T) {
	h, admin, cfg, _ := dockerFixture(t)
	for _, n := range []int{31, (5 << 20) + 31} {
		data := bytes.Repeat([]byte("z"), n)
		size := int64(n)
		key := fmt.Sprintf("atomic-%d", n)
		good := putFrames(data, false)
		for _, tc := range []struct {
			name string
			body []byte
			size int64
		}{{"checksum", putFrames(data, true), size}, {"trailing", append(append([]byte{}, good...), 1), size}, {"short", good, size + 1}, {"missing-trailer", good[:len(good)-90], size}, {"reset", good[:len(good)/2], size}} {
			t.Run(fmt.Sprintf("%d/%s", n, tc.name), func(t *testing.T) {
				r, _ := storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64(key), Size: &tc.size}, tc.body)
				if r["ok"] == true {
					t.Fatal("bad upload committed")
				}
				r, _ = storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64(key)}, nil)
				if errorCode(r) != "not_found" {
					t.Fatal(r)
				}
				for upload := range admin.ListIncompleteUploads(context.Background(), cfg.Bucket, cfg.Prefix, true) {
					if upload.Err != nil {
						t.Fatal(upload.Err)
					}
					t.Fatal("multipart left behind")
				}
			})
		}
	}
	for _, key := range []string{"../escape", "/absolute", "double//slash", "bad\\slash", "bad\x00key", "bad\u0085key", ".cletus-connector/probe/x", strings.Repeat("k", 1024)} {
		r, _ := storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64(key)}, nil)
		if c := errorCode(r); c != "invalid_key" && c != "key_too_long" {
			t.Fatalf("key accepted: %q %v", key, r)
		}
	}
}

func TestDockerReplacementPaginationAndBudget(t *testing.T) {
	h, admin, cfg, _ := dockerFixture(t)
	for _, key := range []string{"a", "b", "c", "folder/item"} {
		data := []byte("original")
		size := int64(len(data))
		result, _ := storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64(key), Size: &size}, putFrames(data, false))
		resultOK(t, result)
	}
	_, err := admin.PutObject(context.Background(), cfg.Bucket, "outside/object", strings.NewReader("hidden"), 6, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.PutObject(context.Background(), cfg.Bucket, cfg.Prefix+".cletus-connector/hidden", strings.NewReader("hidden"), 6, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{31, (5 << 20) + 31} {
		data := bytes.Repeat([]byte("z"), size)
		n := int64(size)
		result, _ := storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64("a"), Size: &n}, putFrames(data, true))
		if result["ok"] == true {
			t.Fatal("replacement committed")
		}
		result, body := storageExchange(t, h, protocol.StreamHeader{Op: "get", Key: b64("a")}, nil)
		resultOK(t, result)
		if string(decodeGet(t, body)) != "original" {
			t.Fatal("existing object changed")
		}
	}
	token := ""
	seen := map[string]bool{}
	for page := 0; page < 10; page++ {
		result, _ := storageExchange(t, h, protocol.StreamHeader{Op: "list", Max: 1, Recursive: true, Token: token}, nil)
		resultOK(t, result)
		for _, raw := range result["objects"].([]any) {
			entry := raw.(map[string]any)
			key, err := base64.RawURLEncoding.DecodeString(entry["key"].(string))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(key), "hidden") || strings.HasPrefix(string(key), "outside/") {
				t.Fatal("namespace escaped")
			}
			if seen[string(key)] {
				t.Fatal("repeated page")
			}
			seen[string(key)] = true
		}
		if result["truncated"] != true {
			break
		}
		token = result["next_token"].(string)
	}
	if len(seen) != 4 {
		t.Fatalf("pagination lost objects: %v", seen)
	}
	h.mu.Lock()
	h.cfg.MaxReadBytesPerHour = 8
	h.reads = 0
	h.mu.Unlock()
	result, body := storageExchange(t, h, protocol.StreamHeader{Op: "get", Key: b64("a")}, nil)
	resultOK(t, result)
	decodeGet(t, body)
	result, _ = storageExchange(t, h, protocol.StreamHeader{Op: "get", Key: b64("b")}, nil)
	if errorCode(result) != "rate_limited" {
		t.Fatalf("read budget bypass: %v", result)
	}
	h.mu.Lock()
	h.tokens = 0
	h.cfg.MaxOpsPerSecond = 0.000001
	h.last = time.Now()
	h.mu.Unlock()
	result, _ = storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64("a")}, nil)
	if errorCode(result) != "rate_limited" {
		t.Fatalf("op budget bypass: %v", result)
	}
}

func TestDockerKeyBoundaryAndOversizeAbort(t *testing.T) {
	h, admin, cfg, _ := dockerFixture(t)
	key := strings.Repeat(strings.Repeat("k", 100)+"/", 10)
	key += strings.Repeat("k", 1024-len(cfg.Prefix)-len(key))
	size := int64(1)
	result, _ := storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64(key), Size: &size}, putFrames([]byte("x"), false))
	resultOK(t, result)
	result, _ = storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64(key + "x")}, nil)
	if errorCode(result) != "key_too_long" {
		t.Fatal(result)
	}
	h.max = 5 << 20
	unknown := int64(-1)
	result, _ = storageExchange(t, h, protocol.StreamHeader{Op: "put", Key: b64("oversize"), Size: &unknown}, putFrames(bytes.Repeat([]byte("x"), (5<<20)+1), false))
	if errorCode(result) != "too_large" {
		t.Fatal(result)
	}
	result, _ = storageExchange(t, h, protocol.StreamHeader{Op: "head", Key: b64("oversize")}, nil)
	if errorCode(result) != "not_found" {
		t.Fatal(result)
	}
	for upload := range admin.ListIncompleteUploads(context.Background(), cfg.Bucket, cfg.Prefix, true) {
		if upload.Err != nil {
			t.Fatal(upload.Err)
		}
		t.Fatal("oversize left multipart upload")
	}
}
