package connector

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func TestStorageDispatchAndPrivateAdvertisement(t *testing.T) {
	st := config.StorageDefaults()
	st.Endpoint, st.Bucket, st.AccessKey, st.SecretKey, st.EncryptionKey, st.Encryption, st.ReadOnly = "https://storage.corp.example", "private-bucket-sentinel", "private-access-sentinel", "private-secret-sentinel", []byte("0123456789abcdef0123456789abcdef"), "sse-c", true
	files := config.Service{Name: "files", Kind: config.KindS3, MaxConcurrentRequests: 16, Storage: &st}
	x := &Connector{services: map[string]*service{"files": {cfg: files}, "chat": {cfg: config.Service{Name: "chat", Kind: config.KindHTTP}}}, order: []string{"files", "chat"}}
	encoded, err := json.Marshal(x.HelloServices())
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{st.Bucket, st.Endpoint, st.AccessKey, st.SecretKey, string(st.EncryptionKey)} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("private configuration advertised")
		}
	}
	svc := x.HelloServices()[0]
	if svc.Kind != protocol.KindS3 || svc.Storage == nil || svc.Storage.MaxKeyBytes != protocol.MaxStorageKeyBytes || svc.Storage.MaxObjectBytes != 5<<30 || svc.Storage.Encryption != "sse-c" || !svc.Storage.ReadOnly || svc.Healthy || svc.MaxConcurrentRequests != 16 || len(svc.Endpoints) != 0 {
		t.Fatal(svc)
	}
	for _, tc := range []struct{ kind, service, want string }{{"future", "chat", "unsupported_kind"}, {"storage", "missing", "route_not_found"}, {"storage", "chat", "wrong_route_type"}, {"storage", "files", "backend_unavailable"}} {
		a, b := net.Pipe()
		done := make(chan struct{})
		go func() { defer close(done); x.ServeStream(context.Background(), b) }()
		a.SetDeadline(time.Now().Add(time.Second))
		protocol.WriteStreamHeader(a, protocol.StreamHeader{V: protocol.Version, Service: tc.service, Kind: tc.kind, RequestID: "r-1", Op: "head", Key: protocol.EncodeStorageKey("a")})
		result, err := protocol.ReadStorageResult(bufio.NewReader(a), "head")
		a.Close()
		<-done
		if err != nil || result.Error == nil || result.Error.Code != tc.want {
			t.Fatalf("%s: %+v %v", tc.kind, result, err)
		}
	}
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); x.ServeStream(context.Background(), b) }()
	a.SetDeadline(time.Now().Add(time.Second))
	protocol.WriteStreamHeader(a, protocol.StreamHeader{V: protocol.Version, Service: "files", Kind: "http", RequestID: "r-1"})
	response, _ := io.ReadAll(a)
	a.Close()
	<-done
	if !strings.Contains(string(response), "403") || !strings.Contains(string(response), "path_not_allowed") {
		t.Fatal(string(response))
	}
}

func TestStorageInitializationFailureKeepsInferenceAvailable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ready") }))
	defer upstream.Close()
	x, err := New(chatConfig(t, upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := config.StorageDefaults()
	st.Endpoint = "invalid"
	storageService := &service{cfg: config.Service{Name: "files", Kind: config.KindS3, Storage: &st}}
	x.mu.Lock()
	x.services["files"] = storageService
	x.order = append(x.order, "files")
	x.mu.Unlock()
	x.storageInit.Add(1)
	go func() { defer x.storageInit.Done(); x.startStorage(ctx, storageService) }()
	response, body := exchange(t, x, "chat", get("/v1/models"))
	if response.StatusCode != 200 || string(body) != "ready" {
		t.Fatal("storage initialization interrupted inference")
	}
	cancel()
	done := make(chan struct{})
	go func() { x.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("storage retry did not stop on shutdown")
	}
}
