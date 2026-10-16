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

	"github.com/eidonix-ai/cletus-connector/internal/config"
	"github.com/eidonix-ai/cletus-connector/protocol"
)

func TestStorageDispatchAndPrivateAdvertisement(t *testing.T) {
	cfg := config.Route{ID: "files", Type: "object_storage", Endpoint: "https://storage.corp.example", Bucket: "private-bucket-sentinel", Prefix: "documents/", CredentialsFile: "private-credential-sentinel", Encryption: config.Encryption{Mode: "sse-c", KeyFile: "private-key-sentinel"}, MaxObjectBytes: "5GiB", MaxConcurrency: 16}
	x := &Connector{routes: map[string]*route{"files": {cfg: cfg}, "chat": {cfg: config.Route{ID: "chat", Type: "openai_chat"}}}, order: []string{"files", "chat"}}
	encoded, err := json.Marshal(x.HelloServices())
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{cfg.Bucket, cfg.Prefix, cfg.Endpoint, cfg.CredentialsFile, cfg.Encryption.KeyFile} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("private configuration advertised")
		}
	}
	route := x.HelloServices()[0]
	if route.Storage == nil || route.Storage.MaxKeyBytes != 1024-len(cfg.Prefix) || route.Storage.MaxObjectBytes != 5<<30 || route.Healthy {
		t.Fatal(route)
	}
	for _, tc := range []struct{ kind, route, want string }{{"future", "chat", "unsupported_kind"}, {"storage", "missing", "route_not_found"}, {"storage", "chat", "wrong_route_type"}, {"storage", "files", "backend_unavailable"}} {
		a, b := net.Pipe()
		done := make(chan struct{})
		go func() { defer close(done); x.ServeStream(context.Background(), b) }()
		a.SetDeadline(time.Now().Add(time.Second))
		protocol.WriteStreamHeader(a, protocol.StreamHeader{V: protocol.Version, Service: tc.route, Kind: tc.kind, RequestID: "r-1", Op: "head", Key: protocol.EncodeStorageKey("a")})
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
	x, err := New(reviewConfig(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storageRoute := &route{cfg: config.Route{ID: "files", Type: "object_storage", Endpoint: "invalid"}}
	x.mu.Lock()
	x.routes["files"] = storageRoute
	x.order = append(x.order, "files")
	x.mu.Unlock()
	x.storageInit.Add(1)
	go func() { defer x.storageInit.Done(); x.startStorage(ctx, storageRoute) }()
	response, body := reviewRequest(t, x, "GET /v1/models HTTP/1.1\r\nHost: ignored\r\n\r\n")
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
