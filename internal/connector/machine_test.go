package connector

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// machineRig is a machine on 127.0.0.1 with three ports: two chat servers
// sharing chat completions (routed by model) and a document parser.
func machineRig(t *testing.T) (*Connector, *config.Config, map[string]*fakeServer) {
	t.Helper()
	servers := map[string]*fakeServer{"qwen": {}, "ocr": {}, "doc": {}}
	port := map[string]string{}
	for name, f := range servers {
		u, _ := url.Parse(f.start(t).URL)
		port[name] = u.Port()
	}
	yaml := fmt.Sprintf(`  gpu:
    address: 127.0.0.1
    ports:
      %s:
        allowed-endpoints: [POST /v1/*, GET /v1/models]
        models: [qwen-vl]
        max-concurrent-requests: 8
      %s:
        allowed-endpoints: [POST /v1/chat/completions, GET /v1/models]
        models:
          PaddleOCR-VL-0.9B: {alias: ocr-vl}
      %s:
        allowed-endpoints: [POST /layout-parsing]
`, port["qwen"], port["ocr"], port["doc"])
	c, err := config.Parse([]byte(upstreamYAML + yaml))
	if err != nil {
		t.Fatal(err)
	}
	c.AccessLog = t.TempDir() + "/access.log"
	return newConnector(t, c), c, servers
}

// A request to a machine goes to the only port that allows it, or to the
// port listing the model it names.
func TestMachineRoutesByEndpointThenModel(t *testing.T) {
	x, cfg, f := machineRig(t)
	resp, _ := exchange(t, x, "gpu", post("/v1/chat/completions", "application/json", `{"model":"qwen-vl","messages":[]}`))
	if resp.StatusCode != 200 || f["qwen"].hits.Load() != 1 || f["ocr"].hits.Load() != 0 {
		t.Fatalf("qwen-vl: %d (qwen %d, ocr %d)", resp.StatusCode, f["qwen"].hits.Load(), f["ocr"].hits.Load())
	}
	if rec := lastAccess(t, cfg.AccessLog); rec["service"] != "gpu" || rec["model"] != "qwen-vl" || !strings.HasPrefix(rec["upstream_host"].(string), "127.0.0.1:") {
		t.Fatalf("access log %v", rec)
	}
	// The other port's model; its alias is rewritten as for any service.
	resp, _ = exchange(t, x, "gpu", post("/v1/chat/completions", "application/json", `{"model":"ocr-vl","messages":[]}`))
	if _, body, _ := f["ocr"].last(); resp.StatusCode != 200 || body != `{"model":"PaddleOCR-VL-0.9B","messages":[]}` {
		t.Fatalf("ocr-vl: %d %q", resp.StatusCode, body)
	}
	// An endpoint only one port allows needs no model.
	resp, _ = exchange(t, x, "gpu", post("/layout-parsing", "application/json", `{"file":"x"}`))
	if resp.StatusCode != 200 || f["doc"].hits.Load() != 1 {
		t.Fatalf("layout-parsing: %d", resp.StatusCode)
	}
	// Only one port allows /v1/responses: it goes there.
	if resp, _ = exchange(t, x, "gpu", post("/v1/responses", "application/json", `{"model":"qwen-vl"}`)); resp.StatusCode != 200 || f["qwen"].hits.Load() != 2 {
		t.Fatalf("responses: %d", resp.StatusCode)
	}
}

func TestMachineRefusals(t *testing.T) {
	x, _, f := machineRig(t)
	for _, tc := range []struct{ request, code string }{
		{post("/v1/chat/completions", "application/json", `{"messages":[]}`), "model_required"},
		{post("/v1/chat/completions", "application/json", `{"model":"gpt-9"}`), "model_not_allowed"},
		{post("/v1/chat/completions", "multipart/form-data; boundary=b", "--b--\r\n"), "model_required"},
		{post("/v1/chat/completions", "application/json", `{"model":`), "invalid_request"},
		{post("/restructure-pages", "application/json", `{"model":"qwen-vl"}`), "path_not_allowed"},
	} {
		resp, _ := exchange(t, x, "gpu", tc.request)
		if resp.Header.Get(protocol.HeaderError) != tc.code {
			t.Errorf("%q: %d %s, want %s", tc.request[:40], resp.StatusCode, resp.Header.Get(protocol.HeaderError), tc.code)
		}
	}
	if f["qwen"].hits.Load()+f["ocr"].hits.Load()+f["doc"].hits.Load() != 0 {
		t.Fatal("a refused request reached a server")
	}
}

// GET /v1/models, which two ports share, lists their models.
func TestMachineListsItsModels(t *testing.T) {
	x, _, f := machineRig(t)
	resp, body := exchange(t, x, "gpu", get("/v1/models"))
	var list struct {
		Data []struct {
			ID      string `json:"id"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}
	if resp.StatusCode != 200 || json.Unmarshal(body, &list) != nil || len(list.Data) != 2 || list.Data[0].ID != "qwen-vl" || list.Data[1].ID != "ocr-vl" || list.Data[0].OwnedBy != "gpu" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if f["qwen"].hits.Load()+f["ocr"].hits.Load() != 0 {
		t.Fatal("the list reached a server")
	}
}

// A machine is healthy while any port is; its parts say which are down,
// and a request for a down port is refused on its own.
func TestMachineHealth(t *testing.T) {
	x, _, _ := machineRig(t)
	gpu := x.services["gpu"]
	hello := x.HelloServices()[0]
	if !hello.Healthy || len(hello.Parts) != 3 || protocol.Health(hello.Healthy, hello.Parts) != protocol.HealthOK ||
		hello.MaxConcurrentRequests != 8+2*config.DefaultMaxConcurrentRequests || strings.Join(hello.Models, ",") != "qwen-vl,ocr-vl" {
		t.Fatalf("hello = %+v", hello)
	}
	x.mu.Lock()
	gpu.ports[1].eject = time.Now().Add(time.Minute) // the OCR model's server is down
	x.mu.Unlock()
	st := x.StatusServices()[0]
	if !st.Healthy || st.Parts[1].Healthy || !st.Parts[0].Healthy || protocol.Health(st.Healthy, st.Parts) != protocol.HealthDegraded {
		t.Fatalf("status = %+v", st)
	}
	if snap := x.Snapshot()[0]; snap.Health != protocol.HealthDegraded {
		t.Fatalf("snapshot = %+v", snap)
	}
	if resp, _ := exchange(t, x, "gpu", post("/v1/chat/completions", "application/json", `{"model":"ocr-vl"}`)); resp.Header.Get(protocol.HeaderError) != "upstream_unavailable" {
		t.Fatalf("down port: %d %s", resp.StatusCode, resp.Header.Get(protocol.HeaderError))
	}
	if resp, _ := exchange(t, x, "gpu", post("/v1/chat/completions", "application/json", `{"model":"qwen-vl"}`)); resp.StatusCode != 200 {
		t.Fatalf("healthy port: %d", resp.StatusCode)
	}
	x.mu.Lock()
	for _, p := range gpu.ports {
		p.eject = time.Now().Add(time.Minute)
	}
	x.mu.Unlock()
	if st := x.StatusServices()[0]; st.Healthy || protocol.Health(st.Healthy, st.Parts) != protocol.HealthDown {
		t.Fatalf("all down = %+v", st)
	}
}
