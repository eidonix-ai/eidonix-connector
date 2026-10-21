package config

import (
	"strings"
	"testing"
)

// A GPU site's file: a machine with ten ports beside a one-port
// server, both forms in one file.
const gpuSite = `services:
  cn0:
    address: 10.0.0.30
    ports:
      8004:
        allowed-endpoints: [POST /v1/*, GET /v1/models]
        models: [qwen3.5-9b-vision]
        health: /health
        max-concurrent-requests: 8
      8012:
        allowed-endpoints: [POST /v1/chat/completions, GET /v1/models]
        models:
          PaddleOCR-VL-1.6-0.9B: {alias: paddle-ocr-vl}
      8002:
        allowed-endpoints: [POST /layout-parsing, POST /restructure-pages]
        health: /health
      8001:
        allowed-endpoints: [POST /embeddings]
        models:
          nomic-ai/nomic-embed-text-v1.5: {alias: nomic-embed}
      8006:
        allowed-endpoints: [POST /v1/rerank]
        models:
          jina-reranker-v3: {alias: jina-reranker}
      8008:
        allowed-endpoints: [POST /v1/rerank]
        models:
          bge-reranker-v2-m3-Q8_0.gguf: {alias: bge-reranker}
      8007:
        allowed-endpoints: [POST /v1/audio/transcriptions]
        models:
          /models/faster-whisper-large-v3-turbo: {alias: whisper}
      8009:
        allowed-endpoints: [POST /v1/audio/speech, GET /v1/audio/voices]
        models: [kokoro]
      8010:
        allowed-endpoints: [POST /v1/images/*]
        models: [qwen-image-2.1]
        max-concurrent-requests: 1
      8011:
        allowed-endpoints: [WEBSOCKET /v1/stream, GET /v1/config]
        health: /health
        max-concurrent-requests: 4
        idle-timeout: 30m
  cn3:
    address: 10.0.0.33:8000
    allowed-endpoints: [POST /v1/*, GET /v1/models]
    models: [DeepSeek-V4.1-Flash-1017]
    health: /health
    max-concurrent-requests: 32
`

func TestParseMachine(t *testing.T) {
	c, err := parse(t, gpuSite)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Services) != 2 || !c.Services[0].Machine() || c.Services[1].Machine() {
		t.Fatalf("services = %+v", c.Services)
	}
	cn0 := c.Services[0]
	if len(cn0.Ports) != 10 || cn0.Address != "10.0.0.30" || cn0.URL != "" {
		t.Fatalf("cn0 = %+v", cn0)
	}
	q := cn0.Ports[0]
	if q.Name != "cn0:8004" || q.Address != "10.0.0.30:8004" || q.URL != "http://10.0.0.30:8004" ||
		q.MaxConcurrentRequests != 8 || q.Health != "/health" || q.Models.Names[0] != "qwen3.5-9b-vision" {
		t.Fatalf("port 8004 = %+v", q)
	}
	if cn0.MaxConcurrentRequests != 8+5*DefaultMaxConcurrentRequests+1+4+2*DefaultMaxConcurrentRequests || cn0.IdleTimeout.String() != "30m0s" {
		t.Fatalf("machine limits: %d %v", cn0.MaxConcurrentRequests, cn0.IdleTimeout)
	}
	d := c.Describe()
	for _, want := range []string{"cn0:8004", "10.0.0.30:8004", "cn0:8011", "cn3"} {
		if !strings.Contains(d, want) {
			t.Fatalf("check output lacks %q:\n%s", want, d)
		}
	}
}

func TestParseMachineTLS(t *testing.T) {
	c, err := parse(t, "services:\n  gpu:\n    address: https://gpu.example\n    ports:\n      8443:\n        allowed-endpoints: [POST /v1/*]\n")
	if err != nil || c.Services[0].Ports[0].URL != "https://gpu.example:8443" {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestParseMachineRefusals(t *testing.T) {
	port := func(n, body string) string { return "      " + n + ":\n" + body }
	chat := "        allowed-endpoints: [POST /v1/chat/completions]\n"
	for want, body := range map[string]string{
		"put it under the port":  "services:\n  m:\n    address: 10.0.0.5\n    models: [a]\n    ports:\n" + port("1", chat),
		"without a port":         "services:\n  m:\n    address: 10.0.0.5:8000\n    ports:\n" + port("1", chat),
		"without http://":        "services:\n  m:\n    address: http://10.0.0.5\n    ports:\n" + port("1", chat),
		"address is required":    "services:\n  m:\n    ports:\n" + port("1", chat),
		"ports must list":        "services:\n  m:\n    address: 10.0.0.5\n    ports: {}\n",
		"must be a number":       "services:\n  m:\n    address: 10.0.0.5\n    ports:\n" + port("http", chat),
		"port already listed":    "services:\n  m:\n    address: 10.0.0.5\n    ports:\n" + port("80", chat) + port("80", chat),
		`unknown setting "kind"`: "services:\n  m:\n    address: 10.0.0.5\n    ports:\n" + port("80", chat+"        kind: s3\n"),
		`model "a" is on port 80 and port 81`: "services:\n  m:\n    address: 10.0.0.5\n    ports:\n" +
			port("80", chat+"        models: [a]\n") + port("81", chat+"        models: [a, b]\n"),
		"both must list their models": "services:\n  m:\n    address: 10.0.0.5\n    ports:\n" +
			port("80", "        allowed-endpoints: [POST /v1/*]\n        models: [a]\n") + port("81", chat),
	} {
		_, err := parse(t, body)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: got %v", want, err)
		}
	}
}

func TestEndpointOverlaps(t *testing.T) {
	e := func(s string) Endpoint { x, _ := ParseEndpoint(s); return x }
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"POST /v1/*", "POST /v1/chat/completions", true},
		{"POST /v1/chat/completions", "POST /v1/*", true},
		{"POST /v1/*", "POST /v1/images/*", true},
		{"POST /v1/rerank", "POST /v1/rerank", true},
		{"POST /v1/rerank", "GET /v1/rerank", false},
		{"POST /v1/*", "POST /layout-parsing", false},
		{"POST /v1/a", "POST /v1/b", false},
	} {
		if got := e(tc.a).Overlaps(e(tc.b)); got != tc.want {
			t.Errorf("%s ∩ %s = %v", tc.a, tc.b, got)
		}
	}
}
