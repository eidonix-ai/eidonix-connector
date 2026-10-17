package convert

// The v0.2 service types: their fixed request tables, API prefixes and
// defaults. v0.3 has no types; these are an input-format reference for
// converting v0.1 and v0.2 files.

import (
	"strings"
	"time"
)

// webSocket is the pseudo-method of a WebSocket upgrade request.
const webSocket = "WEBSOCKET"

// v02Request is one accepted request: method (GET, POST, PUT or WEBSOCKET) and
// the exact path as the gateway sends it.
type v02Request struct{ Method, Path string }

func (r v02Request) String() string { return r.Method + " " + r.Path }

// v02Type is one v0.2 service type's fixed contract and defaults.
type v02Type struct {
	Name     string
	Requests []v02Request
	// Prefix is stripped from request paths when the service sets api_path.
	Prefix        string
	Models        bool
	MaxConcurrent int
	Idle          time.Duration
}

// v02HealthPath is the path v0.2 probed on every non-storage service.
const v02HealthPath = "/health"

func post(p string) v02Request { return v02Request{"POST", p} }
func get(p string) v02Request  { return v02Request{"GET", p} }

var v02Table = []v02Type{
	{Name: "chat", Requests: []v02Request{post("/v1/chat/completions"), post("/v1/responses"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 15 * time.Minute},
	{Name: "anthropic", Requests: []v02Request{post("/v1/messages"), post("/v1/messages/count_tokens"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 15 * time.Minute},
	{Name: "embeddings", Requests: []v02Request{post("/v1/embeddings"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 2 * time.Minute},
	{Name: "rerank", Requests: []v02Request{post("/v1/rerank"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 2 * time.Minute},
	{Name: "transcription", Requests: []v02Request{post("/v1/audio/transcriptions"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 8, Idle: 5 * time.Minute},
	{Name: "speech", Requests: []v02Request{post("/v1/audio/speech"), get("/v1/audio/voices")}, Prefix: "/v1", Models: true, MaxConcurrent: 8, Idle: 2 * time.Minute},
	{Name: "image", Requests: []v02Request{post("/v1/images/generations"), post("/v1/images/edits")}, Prefix: "/v1", Models: true, MaxConcurrent: 2, Idle: 10 * time.Minute},
	{Name: "document", Requests: []v02Request{post("/layout-parsing")}, MaxConcurrent: 4, Idle: 10 * time.Minute},
	{Name: "conversion", Requests: []v02Request{post("/convert"), post("/extract")}, MaxConcurrent: 4, Idle: 10 * time.Minute},
	{Name: "realtime", Requests: []v02Request{{webSocket, "/v1/stream"}, get("/v1/config")}, Prefix: "/v1", MaxConcurrent: 4, Idle: 5 * time.Minute},
	// Storage requests are typed storage streams, never HTTP.
	{Name: "storage", MaxConcurrent: 16, Idle: 5 * time.Minute},
	// Custom requests are exactly the service's own paths list.
	{Name: "custom", MaxConcurrent: 32, Idle: 2 * time.Minute},
}

// v02Types returns every v0.2 type in canonical order.
func v02Types() []v02Type { return append([]v02Type(nil), v02Table...) }

// v02Lookup returns the named v0.2 type.
func v02Lookup(name string) (v02Type, bool) {
	for _, t := range v02Table {
		if t.Name == name {
			return t, true
		}
	}
	return v02Type{}, false
}

// Allows reports whether the type's fixed contract contains method and path.
func (t v02Type) Allows(method, path string) bool {
	for _, r := range t.Requests {
		if r.Method == method && r.Path == path {
			return true
		}
	}
	return false
}

// UpstreamPath maps a gateway-facing path to the server's path: with apiPath
// set, the type's prefix is replaced by apiPath ("/" meaning the server root);
// without it the path is unchanged.
func (t v02Type) UpstreamPath(apiPath, path string) string {
	if apiPath == "" {
		return path
	}
	rest := strings.TrimPrefix(path, t.Prefix)
	if apiPath == "/" {
		return rest
	}
	return apiPath + rest
}
