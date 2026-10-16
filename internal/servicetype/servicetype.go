// Package servicetype is the compiled, default-deny contract of every service
// type: exactly which requests it accepts, their default API prefix, whether it
// has models, and its default limits. Nothing in a configuration file can add
// a request to a type; a custom service lists its own requests explicitly.
package servicetype

import (
	"fmt"
	"strings"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// WebSocket is the pseudo-method of a WebSocket upgrade request.
const WebSocket = "WEBSOCKET"

// Request is one accepted request: method (GET, POST, PUT or WEBSOCKET) and
// the exact path as the gateway sends it.
type Request struct{ Method, Path string }

func (r Request) String() string { return r.Method + " " + r.Path }

// Type is one service type's fixed contract and defaults.
type Type struct {
	Name     string
	Requests []Request
	// Prefix is stripped from request paths when the service sets api_path.
	Prefix          string
	Models          bool
	MaxConcurrent   int
	Idle            time.Duration
	MaxRequestBytes int64
	ContentTypes    []string
	// Headers are request headers forwarded in addition to Content-Type, Accept and X-Request-Id.
	Headers []string
}

// Internal defaults shared by every type.
const (
	MaxResponseBytes         int64 = 256 << 20
	MaxWebSocketMessageBytes       = 4 << 20
	HealthInterval                 = 15 * time.Second
	HealthPath                     = "/health"
	EjectFor                       = 30 * time.Second
)

var (
	json      = []string{"application/json"}
	multipart = []string{"multipart/form-data"}
)

func post(p string) Request { return Request{"POST", p} }
func get(p string) Request  { return Request{"GET", p} }

var table = []Type{
	{Name: protocol.TypeChat, Requests: []Request{post("/v1/chat/completions"), post("/v1/responses"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 15 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: json},
	{Name: protocol.TypeAnthropic, Requests: []Request{post("/v1/messages"), post("/v1/messages/count_tokens"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 15 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: json, Headers: []string{"Anthropic-Version", "Anthropic-Beta"}},
	{Name: protocol.TypeEmbeddings, Requests: []Request{post("/v1/embeddings"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 2 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: json},
	{Name: protocol.TypeRerank, Requests: []Request{post("/v1/rerank"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 32, Idle: 2 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: json},
	{Name: protocol.TypeTranscription, Requests: []Request{post("/v1/audio/transcriptions"), get("/v1/models")}, Prefix: "/v1", Models: true, MaxConcurrent: 8, Idle: 5 * time.Minute, MaxRequestBytes: 50 << 20, ContentTypes: multipart},
	{Name: protocol.TypeSpeech, Requests: []Request{post("/v1/audio/speech"), get("/v1/audio/voices")}, Prefix: "/v1", Models: true, MaxConcurrent: 8, Idle: 2 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: json},
	{Name: protocol.TypeImage, Requests: []Request{post("/v1/images/generations"), post("/v1/images/edits")}, Prefix: "/v1", Models: true, MaxConcurrent: 2, Idle: 10 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: []string{"application/json", "multipart/form-data"}},
	{Name: protocol.TypeDocument, Requests: []Request{post("/layout-parsing")}, MaxConcurrent: 4, Idle: 10 * time.Minute, MaxRequestBytes: 64 << 20, ContentTypes: json},
	{Name: protocol.TypeConversion, Requests: []Request{post("/convert"), post("/extract")}, MaxConcurrent: 4, Idle: 10 * time.Minute, MaxRequestBytes: 100 << 20, ContentTypes: []string{"multipart/form-data", "application/octet-stream", "application/pdf", "text/plain"}},
	{Name: protocol.TypeRealtime, Requests: []Request{{WebSocket, "/v1/stream"}, get("/v1/config")}, Prefix: "/v1", MaxConcurrent: 4, Idle: 5 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: json},
	// Storage requests are typed storage streams, never HTTP.
	{Name: protocol.TypeStorage, MaxConcurrent: 16, Idle: 5 * time.Minute},
	// Custom requests are exactly the service's own paths list.
	{Name: protocol.TypeCustom, MaxConcurrent: 32, Idle: 2 * time.Minute, MaxRequestBytes: 16 << 20, ContentTypes: []string{"application/json", "multipart/form-data", "application/octet-stream", "text/plain"}},
}

// All returns every type in canonical order.
func All() []Type { return append([]Type(nil), table...) }

// Lookup returns the named type.
func Lookup(name string) (Type, bool) {
	for _, t := range table {
		if t.Name == name {
			return t, true
		}
	}
	return Type{}, false
}

// Allows reports whether the type's fixed contract contains method and path.
func (t Type) Allows(method, path string) bool {
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
func (t Type) UpstreamPath(apiPath, path string) string {
	if apiPath == "" {
		return path
	}
	rest := strings.TrimPrefix(path, t.Prefix)
	if apiPath == "/" {
		return rest
	}
	return apiPath + rest
}

// ValidatePath accepts only a plain absolute path: no encoding, query,
// fragment, backslash, empty or dot segments.
func ValidatePath(p string) error {
	if len(p) == 0 || len(p) > 256 || p[0] != '/' || strings.ContainsAny(p, "%\\\x00\r\n?#") || strings.Contains(p, "//") {
		return fmt.Errorf("unsafe path %q", p)
	}
	for _, s := range strings.Split(p, "/") {
		if s == "." || s == ".." {
			return fmt.Errorf("unsafe path %q", p)
		}
	}
	return nil
}

// ParseRequest parses a custom service entry such as "POST /v1/run" or "WEBSOCKET /v1/stream".
func ParseRequest(s string) (Request, error) {
	method, path, ok := strings.Cut(strings.TrimSpace(s), " ")
	path = strings.TrimSpace(path)
	if !ok {
		return Request{}, fmt.Errorf("%q must be METHOD /path, e.g. \"POST /v1/run\"", s)
	}
	switch method {
	case "GET", "POST", "PUT", WebSocket:
	default:
		return Request{}, fmt.Errorf("%q: method must be GET, POST, PUT or WEBSOCKET", s)
	}
	if err := ValidatePath(path); err != nil {
		return Request{}, fmt.Errorf("%q: %v", s, err)
	}
	return Request{method, path}, nil
}
