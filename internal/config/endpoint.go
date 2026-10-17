package config

import (
	"fmt"
	"strings"
)

// WebSocket is the pseudo-method of a WebSocket upgrade request.
const WebSocket = "WEBSOCKET"

// Methods are the methods an allowed-endpoints entry may name.
var Methods = []string{"GET", "POST", "PUT", "DELETE", WebSocket}

// Endpoint is one allowed-endpoints entry: a method and a path. A path that
// ends in /* matches every path one or more segments below it.
type Endpoint struct{ Method, Path string }

func (e Endpoint) String() string { return e.Method + " " + e.Path }

// Matches reports whether a request (method WEBSOCKET for an upgrade) on an
// already canonical path is allowed by e.
func (e Endpoint) Matches(method, path string) bool {
	if method != e.Method {
		return false
	}
	if base, ok := strings.CutSuffix(e.Path, "*"); ok {
		return len(path) > len(base) && strings.HasPrefix(path, base)
	}
	return path == e.Path
}

// ParseEndpoint parses an entry such as "POST /v1/*" or "WEBSOCKET /v1/stream".
func ParseEndpoint(s string) (Endpoint, error) {
	method, path, ok := strings.Cut(strings.TrimSpace(s), " ")
	path = strings.TrimSpace(path)
	if !ok || path == "" {
		return Endpoint{}, fmt.Errorf("%q must be METHOD /path, e.g. \"POST /v1/*\"", s)
	}
	known := false
	for _, m := range Methods {
		known = known || m == method
	}
	if !known {
		return Endpoint{}, fmt.Errorf("%q: method must be %s", s, strings.Join(Methods, ", "))
	}
	if !strings.HasPrefix(path, "/") {
		return Endpoint{}, fmt.Errorf("%q: the path must start with /", s)
	}
	exact := path
	if strings.Contains(path, "*") {
		base, ok := strings.CutSuffix(path, "/*")
		if !ok || strings.Contains(base, "*") {
			return Endpoint{}, fmt.Errorf("%q: * is allowed only as the whole last segment, as in /v1/*", s)
		}
		if base == "" {
			return Endpoint{method, path}, nil
		}
		if strings.HasSuffix(base, "/") {
			return Endpoint{}, fmt.Errorf("%q: unsafe path", s)
		}
		exact = base
	}
	if err := ValidatePath(exact); err != nil {
		return Endpoint{}, fmt.Errorf("%q: %v", s, err)
	}
	return Endpoint{method, path}, nil
}

// ValidatePath accepts only a plain absolute path: no encoding, query,
// fragment, backslash, empty or dot segments. Request paths are checked with
// it before they are matched.
func ValidatePath(p string) error {
	if len(p) == 0 || len(p) > 256 || p[0] != '/' || strings.ContainsAny(p, "%\\\x00\r\n?#") || strings.Contains(p, "//") {
		return fmt.Errorf("unsafe path %q", p)
	}
	for _, s := range strings.Split(p, "/") {
		if s == "." || s == ".." {
			return fmt.Errorf("unsafe path %q", p)
		}
	}
	for _, r := range p {
		if r < 0x21 || r == 0x7f {
			return fmt.Errorf("unsafe path %q", p)
		}
	}
	return nil
}
