package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func good() Config {
	return Config{Server: "https://example.com", TokenFile: "/tmp/token", ServerKeys: map[string]string{"test": base64.StdEncoding.EncodeToString(make([]byte, 32))}, Routes: []Route{{ID: "chat", Type: "openai_chat", Upstreams: []string{"http://127.0.0.1:1234"}}}}
}
func TestValidation(t *testing.T) {
	for _, change := range []func(*Config){func(c *Config) { c.Routes[0].Upstreams = []string{"http://host:80/path"} }, func(c *Config) { c.HealthListen = "0.0.0.0:1234" }, func(c *Config) {
		c.Routes[0].Flavor = "ollama"
		c.Routes[0].ExtraPaths = [][2]string{{"POST", "/api/pull"}}
	}, func(c *Config) { c.Routes[0].UpstreamHeaders = map[string]string{"Bad(Header": "secret"} }} {
		c := good()
		change(&c)
		if c.Validate() == nil {
			t.Fatal("unsafe config accepted")
		}
	}
	c := good()
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	before := len(c.Routes[0].MethodsPaths)
	if e := c.Validate(); e != nil || len(c.Routes[0].MethodsPaths) != before {
		t.Fatal("validation not idempotent")
	}
}
func TestStrictTOML(t *testing.T) {
	base := "server = \"https://example.com\"\ntoken_file = \"/tmp/token\"\n"
	for _, tc := range []struct{ data, want string }{
		{"remote_command = \"bad\"\n" + base, "remote_command"},
		{base + "[[routes]]\nid = \"chat\"\ncommand = \"x\"\n", "routes.command"},
		{base + "[[routes]]\nid = \"chat\"\nhealth = { path = \"/health\", exec = \"x\" }\n", "routes.health.exec"},
		{base + "[tls]\nca = \"system\"\ninsecure = true\n", "tls.insecure"},
		{base + "[[routes]]\nid = \"chat\"\nmax_concurrency = \"32\"\n", "max_concurrency"},
		{base + "[[routes]]\nid = \"chat\"\nextra_paths = [[\"GET\", \"/a\", \"x\"]]\n", "array length"},
		{base + "[[routes]]\nid = \"chat\"\n[routes.models]\na = 1\n", "routes.models.a"},
		{base + "server = \"https://other.example.com\"\n", "already been defined"},
		{"server: https://example.com\n", ""},
	} {
		p := filepath.Join(t.TempDir(), "config.toml")
		os.WriteFile(p, []byte(tc.data), 0600)
		if _, e := Load(p); e == nil || !strings.Contains(e.Error(), tc.want) {
			t.Fatalf("%q: got %v, want error mentioning %q", tc.data, e, tc.want)
		}
	}
}

func TestModelAliasValidation(t *testing.T) {
	long := strings.Repeat("m", MaxModelNameBytes+1)
	many := map[string]string{}
	for i := 0; i <= MaxModelAliases; i++ {
		many[strings.Repeat("a", i+1)] = "target"
	}
	for name, change := range map[string]func(*Route){
		"empty alias":       func(r *Route) { r.Models = map[string]string{"": "x"} },
		"empty target":      func(r *Route) { r.Models = map[string]string{"x": ""} },
		"long alias":        func(r *Route) { r.Models = map[string]string{long: "x"} },
		"long target":       func(r *Route) { r.Models = map[string]string{"x": long} },
		"control character": func(r *Route) { r.Models = map[string]string{"x\n": "y"} },
		"invalid utf-8":     func(r *Route) { r.Models = map[string]string{"x": "\xff"} },
		"self alias":        func(r *Route) { r.Models = map[string]string{"x": "x"} },
		"too many":          func(r *Route) { r.Models = many },
		"document_ocr":      func(r *Route) { r.Type = "document_ocr"; r.Models = map[string]string{"x": "y"} },
		"document_conversion": func(r *Route) {
			r.Type = "document_conversion"
			r.Flavor = "gotenberg"
			r.Models = map[string]string{"x": "y"}
		},
		"object_storage": func(r *Route) { *r = Route{ID: "files", Type: "object_storage", Models: map[string]string{"x": "y"}} },
	} {
		c := good()
		change(&c.Routes[0])
		if c.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
	for _, kind := range []string{"openai_chat", "openai_embeddings", "anthropic_messages", "rerank", "audio_transcription", "audio_speech", "image"} {
		c := good()
		c.Routes[0].Type = kind
		c.Routes[0].Models = map[string]string{"whisper": "/models/faster-whisper-large-v3-turbo", "jina-reranker": "jina-reranker-v3"}
		if err := c.Validate(); err != nil {
			t.Errorf("%s: %v", kind, err)
		}
	}
	c := good()
	c.Routes[0].Models = map[string]string{"a": "t"}
	n, err := c.Normalized()
	if err != nil {
		t.Fatal(err)
	}
	n.Routes[0].Models["a"] = "changed"
	if c.Routes[0].Models["a"] != "t" {
		t.Fatal("normalization shares the alias map")
	}
}

func TestExampleConfigsParse(t *testing.T) {
	paths, _ := filepath.Glob("../../examples/*.toml")
	if len(paths) == 0 {
		t.Fatal("no examples")
	}
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		c, err := Decode(f)
		f.Close()
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if len(c.Routes) == 0 {
			t.Fatalf("%s: no routes", p)
		}
		// Examples ship a placeholder key and must fail until an operator installs a real one.
		if c.Validate() == nil {
			t.Fatalf("%s validated with a placeholder key", p)
		}
		for k := range c.ServerKeys {
			c.ServerKeys[k] = base64.StdEncoding.EncodeToString(make([]byte, 32))
		}
		var inference []Route
		for _, r := range c.Routes {
			if r.Type != "object_storage" { // storage needs root-owned local secrets
				r.UpstreamHeaders = nil
				inference = append(inference, r)
			}
		}
		c.Routes = inference
		if err := c.Validate(); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
}
