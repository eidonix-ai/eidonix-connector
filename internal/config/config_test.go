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
func TestStrictYAML(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	os.WriteFile(p, []byte("remote_command: bad\n"), 0600)
	if _, e := Load(p); e == nil || !strings.Contains(e.Error(), "remote_command") {
		t.Fatal("unknown YAML field accepted")
	}
}
