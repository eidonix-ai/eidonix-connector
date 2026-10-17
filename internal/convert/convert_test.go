package convert

import (
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, name string, got []byte) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, got, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return want
}

// convertGolden converts testdata/in and compares the YAML and the review
// notes with testdata/<out>.v03.yaml and <out>.notes.txt. The result must
// pass check.
func convertGolden(t *testing.T, in, out, site string) *config.Config {
	t.Helper()
	yaml, notes, err := Convert(filepath.Join("testdata", in), site)
	if err != nil {
		t.Fatal(err)
	}
	if want := golden(t, out+".v03.yaml", yaml); string(yaml) != string(want) {
		t.Fatalf("converted YAML differs:\n%s", yaml)
	}
	review := strings.Join(notes, "\n") + "\n"
	if want := golden(t, out+".notes.txt", []byte(review)); review != string(want) {
		t.Fatalf("review list differs:\n%s", review)
	}
	c, err := config.Parse(yaml)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func byName(c *config.Config) map[string]config.Service {
	out := map[string]config.Service{}
	for _, s := range c.Services {
		out[s.Name] = s
	}
	return out
}

// TestConvertSiteA: a GPU site's v0.2 file (design doc §2.1 shape).
func TestConvertSiteA(t *testing.T) {
	c := convertGolden(t, "site-a.v02.yaml", "site-a", "example-gpu-site")
	if c.Site != "example-gpu-site" || len(c.Services) != 11 {
		t.Fatal(c.Site, len(c.Services))
	}
	s := byName(c)
	endpoints := func(name string) string {
		out := []string{}
		for _, e := range s[name].Endpoints {
			out = append(out, e.String())
		}
		return strings.Join(out, ", ")
	}
	for name, want := range map[string]string{
		"glm":         "POST /v1/*, GET /v1/models",
		"paddle-ocr":  "POST /layout-parsing",
		"nomic-embed": "POST /embeddings",
		"jina-rerank": "POST /v1/rerank",
		"whisper":     "POST /v1/audio/transcriptions",
		"kokoro":      "POST /v1/audio/speech, GET /v1/audio/voices",
		"qwen-image":  "POST /v1/images/*",
		"vibevoice":   "WEBSOCKET /v1/stream, GET /v1/config",
	} {
		if got := endpoints(name); got != want {
			t.Errorf("%s: %s, want %s", name, got, want)
		}
	}
	if m := s["nomic-embed"].Models; !reflect.DeepEqual(m.Names, []string{"nomic-embed"}) || m.Target["nomic-embed"] != "nomic-ai/nomic-embed-text-v1.5" {
		t.Fatal(m)
	}
	if m := s["kokoro"].Models; !reflect.DeepEqual(m.Names, []string{"kokoro"}) || m.Renamed() {
		t.Fatal(m)
	}
	// Limits the v0.2 types implied are kept.
	for name, want := range map[string]int{"glm": 32, "qwen-next": 16, "qwen-chat": 8, "paddle-ocr": 4, "nomic-embed": 32, "qwen-image": 1, "vibevoice": 4} {
		if got := s[name].MaxConcurrentRequests; got != want {
			t.Errorf("%s: max-concurrent-requests %d, want %d", name, got, want)
		}
	}
	if s["glm"].IdleTimeout.String() != "15m0s" || s["qwen-chat"].IdleTimeout != config.DefaultIdleTimeout || s["glm"].Health != "/health" {
		t.Fatal(s["glm"], s["qwen-chat"])
	}
}

// TestConvertSiteB: a plant site's v0.2 file with a storage folder (design
// doc §2.2 shape).
func TestConvertSiteB(t *testing.T) {
	c := convertGolden(t, "site-b.v02.yaml", "site-b", "example-plant")
	s := byName(c)
	if g := s["glm"]; !reflect.DeepEqual(g.Models.Names, []string{"GLM-5.3-Flash"}) || g.Models.Target["GLM-5.3-Flash"] != "GLM-5.3-Flash-NVFP4" || g.MaxConcurrentRequests != 64 || g.APIKey != "sk-placeholder-chat" {
		t.Fatalf("%+v", g)
	}
	st := s["s3-storage"].Storage
	if st == nil || st.Encryption != "sse-c" || len(st.EncryptionKey) != 32 || st.CACertificate == "" || st.Bucket != "cletus-example" || st.Endpoint != "https://198.51.100.81:9000" {
		t.Fatalf("%+v", st)
	}
	if c.Upstream.Proxy != "http://198.51.100.3:3128" {
		t.Fatal(c.Upstream)
	}
	notes, _ := os.ReadFile("testdata/site-b.notes.txt")
	if !strings.Contains(string(notes), "eidonix-connector storage move-to-root s3-storage --from cletus/") {
		t.Fatal(string(notes))
	}
}

// TestConvertV01: a v0.1 TOML file goes through v0.2 to v0.3.
func TestConvertV01(t *testing.T) {
	c := convertGolden(t, "v01/config.toml", "v01", "example-gpu-site")
	if c.Upstream.Token == "" || len(c.Upstream.PublicKeys) != 2 || len(c.Services) != 12 {
		t.Fatalf("%+v", c.Upstream)
	}
	s := byName(c)
	if e := s["embed"]; e.APIKey != "sk-placeholder-embed" || e.Endpoints[0].String() != "POST /embeddings" || e.Models.Target["nomic-embed"] != "nomic-ai/nomic-embed-text-v1.5" {
		t.Fatalf("%+v", e)
	}
	if g := s["gpu2"]; g.URL != "http://10.0.0.34:8000" || g.MaxRequestsPerMinute != 150 || g.Models.Set() {
		t.Fatalf("%+v", g)
	}
	if f := s["files"].Storage; f == nil || f.AccessKey != "placeholder-access-key" || len(f.EncryptionKey) != 32 || f.CACertificate == "" {
		t.Fatalf("%+v", f)
	}
	if tool := s["tool"]; len(tool.Endpoints) != 2 || tool.Endpoints[1].String() != "WEBSOCKET /v1/events" {
		t.Fatalf("%+v", tool.Endpoints)
	}
}

func TestConvertRefusals(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	os.WriteFile(path, []byte("server = \"https://connect.example.com\"\ntoken_file = \"token\"\nsurprise = 1\n"), 0600)
	if _, _, err := Convert(path, "s"); err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatal(err)
	}
	os.WriteFile(path, []byte("server = \"https://connect.example.com\"\ntoken_file = \"missing-token\"\n"), 0600)
	if _, _, err := Convert(path, "s"); err == nil || !strings.Contains(err.Error(), "token_file") {
		t.Fatal(err)
	}
	if _, _, err := Convert("testdata/site-a.v02.yaml", "Not A Site"); err == nil || !strings.Contains(err.Error(), "--site") {
		t.Fatal(err)
	}
	v3 := filepath.Join(dir, "connector.yaml")
	os.WriteFile(v3, []byte("site: s\nservices: {}\n"), 0600)
	if _, _, err := Convert(v3, "s"); err == nil || !strings.Contains(err.Error(), "already a v0.3 configuration") {
		t.Fatal(err)
	}
	v2 := filepath.Join(dir, "v02.yaml")
	os.WriteFile(v2, []byte("upstream:\n  url: https://connect.example.com\nchat:\n  - name: a\n    url: http://192.0.2.1:8000\n    flavour: x\n"), 0600)
	if _, _, err := Convert(v2, "s"); err == nil || !strings.Contains(err.Error(), `unknown v0.2 setting "flavour"`) {
		t.Fatal(err)
	}
}
