package convert

import (
	"flag"
	"os"
	"path/filepath"
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

// TestConvertGolden: a placeholder v0.1 file shaped like a production GPU
// server converts to the expected v0.2 YAML and review list.
func TestConvertGolden(t *testing.T) {
	out, notes, err := Convert("testdata/v01/config.toml")
	if err != nil {
		t.Fatal(err)
	}
	if want := golden(t, "v02.yaml", out); string(out) != string(want) {
		t.Fatalf("converted YAML differs:\n%s", out)
	}
	review := strings.Join(notes, "\n") + "\n"
	if want := golden(t, "v02.review.txt", []byte(review)); review != string(want) {
		t.Fatalf("review list differs:\n%s", review)
	}
	// The result is a valid v0.2 file with the secrets carried inline.
	c, err := config.Parse(out)
	if err != nil {
		t.Fatal(err)
	}
	if c.Upstream.Token == "" || c.Upstream.CACertificate != "" || len(c.Upstream.PublicKeys) != 2 || len(c.Services) != 12 {
		t.Fatalf("%+v", c.Upstream)
	}
	byName := map[string]config.Service{}
	for _, s := range c.Services {
		byName[s.Name] = s
	}
	if e := byName["embed"]; e.APIKey != "sk-placeholder-embed" || e.APIPath != "/" || e.Models.Target["nomic-embed"] != "nomic-ai/nomic-embed-text-v1.5" {
		t.Fatalf("%+v", e)
	}
	if g := byName["gpu2"]; len(g.URLs) != 2 || g.MaxRequestsPerMinute != 150 || !g.Models.Any {
		t.Fatalf("%+v", g)
	}
	if f := byName["files"].Storage; f == nil || f.AccessKey != "placeholder-access-key" || len(f.EncryptionKey) != 32 || f.CACertificate == "" || f.MaxReadPerHour != 50<<30 {
		t.Fatalf("%+v", f)
	}
	if tool := byName["tool"]; len(tool.Paths) != 2 || tool.Paths[1].String() != "WEBSOCKET /v1/events" {
		t.Fatalf("%+v", tool.Paths)
	}
	// Types in canonical order after upstream.
	order := []string{}
	for _, s := range c.Services {
		order = append(order, s.Type)
	}
	if strings.Join(order, ",") != "chat,chat,embeddings,rerank,transcription,speech,image,document,conversion,realtime,storage,custom" {
		t.Fatal(order)
	}
}
func TestConvertRefusesUnknownSettingsAndMissingFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	os.WriteFile(path, []byte("server = \"https://connect.example.com\"\ntoken_file = \"token\"\nsurprise = 1\n"), 0600)
	if _, _, err := Convert(path); err == nil || !strings.Contains(err.Error(), "surprise") {
		t.Fatal(err)
	}
	os.WriteFile(path, []byte("server = \"https://connect.example.com\"\ntoken_file = \"missing-token\"\n"), 0600)
	if _, _, err := Convert(path); err == nil || !strings.Contains(err.Error(), "token_file") {
		t.Fatal(err)
	}
}
