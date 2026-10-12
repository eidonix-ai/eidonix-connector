package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestValidateDoesNotMutate(t *testing.T) {
	c := good()
	c.Routes[0].ExtraPaths = [][2]string{{"POST", "/v1/responses"}}
	c.Routes[0].UpstreamHeaders = map[string]string{"Authorization": "Bearer local"}
	before, _ := json.Marshal(c)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(c)
	if string(before) != string(after) {
		t.Fatalf("validation mutated config\n%s", after)
	}
	n, err := c.Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if len(n.Routes[0].MethodsPaths) == 0 || c.Routes[0].MethodsPaths != nil || n.Routes[0].MaxConcurrency != 32 {
		t.Fatal("normalization failed or shared mutable data")
	}
	n.Routes[0].UpstreamHeaders["Authorization"] = "changed"
	if c.Routes[0].UpstreamHeaders["Authorization"] != "Bearer local" {
		t.Fatal("normalization shares header map")
	}
}
func TestConfigPermissionChecks(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.toml")
	var buf bytes.Buffer
	g := good()
	if err := Encode(&buf, &g); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	for _, mode := range []os.FileMode{0600, 0640, 0644, 0660, 0777} {
		if err := os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
		os.Chmod(p, mode)
		_, err := Load(p)
		if (mode == 0600 || mode == 0640) != (err == nil) {
			t.Fatalf("mode %o: %v", mode, err)
		}
	}
}
func TestNormalizedSecretNotExpandedRecursively(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("root-owned secret test")
	}
	secret := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(secret, []byte("literal-${file:/must-not-read}"), 0600)
	c := good()
	c.Routes[0].UpstreamHeaders = map[string]string{"Authorization": "Bearer ${file:" + secret + "}"}
	n, err := c.Normalized()
	if err != nil {
		t.Fatal(err)
	}
	before := n.Routes[0].UpstreamHeaders["Authorization"]
	again, err := n.Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(n, again) || !strings.Contains(before, "${file:/must-not-read}") {
		t.Fatal("secret changed on second validation")
	}
}
