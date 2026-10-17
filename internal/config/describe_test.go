package config

import (
	"flag"
	"os"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, path string, got []byte) []byte {
	t.Helper()
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

// TestTemplateIsTheExample: examples/connector.yaml is exactly what init
// writes, and it fails check only on the placeholders to paste.
func TestTemplateIsTheExample(t *testing.T) {
	example := golden(t, "../../examples/connector.yaml", []byte(Template))
	if string(example) != Template {
		t.Fatal("examples/connector.yaml differs from config.Template")
	}
	_, err := Parse([]byte(Template))
	if err == nil || !strings.Contains(err.Error(), `site "PASTE_CONNECTOR_NAME_FROM_SETUP" must be the connector's name`) {
		t.Fatal(err)
	}
	_, err = Parse([]byte(strings.Replace(Template, "PASTE_CONNECTOR_NAME_FROM_SETUP", "example-site", 1)))
	if err == nil || !strings.Contains(err.Error(), "upstream: token is not a connection token") {
		t.Fatal(err)
	}
	filled := strings.NewReplacer("PASTE_CONNECTOR_NAME_FROM_SETUP", "example-site", "PASTE_TOKEN_FROM_SETUP", testToken, "PASTE_KEY_FROM_SETUP", testKey).Replace(Template)
	c, err := Parse([]byte(filled))
	if err != nil || len(c.Services) != 0 || c.AccessLog != DefaultAccessLog {
		t.Fatal(c, err)
	}
}

// TestDescribeGolden: `check` output for examples/gpu-server.yaml.
func TestDescribeGolden(t *testing.T) {
	data, err := os.ReadFile("../../examples/gpu-server.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	c.File = FileInfo{Path: "/etc/eidonix-connector/connector.yaml", Mode: 0640, Owner: "root", Group: "eidonix-connector"}
	got := c.Describe()
	if want := golden(t, "testdata/gpu-server.check.txt", []byte(got)); got != string(want) {
		t.Fatalf("check output differs:\n%s", got)
	}
}
// TestExamplesParse: every example file is a valid configuration.
func TestExamplesParse(t *testing.T) {
	for _, name := range []string{"gpu-server.yaml", "plant-site.yaml"} {
		data, err := os.ReadFile("../../examples/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Parse(data); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
func TestDescribeStorageAndEmpty(t *testing.T) {
	c, err := parse(t, "services:\n  files:\n    kind: s3\n    address: 192.0.2.9:9000\n    bucket: cletus\n    access-key: a\n    secret-key: s\n    read-only: true\n")
	if err != nil {
		t.Fatal(err)
	}
	got := c.Describe()
	for _, want := range []string{"Site test-site", "Shares 1 service and can reach only this machine:", "files  s3  192.0.2.9:9000  bucket cletus; files are stored without encryption   read-only   max concurrent: 16", "Secrets set: token, access-key (files), secret-key (files)."} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	c, _ = parse(t, "")
	if got := c.Describe(); !strings.Contains(got, "Shares no services") {
		t.Fatal(got)
	}
}

// TestRedacted: --redacted keeps comments and hides every secret value.
func TestRedacted(t *testing.T) {
	secrets := []string{testToken, "sk-SECRET-api", "AKIA-SECRET-access", "SECRET-secret-key", testKey}
	src := upstreamYAML() + `# a comment that must survive
services:
  gpu1:
    address: 192.0.2.5:8000
    allowed-endpoints: [POST /v1/*]
    api-key: sk-SECRET-api   # line comment
  files:
    kind: s3
    address: https://s3.example.com
    bucket: cletus
    access-key: AKIA-SECRET-access
    secret-key: "SECRET-secret-key"
    encryption: sse-c
    encryption-key: ` + testKey + `
`
	if _, err := Parse([]byte(src)); err != nil {
		t.Fatal(err)
	}
	out, err := Redact([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range secrets[:4] {
		if strings.Contains(string(out), s) {
			t.Fatalf("secret %q in redacted output:\n%s", s, out)
		}
	}
	if strings.Contains(string(out), "encryption-key: "+testKey) {
		t.Fatal("encryption key shown")
	}
	for _, want := range []string{"# a comment that must survive", "# line comment", "token: " + Redacted, "api-key: " + Redacted, "secret-key: " + Redacted, "public-keys: [" + testKey + "]", "address: 192.0.2.5:8000"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("missing %q:\n%s", want, out)
		}
	}
	proxied, _ := Redact([]byte("upstream:\n  proxy: http://user:PROXY-SECRET@proxy.example.com:3128\n"))
	if strings.Contains(string(proxied), "PROXY-SECRET") {
		t.Fatal(string(proxied))
	}
	// The public key is public; only secret settings are replaced.
	if strings.Count(string(out), Redacted) != 5 {
		t.Fatalf("%d redactions:\n%s", strings.Count(string(out), Redacted), out)
	}
}
