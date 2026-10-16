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
	if err == nil || !strings.Contains(err.Error(), "upstream: token is not a connection token") {
		t.Fatal(err)
	}
	filled := strings.Replace(strings.Replace(Template, "PASTE_TOKEN_FROM_SETUP", testToken, 1), "PASTE_KEY_FROM_SETUP", testKey, 1)
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
func TestDescribeStorageAndEmpty(t *testing.T) {
	c, err := parse(t, "storage:\n  - name: files\n    endpoint: http://10.0.0.9:9000\n    allow_http: true\n    bucket: cletus\n    prefix: cletus/\n    access_key: a\n    secret_key: s\n    encryption: none\n")
	if err != nil {
		t.Fatal(err)
	}
	got := c.Describe()
	for _, want := range []string{"Shares 1 service and can reach only this machine:", "storage  files  10.0.0.9:9000  bucket cletus, folder cletus/; files are stored without encryption", "Secrets set: token, access_key (files), secret_key (files)."} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	c, _ = parse(t, "")
	if got := c.Describe(); !strings.Contains(got, "Shares no services") || !strings.Contains(got, "Not shared: chat, anthropic") {
		t.Fatal(got)
	}
}

// TestRedacted: --redacted keeps comments and hides every secret value.
func TestRedacted(t *testing.T) {
	secrets := []string{testToken, "sk-SECRET-api", "AKIA-SECRET-access", "SECRET-secret-key", testKey}
	src := upstreamYAML() + `# a comment that must survive
chat:
  - name: cn3
    url: http://10.0.0.5:8000
    models: [a]
    api_key: sk-SECRET-api   # line comment
storage:
  - name: files
    endpoint: https://s3.example.com
    bucket: cletus
    prefix: cletus/
    access_key: AKIA-SECRET-access
    secret_key: "SECRET-secret-key"
    encryption_key: ` + testKey + `
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
	if strings.Contains(string(out), "encryption_key: "+testKey) {
		t.Fatal("encryption key shown")
	}
	for _, want := range []string{"# a comment that must survive", "# line comment", "token: " + Redacted, "api_key: " + Redacted, "secret_key: " + Redacted, "public_keys: [" + testKey + "]", "url: http://10.0.0.5:8000"} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("missing %q:\n%s", want, out)
		}
	}
	// The public key is public; only secret settings are replaced.
	if strings.Count(string(out), Redacted) != 5 {
		t.Fatalf("%d redactions:\n%s", strings.Count(string(out), Redacted), out)
	}
}
