package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
)

// Synthetic, all-placeholder credentials used only by tests.
const (
	testToken = "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	testKey   = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
)

func upstreamYAML() string {
	return "upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\n"
}
func parse(t *testing.T, body string) (*Config, error) {
	t.Helper()
	return Parse([]byte(upstreamYAML() + body))
}

func TestParseAllForms(t *testing.T) {
	c, err := parse(t, `
name: Example GPUs
chat:
  - name: cn3
    url: http://10.0.0.33:8000
    models: [model-a]
    max_concurrent_requests: 16
    idle_timeout: 15m
    api_key: sk-placeholder
embeddings:
  - name: embed
    urls: [http://10.0.0.30:8001, http://10.0.0.31:8001]
    api_path: /
    models:
      nomic-embed: nomic-ai/nomic-embed-text-v1.5
rerank:
  - name: rerank
    url: http://10.0.0.30:8006
    models: any
    max_requests_per_minute: 600
document:
  - name: ocr
    url: http://10.0.0.30:8002/
custom:
  - name: tool
    url: https://tool.example.com
    paths: ["POST /v1/run", "WEBSOCKET /v1/stream"]
access_log: /tmp/access.log
`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "Example GPUs" || c.AccessLog != "/tmp/access.log" || len(c.Services) != 5 {
		t.Fatal(c)
	}
	chat, emb, rr, ocr, tool := c.Services[0], c.Services[1], c.Services[2], c.Services[3], c.Services[4]
	if chat.MaxConcurrentRequests != 16 || chat.IdleTimeout != 15*time.Minute || chat.APIKey != "sk-placeholder" || !chat.Models.Allowlist() || chat.Models.Renamed() {
		t.Fatalf("%+v", chat)
	}
	if len(emb.URLs) != 2 || emb.APIPath != "/" || emb.Models.Target["nomic-embed"] != "nomic-ai/nomic-embed-text-v1.5" || !emb.Models.Renamed() || emb.IdleTimeout != 2*time.Minute || emb.MaxConcurrentRequests != 32 {
		t.Fatalf("%+v", emb)
	}
	if !rr.Models.Any || rr.MaxRequestsPerMinute != 600 {
		t.Fatalf("%+v", rr)
	}
	if ocr.URLs[0] != "http://10.0.0.30:8002" || ocr.MaxConcurrentRequests != 4 {
		t.Fatalf("%+v", ocr)
	}
	if !reflect.DeepEqual(tool.Paths, []servicetype.Request{{Method: "POST", Path: "/v1/run"}, {Method: servicetype.WebSocket, Path: "/v1/stream"}}) {
		t.Fatalf("%+v", tool.Paths)
	}
}
func TestEmptySectionsAndNoServices(t *testing.T) {
	for _, body := range []string{"", "chat:\n", "chat: []\nembeddings: null\nstorage:\n"} {
		c, err := parse(t, body)
		if err != nil || len(c.Services) != 0 || c.AccessLog != DefaultAccessLog {
			t.Fatalf("%q: %v %v", body, c, err)
		}
	}
}
func TestUnknownSettingNamesLineAndService(t *testing.T) {
	_, err := parse(t, "chat:\n  - name: cn3\n    url: http://10.0.0.5:8000\n    models: [a]\n    max_concurrency: 4\n")
	want := `line 9: unknown setting "max_concurrency" in chat service "cn3" (expected one of: name, url, urls, api_path, models, max_concurrent_requests, max_requests_per_minute, idle_timeout, api_key)`
	if err == nil || err.Error() != want {
		t.Fatalf("got  %v\nwant %s", err, want)
	}
	_, err = parse(t, "embedding:\n  - name: e\n")
	if err == nil || !strings.HasPrefix(err.Error(), `line 5: unknown setting "embedding" (expected one of: name, upstream, chat,`) {
		t.Fatal(err)
	}
	_, err = Parse([]byte("upstream:\n  url: https://connect.example.com\n  tokn: x\n"))
	if err == nil || !strings.Contains(err.Error(), `line 3: unknown setting "tokn" in upstream`) {
		t.Fatal(err)
	}
}
func TestValidationErrors(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{"chat:\n  - name: cn3\n    url: http://10.0.0.5:8000\n", `line 6: chat service "cn3": models is required`},
		{"chat:\n  - name: cn3\n    models: [a]\n", `line 6: chat service "cn3": url is required`},
		{"chat:\n  - url: http://10.0.0.5:8000\n    models: [a]\n", `line 6: chat service #1: name is required`},
		{"chat:\n  - name: CN3\n", `line 6: chat service #1: name "CN3" must be`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: []\n", `line 8: chat service "a": models must be`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: [x, x]\n", `line 8: chat service "a": models lists "x" twice`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000/v1\n    models: [x]\n", `line 7: chat service "a": url must not include a path; use api_path`},
		{"chat:\n  - name: a\n    url: ftp://10.0.0.5\n    models: [x]\n", `line 7: chat service "a": url must be http://host:port`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000?x=1\n    models: [x]\n", `line 7: chat service "a": url must be`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    urls: [http://10.0.0.6:8000]\n    models: [x]\n", `line 8: chat service "a": set url or urls, not both`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: [x]\n    idle_timeout: 900\n", `line 9: chat service "a": idle_timeout must be a duration`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: [x]\n    max_concurrent_requests: 0\n", `line 9: chat service "a": max_concurrent_requests must be a whole number from 1 to 10000`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: [x]\n    api_path: /v1/\n", `line 9: chat service "a": api_path must be`},
		{"document:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: [x]\n", `line 8: unknown setting "models" in document service "a"`},
		{"custom:\n  - name: a\n    url: http://10.0.0.5:8000\n    api_path: /\n    paths: [\"POST /x\"]\n", `line 8: unknown setting "api_path" in custom service "a"`},
		{"custom:\n  - name: a\n    url: http://10.0.0.5:8000\n", `line 6: custom service "a": paths is required`},
		{"custom:\n  - name: a\n    url: http://10.0.0.5:8000\n    paths: [\"DELETE /x\"]\n", `line 8: custom service "a": paths entry`},
		{"chat:\n  - name: a\n    url: http://10.0.0.5:8000\n    models: [x]\nrerank:\n  - name: a\n    url: http://10.0.0.6:8000\n    models: [y]\n", `line 10: rerank service "a": name already used by the chat service on line 6`},
		{"chat: {name: a}\n", `line 5: chat must be a list of services`},
	} {
		_, err := parse(t, tc.body)
		if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("%q\n got  %v\n want %s…", tc.body, err, tc.want)
		}
	}
}
func TestUpstreamValidation(t *testing.T) {
	for _, tc := range []struct{ yaml, want string }{
		{"upstream:\n  url: http://connect.example.com\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\n", "line 2: upstream: url must be https://"},
		{"upstream:\n  url: https://connect.example.com/v1\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\n", "line 2: upstream: url must be"},
		{"upstream:\n  url: https://connect.example.com\n  token: PASTE_TOKEN_FROM_SETUP\n  public_keys: [" + testKey + "]\n", "line 3: upstream: token is not a connection token"},
		{"upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public_keys: [PASTE_KEY_FROM_SETUP]\n", "line 4: upstream: public key \"PASTE_KEY_FROM_SETUP\""},
		{"upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public_keys: [" + testKey + ", " + testKey + ", " + testKey + "]\n", "line 4: upstream: public_keys must list one or two"},
		{"upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n", "line 2: upstream: public_keys is required"},
		{"upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\n  proxy_url: socks5://proxy:1080\n", "line 5: upstream: proxy_url must be"},
		{"upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\n  ca_certificate: not a cert\n", "line 5: upstream: ca_certificate must be a PEM certificate"},
		{"name: x\n", "line 1: upstream is required"},
		{"", "the file is empty"},
		{"upstream: [\n", "not valid YAML"},
	} {
		_, err := Parse([]byte(tc.yaml))
		if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("%q\n got  %v\n want %s…", tc.yaml, err, tc.want)
		}
	}
	c, err := Parse([]byte("upstream:\n  url: https://connect.example.com:8443\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\n  proxy_url: http://proxy.example.com:3128\n"))
	if err != nil || c.Upstream.ProxyURL != "http://proxy.example.com:3128" || HostPort(c.Upstream.URL) != "connect.example.com:8443" {
		t.Fatal(c, err)
	}
}
func TestStorageValidation(t *testing.T) {
	base := "storage:\n  - name: files\n    endpoint: %s\n    bucket: cletus\n    prefix: cletus/\n    access_key: placeholder-access\n    secret_key: placeholder-secret\n"
	key := "    encryption_key: " + testKey + "\n"
	c, err := parse(t, strings.Replace(base, "%s", "https://s3.example.com:9000", 1)+key)
	if err != nil {
		t.Fatal(err)
	}
	st := c.Services[0].Storage
	if st.Encryption != "sse-c" || len(st.EncryptionKey) != 32 || !st.PathStyle || st.Region != "us-east-1" || st.MaxFileSize != 5<<30 || c.Services[0].MaxConcurrentRequests != 16 {
		t.Fatalf("%+v", st)
	}
	c, err = parse(t, strings.Replace(base, "%s", "https://s3.us-east-1.amazonaws.com", 1)+"    encryption: none\n    max_file_size: 500MB\n    max_read_per_hour: 50GB\n")
	if err != nil || c.Services[0].Storage.PathStyle || c.Services[0].Storage.MaxFileSize != 500e6 || c.Services[0].Storage.MaxReadPerHour != 50e9 {
		t.Fatal(c, err)
	}
	for _, tc := range []struct{ endpoint, extra, want string }{
		{"https://s3.example.com", "", "needs encryption_key"},
		{"http://s3.example.com", key, "endpoint uses http"},
		{"http://s3.example.com", key + "    allow_http: true\n", "encryption sse-c needs an https endpoint"},
		{"https://s3.example.com", "    encryption: sse-s3\n" + key, "encryption_key is used only with encryption: sse-c"},
		{"https://s3.example.com", "    encryption: aes\n", "encryption must be"},
		{"https://s3.example.com", "    encryption_key: c2hvcnQ=\n", "encryption_key must be base64 of exactly 32 bytes"},
		{"https://s3.example.com", key + "    max_file_size: 6GiB\n", "max_file_size must be a size"},
		{"https://s3.example.com", key + "    api_key: x\n", `unknown setting "api_key" in storage service "files"`},
	} {
		_, err := parse(t, strings.Replace(base, "%s", tc.endpoint, 1)+tc.extra)
		if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "line ") {
			t.Errorf("%s %q: %v", tc.endpoint, tc.extra, err)
		}
	}
	if _, err := parse(t, strings.Replace(base, "%s", "http://10.0.0.9:9000", 1)+"    encryption: none\n    allow_http: true\n"); err != nil {
		t.Fatal(err)
	}
}
func TestSizes(t *testing.T) {
	for in, want := range map[string]int64{"5GB": 5e9, "5GiB": 5 << 30, "500MB": 500e6, "64KiB": 64 << 10, "1024": 1024} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Fatalf("%s: %d %v", in, got, err)
		}
	}
	for _, in := range []string{"", "0", "-1GB", "5TB", "1.5GB"} {
		if _, err := ParseSize(in); err == nil {
			t.Fatal(in)
		}
	}
	if FormatSize(5<<30) != "5GiB" || FormatSize(50e9) != "50GB" {
		t.Fatal(FormatSize(5<<30), FormatSize(50e9))
	}
}

// TestPermissionGate: a config readable by other users is refused with the fix.
func TestPermissionGate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connector.yaml")
	if err := os.WriteFile(path, []byte(upstreamYAML()), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0600, 0640, 0400, 0440} {
		os.Chmod(path, mode)
		c, err := Load(path)
		if err != nil || c.File.Mode != mode {
			t.Fatalf("%04o refused: %v", mode, err)
		}
	}
	for _, mode := range []os.FileMode{0644, 0660, 0604, 0666} {
		os.Chmod(path, mode)
		_, err := Load(path)
		if err == nil {
			t.Fatalf("%04o accepted", mode)
		}
		fix := "Fix: chmod 0600 " + path
		if os.Geteuid() == 0 {
			fix = "Fix: chown root:eidonix-connector " + path + " && chmod 0640 " + path
		}
		if !strings.Contains(err.Error(), "has mode ") || !strings.HasSuffix(err.Error(), fix) {
			t.Fatalf("%04o: %v", mode, err)
		}
	}
	os.Chmod(path, 0644)
	_, err := Load(path)
	t.Logf("0644 refusal: %v", err)
	if fi, _ := os.Stat(path); fi != nil {
		if _, err := CheckPermissions(path, fi, os.Geteuid()+1); err == nil || !strings.Contains(err.Error(), "must be owned by root. Fix: chown root:eidonix-connector "+path+" && chmod 0640 "+path) {
			t.Fatalf("foreign owner: %v", err)
		}
	}
	dir := t.TempDir()
	if _, err := Load(dir); err == nil {
		t.Fatal("directory accepted")
	}
}
func TestSecretsListed(t *testing.T) {
	c, err := parse(t, "chat:\n  - name: cn3\n    url: http://10.0.0.5:8000\n    models: [a]\n    api_key: sk-x\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.Secrets(), ", "); got != "token, api_key (cn3)" {
		t.Fatal(got)
	}
}
