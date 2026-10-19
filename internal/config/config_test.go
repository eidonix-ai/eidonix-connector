package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Synthetic, all-placeholder credentials used only by tests.
const (
	testToken = "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	testKey   = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
)

// upstreamYAML is five lines: services start on line 6.
func upstreamYAML() string {
	return "site: test-site\nupstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public-keys: [" + testKey + "]\n"
}
func parse(t *testing.T, body string) (*Config, error) {
	t.Helper()
	return Parse([]byte(upstreamYAML() + body))
}

func TestParseAllForms(t *testing.T) {
	c, err := parse(t, `services:
  gpu1:
    address: 192.0.2.33:8000
    allowed-endpoints: [POST /v1/*, GET /v1/models]
    models: [model-a]
    health: /health
    max-concurrent-requests: 32
    idle-timeout: 15m
    api-key: sk-placeholder
  embed:
    address: https://embed.example.com:8443
    allowed-endpoints: [POST /embeddings]
    models:
      nomic-ai/nomic-embed-text-v1.5: {alias: nomic-embed}
      other-model:
    max-requests-per-minute: 600
  voice:
    address: "[2001:db8::1]:8011"
    allowed-endpoints: [WEBSOCKET /v1/stream, GET /v1/config, DELETE /v1/session, PUT /v1/x]
  files:
    kind: s3
    address: https://s3.example.com:9000
    bucket: cletus
    access-key: placeholder-access
    secret-key: placeholder-secret
    read-only: true
    max-concurrent-requests: 4
access-log: /tmp/access.log
`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Site != "test-site" || c.AccessLog != "/tmp/access.log" || len(c.Services) != 4 {
		t.Fatal(c)
	}
	chat, emb, voice, files := c.Services[0], c.Services[1], c.Services[2], c.Services[3]
	if chat.Kind != KindHTTP || chat.URL != "http://192.0.2.33:8000" || chat.Address != "192.0.2.33:8000" || chat.MaxConcurrentRequests != 32 || chat.IdleTimeout != 15*time.Minute || chat.APIKey != "sk-placeholder" || chat.Health != "/health" || !chat.Models.Set() || chat.Models.Renamed() {
		t.Fatalf("%+v", chat)
	}
	if !reflect.DeepEqual(chat.Endpoints, []Endpoint{{"POST", "/v1/*"}, {"GET", "/v1/models"}}) {
		t.Fatal(chat.Endpoints)
	}
	if emb.URL != "https://embed.example.com:8443" || !reflect.DeepEqual(emb.Models.Names, []string{"nomic-embed", "other-model"}) || emb.Models.Target["nomic-embed"] != "nomic-ai/nomic-embed-text-v1.5" || emb.Models.Target["other-model"] != "other-model" || !emb.Models.Renamed() || emb.IdleTimeout != DefaultIdleTimeout || emb.MaxConcurrentRequests != DefaultMaxConcurrentRequests || emb.MaxRequestsPerMinute != 600 {
		t.Fatalf("%+v", emb)
	}
	if voice.URL != "http://[2001:db8::1]:8011" || voice.Models.Set() || voice.Health != "" || len(voice.Endpoints) != 4 {
		t.Fatalf("%+v", voice)
	}
	if files.Kind != KindS3 || files.Storage == nil || files.Storage.Endpoint != "https://s3.example.com:9000" || !files.Storage.ReadOnly || files.Storage.Encryption != "none" || files.MaxConcurrentRequests != 4 || files.Storage.MaxFileSize != 5<<30 {
		t.Fatalf("%+v %+v", files, files.Storage)
	}
}
func TestEndpointMatching(t *testing.T) {
	for _, tc := range []struct {
		entry, method, path string
		want                bool
	}{
		{"POST /v1/*", "POST", "/v1/chat/completions", true},
		{"POST /v1/*", "POST", "/v1/responses", true},
		{"POST /v1/*", "POST", "/v1", false},
		{"POST /v1/*", "POST", "/v1/", false},
		{"POST /v1/*", "POST", "/v10/x", false},
		{"POST /v1/*", "GET", "/v1/models", false},
		{"GET /v1/models", "GET", "/v1/models", true},
		{"GET /v1/models", "GET", "/v1/models/x", false},
		{"WEBSOCKET /v1/stream", "GET", "/v1/stream", false},
		{"WEBSOCKET /v1/stream", "WEBSOCKET", "/v1/stream", true},
		{"POST /*", "POST", "/anything/below", true},
		{"POST /*", "POST", "/", false},
	} {
		e, err := ParseEndpoint(tc.entry)
		if err != nil {
			t.Fatal(tc.entry, err)
		}
		if got := e.Matches(tc.method, tc.path); got != tc.want {
			t.Errorf("%s matches %s %s = %v", tc.entry, tc.method, tc.path, got)
		}
	}
	for _, bad := range []string{"POST", "POST v1/x", "PATCH /x", "post /x", "POST /v1/*/x", "POST /v1*", "POST /v1/**", "POST /v1//*", "POST /a/../b", "POST /a%2fb", "POST /a?b", "POST /a b", "POST /*/"} {
		if _, err := ParseEndpoint(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
func TestOldFormatRefused(t *testing.T) {
	for _, body := range []string{
		"upstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public_keys: [" + testKey + "]\nchat:\n  - name: gpu1\n    url: http://192.0.2.5:8000\n    models: [a]\n",
		"name: x\nupstream:\n  url: https://connect.example.com\n",
		"upstream:\n  public_keys: [x]\n",
	} {
		if _, err := Parse([]byte(body)); !errors.Is(err, ErrOldFormat) {
			t.Fatalf("%q: %v", body, err)
		}
	}
	path := filepath.Join(t.TempDir(), "connector.yaml")
	os.WriteFile(path, []byte("upstream:\n  public_keys: [x]\nchat: []\n"), 0600)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "v0.2 configuration") || !strings.Contains(err.Error(), "eidonix-connector convert --site <connector name in Setup> "+path) {
		t.Fatal(err)
	}
}
func TestEmptyServices(t *testing.T) {
	for _, body := range []string{"", "services:\n", "services: {}\n"} {
		c, err := parse(t, body)
		if err != nil || len(c.Services) != 0 || c.AccessLog != DefaultAccessLog {
			t.Fatalf("%q: %v %v", body, c, err)
		}
	}
}
func TestUnknownSettingNamesLineAndService(t *testing.T) {
	_, err := parse(t, "services:\n  gpu1:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [POST /v1/*]\n    max_concurrency: 4\n")
	want := `line 10: unknown setting "max_concurrency" in service "gpu1" (expected one of: kind, address, allowed-endpoints, models, health, api-key, max-concurrent-requests, max-requests-per-minute, idle-timeout)`
	if err == nil || err.Error() != want {
		t.Fatalf("got  %v\nwant %s", err, want)
	}
	_, err = parse(t, "service:\n  a: {}\n")
	if err == nil || !strings.HasPrefix(err.Error(), `line 6: unknown setting "service" (expected one of: site, upstream, access-log, status-file, services)`) {
		t.Fatal(err)
	}
	_, err = Parse([]byte("site: s\nupstream:\n  url: https://connect.example.com\n  tokn: x\n"))
	if err == nil || !strings.Contains(err.Error(), `line 4: unknown setting "tokn" in upstream`) {
		t.Fatal(err)
	}
}
func TestValidationErrors(t *testing.T) {
	const svc = "services:\n  a:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [POST /v1/*]\n"
	for _, tc := range []struct{ body, want string }{
		{"services:\n  a:\n    allowed-endpoints: [POST /v1/*]\n", `line 7: service "a": address is required`},
		{"services:\n  a:\n    address: 192.0.2.5:8000\n", `line 7: service "a": allowed-endpoints is required`},
		{"services:\n  A:\n    address: 192.0.2.5:8000\n", `line 7: service name "A" must be`},
		{"services:\n  a: x\n", `line 7: service "a": needs settings`},
		{"services:\n  a:\n    address: 192.0.2.5:8000\n    allowed-endpoints: []\n", `line 9: service "a": allowed-endpoints must list 1 to 64`},
		{"services:\n  a:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [POST /x, POST /x]\n", `line 9: service "a": allowed-endpoints lists POST /x twice`},
		{"services:\n  a:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [PATCH /x]\n", `line 9: service "a": allowed-endpoints entry "PATCH /x": method must be`},
		{"services:\n  a:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [POST /v1/*/x]\n", `line 9: service "a": allowed-endpoints entry "POST /v1/*/x": * is allowed only`},
		{svc + "    models: []\n", `line 10: service "a": models must be`},
		{svc + "    models: [x, x]\n", `line 10: service "a": models uses the name "x" twice`},
		{svc + "    models:\n      srv-a: {alias: x}\n      x:\n", `line 12: service "a": models uses the name "x" twice`},
		{svc + "    models: any\n", `line 10: service "a": models: any is gone in v0.3`},
		{svc + "    models:\n      srv: {name: x}\n", `line 11: unknown setting "name" in service "a" model "srv"`},
		{svc + "    models:\n      srv: x\n", `line 11: service "a": model "srv": expected {alias: <name applications use>}`},
		{"services:\n  a:\n    address: http://192.0.2.5:8000\n    allowed-endpoints: [POST /x]\n", `line 8: service "a": address must be host:port (plain HTTP) or https://host:port; write "192.0.2.5:8000" without http://`},
		{"services:\n  a:\n    address: 192.0.2.5\n    allowed-endpoints: [POST /x]\n", `line 8: service "a": address must be host:port`},
		{"services:\n  a:\n    address: https://192.0.2.5:8000/v1\n    allowed-endpoints: [POST /x]\n", `line 8: service "a": address must be host:port (plain HTTP) or https://host:port, without a path`},
		{"services:\n  a:\n    address: 192.0.2.5:99999\n    allowed-endpoints: [POST /x]\n", `line 8: service "a": address must be`},
		{svc + "    health: health\n", `line 10: service "a": health must be a path`},
		{svc + "    idle-timeout: 900\n", `line 10: service "a": idle-timeout must be a duration`},
		{svc + "    max-concurrent-requests: 0\n", `line 10: service "a": max-concurrent-requests must be a whole number from 1 to 10000`},
		{svc + "    bucket: x\n", `line 10: service "a": bucket is a setting of s3 services; add kind: s3`},
		{svc + "    kind: ftp\n", `line 10: service "a": kind must be http (the default) or s3`},
		{"services:\n  a:\n    kind: s3\n    address: https://s3.example.com\n    bucket: b-1\n    access-key: a\n    secret-key: s\n    allowed-endpoints: [POST /x]\n", `line 13: service "a": allowed-endpoints is not used by s3 services`},
		{"services:\n  a:\n    kind: s3\n    address: https://s3.example.com\n    bucket: b-1\n    access-key: a\n    secret-key: s\n    models: [x]\n", `line 13: service "a": models is not used by s3 services`},
		{"services:\n  a:\n    kind: s3\n    address: https://s3.example.com\n    bucket: b-1\n    access-key: a\n    secret-key: s\n    health: /h\n", `line 13: service "a": health is not used by s3 services`},
		{svc + "  a:\n    address: 192.0.2.6:8000\n    allowed-endpoints: [POST /y]\n", `line 10: service "a": name already used on line 7`},
		{"services: [a]\n", `line 6: services must be a set of services by name`},
	} {
		_, err := parse(t, tc.body)
		if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("%q\n got  %v\n want %s…", tc.body, err, tc.want)
		}
	}
}
func TestUpstreamValidation(t *testing.T) {
	up := func(lines string) string { return "site: s\nupstream:\n" + lines }
	ok := "  url: https://connect.example.com\n  token: " + testToken + "\n  public-keys: [" + testKey + "]\n"
	for _, tc := range []struct{ yaml, want string }{
		{up("  url: http://connect.example.com\n  token: " + testToken + "\n  public-keys: [" + testKey + "]\n"), "line 3: upstream: url must be https://"},
		{up("  url: https://connect.example.com/v1\n  token: " + testToken + "\n  public-keys: [" + testKey + "]\n"), "line 3: upstream: url must be"},
		{up("  url: https://connect.example.com\n  token: PASTE_TOKEN_FROM_SETUP\n  public-keys: [" + testKey + "]\n"), "line 4: upstream: token is not a connection token"},
		{up("  url: https://connect.example.com\n  token: " + testToken + "\n  public-keys: [PASTE_KEY_FROM_SETUP]\n"), "line 5: upstream: public key \"PASTE_KEY_FROM_SETUP\""},
		{up("  url: https://connect.example.com\n  token: " + testToken + "\n  public-keys: [" + testKey + ", " + testKey + ", " + testKey + "]\n"), "line 5: upstream: public-keys must list one or two"},
		{up("  url: https://connect.example.com\n  token: " + testToken + "\n"), "line 3: upstream: public-keys is required"},
		{up(ok + "  proxy: socks5://proxy:1080\n"), "line 6: upstream: proxy must be"},
		{up(ok + "  ca-certificate: not a cert\n"), "line 6: upstream: ca-certificate must be a PEM certificate"},
		{"upstream:\n" + ok, "line 1: site is required"},
		{"site: Not_A_Site\nupstream:\n" + ok, "line 1: site \"Not_A_Site\" must be the connector's name in Eidonix Setup"},
		{"site: s\n", "line 1: upstream is required"},
		{"", "the file is empty"},
		{"upstream: [\n", "not valid YAML"},
	} {
		_, err := Parse([]byte(tc.yaml))
		if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("%q\n got  %v\n want %s…", tc.yaml, err, tc.want)
		}
	}
	c, err := Parse([]byte("site: s\nupstream:\n  url: https://connect.example.com:8443\n  token: " + testToken + "\n  public-keys: [" + testKey + "]\n  proxy: http://proxy.example.com:3128\n"))
	if err != nil || c.Upstream.Proxy != "http://proxy.example.com:3128" || HostPort(c.Upstream.URL) != "connect.example.com:8443" {
		t.Fatal(c, err)
	}
}
func TestStorageValidation(t *testing.T) {
	base := "services:\n  files:\n    kind: s3\n    address: %s\n    bucket: cletus\n    access-key: placeholder-access\n    secret-key: placeholder-secret\n"
	key := "    encryption: sse-c\n    encryption-key: " + testKey + "\n"
	c, err := parse(t, strings.Replace(base, "%s", "https://s3.example.com:9000", 1)+key)
	if err != nil {
		t.Fatal(err)
	}
	st := c.Services[0].Storage
	if st.Encryption != "sse-c" || len(st.EncryptionKey) != 32 || !st.PathStyle || st.Region != "us-east-1" || st.MaxFileSize != 5<<30 || c.Services[0].MaxConcurrentRequests != 16 || st.ReadOnly {
		t.Fatalf("%+v", st)
	}
	c, err = parse(t, strings.Replace(base, "%s", "https://s3.us-east-1.amazonaws.com", 1)+"    encryption: sse-s3\n    max-file-size: 500MB\n    region: eu-west-1\n")
	if err != nil || c.Services[0].Storage.PathStyle || c.Services[0].Storage.MaxFileSize != 500e6 || c.Services[0].Storage.Region != "eu-west-1" {
		t.Fatal(c, err)
	}
	for _, tc := range []struct{ address, extra, want string }{
		{"https://s3.example.com", "    encryption: sse-c\n", "needs encryption-key"},
		{"192.0.2.9:9000", key, "encryption sse-c needs an https:// address"},
		{"https://s3.example.com", "    encryption: sse-s3\n    encryption-key: " + testKey + "\n", "encryption-key is used only with encryption: sse-c"},
		{"https://s3.example.com", "    encryption-key: " + testKey + "\n", "encryption-key is used only with encryption: sse-c"},
		{"https://s3.example.com", "    encryption: sse-kms\n", "encryption must be none, sse-s3 or sse-c"},
		{"https://s3.example.com", "    encryption: sse-c\n    encryption-key: c2hvcnQ=\n", "encryption-key must be base64 of exactly 32 bytes"},
		{"https://s3.example.com", "    max-file-size: 6GiB\n", "max-file-size must be a size"},
		{"https://s3.example.com", "    read-only: yes please\n", "read-only must be true or false"},
		{"https://s3.example.com", "    api-key: x\n", `api-key is not used by s3 services`},
		{"https://s3.example.com", "    prefix: cletus/\n", `unknown setting "prefix" in service "files"`},
		{"192.0.2.9:9000", "    ca-certificate: x\n", "ca-certificate is used only with an https:// address"},
	} {
		_, err := parse(t, strings.Replace(base, "%s", tc.address, 1)+tc.extra)
		if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "line ") || !strings.Contains(err.Error(), `service "files"`) {
			t.Errorf("%s %q: %v", tc.address, tc.extra, err)
		}
	}
	for _, missing := range []string{"bucket", "access-key", "secret-key"} {
		body := strings.Replace(base, "%s", "https://s3.example.com", 1)
		var kept []string
		for _, l := range strings.Split(body, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), missing+":") {
				kept = append(kept, l)
			}
		}
		if _, err := parse(t, strings.Join(kept, "\n")); err == nil || !strings.Contains(err.Error(), missing+" is required") {
			t.Errorf("%s: %v", missing, err)
		}
	}
	if c, err := parse(t, strings.Replace(base, "%s", "192.0.2.9:9000", 1)); err != nil || c.Services[0].Storage.Endpoint != "http://192.0.2.9:9000" {
		t.Fatal(c, err)
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
	c, err := parse(t, "services:\n  gpu1:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [POST /v1/*]\n    api-key: sk-x\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.Secrets(), ", "); got != "token, api-key (gpu1)" {
		t.Fatal(got)
	}
}

func TestStatusFileSetting(t *testing.T) {
	base := "site: test-site\nupstream:\n  url: https://connect.example.com\n  token: " + testToken + "\n  public-keys: [" + testKey + "]\n"
	c, err := Parse([]byte(base))
	if err != nil || c.StatusFile != DefaultStatusFile {
		t.Fatalf("default %q %v", c.StatusFile, err)
	}
	c, err = Parse([]byte(base + "status-file: /tmp/s.json\n"))
	if err != nil || c.StatusFile != "/tmp/s.json" {
		t.Fatalf("%q %v", c.StatusFile, err)
	}
	if _, err = Parse([]byte(base + "status-file: s.json\n")); err == nil || !strings.Contains(err.Error(), "status-file must be an absolute path") {
		t.Fatal(err)
	}
}
