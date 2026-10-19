package hub

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// Synthetic all-zero test credentials.
const (
	tokenA = "cxn_aaaaaaaaaaaaaaaa_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	tokenB = "cxn_bbbbbbbbbbbbbbbb_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	tokenC = "cxn_cccccccccccccccc_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
)

var zeroSeed = base64.StdEncoding.EncodeToString(make([]byte, 32))

func validFile() string {
	return `public: :8443
local: :8080
server-key: ` + zeroSeed + `
sites:
  site-a:
    token: ` + tokenA + `
  site-b:
    token: ` + tokenB + `
    next-token: ` + tokenC + `
    disabled: true
    applications: [app-one]
    services:
      files:
        applications: [app-two]
`
}

func TestParseValid(t *testing.T) {
	c, err := Parse([]byte(validFile()))
	if err != nil {
		t.Fatal(err)
	}
	if c.Public != ":8443" || c.Local != ":8080" || c.AccessLog != DefaultAccessLog || strings.Join(c.Order, ",") != "site-a,site-b" {
		t.Fatalf("%+v", c)
	}
	if want := protocol.KeyID(ed25519.NewKeyFromSeed(make([]byte, 32)).Public().(ed25519.PublicKey)); c.KeyID != want {
		t.Fatal(c.KeyID)
	}
	a, b := c.Sites["site-a"], c.Sites["site-b"]
	if a.Applications != nil || a.Disabled || !a.Accepts(tokenA) || a.Accepts(tokenB) {
		t.Fatalf("%+v", a)
	}
	if !b.Disabled || !b.Accepts(tokenB) || !b.Accepts(tokenC) {
		t.Fatalf("%+v", b)
	}
	// Per-service applications replace the site's list for that service.
	for _, tc := range []struct {
		service, app string
		want         bool
	}{{"chat", "app-one", true}, {"chat", "app-two", false}, {"chat", "", false}, {"files", "app-two", true}, {"files", "app-one", false}} {
		if got := b.Allows(tc.service, tc.app); got != tc.want {
			t.Errorf("site-b %s by %q: %v", tc.service, tc.app, got)
		}
	}
	if !a.Allows("anything", "") {
		t.Fatal("a site without a list must allow everyone")
	}
	if s := c.siteForToken(tokenC); s == nil || s.Name != "site-b" {
		t.Fatal("next-token not matched")
	}
	if c.siteForToken("cxn_dddddddddddddddd_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") != nil {
		t.Fatal("unknown token matched")
	}
	d := c.Describe()
	for _, want := range []string{c.PublicKey, "site-a  token set; any application", "DISABLED", "service files: only for applications: app-two"} {
		if !strings.Contains(d, want) {
			t.Errorf("describe lacks %q:\n%s", want, d)
		}
	}
	if strings.Contains(d, tokenA) || strings.Contains(d, zeroSeed) {
		t.Fatal("describe printed a secret")
	}
}

func TestParseErrorsNameTheLine(t *testing.T) {
	base := "public: :8443\nlocal: :8080\nserver-key: " + zeroSeed + "\n"
	for _, tc := range []struct{ file, want string }{
		{"", "the file is empty"},
		{"public: :8443\nlocal: :8080\n", "line 1: server-key is required"},
		{base + "listen: :1\n", `line 4: unknown setting "listen" (expected one of: public, local, server-key, access-log, sites)`},
		{"public: 8443\nlocal: :8080\nserver-key: " + zeroSeed + "\n", "line 1: public must be an address"},
		{"public: :8080\nlocal: :8080\nserver-key: " + zeroSeed + "\n", "line 2: local must not be the same address as public"},
		{"public: :8443\nlocal: :8080\nserver-key: c2hvcnQ=\n", "line 3: server-key must be a 32-byte"},
		{base + "access-log: relative.log\n", "line 4: access-log must be an absolute path"},
		{base + "sites:\n  Site-A:\n    token: " + tokenA + "\n", `line 5: site name "Site-A"`},
		{base + "sites:\n  v1:\n    token: " + tokenA + "\n", `line 5: site name "v1" is reserved`},
		{base + "sites:\n  site-a:\n    disabled: true\n", `line 5: site "site-a": token is required`},
		{base + "sites:\n  site-a:\n    token: secret\n", `line 6: site "site-a": token is not a connection token`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n    tokn: x\n", `line 7: unknown setting "tokn" in site "site-a" (expected one of: token, next-token, disabled, applications, services)`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n  site-b:\n    token: " + tokenA + "\n", `line 7: site "site-b": a token with id aaaaaaaaaaaaaaaa is already used by site "site-a"`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n    disabled: yes please\n", `line 7: site "site-a": disabled must be true or false`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n    applications: []\n", `line 7: site "site-a": applications must list 1 to 64`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n    applications: [a, a]\n", `site "site-a": application "a" is listed twice`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n    services:\n      files:\n        apps: [x]\n", `line 9: unknown setting "apps" in site "site-a" service "files"`},
		{base + "sites:\n  site-a:\n    token: " + tokenA + "\n    services:\n      Files: {}\n", `line 8: site "site-a": service name "Files"`},
		{base + "---\n---\n", "exactly one YAML document"},
	} {
		_, err := Parse([]byte(tc.file))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q:\n got %v\nwant %s", tc.file, err, tc.want)
		}
	}
}

func TestServerKeyEncodings(t *testing.T) {
	seed := make([]byte, 32)
	seed[0] = 7
	key := ed25519.NewKeyFromSeed(seed)
	for _, enc := range []string{base64.StdEncoding.EncodeToString(seed), base64.StdEncoding.EncodeToString(key)} {
		got, err := ParseServerKey(enc)
		if err != nil || !got.Equal(key) {
			t.Fatalf("%v", err)
		}
	}
	bad := append([]byte{}, key...)
	bad[40] ^= 1
	if _, err := ParseServerKey(base64.StdEncoding.EncodeToString(bad)); err == nil {
		t.Fatal("a 64-byte key whose public half doesn't match its seed was accepted")
	}
}

func TestNewTokenAndServerKey(t *testing.T) {
	tok, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := protocol.ParseToken(tok); err != nil {
		t.Fatalf("%s: %v", tok, err)
	}
	priv, pub, err := NewServerKey()
	if err != nil {
		t.Fatal(err)
	}
	k, err := ParseServerKey(priv)
	if err != nil || base64.StdEncoding.EncodeToString(k.Public().(ed25519.PublicKey)) != pub {
		t.Fatal(err)
	}
}

func TestPermissionGate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connect.yaml")
	if err := os.WriteFile(path, []byte(validFile()), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "Fix: chmod 0600 "+path) {
		t.Fatalf("world-readable file: %v", err)
	}
	os.Chmod(path, 0o640)
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(path)
	if err := CheckPermissions(path, fi, os.Geteuid()+1); os.Geteuid() != 0 && (err == nil || !strings.Contains(err.Error(), "must be owned by root")) {
		t.Fatalf("other owner: %v", err)
	}
}

func TestLocalURL(t *testing.T) {
	for in, want := range map[string]string{":8080": "http://127.0.0.1:8080", "10.0.0.5:9000": "http://10.0.0.5:9000", "[::]:80": "http://127.0.0.1:80"} {
		if got := (&Config{Local: in}).LocalURL(); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
}

func TestRateLimiter(t *testing.T) {
	l := newRateLimiter(1, 2)
	if !l.allow("a") || !l.allow("a") || l.allow("a") {
		t.Fatal("burst not enforced")
	}
	if !l.allow("b") {
		t.Fatal("keys share buckets")
	}
}
