package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const goodKeyServer = `key-server:
  allowed-domains: [example.com, mail.example.org]
  smtp:
    address: mail.example.com:465
    username: keys@example.com
    password-file: /etc/eidonix-connector/smtp.pass
    from: keys@example.com
`

func TestParseKeyServer(t *testing.T) {
	c, err := parse(t, goodKeyServer)
	if err != nil {
		t.Fatal(err)
	}
	k := c.KeyServer
	if k == nil || strings.Join(k.AllowedDomains, ",") != "example.com,mail.example.org" || k.ClaimTTL != 24*time.Hour ||
		k.StateFile != DefaultKeyStateFile || k.Link != "cletus://claim" || k.SMTP.Address != "mail.example.com:465" ||
		k.SMTP.Username != "keys@example.com" || k.SMTP.PasswordFile != "/etc/eidonix-connector/smtp.pass" || k.SMTP.From != "keys@example.com" {
		t.Fatalf("%+v", k)
	}
	if c, _ = parse(t, ""); c.KeyServer != nil {
		t.Fatal("key-server set without a block")
	}
	c, err = parse(t, strings.Replace(goodKeyServer, "  smtp:", "  claim-ttl: 2h\n  state-file: /tmp/k.json\n  link: myapp://claim\n  smtp:", 1))
	if err != nil || c.KeyServer.ClaimTTL != 2*time.Hour || c.KeyServer.StateFile != "/tmp/k.json" || c.KeyServer.Link != "myapp://claim" {
		t.Fatalf("%+v %v", c.KeyServer, err)
	}
	if c, err = parse(t, strings.Replace(goodKeyServer, ":465", ":587", 1)); err != nil || c.KeyServer.SMTP.Address != "mail.example.com:587" {
		t.Fatal(err)
	}
}

func TestKeyServerErrors(t *testing.T) {
	for _, tc := range []struct{ name, from, to, want string }{
		{"no domains", "  allowed-domains: [example.com, mail.example.org]\n", "", "allowed-domains is required"},
		{"empty domains", "[example.com, mail.example.org]", "[]", "allowed-domains must list"},
		{"uppercase domain", "[example.com,", "[Example.com,", `domain "Example.com" must be lowercase`},
		{"domain without dot", "[example.com,", "[localhost,", `domain "localhost"`},
		{"no smtp", "  smtp:\n    address: mail.example.com:465\n    username: keys@example.com\n    password-file: /etc/eidonix-connector/smtp.pass\n    from: keys@example.com\n", "", "smtp is required"},
		{"smtp port", ":465", ":25", "address must be host:465"},
		{"smtp no port", "mail.example.com:465", "mail.example.com", "address must be host:465"},
		{"relative password file", "/etc/eidonix-connector/smtp.pass", "smtp.pass", "password-file must be an absolute path"},
		{"missing username", "    username: keys@example.com\n", "", "username is required"},
		{"from with a name", "from: keys@example.com", "from: Keys <keys@example.com>", "from must be an e-mail address"},
		{"unknown smtp setting", "    from:", "    tls: true\n    from:", `unknown setting "tls" in key-server smtp`},
		{"from-name with angle brackets", "    from: keys@example.com\n", "    from: keys@example.com\n    from-name: Keys <x>\n", "from-name must be a short display name"},
		{"unknown setting", "  smtp:", "  port: 1\n  smtp:", `unknown setting "port" in key-server`},
		{"relative state file", "  smtp:", "  state-file: keys.json\n  smtp:", "state-file must be an absolute path"},
		{"long claim ttl", "  smtp:", "  claim-ttl: 48h\n  smtp:", "claim-ttl must be a duration"},
		{"link with query", "  smtp:", "  link: cletus://claim?x=1\n  smtp:", "link must be a URL without a query"},
	} {
		body := goodKeyServer
		if !strings.Contains(body, tc.from) {
			t.Fatalf("%s: %q not in the block", tc.name, tc.from)
		}
		_, err := parse(t, strings.Replace(body, tc.from, tc.to, 1))
		if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "line ") {
			t.Errorf("%s: got %v, want %q", tc.name, err, tc.want)
		}
	}
}

func TestLoadGatesTheKeyServerFiles(t *testing.T) {
	dir := t.TempDir()
	pass, state, file := filepath.Join(dir, "smtp.pass"), filepath.Join(dir, "keys.json"), filepath.Join(dir, "connector.yaml")
	body := upstreamYAML() + strings.NewReplacer("/etc/eidonix-connector/smtp.pass", pass).Replace(goodKeyServer) + "  state-file: " + state + "\n"
	for p, mode := range map[string]os.FileMode{pass: 0o600, file: 0o600} {
		if err := os.WriteFile(p, []byte(map[string]string{pass: "pw\n", file: body}[p]), mode); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Load(file); err != nil {
		t.Fatal(err)
	}
	os.Chmod(pass, 0o644)
	if _, err := Load(file); err == nil || !strings.Contains(err.Error(), "key-server smtp password-file") || !strings.Contains(err.Error(), "chmod") {
		t.Fatalf("loose password file: %v", err)
	}
	os.Chmod(pass, 0o600)
	os.Remove(pass)
	if _, err := Load(file); err == nil || !strings.Contains(err.Error(), "password-file") {
		t.Fatalf("missing password file: %v", err)
	}
	os.WriteFile(pass, []byte("pw\n"), 0o600)
	os.WriteFile(state, []byte("{}"), 0o666)
	os.Chmod(state, 0o666)
	if _, err := Load(file); err == nil || !strings.Contains(err.Error(), "key-server state-file") {
		t.Fatalf("loose state file: %v", err)
	}
	os.Chmod(state, 0o600)
	c, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if d := c.Describe(); !strings.Contains(d, "Issues account keys for example.com, mail.example.org") || !strings.Contains(d, state) {
		t.Fatalf("describe:\n%s", d)
	}
}

func TestRedactKeepsTheKeyServerBlock(t *testing.T) {
	out, err := Redact([]byte(upstreamYAML() + goodKeyServer))
	if err != nil {
		t.Fatal(err)
	}
	// The block holds no secret (the password is in its own file); the upstream token is hidden.
	if !strings.Contains(string(out), "password-file: /etc/eidonix-connector/smtp.pass") || strings.Contains(string(out), testToken) {
		t.Fatalf("redacted:\n%s", out)
	}
	if _, err = Parse([]byte(strings.ReplaceAll(string(out), Redacted, testToken))); err != nil {
		t.Fatal(err)
	}
}

func TestKeyServerFromName(t *testing.T) {
	c, err := parse(t, strings.Replace(goodKeyServer, "    from: keys@example.com\n", "    from: keys@example.com\n    from-name: Cletus\n", 1))
	if err != nil {
		t.Fatal(err)
	}
	if c.KeyServer.SMTP.FromName != "Cletus" {
		t.Fatalf("from-name = %q", c.KeyServer.SMTP.FromName)
	}
}
