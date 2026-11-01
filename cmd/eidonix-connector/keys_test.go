package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/keyserver"
)

// keyServerConfig writes a connector file with a key-server block and
// returns its path and state file.
func keyServerConfig(t *testing.T) (path, state string) {
	t.Helper()
	dir := t.TempDir()
	pass := filepath.Join(dir, "smtp.pass")
	state = filepath.Join(dir, "keys.json")
	if err := os.WriteFile(pass, []byte("pw\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	example, err := os.ReadFile("../../examples/gpu-server.yaml")
	if err != nil {
		t.Fatal(err)
	}
	block := "key-server:\n  allowed-domains: [example.com]\n  state-file: " + state + "\n  smtp:\n    address: mail.example.com:465\n    username: keys@example.com\n    password-file: " + pass + "\n    from: keys@example.com\n"
	return writeConfig(t, append([]byte(block), example...)), state
}

func TestKeysCommand(t *testing.T) {
	path, state := keyServerConfig(t)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	srv := keyserver.New(cfg.Site, *cfg.KeyServer, nil)
	body, _ := json.Marshal(map[string]string{"uid": "u1", "email": "pat@example.com", "name": "Pat Doe", "k_conn": strings.Repeat("ab", 32)})
	if ans, _ := srv.Handle("import", body); ans["ok"] != true {
		t.Fatal(ans)
	}
	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := execute(append(args, "--config", path), &out, &out, os.Geteuid())
		return out.String(), err
	}
	out, err := run("keys", "list")
	if err != nil || !strings.Contains(out, "UID") || !strings.Contains(out, "u1") || !strings.Contains(out, "pat@example.com") || !strings.Contains(out, "active") {
		t.Fatalf("list: %v\n%s", err, out)
	}
	if strings.Contains(out, strings.Repeat("ab", 32)) {
		t.Fatal("list prints k_conn")
	}
	if out, err = run("keys", "revoke", "u1"); err != nil || !strings.Contains(out, "u1 revoked") {
		t.Fatalf("revoke: %v %q", err, out)
	}
	if out, _ = run("keys", "list"); !strings.Contains(out, "revoked") {
		t.Fatalf("list after revoke:\n%s", out)
	}
	if out, err = run("keys", "restore", "u1"); err != nil || !strings.Contains(out, "u1 restored") {
		t.Fatalf("restore: %v %q", err, out)
	}
	if fi, err := os.Stat(state); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file %v %v", fi, err)
	}
	if _, err = run("keys", "revoke", "nobody"); err == nil || !strings.Contains(err.Error(), "unknown_uid") {
		t.Fatalf("unknown uid: %v", err)
	}
	for _, args := range [][]string{{"keys"}, {"keys", "revoke"}, {"keys", "list", "u1"}, {"keys", "delete", "u1"}} {
		if _, err = run(args...); err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
			t.Fatalf("%v: %v", args, err)
		}
	}
	// check explains the block and --redacted prints it.
	if out, err = run("check"); err != nil || !strings.Contains(out, "Issues account keys for example.com") {
		t.Fatalf("check: %v\n%s", err, out)
	}
	if out, err = run("check", "--redacted"); err != nil || !strings.Contains(out, "allowed-domains") || strings.Contains(out, "cxn_") {
		t.Fatalf("check --redacted: %v\n%s", err, out)
	}
}

func TestKeysNeedsAKeyServerBlock(t *testing.T) {
	example, _ := os.ReadFile("../../examples/gpu-server.yaml")
	var out bytes.Buffer
	err := execute([]string{"keys", "list", "--config", writeConfig(t, example)}, &out, &out, os.Geteuid())
	if err == nil || !strings.Contains(err.Error(), "no key-server block") {
		t.Fatal(err)
	}
}
