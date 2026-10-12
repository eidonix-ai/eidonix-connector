package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/eidonix-ai/cletus-connector/internal/config"
	"github.com/eidonix-ai/cletus-connector/protocol"
)

func TestInitializeDevelopmentCredentials(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := initialize(dir, "http://127.0.0.1:8000"); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(filepath.Join(dir, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server != "https://127.0.0.1" || len(cfg.Routes) != 1 {
		t.Fatal("unexpected development config")
	}
	token, err := os.ReadFile(filepath.Join(dir, "token"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = protocol.ParseToken(string(token[:len(token)-1])); err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(filepath.Join(dir, "identity.key"))
	if err != nil || len(key) != ed25519.PrivateKeySize {
		t.Fatal("invalid identity key")
	}
	if _, err = tls.LoadX509KeyPair(filepath.Join(dir, "tls.pem"), filepath.Join(dir, "tls.key")); err != nil {
		t.Fatal(err)
	}
	if err = initialize(dir, "http://127.0.0.1:9999"); err == nil {
		t.Fatal("existing state directory overwritten")
	}
	for _, name := range []string{"token", "identity.key", "tls.key"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("unsafe %s permissions", name)
		}
	}
}
