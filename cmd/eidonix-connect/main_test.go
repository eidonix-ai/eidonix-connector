package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eidonix-ai/eidonix-connector/internal/hub"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := execute(args, &out, &errOut)
	return out.String(), errOut.String(), err
}

func TestTokenServerKeyVersion(t *testing.T) {
	out, _, err := run(t, "token")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := protocol.ParseToken(strings.TrimSpace(out)); err != nil {
		t.Fatalf("%q: %v", out, err)
	}
	out, errOut, err := run(t, "server-key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.ParseServerKey(strings.TrimSpace(out)); err != nil || !strings.Contains(errOut, "public key") {
		t.Fatalf("%q %q %v", out, errOut, err)
	}
	if out, _, _ := run(t, "version"); strings.TrimSpace(out) != version {
		t.Fatal(out)
	}
	if _, _, err := run(t, "bogus"); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatal(err)
	}
	if _, _, err := run(t, "token", "--url", "x"); err == nil {
		t.Fatal("--url accepted outside status")
	}
}

func TestCheckAndStatus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "connect.yaml")
	tok, _ := hub.NewToken()
	file := "public: 127.0.0.1:0\nlocal: 127.0.0.1:0\nserver-key: " + base64.StdEncoding.EncodeToString(make([]byte, 32)) +
		"\naccess-log: " + filepath.Join(dir, "access.log") + "\nsites:\n  site-a:\n    token: " + tok + "\n    services:\n      files:\n        applications: [app-one]\n"
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}
	out, _, err := run(t, "check", "--config", path)
	if err != nil || !strings.Contains(out, "site-a  token set; any application") || strings.Contains(out, tok) {
		t.Fatalf("%q %v", out, err)
	}
	srv := &hub.Server{Path: path}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Serve(ctx)
	<-srv.Ready()
	url := "http://" + srv.LocalAddr.String()
	out, _, err = run(t, "status", "--url", url)
	if err != nil || !strings.Contains(out, "SITE") || !strings.Contains(out, "site-a") || !strings.Contains(out, "offline") {
		t.Fatalf("%q %v", out, err)
	}
	if out, _, err = run(t, "status", "--url", url, "--healthz"); err != nil || strings.TrimSpace(out) != "ok" {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = run(t, "status", "--url", "http://127.0.0.1:1", "--healthz"); err == nil {
		t.Fatal("healthz of nothing succeeded")
	}
	os.Chmod(path, 0o644)
	if _, _, err := run(t, "check", "--config", path); err == nil || !strings.Contains(err.Error(), "chmod 0600") {
		t.Fatal(err)
	}
}
