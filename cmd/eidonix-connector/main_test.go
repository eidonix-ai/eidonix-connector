package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootRefusedBeforeConfigRead(t *testing.T) {
	var b bytes.Buffer
	err := execute([]string{"run", "--config", "/does/not/exist"}, &b, &b, 0)
	if err == nil || !strings.Contains(err.Error(), "root") {
		t.Fatalf("got %v", err)
	}
}
func TestUnknownCommand(t *testing.T) {
	var b bytes.Buffer
	for _, args := range [][]string{{"exec"}, {"check-config"}, {"print-systemd-network"}, {}} {
		if execute(args, &b, &b, 1000) == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
func TestCheckRefusesReadableConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connector.yaml")
	os.WriteFile(path, []byte("upstream:\n  url: https://connect.example.com\n"), 0644)
	var out bytes.Buffer
	err := execute([]string{"check", "--config", path}, &out, &out, os.Geteuid())
	if err == nil || !strings.Contains(err.Error(), "Fix: ") {
		t.Fatal(err)
	}
}

func writeConfig(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "connector.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCheckExampleGolden: check on examples/gpu-server.yaml prints the
// golden policy text (the permission line names this test's owner and mode).
func TestCheckExampleGolden(t *testing.T) {
	example, err := os.ReadFile("../../examples/gpu-server.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := writeConfig(t, example)
	var out bytes.Buffer
	if err := execute([]string{"check", "--config", path}, &out, &out, os.Geteuid()); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("../../internal/config/testdata/gpu-server.check.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, golden := strings.Split(out.String(), "\n"), strings.Split(string(want), "\n")
	if len(got) != len(golden) {
		t.Fatalf("check output:\n%s", out.String())
	}
	for i := range got {
		if strings.HasPrefix(golden[i], "File permissions: ok (") {
			if !strings.HasPrefix(got[i], "File permissions: ok (0600 ") || !strings.HasSuffix(got[i], "). Secrets set: token.") {
				t.Fatalf("permission line %q", got[i])
			}
			continue
		}
		if got[i] != golden[i] {
			t.Fatalf("line %d:\n got  %q\n want %q", i+1, got[i], golden[i])
		}
	}
}
func TestCheckRedactedHidesSecrets(t *testing.T) {
	secrets := []string{"sk-SENTINEL-api-key", "SENTINEL-access-key", "SENTINEL-secret-key", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=", "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	data := `# Example server
site: example-site
upstream:
  url: https://connect.example.com
  token: ` + secrets[4] + `
  public-keys: [AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=]
services:
  gpu1:
    address: 192.0.2.5:8000   # the big card
    allowed-endpoints: [POST /v1/*]
    models: [model-a]
    api-key: ` + secrets[0] + `
  files:
    kind: s3
    address: https://s3.example.com
    bucket: cletus
    access-key: ` + secrets[1] + `
    secret-key: ` + secrets[2] + `
    encryption: sse-c
    encryption-key: ` + secrets[3] + `
`
	path := writeConfig(t, []byte(data))
	var out bytes.Buffer
	if err := execute([]string{"check", "--config", path, "--redacted"}, &out, &out, os.Geteuid()); err != nil {
		t.Fatal(err)
	}
	for _, s := range secrets {
		if strings.Contains(out.String(), s) {
			t.Fatalf("secret %q in:\n%s", s, out.String())
		}
	}
	for _, want := range []string{"# Example server", "# the big card", "api-key: •••• (set)", "token: •••• (set)", "public-keys: [AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=]"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in:\n%s", want, out.String())
		}
	}
	// Plain check never prints a secret either.
	out.Reset()
	execute([]string{"check", "--config", path}, &out, &out, os.Geteuid())
	for _, s := range secrets {
		if strings.Contains(out.String(), s) {
			t.Fatalf("check printed secret %q", s)
		}
	}
}
func TestInitWritesTemplateOnce(t *testing.T) {
	var out, errs bytes.Buffer
	if err := execute([]string{"init"}, &out, &errs, 1000); err != nil || !strings.Contains(out.String(), "PASTE_TOKEN_FROM_SETUP") {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "connector.yaml")
	if err := execute([]string{"init", path}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&^0640 != 0 {
		t.Fatal(info.Mode(), err)
	}
	if err := execute([]string{"init", path}, &out, &errs, 1000); err == nil || !strings.Contains(err.Error(), "never overwrites") {
		t.Fatal(err)
	}
	// The blank template fails check only because of the placeholders.
	os.Chmod(path, 0600)
	err = execute([]string{"check", "--config", path}, &out, &errs, os.Geteuid())
	if err == nil || !strings.Contains(err.Error(), "must be the connector's name in Eidonix Setup") {
		t.Fatal(err)
	}
}
func TestGenerateKey(t *testing.T) {
	var out bytes.Buffer
	if err := execute([]string{"generate-key"}, &out, &out, 1000); err != nil {
		t.Fatal(err)
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out.String()))
	if err != nil || len(key) != 32 {
		t.Fatal(out.String(), err)
	}
}
func TestConvertCommand(t *testing.T) {
	var out, errs bytes.Buffer
	if err := execute([]string{"convert", "../../internal/convert/testdata/site-b.v02.yaml", "--site", "example-plant"}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	want, _ := os.ReadFile("../../internal/convert/testdata/site-b.v03.yaml")
	if out.String() != string(want) || !strings.HasPrefix(errs.String(), "Review before use:\n  - ") || !strings.Contains(errs.String(), "eidonix-connector storage move-to-root s3-storage --from cletus/") {
		t.Fatalf("stdout:\n%s\nstderr:\n%s", out.String(), errs.String())
	}
	out.Reset()
	if err := execute([]string{"convert", "--site", "example-gpu-site", "../../internal/convert/testdata/v01/config.toml"}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	if want, _ := os.ReadFile("../../internal/convert/testdata/v01.v03.yaml"); out.String() != string(want) {
		t.Fatal(out.String())
	}
	if err := execute([]string{"convert", "../../internal/convert/testdata/site-b.v02.yaml"}, &out, &errs, 1000); err == nil || !strings.Contains(err.Error(), "--site") {
		t.Fatal(err)
	}
	if execute([]string{"convert", "--site", "x"}, &out, &errs, 1000) == nil {
		t.Fatal("convert without a file accepted")
	}
}

// TestOldConfigPointsToConvert: run and check refuse a v0.2 file and say how to convert it.
func TestOldConfigPointsToConvert(t *testing.T) {
	old, _ := os.ReadFile("../../internal/convert/testdata/site-a.v02.yaml")
	path := writeConfig(t, old)
	var out bytes.Buffer
	err := execute([]string{"check", "--config", path}, &out, &out, os.Geteuid())
	if err == nil || !strings.Contains(err.Error(), "v0.2 configuration") || !strings.Contains(err.Error(), "eidonix-connector convert --site") {
		t.Fatal(err)
	}
}
func TestStorageCommandArguments(t *testing.T) {
	path := writeConfig(t, []byte("site: example-site\nupstream:\n  url: https://connect.example.com\n  token: cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n  public-keys: [AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=]\nservices:\n  gpu1:\n    address: 192.0.2.5:8000\n    allowed-endpoints: [POST /v1/*]\n"))
	var out bytes.Buffer
	for args, want := range map[string]string{
		"storage move-to-root files":                "needs --from",
		"storage move-to-root files --from cletus/": "has no service \"files\"",
		"storage move-to-root gpu1 --from cletus/":  "not an s3 service",
		"storage copy files --from cletus/":         "unexpected arguments",
		"storage move-to-root --from cletus/":       "unexpected arguments",
		"check --from cletus/":                      "apply to storage move-to-root only",
		"check --site x":                            "applies to convert only",
		"storage move-to-root a b --from cletus/":   "unexpected arguments",
	} {
		err := execute(append(strings.Fields(args), "--config", path), &out, &out, os.Geteuid())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", args, err)
		}
	}
}
