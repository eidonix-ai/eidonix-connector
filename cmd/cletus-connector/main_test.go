package main

import (
	"bytes"
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
func TestStarterFailsClosed(t *testing.T) {
	var out, errs bytes.Buffer
	if err := execute([]string{"init", "--profile", "vllm", "--upstream", "http://192.0.2.20:8000"}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	os.WriteFile(path, out.Bytes(), 0600)
	if err := execute([]string{"check-config", "--config", path}, &errs, &errs, 1000); err == nil {
		t.Fatal("placeholder key accepted")
	}
}
func TestStarterRejectsPath(t *testing.T) {
	var b bytes.Buffer
	if err := execute([]string{"init", "--upstream", "http://192.0.2.20:8000/admin"}, &b, &b, 1000); err == nil {
		t.Fatal("upstream path accepted")
	}
}
func TestUnknownCommand(t *testing.T) {
	var b bytes.Buffer
	if execute([]string{"exec"}, &b, &b, 1000) == nil {
		t.Fatal("unknown command accepted")
	}
}

func TestCheckConfigPrintsEffectivePolicyWithoutSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := `server: https://127.0.0.1
 token_file: /unused
 server_keys: {test: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=}
 routes:
   - id: chat
     type: openai_chat
     upstreams: [http://127.0.0.1:8000]
     upstream_headers: {Authorization: "Bearer SECRET_SENTINEL"}
 `
	data = strings.ReplaceAll(data, "\n ", "\n")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	if err := execute([]string{"check-config", "--config", path}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "/v1/chat/completions") || !strings.Contains(out.String(), "16MiB") {
		t.Fatalf("missing effective policy: %s", out.String())
	}
	if strings.Contains(out.String(), "SECRET_SENTINEL") {
		t.Fatal("secret disclosed")
	}
}

func TestGPUNodeStarterAndRepeatedUpstreams(t *testing.T) {
	var out, errs bytes.Buffer
	if err := execute([]string{"init", "--profile", "cletus-gpu-node", "--upstream", "http://192.0.2.20:8000", "--upstream", "http://192.0.2.21:8000"}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"192.0.2.20:8004", "192.0.2.21:8004", "document_ocr", "audio_transcription", "rerank-jina"} {
		if !strings.Contains(out.String(), value) {
			t.Fatalf("missing %s", value)
		}
	}
}
