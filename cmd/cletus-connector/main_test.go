package main

import (
	"bytes"
	"github.com/eidonix-ai/cletus-connector/internal/config"
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
	path := filepath.Join(t.TempDir(), "config.toml")
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
	path := filepath.Join(t.TempDir(), "config.toml")
	data := `server = "https://127.0.0.1"
token_file = "/unused"
server_keys = { test = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" }

[[routes]]
id = "chat"
type = "openai_chat"
upstreams = ["http://127.0.0.1:8000"]
upstream_headers = { Authorization = "Bearer SECRET_SENTINEL" }

[routes.models]
"qwen" = "Qwen/Qwen3-32B"
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	if err := execute([]string{"check-config", "--config", path}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "/v1/chat/completions") || !strings.Contains(out.String(), "16MiB") || !strings.Contains(out.String(), `"models":{"qwen":"Qwen/Qwen3-32B"}`) {
		t.Fatalf("missing effective policy: %s", out.String())
	}
	if strings.Contains(out.String(), "SECRET_SENTINEL") {
		t.Fatal("secret disclosed")
	}
}

func TestGenericStarterAndRepeatedUpstreams(t *testing.T) {
	var out, errs bytes.Buffer
	if err := execute([]string{"init", "--profile", "generic", "--upstream", "http://192.0.2.20:8000", "--upstream", "http://192.0.2.21:8000"}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"192.0.2.20:8000", "192.0.2.21:8000", "openai_chat"} {
		if !strings.Contains(out.String(), value) {
			t.Fatalf("missing %s", value)
		}
	}
}

func TestGPUStarterDoesNotGuessAdditionalServices(t *testing.T) {
	var out, errs bytes.Buffer
	if err := execute([]string{"init", "--profile", "generic", "--upstream", "http://192.0.2.20:8080"}, &out, &errs, 1000); err != nil {
		t.Fatal(err)
	}
	decoded, err := config.Decode(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Routes) != 1 || decoded.Routes[0].Type != "openai_chat" || len(decoded.Routes[0].Upstreams) != 1 || decoded.Routes[0].Upstreams[0] != "http://192.0.2.20:8080" {
		t.Fatalf("starter guessed services: %s", out.String())
	}
}

func TestStorageSSECOverHTTPRefusesRunAndCheck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	data := `server = "https://gateway.example.com"
token_file = "unused"

[server_keys]
test = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

[[routes]]
id = "files"
type = "object_storage"
endpoint = "http://127.0.0.1:9000"
path_style = true
allow_insecure_http = true

[routes.encryption]
mode = "sse-c"
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"run", "check-config"} {
		var out, errs bytes.Buffer
		err := execute([]string{command, "--config", path}, &out, &errs, 1000)
		if err == nil || !strings.Contains(err.Error(), "sse-c requires HTTPS") {
			t.Fatalf("%s: %v", command, err)
		}
	}
}

func TestCheckConfigRejectsAliasesOnStorageAndConversion(t *testing.T) {
	for _, route := range []string{
		"id = \"files\"\ntype = \"object_storage\"\nendpoint = \"https://storage.example.com\"\n[routes.models]\na = \"b\"\n",
		"id = \"ocr\"\ntype = \"document_ocr\"\nupstreams = [\"http://127.0.0.1:8002\"]\n[routes.models]\na = \"b\"\n",
		"id = \"conv\"\ntype = \"document_conversion\"\nflavor = \"gotenberg\"\nupstreams = [\"http://127.0.0.1:3000\"]\n[routes.models]\na = \"b\"\n",
	} {
		path := filepath.Join(t.TempDir(), "config.toml")
		data := "server = \"https://gateway.example.com\"\ntoken_file = \"unused\"\n[server_keys]\ntest = \"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\"\n[[routes]]\n" + route
		os.WriteFile(path, []byte(data), 0600)
		var out bytes.Buffer
		if err := execute([]string{"check-config", "--config", path}, &out, &out, 1000); err == nil || !strings.Contains(err.Error(), "model aliases are not supported") {
			t.Fatalf("%s: %v", route, err)
		}
	}
}
