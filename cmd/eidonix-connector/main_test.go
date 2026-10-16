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
