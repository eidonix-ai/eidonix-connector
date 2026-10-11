package session

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTokenFileBounds(t *testing.T) {
	p := filepath.Join(t.TempDir(), "token")
	if e := os.WriteFile(p, make([]byte, 513), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := readToken(p); e == nil {
		t.Fatal("oversize accepted")
	}
	if e := os.WriteFile(p, []byte("token"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(p, 0666); e != nil {
		t.Fatal(e)
	}
	if _, e := readToken(p); e == nil {
		t.Fatal("world-readable token accepted")
	}
	if e := os.Chmod(p, 0640); e != nil {
		t.Fatal(e)
	}
	if _, e := readToken(p); e != nil {
		t.Fatal(e)
	}
}
