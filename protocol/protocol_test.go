package protocol

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGoldenHeader(t *testing.T) {
	var b bytes.Buffer
	err := WriteStreamHeader(&b, StreamHeader{V: 1, Route: "chat", Kind: "http", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/stream-header.json")
	if err != nil {
		t.Fatal(err)
	}
	want := string(golden)
	if b.String() != want {
		t.Fatalf("%q", b.String())
	}
	h, e := ReadStreamHeader(bufio.NewReader(&b))
	if e != nil || h.Route != "chat" {
		t.Fatal(h, e)
	}
}
func TestBoundAndReadAhead(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("{\"v\":1,\"route\":\"r\",\"kind\":\"http\"}\nPOST /x HTTP/1.1\r\n"))
	if _, e := ReadStreamHeader(r); e != nil {
		t.Fatal(e)
	}
	s, _ := r.ReadString('\n')
	if s != "POST /x HTTP/1.1\r\n" {
		t.Fatal(s)
	}
	if e := ReadJSONLine(bufio.NewReader(strings.NewReader(strings.Repeat("x", MaxHeaderBytes)+"\n")), MaxHeaderBytes, new(any)); e == nil {
		t.Fatal("oversize accepted")
	}
}
func TestIdentity(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().UTC().Truncate(time.Second)
	ts := now.Format(time.RFC3339)
	h := http.Header{}
	h.Set("X-Cletus-Server-Key-Id", "k")
	h.Set("X-Cletus-Server-Time", ts)
	h.Set("X-Cletus-Server-Signature", SignIdentity(key, "n", "id", "i", ts))
	keys := map[string]ed25519.PublicKey{"k": pub}
	if e := VerifyIdentity(h, "n", "id", "i", keys, now); e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"replay", ""} {
		if VerifyIdentity(h, n, "id", "i", keys, now) == nil {
			t.Fatal("replay accepted")
		}
	}
	if VerifyIdentity(h, "n", "id", "i", keys, now.Add(6*time.Minute)) == nil {
		t.Fatal("stale accepted")
	}
}
func TestToken(t *testing.T) {
	token := "cxn_ABCDEFGHIJKLMNOP_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{255}, 32))
	if _, _, e := ParseToken(token); e != nil {
		t.Fatal(e)
	}
}
func TestFrames(t *testing.T) {
	var b bytes.Buffer
	f := Frame{2, []byte("hello")}
	if e := WriteFrame(&b, f, 5); e != nil {
		t.Fatal(e)
	}
	got, e := ReadFrame(&b, 5)
	if e != nil || !bytes.Equal(got.Payload, f.Payload) {
		t.Fatal(got, e)
	}
	if e := WriteFrame(&b, Frame{9, nil}, 5); e == nil {
		t.Fatal("bad type")
	}
}
func FuzzHeader(f *testing.F) {
	f.Add([]byte("{\"v\":1,\"route\":\"r\",\"kind\":\"http\"}\n"))
	f.Fuzz(func(t *testing.T, b []byte) { ReadStreamHeader(bufio.NewReader(bytes.NewReader(b))) })
}
func FuzzFrame(f *testing.F) {
	f.Add([]byte{2, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) { ReadFrame(bytes.NewReader(b), 4096) })
}
func FuzzControl(f *testing.F) {
	f.Add([]byte("{\"v\":1,\"type\":\"ping\"}\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		ReadJSONLine(bufio.NewReader(bytes.NewReader(b)), MaxControlBytes, new(Message))
	})
}
func FuzzToken(f *testing.F) {
	f.Add("cxn_ABCDEFGHIJKLMNOP_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Fuzz(func(t *testing.T, s string) { ParseToken(s) })
}
