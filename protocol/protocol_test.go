package protocol

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite golden files under testdata")

// readGolden returns testdata/name, rewriting it first with got under -update.
func readGolden(t *testing.T, name string, got []byte) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if e := os.WriteFile(path, got, 0644); e != nil {
			t.Fatal(e)
		}
	}
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestGoldenHeader(t *testing.T) {
	var b bytes.Buffer
	err := WriteStreamHeader(&b, StreamHeader{V: Version, Service: "chat", Kind: "http", RequestID: "r1", OrgID: "7", OrgSlug: "example-org", UserKeyID: "42"})
	if err != nil {
		t.Fatal(err)
	}
	if want := readGolden(t, "stream-header.json", b.Bytes()); b.String() != string(want) {
		t.Fatalf("%q", b.String())
	}
	h, e := ReadStreamHeader(bufio.NewReader(&b))
	if e != nil || h.Service != "chat" || h.OrgSlug != "example-org" || h.UserKeyID != "42" || h.OrgID != "7" {
		t.Fatal(h, e)
	}
}
func TestStreamHeaderRejectsV1AndBadMetadata(t *testing.T) {
	for _, line := range []string{
		`{"v":1,"route":"chat","kind":"http"}`,
		`{"v":2,"route":"chat","kind":"http"}`,
		`{"v":2,"service":"chat","kind":"http","org_slug":"a\nb"}`,
		`{"v":2,"service":"chat","kind":"http","user_key_id":"` + strings.Repeat("1", MaxStreamMetadataBytes+1) + `"}`,
	} {
		if _, e := ReadStreamHeader(bufio.NewReader(strings.NewReader(line + "\n"))); e == nil {
			t.Fatalf("accepted %s", line)
		}
	}
}
func TestBoundAndReadAhead(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("{\"v\":2,\"service\":\"r\",\"kind\":\"http\"}\nPOST /x HTTP/1.1\r\n"))
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

// identityVector is the fixed v2 signature vector in testdata/identity-vector.json.
type identityVector struct {
	Seed       string `json:"ed25519_seed_hex"`
	PublicKey  string `json:"public_key"`
	KeyID      string `json:"key_id"`
	Nonce      string `json:"nonce"`
	Instance   string `json:"instance"`
	ServerTime string `json:"server_time"`
	Payload    string `json:"payload"`
	Signature  string `json:"signature"`
}

func TestIdentityVector(t *testing.T) {
	seed := bytes.Repeat([]byte{7}, ed25519.SeedSize)
	key := ed25519.NewKeyFromSeed(seed)
	pub := key.Public().(ed25519.PublicKey)
	v := identityVector{Seed: hex.EncodeToString(seed), PublicKey: base64.StdEncoding.EncodeToString(pub), KeyID: KeyID(pub), Nonce: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Instance: "00000000-0000-4000-8000-000000000001", ServerTime: "2026-10-16T12:00:00Z"}
	v.Payload = string(IdentityPayload(v.Nonce, v.Instance, v.ServerTime))
	v.Signature = SignIdentity(key, v.Nonce, v.Instance, v.ServerTime)
	encoded, _ := json.MarshalIndent(v, "", "  ")
	encoded = append(encoded, '\n')
	golden := readGolden(t, "identity-vector.json", encoded)
	var want identityVector
	if e := json.Unmarshal(golden, &want); e != nil {
		t.Fatal(e)
	}
	if want != v {
		t.Fatalf("identity vector changed:\n%s", encoded)
	}
	if !strings.HasPrefix(want.Payload, "eidonix-connector.v2|") || len(want.KeyID) != len("eidonix-")+8 {
		t.Fatal(want)
	}
	h := http.Header{}
	h.Set(HeaderServerKeyID, want.KeyID)
	h.Set(HeaderServerTime, want.ServerTime)
	h.Set(HeaderServerSignature, want.Signature)
	parsed, e := ParsePublicKey(want.PublicKey)
	if e != nil {
		t.Fatal(e)
	}
	now, _ := time.Parse(time.RFC3339, want.ServerTime)
	if e := VerifyIdentity(h, want.Nonce, want.Instance, []ed25519.PublicKey{parsed}, now); e != nil {
		t.Fatal(e)
	}
}
func TestIdentity(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().UTC().Truncate(time.Second)
	h := IdentityHeaders(key, "n", "i", now)
	if h.Get(HeaderServerKeyID) != KeyID(pub) {
		t.Fatal(h)
	}
	keys := []ed25519.PublicKey{other, pub}
	if e := VerifyIdentity(h, "n", "i", keys, now); e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"replay", ""} {
		if VerifyIdentity(h, n, "i", keys, now) == nil {
			t.Fatal("replay accepted")
		}
	}
	if VerifyIdentity(h, "n", "other-instance", keys, now) == nil {
		t.Fatal("signature for another instance accepted")
	}
	if VerifyIdentity(h, "n", "i", keys, now.Add(6*time.Minute)) == nil {
		t.Fatal("stale accepted")
	}
	if VerifyIdentity(h, "n", "i", []ed25519.PublicKey{other}, now) == nil {
		t.Fatal("unlisted key accepted")
	}
	forged := h.Clone()
	forged.Set(HeaderServerKeyID, KeyID(other))
	if VerifyIdentity(forged, "n", "i", keys, now) == nil {
		t.Fatal("signature under a different key id accepted")
	}
}
func TestToken(t *testing.T) {
	token := "cxn_ABCDEFGHIJKLMNOP_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{255}, 32))
	if _, _, e := ParseToken(token); e != nil {
		t.Fatal(e)
	}
}
func TestControlGolden(t *testing.T) {
	var b bytes.Buffer
	for _, m := range []any{
		Auth{V: Version, Type: MessageAuth, Token: "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		AuthResult{V: Version, Type: MessageAuthOK, SessionID: "s-1", Org: "Example Org"},
		AuthResult{V: Version, Type: MessageAuthFailed, Reason: "invalid_token"},
		Hello{V: Version, Type: MessageHello, InstanceID: "00000000-0000-4000-8000-000000000001", Version: "0.2.0", OS: "linux", Arch: "amd64", Capabilities: []string{CapabilityInference, CapabilityStorage}, Services: []Service{
			{Name: "gpu1", Type: TypeChat, Models: []string{"model-a"}, MaxConcurrentRequests: 32, Healthy: true},
			{Name: "embed", Type: TypeEmbeddings, Models: []string{}, ModelsAny: true, MaxConcurrentRequests: 32},
			{Name: "files", Type: TypeStorage, Models: []string{}, MaxConcurrentRequests: 16, Storage: &StorageService{MaxKeyBytes: 1017, MaxObjectBytes: MaxStorageObjectBytes, Encryption: "sse-c"}},
		}},
		HelloAck{V: Version, Type: MessageHelloAck, Accepted: []string{"gpu1", "files"}, Rejected: []Rejected{{Name: "embed", Reason: "unknown_service"}}, Limits: Limits{MaxStreams: 64}},
		Status{V: Version, Type: MessageStatus, Services: []ServiceStatus{{Name: "gpu1", Healthy: true, InFlight: 2, Models: []string{"model-a"}}, {Name: "embed", Models: []string{}}}, UptimeS: 60},
		Control{V: Version, Type: MessagePing},
		Control{V: Version, Type: MessageClose, Reason: "revoked"},
	} {
		if e := WriteMessage(&b, m); e != nil {
			t.Fatal(e)
		}
	}
	if want := readGolden(t, "control.jsonl", b.Bytes()); b.String() != string(want) {
		t.Fatalf("control golden differs:\n%s", b.String())
	}
	r := bufio.NewReader(&b)
	types := []string{}
	for {
		env, e := ReadMessage(r)
		if e != nil {
			break
		}
		types = append(types, env.Type)
		if env.Type == MessageStatus {
			var s Status
			if e := env.Decode(&s); e != nil || s.Services[1].Models == nil {
				t.Fatal(s, e)
			}
		}
	}
	if strings.Join(types, ",") != "auth,auth_ok,auth_failed,hello,hello_ack,status,ping,close" {
		t.Fatal(types)
	}
}
func TestStatusAlwaysEncodesModels(t *testing.T) {
	for _, v := range []any{ServiceStatus{Name: "x"}, Service{Name: "x"}, Status{V: Version, Type: MessageStatus, Services: []ServiceStatus{{Name: "x"}}}} {
		b, _ := json.Marshal(v)
		if !strings.Contains(string(b), `"models":[]`) {
			t.Fatal(string(b))
		}
	}
}
func TestReadMessageRejectsOtherVersions(t *testing.T) {
	for _, line := range []string{`{"v":1,"type":"hello"}`, `{"v":2}`, `{"v":3,"type":"ping"}`} {
		if _, e := ReadMessage(bufio.NewReader(strings.NewReader(line + "\n"))); e == nil {
			t.Fatal(line)
		}
	}
}
func TestErrorCodes(t *testing.T) {
	for code, status := range map[string]int{"model_not_allowed": 403, "model_required": 400, "path_not_allowed": 403, "busy": 429, "upstream_timeout": 504} {
		if !ValidErrorCode(code) || HTTPStatus(code) != status {
			t.Fatal(code)
		}
	}
	if ValidErrorCode("invented") || NewStorageError("invented").Code != "internal" {
		t.Fatal("closed set accepted an invented code")
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
	f.Add([]byte("{\"v\":2,\"service\":\"r\",\"kind\":\"http\"}\n"))
	f.Fuzz(func(t *testing.T, b []byte) { ReadStreamHeader(bufio.NewReader(bytes.NewReader(b))) })
}
func FuzzFrame(f *testing.F) {
	f.Add([]byte{2, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) { ReadFrame(bytes.NewReader(b), 4096) })
}
func FuzzControl(f *testing.F) {
	f.Add([]byte("{\"v\":2,\"type\":\"ping\"}\n"))
	f.Fuzz(func(t *testing.T, b []byte) { ReadMessage(bufio.NewReader(bytes.NewReader(b))) })
}
func FuzzToken(f *testing.F) {
	f.Add("cxn_ABCDEFGHIJKLMNOP_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Fuzz(func(t *testing.T, s string) { ParseToken(s) })
}
