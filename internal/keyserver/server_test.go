package keyserver

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
)

type sentMail struct{ to, subject, body string }

// fakeMailer records mail; err makes every send fail.
type fakeMailer struct {
	sent []sentMail
	err  error
}

func (f *fakeMailer) Send(to, subject, body string) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, sentMail{to, subject, body})
	return nil
}

type rig struct {
	t    *testing.T
	s    *Server
	mail *fakeMailer
	file string
	now  time.Time
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{t: t, mail: &fakeMailer{}, file: filepath.Join(t.TempDir(), "state", "keys.json"), now: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	cfg := config.KeyServer{AllowedDomains: []string{"example.com"}, ClaimTTL: 24 * time.Hour, StateFile: r.file, Link: "cletus://claim"}
	r.s = New("site-a", cfg, r.mail)
	r.s.now = func() time.Time { return r.now }
	return r
}

// do runs an operation and returns its answer; ok says whether it succeeded.
func (r *rig) do(op string, req map[string]string) (map[string]any, bool) {
	r.t.Helper()
	body, _ := json.Marshal(req)
	ans, _ := r.s.Handle(op, body)
	return ans, ans["ok"] == true
}

// refused runs an operation that must fail with code.
func (r *rig) refused(op string, req map[string]string, code string) {
	r.t.Helper()
	if ans, ok := r.do(op, req); ok || ans["error"] != code || ans["message"] == "" {
		r.t.Fatalf("%s %v: want %s, got %v", op, req, code, ans)
	}
}

func (r *rig) issue(uid string) (string, string) {
	r.t.Helper()
	ans, ok := r.do("issue", map[string]string{"uid": uid, "email": uid + "@example.com", "name": "Pat Doe", "org_slug": "acme"})
	if !ok {
		r.t.Fatalf("issue: %v", ans)
	}
	return r.claimCode(ans)
}

// claimCode reads the last mail's manual code (<site>.<claim id>.<secret>)
// and checks the link carries the same values.
func (r *rig) claimCode(ans map[string]any) (id, secret string) {
	r.t.Helper()
	m := r.mail.sent[len(r.mail.sent)-1]
	if m.subject != "Your Cletus key" {
		r.t.Fatalf("subject %q", m.subject)
	}
	var link string
	for _, f := range strings.Fields(m.body) {
		if strings.HasPrefix(f, "cletus://claim?") {
			link = f
		}
	}
	u, err := url.Parse(link)
	if err != nil || u.Query().Get("site") != "site-a" {
		r.t.Fatalf("link %q in %q", link, m.body)
	}
	id, secret = u.Query().Get("id"), u.Query().Get("s")
	if !strings.Contains(m.body, "site-a."+id+"."+secret) {
		r.t.Fatalf("manual code missing from %q", m.body)
	}
	if ans["claim_expires"] != r.now.Add(24*time.Hour).Format(time.RFC3339) {
		r.t.Fatalf("claim_expires %v", ans["claim_expires"])
	}
	return id, secret
}

func proofOf(t *testing.T, secret string) string {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(b) != 32 {
		t.Fatalf("secret %q", secret)
	}
	return claimProof(b)
}

// open decrypts a claim answer the way a client does, independently of seal.
func open(t *testing.T, secret string, ans map[string]any) string {
	t.Helper()
	sb, _ := base64.RawURLEncoding.DecodeString(secret)
	k, err := hkdf.Key(sha256.New, sb, nil, "cletus-claim-resp-v1", 32)
	if err != nil {
		t.Fatal(err)
	}
	nonce, _ := base64.StdEncoding.DecodeString(ans["nonce"].(string))
	box, _ := base64.StdEncoding.DecodeString(ans["box"].(string))
	blk, _ := aes.NewCipher(k)
	g, _ := cipher.NewGCM(blk)
	pt, err := g.Open(nil, nonce, box, nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(pt)
}

func (r *rig) stateText() string {
	b, err := os.ReadFile(r.file)
	if err != nil {
		r.t.Fatal(err)
	}
	return string(b)
}

func TestIssueThenClaimDeliversTheKeyOnce(t *testing.T) {
	r := newRig(t)
	ans, ok := r.do("issue", map[string]string{"uid": "u1", "email": "pat@example.com", "name": "Pat Doe", "org_slug": "acme"})
	if !ok {
		t.Fatal(ans)
	}
	id, secret := r.claimCode(ans)
	if len(r.mail.sent) != 1 || r.mail.sent[0].to != "pat@example.com" || !strings.Contains(r.mail.sent[0].body, "Hello Pat Doe,") {
		t.Fatalf("mail %+v", r.mail.sent)
	}

	// The key is in the claim until claimed; the answer carries the hash and preview.
	var key string
	for _, c := range readState(t, r.file).Claims {
		key = c.Key
	}
	body, ok := strings.CutPrefix(key, "sk-")
	if !ok || len(body) != 32 {
		t.Fatalf("pending key %q", key)
	}
	if ans["ovh_hash"] != ovhHash(bearer(body)) || ans["key_preview"] != "sk-…"+key[len(key)-4:] {
		t.Fatalf("answer %v", ans)
	}
	if h, _ := ans["key_hash"].(string); !argon2Matches(t, h, key) {
		t.Fatalf("key_hash %q does not verify the key", h)
	}
	if !strings.Contains(r.mail.sent[0].body, "update Cletus") {
		t.Fatal("mail lacks the update hint")
	}
	st := readState(t, r.file)
	if e := st.Keys["u1"]; e == nil || e.KConn != connectorKey(body, "site-a") || e.Status != "active" || e.Email != "pat@example.com" || e.Name != "Pat Doe" || e.Created == "" {
		t.Fatalf("entry %+v", e)
	}
	if fi, _ := os.Stat(r.file); fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode %v", fi.Mode())
	}

	// A wrong proof gets nothing and does not spend the claim.
	r.refused("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)[1:] + "0"}, "claim_invalid")
	r.refused("claim", map[string]string{"claim_id": id, "proof": ""}, "claim_invalid")
	r.refused("claim", map[string]string{"claim_id": "nope", "proof": proofOf(t, secret)}, "claim_invalid")

	got, ok := r.do("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)})
	if !ok || open(t, secret, got) != key {
		t.Fatalf("claim %v", got)
	}
	// Single use, and the raw key is gone from the file.
	r.refused("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)}, "claim_used")
	if text := r.stateText(); strings.Contains(text, key) || strings.Contains(text, body) {
		t.Fatalf("the key is still in the state file:\n%s", text)
	}
}

func readState(t *testing.T, path string) *state {
	t.Helper()
	st, unlock, err := lock(path)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	return st
}

func TestClaimExpires(t *testing.T) {
	r := newRig(t)
	id, secret := r.issue("u1")
	r.now = r.now.Add(24*time.Hour + time.Second)
	r.refused("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)}, "claim_expired")
	for _, c := range readState(t, r.file).Claims {
		if c.Key != "" {
			t.Fatal("an expired claim still holds the key")
		}
	}
	// Long after, the record is forgotten.
	r.now = r.now.Add(8 * 24 * time.Hour)
	r.refused("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)}, "claim_invalid")
	if len(readState(t, r.file).Claims) != 0 {
		t.Fatal("old claim kept")
	}
}

func TestIssueRefusals(t *testing.T) {
	r := newRig(t)
	r.refused("issue", map[string]string{"uid": "u1", "email": "pat@evil.example.org"}, "domain_not_allowed")
	r.refused("issue", map[string]string{"uid": "u1", "email": "Pat <pat@example.com>"}, "invalid_request")
	r.refused("issue", map[string]string{"uid": "u1", "email": "pat@example.com\r\nBcc: x@y.z"}, "invalid_request")
	r.refused("issue", map[string]string{"uid": "u1", "email": "pat@example.com", "name": "a\nb"}, "invalid_request")
	r.refused("issue", map[string]string{"uid": "bad uid", "email": "pat@example.com"}, "invalid_request")
	r.refused("issue", map[string]string{"email": "pat@example.com"}, "invalid_request")
	r.refused("bogus", map[string]string{"uid": "u1"}, "invalid_request")
	if ans, _ := r.s.Handle("issue", []byte("not json")); ans["error"] != "invalid_request" {
		t.Fatal(ans)
	}
	if len(r.mail.sent) != 0 {
		t.Fatal("mail sent for a refused request")
	}
	if _, err := os.Stat(r.file); err == nil {
		t.Fatal("a refused request wrote state")
	}
	// Domains match exactly, case-insensitively; a subdomain is another domain.
	r.issue("u2")
	r.refused("issue", map[string]string{"uid": "u3", "email": "pat@sub.example.com"}, "domain_not_allowed")
	if _, ok := r.do("issue", map[string]string{"uid": "u4", "email": "pat@EXAMPLE.com"}); !ok {
		t.Fatal("domain match is case-sensitive")
	}
	r.refused("issue", map[string]string{"uid": "u2", "email": "pat@example.com"}, "invalid_request") // exists: reissue
}

func TestMailFailureChangesNothing(t *testing.T) {
	r := newRig(t)
	id, secret := r.issue("u1")
	before := r.stateText()
	r.mail.err = errors.New("smtp down")
	r.refused("issue", map[string]string{"uid": "u2", "email": "x@example.com"}, "unavailable")
	r.refused("reissue", map[string]string{"uid": "u1"}, "unavailable")
	if r.stateText() != before {
		t.Fatal("a failed send changed the state")
	}
	// The earlier claim still works.
	if _, ok := r.do("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)}); !ok {
		t.Fatal("claim spoiled by the failed reissue")
	}
}

func TestReissueReplacesTheKey(t *testing.T) {
	r := newRig(t)
	id1, secret1 := r.issue("u1")
	k1 := readState(t, r.file).Keys["u1"].KConn
	r.refused("reissue", map[string]string{"uid": "nobody"}, "unknown_uid")

	ans, ok := r.do("reissue", map[string]string{"uid": "u1"})
	if !ok {
		t.Fatal(ans)
	}
	id2, secret2 := r.claimCode(ans)
	st := readState(t, r.file)
	if st.Keys["u1"].KConn == k1 || len(r.mail.sent) != 2 || r.mail.sent[1].to != "u1@example.com" {
		t.Fatalf("k_conn %s (was %s), mail %d", st.Keys["u1"].KConn, k1, len(r.mail.sent))
	}
	// The first claim is gone; the second delivers the new key.
	r.refused("claim", map[string]string{"claim_id": id1, "proof": proofOf(t, secret1)}, "claim_invalid")
	got, ok := r.do("claim", map[string]string{"claim_id": id2, "proof": proofOf(t, secret2)})
	if !ok {
		t.Fatal(got)
	}
	key := open(t, secret2, got)
	if connectorKey(strings.TrimPrefix(key, "sk-"), "site-a") != st.Keys["u1"].KConn || ans["ovh_hash"] != ovhHash(bearer(strings.TrimPrefix(key, "sk-"))) {
		t.Fatal("the claimed key does not match k_conn and ovh_hash")
	}
}

func TestRevokeAndRestore(t *testing.T) {
	r := newRig(t)
	id, secret := r.issue("u1")
	r.refused("revoke", map[string]string{"uid": "nobody"}, "unknown_uid")
	r.refused("restore", map[string]string{"uid": "nobody"}, "unknown_uid")
	if _, ok := r.do("revoke", map[string]string{"uid": "u1"}); !ok {
		t.Fatal("revoke")
	}
	if readState(t, r.file).Keys["u1"].Status != "revoked" {
		t.Fatal("not revoked")
	}
	// A revoked key's pending claim is dropped.
	r.refused("claim", map[string]string{"claim_id": id, "proof": proofOf(t, secret)}, "claim_invalid")
	if _, ok := r.do("restore", map[string]string{"uid": "u1"}); !ok || readState(t, r.file).Keys["u1"].Status != "active" {
		t.Fatal("restore")
	}
}

func TestImportAndList(t *testing.T) {
	r := newRig(t)
	kconn := connectorKey(goldenSecret, "site-a")
	r.refused("import", map[string]string{"uid": "m1", "email": "a@example.com", "k_conn": "xyz"}, "invalid_request")
	r.refused("import", map[string]string{"uid": "m1", "email": "a@example.com", "k_conn": strings.ToUpper(kconn)}, "invalid_request")
	r.refused("import", map[string]string{"uid": "m1", "email": "nope", "k_conn": kconn}, "invalid_request")
	if ans, ok := r.do("import", map[string]string{"uid": "m1", "email": "a@example.com", "name": "Old Key", "k_conn": kconn}); !ok {
		t.Fatal(ans)
	}
	if len(r.mail.sent) != 0 {
		t.Fatal("import sent mail")
	}
	r.issue("u1")
	r.do("revoke", map[string]string{"uid": "m1"})
	// Importing again (a retried migration) keeps the status.
	r.do("import", map[string]string{"uid": "m1", "email": "a@example.com", "name": "Old Key", "k_conn": kconn})
	ans, ok := r.do("list", nil)
	if !ok {
		t.Fatal(ans)
	}
	b, _ := json.Marshal(ans)
	text := string(b)
	for _, secret := range []string{kconn, "sk-", "secret", "k_conn", "proof"} {
		if strings.Contains(text, secret) {
			t.Fatalf("list leaks %q: %s", secret, text)
		}
	}
	if empty, _ := r.s.Handle("list", nil); empty["ok"] != true {
		t.Fatalf("list without a body: %v", empty)
	}
	keys := ans["keys"].([]map[string]any)
	if len(keys) != 2 || keys[0]["uid"] != "m1" || keys[0]["status"] != "revoked" || keys[0]["pending_claim"] != false || keys[1]["uid"] != "u1" || keys[1]["pending_claim"] != true {
		t.Fatalf("keys %v", keys)
	}
}

func TestStateFileGate(t *testing.T) {
	r := newRig(t)
	r.issue("u1")
	if err := os.Chmod(r.file, 0o644); err != nil {
		t.Fatal(err)
	}
	r.refused("list", nil, "unavailable") // other users may read it: refused like the configuration file
	if err := os.Chmod(r.file, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.do("list", nil); !ok {
		t.Fatal("list after the fix")
	}
	if err := os.WriteFile(r.file, []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.refused("list", nil, "unavailable")
}

// argon2Matches verifies key against a "$argon2id$v=19$m=…,t=…,p=…$salt$hash"
// string the way the Cletus dataproxy does.
func argon2Matches(t *testing.T, encoded, key string) bool {
	t.Helper()
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false
	}
	var m uint32
	var tm uint32
	var p uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &tm, &p); err != nil {
		return false
	}
	salt, err1 := base64.RawStdEncoding.DecodeString(parts[4])
	want, err2 := base64.RawStdEncoding.DecodeString(parts[5])
	if err1 != nil || err2 != nil {
		return false
	}
	return subtle.ConstantTimeCompare(argon2.IDKey([]byte(key), salt, tm, m, p, uint32(len(want))), want) == 1
}
