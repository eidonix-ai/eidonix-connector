// Package keyserver issues, revokes and hands over Cletus account keys for
// one site. The state is a file on this machine (state.go); the raw key of an
// issued account exists only in the claim that delivers it, and is deleted
// when the claim is used or expires.
package keyserver

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
)

// MaxRequestBytes bounds a request body.
const MaxRequestBytes = 64 << 10

// Mailer sends the claim e-mail.
type Mailer interface {
	Send(to, subject, body string) error
}

// Server answers key-server operations for one site.
type Server struct {
	Site string

	cfg  config.KeyServer
	mail Mailer
	now  func() time.Time
}

// New returns a server for site. mail may be nil for local use (the keys
// command), which then cannot issue.
func New(site string, cfg config.KeyServer, mail Mailer) *Server {
	return &Server{Site: site, cfg: cfg, mail: mail, now: time.Now}
}

type request struct {
	UID     string `json:"uid"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	OrgSlug string `json:"org_slug"`
	KConn   string `json:"k_conn"`
	ClaimID string `json:"claim_id"`
	Proof   string `json:"proof"`
}

// failure is an operation's refusal: one of the closed error codes.
type failure struct{ code, message string }

func (f *failure) Error() string { return f.code + ": " + f.message }

func refuse(code, format string, args ...any) error {
	return &failure{code, fmt.Sprintf(format, args...)}
}

var (
	uidPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
	hexKey     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// SetMailer replaces the sender of claim e-mails.
func (s *Server) SetMailer(m Mailer) { s.mail = m }

// Handle runs op with the JSON request body and returns the answer,
// {"ok":true,…} or {"ok":false,"error":<code>,"message":…}, and the uid the
// request named (for the access log).
func (s *Server) Handle(op string, body []byte) (answer map[string]any, uid string) {
	var q request
	var out map[string]any
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}") // list needs no request
	}
	err := json.Unmarshal(body, &q)
	if err != nil {
		err = refuse("invalid_request", "the request is not a JSON object")
	} else {
		out, err = s.run(op, &q)
	}
	if err != nil {
		var f *failure
		if !errors.As(err, &f) {
			slog.Warn("key server", "op", op, "uid", q.UID, "error", err)
			f = &failure{"unavailable", "the key server could not complete the request"}
		}
		out = map[string]any{"ok": false, "error": f.code, "message": f.message}
	}
	return out, q.UID
}

func (s *Server) run(op string, q *request) (map[string]any, error) {
	switch op {
	case "issue", "reissue", "import", "revoke", "restore", "claim", "list":
	default:
		return nil, refuse("invalid_request", "unknown operation %q", printable(op))
	}
	if op != "list" && op != "claim" && !uidPattern.MatchString(q.UID) {
		return nil, refuse("invalid_request", "uid must be 1-64 letters, digits or . _ : -")
	}
	st, unlock, err := lock(s.cfg.StateFile)
	if err != nil {
		return nil, err
	}
	defer unlock()
	st.sweep(s.now())
	if st.dirty {
		if err = st.save(s.cfg.StateFile); err != nil {
			return nil, err
		}
	}
	var out map[string]any
	switch op {
	case "issue", "reissue":
		out, err = s.mint(st, q, op == "reissue")
	case "import":
		out, err = s.importKey(st, q)
	case "revoke", "restore":
		out, err = s.setStatus(st, q, op)
	case "claim":
		out, err = s.claim(st, q)
	case "list":
		out = s.list(st)
	}
	if err == nil && st.dirty {
		err = st.save(s.cfg.StateFile)
	}
	return out, err
}

func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}, s[:min(len(s), 32)])
}

// checkEmail requires a bare address in one of the allowed domains.
func (s *Server) checkEmail(email string) error {
	a, err := mail.ParseAddress(email)
	if err != nil || a.Address != email || len(email) > 254 {
		return refuse("invalid_request", "email is not an e-mail address")
	}
	_, domain, _ := strings.Cut(email, "@")
	for _, d := range s.cfg.AllowedDomains {
		if strings.EqualFold(domain, d) {
			return nil
		}
	}
	return refuse("domain_not_allowed", "this key server does not issue keys for that e-mail domain")
}

func checkName(name string) error {
	if len(name) > 100 || strings.ContainsFunc(name, unicode.IsControl) {
		return refuse("invalid_request", "name must be at most 100 characters, on one line")
	}
	return nil
}

// mint issues a new key for q.UID (or replaces the key of an existing uid)
// and e-mails the claim. Nothing is saved when the e-mail cannot be sent.
func (s *Server) mint(st *state, q *request, reissue bool) (map[string]any, error) {
	e := st.Keys[q.UID]
	switch {
	case reissue && e == nil:
		return nil, refuse("unknown_uid", "no such uid")
	case !reissue && e != nil:
		return nil, refuse("invalid_request", "uid already has a key; use reissue")
	case reissue:
		q.Email, q.Name = e.Email, e.Name
	}
	for _, err := range []error{s.checkEmail(q.Email), checkName(q.Name), checkName(q.OrgSlug)} {
		if err != nil {
			return nil, err
		}
	}
	if s.mail == nil {
		return nil, errors.New("no mail sender")
	}
	key, secret, err := newKey()
	if err != nil {
		return nil, err
	}
	id, err := randomID(16)
	if err != nil {
		return nil, err
	}
	cs, err := randomID(32)
	if err != nil {
		return nil, err
	}
	expires := s.now().Add(s.cfg.ClaimTTL).UTC()
	st.dropClaims(q.UID)
	st.Claims = append(st.Claims, &claim{ClaimID: id, UID: q.UID, Secret: cs, Key: key, Expires: expires})
	if e == nil {
		e = &entry{Status: "active", Created: s.now().UTC().Format(time.RFC3339), Email: q.Email, Name: q.Name}
		st.Keys[q.UID] = e
	}
	e.KConn = connectorKey(secret, s.Site)
	st.dirty = true
	if err = s.mail.Send(e.Email, "Your Cletus key", s.claimMail(e.Name, id, cs, expires)); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true, "ovh_hash": ovhHash(bearer(secret)), "key_preview": "sk-…" + key[len(key)-4:], "claim_expires": expires.Format(time.RFC3339)}, nil
}

func (s *Server) claimMail(name, id, secret string, expires time.Time) string {
	greeting := "Hello,"
	if name != "" {
		greeting = "Hello " + name + ","
	}
	return fmt.Sprintf(`%s

Your Cletus key is ready. On the computer or phone where you use Cletus, open this link:

%s?site=%s&id=%s&s=%s

Or, in Cletus, choose "Paste a claim code" and enter:

%s.%s.%s

The link and the code work once and expire at %s. Anyone who has them can claim your key, so do not forward this message.
`, greeting, s.cfg.Link, s.Site, id, secret, s.Site, id, secret, expires.Format("2006-01-02 15:04 UTC"))
}

// importKey stores a key issued elsewhere (its k_conn only), without e-mail.
func (s *Server) importKey(st *state, q *request) (map[string]any, error) {
	if !hexKey.MatchString(q.KConn) {
		return nil, refuse("invalid_request", "k_conn must be 64 lowercase hex characters")
	}
	if a, err := mail.ParseAddress(q.Email); err != nil || a.Address != q.Email {
		return nil, refuse("invalid_request", "email is not an e-mail address")
	}
	if err := checkName(q.Name); err != nil {
		return nil, err
	}
	e := &entry{Email: q.Email, Name: q.Name, KConn: q.KConn, Status: "active", Created: s.now().UTC().Format(time.RFC3339)}
	if old := st.Keys[q.UID]; old != nil {
		e.Status, e.Created = old.Status, old.Created
	}
	st.Keys[q.UID], st.dirty = e, true
	return map[string]any{"ok": true}, nil
}

func (s *Server) setStatus(st *state, q *request, op string) (map[string]any, error) {
	e := st.Keys[q.UID]
	if e == nil {
		return nil, refuse("unknown_uid", "no such uid")
	}
	status := map[string]string{"revoke": "revoked", "restore": "active"}[op]
	if op == "revoke" {
		st.dropClaims(q.UID)
	}
	e.Status, st.dirty = status, true
	return map[string]any{"ok": true}, nil
}

// claim hands over the key of a claim to whoever proves it holds the claim
// secret, once.
func (s *Server) claim(st *state, q *request) (map[string]any, error) {
	var c *claim
	for _, x := range st.Claims {
		if x.ClaimID == q.ClaimID {
			c = x
		}
	}
	if c == nil {
		return nil, refuse("claim_invalid", "no such claim")
	}
	secret, err := base64.RawURLEncoding.DecodeString(c.Secret)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare([]byte(q.Proof), []byte(claimProof(secret))) != 1 {
		return nil, refuse("claim_invalid", "the proof does not match")
	}
	switch e := st.Keys[c.UID]; {
	case c.Used:
		return nil, refuse("claim_used", "this claim was already used")
	case c.Key == "" || s.now().After(c.Expires):
		return nil, refuse("claim_expired", "this claim has expired")
	case e == nil:
		return nil, refuse("unknown_uid", "no such uid")
	case e.Status != "active":
		return nil, refuse("claim_invalid", "no such claim")
	}
	nonce, box, err := seal(secret, c.Key)
	if err != nil {
		return nil, err
	}
	c.Key, c.Used, st.dirty = "", true, true
	return map[string]any{"ok": true, "nonce": nonce, "box": box}, nil
}

// list returns every uid's metadata; no secret and no k_conn.
func (s *Server) list(st *state) map[string]any {
	pending := map[string]bool{}
	for _, c := range st.Claims {
		pending[c.UID] = pending[c.UID] || c.Key != ""
	}
	keys := []map[string]any{}
	for uid, e := range st.Keys {
		keys = append(keys, map[string]any{"uid": uid, "email": e.Email, "name": e.Name, "status": e.Status, "created": e.Created, "pending_claim": pending[uid]})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i]["uid"].(string) < keys[j]["uid"].(string) })
	return map[string]any{"ok": true, "keys": keys}
}
