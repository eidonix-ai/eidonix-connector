package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// MaxClockSkew bounds the difference between the server's signed time and local time.
const MaxClockSkew = 5 * time.Minute

// ParseToken checks the shape of a connection token cxn_<16 base32>_<base64url 32 bytes>
// and returns its id and the SHA-256 of its secret, which is what verifiers store.
func ParseToken(token string) (string, [32]byte, error) {
	var hash [32]byte
	parts := strings.Split(token, "_")
	// The secret is base64url and may itself contain underscores.
	if len(parts) < 3 || parts[0] != "cxn" || len(parts[1]) != 16 {
		return "", hash, fmt.Errorf("invalid token")
	}
	if _, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(parts[1])); err != nil {
		return "", hash, fmt.Errorf("invalid token id")
	}
	secret, err := base64.RawURLEncoding.DecodeString(strings.Join(parts[2:], "_"))
	if err != nil || len(secret) != 32 {
		return "", hash, fmt.Errorf("invalid token secret")
	}
	return parts[1], sha256.Sum256(secret), nil
}
func NewNonce() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	return base64.StdEncoding.EncodeToString(b), err
}
func NewInstanceID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// ParsePublicKey decodes a standard-base64 Ed25519 public key.
func ParsePublicKey(s string) (ed25519.PublicKey, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key must be a base64 Ed25519 public key (32 bytes)")
	}
	return ed25519.PublicKey(b), nil
}

// KeyID derives the identifier of a server public key: "eidonix-" followed by
// the first four bytes of its SHA-256 in lowercase hex. Owners never type it.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return "eidonix-" + hex.EncodeToString(sum[:4])
}

// IdentityPayload is the exact byte string the server signs.
func IdentityPayload(nonce, instance, serverTime string) []byte {
	return []byte(Subprotocol + "|" + nonce + "|" + instance + "|" + serverTime)
}
func SignIdentity(key ed25519.PrivateKey, nonce, instance, serverTime string) string {
	return base64.StdEncoding.EncodeToString(ed25519.Sign(key, IdentityPayload(nonce, instance, serverTime)))
}

// IdentityHeaders returns the three upgrade response headers a gateway sends.
func IdentityHeaders(key ed25519.PrivateKey, nonce, instance string, now time.Time) http.Header {
	ts := now.UTC().Format(time.RFC3339)
	h := http.Header{}
	h.Set(HeaderServerKeyID, KeyID(key.Public().(ed25519.PublicKey)))
	h.Set(HeaderServerTime, ts)
	h.Set(HeaderServerSignature, SignIdentity(key, nonce, instance, ts))
	return h
}

// VerifyIdentity checks the upgrade response headers against the configured
// public keys: the key id must match one of them, the server time must be
// within MaxClockSkew, and the signature must cover this connection's nonce
// and instance.
func VerifyIdentity(h http.Header, nonce, instance string, keys []ed25519.PublicKey, now time.Time) error {
	var key ed25519.PublicKey
	id := h.Get(HeaderServerKeyID)
	for _, k := range keys {
		if len(k) == ed25519.PublicKeySize && KeyID(k) == id {
			key = k
		}
	}
	if key == nil {
		return fmt.Errorf("server key %q is not one of the configured public keys", id)
	}
	ts := h.Get(HeaderServerTime)
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil || now.Sub(t) > MaxClockSkew || t.Sub(now) > MaxClockSkew {
		return fmt.Errorf("invalid server time")
	}
	sig, err := base64.StdEncoding.DecodeString(h.Get(HeaderServerSignature))
	if err != nil || !ed25519.Verify(key, IdentityPayload(nonce, instance, ts), sig) {
		return fmt.Errorf("invalid server signature")
	}
	return nil
}
