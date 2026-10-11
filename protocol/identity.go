package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"
)

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
func IdentityPayload(nonce, id, instance, serverTime string) []byte {
	return []byte(Subprotocol + "|" + nonce + "|" + id + "|" + instance + "|" + serverTime)
}
func SignIdentity(key ed25519.PrivateKey, nonce, id, instance, serverTime string) string {
	return base64.StdEncoding.EncodeToString(ed25519.Sign(key, IdentityPayload(nonce, id, instance, serverTime)))
}
func VerifyIdentity(h http.Header, nonce, id, instance string, keys map[string]ed25519.PublicKey, now time.Time) error {
	key, ok := keys[h.Get("X-Cletus-Server-Key-Id")]
	if !ok || len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("unknown server key")
	}
	ts := h.Get("X-Cletus-Server-Time")
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil || now.Sub(t) > 5*time.Minute || t.Sub(now) > 5*time.Minute {
		return fmt.Errorf("invalid server time")
	}
	sig, err := base64.StdEncoding.DecodeString(h.Get("X-Cletus-Server-Signature"))
	if err != nil || !ed25519.Verify(key, IdentityPayload(nonce, id, instance, ts), sig) {
		return fmt.Errorf("invalid server signature")
	}
	return nil
}
