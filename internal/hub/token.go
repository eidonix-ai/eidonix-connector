package hub

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"strings"
)

// NewToken returns a fresh connection token in the form connectors expect
// (protocol.ParseToken): cxn_ + a 16-character lowercase base32 id + _ + 32
// random bytes as unpadded base64url.
func NewToken() (string, error) {
	id := make([]byte, 10)
	secret := make([]byte, 32)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	return "cxn_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(id)) + "_" + base64.RawURLEncoding.EncodeToString(secret), nil
}

// NewServerKey returns a fresh Ed25519 identity: the value for server-key
// (base64 of the 32-byte seed) and its public key (base64), which goes in each
// connector's public-keys.
func NewServerKey() (private, public string, err error) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(key.Seed()), base64.StdEncoding.EncodeToString(pub), nil
}
