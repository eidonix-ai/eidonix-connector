package keyserver

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"math/big"
)

// The derivations below are shared with the Cletus app and dataproxy, which
// must agree bit for bit; crypto_test.go pins golden values.

const (
	keyAlphabet  = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	keyBodyLen   = 32
	bearerLabel  = "cletus-ovh-v1"
	connLabel    = "cletus-connector-v1|"
	claimLabel   = "claim"
	claimRespKDF = "cletus-claim-resp-v1"
)

func mac(key []byte, label string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(label))
	return m.Sum(nil)
}

// bearer is the credential the app presents to the dataproxy for the key
// with this secret; ovhHash is what the dataproxy stores for it.
func bearer(secret string) string {
	return "ck1-" + base64.RawURLEncoding.EncodeToString(mac([]byte(secret), bearerLabel))
}

func ovhHash(bearer string) string {
	h := sha256.Sum256([]byte(bearer))
	return hex.EncodeToString(h[:])
}

// connectorKey is the key the app and this site share for the key with this
// secret.
func connectorKey(secret, site string) string {
	return hex.EncodeToString(mac([]byte(secret), connLabel+site))
}

// newKey mints "sk-" and 32 alphanumerics, and returns the key and its secret.
func newKey() (key, secret string, err error) {
	body := make([]byte, keyBodyLen)
	max := big.NewInt(int64(len(keyAlphabet)))
	for i := range body {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", "", err
		}
		body[i] = keyAlphabet[n.Int64()]
	}
	return "sk-" + string(body), string(body), nil
}

// randomID returns n random bytes as unpadded base64url.
func randomID(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// claimProof is what the claimant sends to show it holds the claim secret
// (the decoded bytes of the code's secret part).
func claimProof(secret []byte) string { return hex.EncodeToString(mac(secret, claimLabel)) }

// seal encrypts the key for the claimant: AES-256-GCM under
// HKDF-SHA256(secret, no salt, "cletus-claim-resp-v1"). It returns the nonce
// and the box (ciphertext and tag), both standard base64.
func seal(secret []byte, plaintext string) (nonce, box string, err error) {
	k, err := hkdf.Key(sha256.New, secret, nil, claimRespKDF, 32)
	if err != nil {
		return "", "", err
	}
	blk, err := aes.NewCipher(k)
	if err != nil {
		return "", "", err
	}
	g, err := cipher.NewGCM(blk)
	if err != nil {
		return "", "", err
	}
	n := make([]byte, g.NonceSize())
	if _, err = rand.Read(n); err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(n), base64.StdEncoding.EncodeToString(g.Seal(nil, n, []byte(plaintext), nil)), nil
}
