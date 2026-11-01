package keyserver

import (
	"strings"
	"testing"
)

// Golden values shared with the Cletus app and dataproxy; never change them.
// The bearer and ovh_hash depend on the secret only. k_conn is pinned for a
// neutral site name (checked with openssl: printf 'cletus-connector-v1|plant-a'
// | openssl dgst -sha256 -mac HMAC -macopt key:<secret>); the app's tests pin
// it for their own site.
const (
	goldenSecret    = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
	goldenSite      = "plant-a"
	goldenBearer    = "ck1-VNTycyCs7aCiB-uY1VhIMpiOe_1TwYY-0izMtFDSXjc"
	goldenHash      = "ed25bf9e813be7fbbb7ca5dc4603b734d8b6b10d34c59999d98d3ce33948e83a"
	goldenConnector = "f5df7a6000696d191126fca5b0e093b4c570e97b9b73e59c19ea976e8070281b"
)

func TestDerivationsMatchGoldenValues(t *testing.T) {
	if got := bearer(goldenSecret); got != goldenBearer {
		t.Fatalf("bearer = %s", got)
	}
	if got := ovhHash(goldenBearer); got != goldenHash {
		t.Fatalf("ovh_hash = %s", got)
	}
	if got := connectorKey(goldenSecret, goldenSite); got != goldenConnector {
		t.Fatalf("k_conn = %s", got)
	}
	if connectorKey(goldenSecret, "other-site") == goldenConnector {
		t.Fatal("k_conn does not depend on the site")
	}
}

func TestNewKeyShape(t *testing.T) {
	seen := map[string]bool{}
	for range 50 {
		key, secret, err := newKey()
		if err != nil {
			t.Fatal(err)
		}
		body, ok := strings.CutPrefix(key, "sk-")
		if !ok || len(body) != 32 || body != secret || strings.Trim(body, keyAlphabet) != "" || seen[key] {
			t.Fatalf("key %q", key)
		}
		seen[key] = true
	}
}
