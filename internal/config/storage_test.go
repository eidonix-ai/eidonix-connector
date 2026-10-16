package config

import "testing"

func TestStorageHTTPSAndExplicitUnencrypted(t *testing.T) {
	for _, r := range []Route{{Type: "object_storage", PathStyle: true, Endpoint: "http://127.0.0.1:9000", Encryption: Encryption{Mode: "sse-c"}, AllowInsecureHTTP: true}, {Type: "object_storage", PathStyle: true, Endpoint: "http://127.0.0.1:9000", Encryption: Encryption{Mode: "sse-s3"}}, {Type: "object_storage", Endpoint: "https://storage.example", Encryption: Encryption{Mode: "none"}}, {Type: "object_storage", Endpoint: "https://storage.example/path", Encryption: Encryption{Mode: "sse-s3"}}} {
		if ValidateStorageRoute(&r) == nil {
			t.Fatal("unsafe storage configuration accepted")
		}
	}
}
func TestStorageRawKeyBoundaries(t *testing.T) {
	if got := ValidateStorageKey("safe", 1020, false); got != "" {
		t.Fatal(got)
	}
	if got := ValidateStorageKey("safe", 1021, false); got != "key_too_long" {
		t.Fatal(got)
	}
	if got := ValidateStorageKey("control\u0085", 0, false); got != "invalid_key" {
		t.Fatal(got)
	}
	if got := ValidateStorageKey(".eidonix-connector/probe/key", 0, false); got != "invalid_key" {
		t.Fatal(got)
	}
}
