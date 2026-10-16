package config

import (
	"encoding/base64"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"math"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Encryption struct {
	Mode     string `toml:"mode,omitempty"`
	KeyFile  string `toml:"key_file,omitempty"`
	KMSKeyID string `toml:"kms_key_id,omitempty"`
}

func ValidateStorageRoute(r *Route) error {
	u, e := url.Parse(r.Endpoint)
	if e != nil || u.Hostname() == "" || strings.Contains(u.Hostname(), "%") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("storage endpoint must be an HTTP(S) origin")
	}
	if !r.PathStyle && net.ParseIP(u.Hostname()) != nil {
		return fmt.Errorf("IP storage endpoints require path_style = true")
	}
	if r.Encryption.Mode == "" {
		r.Encryption.Mode = "none"
	}
	switch r.Encryption.Mode {
	case "none", "sse-s3":
		if r.Encryption.Mode == "none" && !r.AllowUnencrypted {
			return fmt.Errorf("none requires allow_unencrypted")
		}
		if r.Encryption.KeyFile != "" || r.Encryption.KMSKeyID != "" {
			return fmt.Errorf("unexpected encryption key configuration")
		}
	case "sse-kms":
		if r.Encryption.KMSKeyID == "" || r.Encryption.KeyFile != "" {
			return fmt.Errorf("sse-kms requires kms_key_id only")
		}
	case "sse-c":
		if u.Scheme != "https" {
			return fmt.Errorf("sse-c requires HTTPS without exception")
		}
		if r.Encryption.KeyFile == "" || r.Encryption.KMSKeyID != "" {
			return fmt.Errorf("sse-c requires key_file only")
		}
		b, e := ReadSecret(r.Encryption.KeyFile)
		if e != nil {
			return e
		}
		key, e := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
		if e != nil || len(key) != 32 {
			return fmt.Errorf("SSE-C key must encode exactly 32 bytes")
		}
	default:
		return fmt.Errorf("invalid encryption mode")
	}
	if u.Scheme == "http" && !r.AllowInsecureHTTP {
		return fmt.Errorf("HTTP storage requires allow_insecure_http")
	}
	if r.Region == "" {
		r.Region = "us-east-1"
	}
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`).MatchString(r.Bucket) || strings.Contains(r.Bucket, "..") {
		return fmt.Errorf("invalid storage bucket")
	}
	if len(r.Prefix) < 1 || len(r.Prefix) > 256 || !strings.HasSuffix(r.Prefix, "/") || ValidateStorageKey(r.Prefix, 0, false) != "" {
		return fmt.Errorf("invalid storage prefix")
	}
	if r.CredentialsFile == "" {
		return fmt.Errorf("storage credentials_file required")
	}
	b, e := ReadSecret(r.CredentialsFile)
	if e != nil {
		return e
	}
	lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" || strings.ContainsAny(string(b), "\r\x00") {
		return fmt.Errorf("credentials file requires access key and secret on two lines")
	}
	if r.MaxConcurrency == 0 {
		r.MaxConcurrency = 16
	}
	if r.MaxConcurrency < 1 || r.MaxConcurrency > 256 {
		return fmt.Errorf("storage concurrency must be 1..256")
	}
	if r.MaxObjectBytes == "" {
		r.MaxObjectBytes = "5GiB"
	}
	n, e := Bytes(r.MaxObjectBytes)
	if e != nil || n > 5<<30 {
		return fmt.Errorf("max_object_bytes must be at most 5 GiB")
	}
	if r.PartSize == "" {
		r.PartSize = "16MiB"
	}
	n, e = Bytes(r.PartSize)
	if e != nil || n < 5<<20 || n > 64<<20 {
		return fmt.Errorf("part_size must be 5..64 MiB")
	}
	if r.MaxOpsPerSecond == 0 {
		r.MaxOpsPerSecond = 200
	}
	if r.MaxOpsPerSecond <= 0 || math.IsNaN(r.MaxOpsPerSecond) || math.IsInf(r.MaxOpsPerSecond, 0) || r.MaxReadBytesPerHour < 0 {
		return fmt.Errorf("invalid storage rate/read budget")
	}
	if r.IdleTimeout == "" {
		r.IdleTimeout = "300s"
	}
	if d, e := time.ParseDuration(r.IdleTimeout); e != nil || d <= 0 {
		return fmt.Errorf("invalid storage idle timeout")
	}
	if r.AbandonedUploadAge == "" {
		r.AbandonedUploadAge = "24h"
	}
	if d, e := time.ParseDuration(r.AbandonedUploadAge); e != nil || d < 24*time.Hour {
		return fmt.Errorf("abandoned_upload_age must be at least 24h")
	}
	if r.MaxUploadLifetime == "" {
		r.MaxUploadLifetime = "6h"
	}
	if d, e := time.ParseDuration(r.MaxUploadLifetime); e != nil || d <= 0 || d > 6*time.Hour {
		return fmt.Errorf("max_upload_lifetime must be at most 6h")
	}
	if len(r.Upstreams) != 0 || r.Flavor != "" || len(r.MethodsPaths) != 0 || len(r.ExtraPaths) != 0 || len(r.WebsocketPaths) != 0 || len(r.UpstreamHeaders) != 0 || len(r.Models) != 0 {
		return fmt.Errorf("storage route cannot contain inference settings")
	}
	return nil
}

// ValidateStorageKey checks raw keys before prefix concatenation.
func ValidateStorageKey(key string, prefixBytes int, allowEmpty bool) string {
	if e := protocol.ValidateRelativeKey(key, prefixBytes, allowEmpty); e != nil {
		return e.(*protocol.StorageError).Code
	}
	return ""
}
