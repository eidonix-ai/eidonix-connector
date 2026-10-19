package config

import (
	"encoding/base64"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

// Storage is an s3 service's S3-compatible backend. The whole bucket belongs
// to the hub's applications. Fields after MaxFileSize are internal defaults, not settings.
type Storage struct {
	// Endpoint is the service URL (http://host:port or https://host[:port]).
	Endpoint      string
	Bucket        string
	Region        string
	AccessKey     string
	SecretKey     string
	Encryption    string
	EncryptionKey []byte
	CACertificate string
	ReadOnly      bool
	MaxFileSize   int64

	PathStyle          bool
	PartSize           int64
	MaxOpsPerSecond    float64
	IdleTimeout        time.Duration
	MaxUploadLifetime  time.Duration
	AbandonedUploadAge time.Duration
}

// Storage defaults (internal).
const (
	DefaultPartSize           = 16 << 20
	DefaultStorageOps         = 200
	DefaultStorageIdle        = 300 * time.Second
	DefaultUploadLifetime     = 6 * time.Hour
	DefaultAbandonedUploadAge = 24 * time.Hour
)

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// StorageDefaults returns a Storage with every default filled in.
func StorageDefaults() Storage {
	return Storage{Region: "us-east-1", Encryption: "none", MaxFileSize: protocol.MaxStorageObjectBytes, PartSize: DefaultPartSize, MaxOpsPerSecond: DefaultStorageOps, IdleTimeout: DefaultStorageIdle, MaxUploadLifetime: DefaultUploadLifetime, AbandonedUploadAge: DefaultAbandonedUploadAge}
}

func parseStorage(s *Service, f map[string]*yaml.Node, n *yaml.Node, ctx string) error {
	st := StorageDefaults()
	st.Endpoint = s.URL
	for _, req := range []string{"bucket", "access-key", "secret-key"} {
		if f[req] == nil {
			return at(n, ctx, "%s is required", req)
		}
	}
	var err error
	strs := map[string]*string{"bucket": &st.Bucket, "region": &st.Region, "access-key": &st.AccessKey, "secret-key": &st.SecretKey, "encryption": &st.Encryption}
	for field, dst := range strs {
		if v := f[field]; v != nil {
			if *dst, err = line(v, ctx, field); err != nil {
				return err
			}
		}
	}
	if v := f["read-only"]; v != nil {
		if st.ReadOnly, err = boolean(v, ctx, "read-only"); err != nil {
			return err
		}
	}
	if v := f["max-file-size"]; v != nil {
		if st.MaxFileSize, err = size(v, ctx, "max-file-size", protocol.MaxStorageObjectBytes); err != nil {
			return err
		}
	}
	https := strings.HasPrefix(s.URL, "https://")
	if v := f["ca-certificate"]; v != nil {
		if !https {
			return at(v, ctx, "ca-certificate is used only with an https:// address")
		}
		if st.CACertificate, err = certificate(v, ctx); err != nil {
			return err
		}
	}
	u, _ := url.Parse(s.URL)
	st.PathStyle = !strings.HasSuffix(strings.ToLower(u.Hostname()), "amazonaws.com")
	if !bucketPattern.MatchString(st.Bucket) || strings.Contains(st.Bucket, "..") {
		return at(f["bucket"], ctx, "bucket %q is not a valid bucket name", st.Bucket)
	}
	key := f["encryption-key"]
	switch st.Encryption {
	case "sse-c":
		if !https {
			return at(f["address"], ctx, "encryption sse-c needs an https:// address (the key is sent with every request)")
		}
		if key == nil {
			return at(n, ctx, "encryption sse-c needs encryption-key (base64 of 32 random bytes; eidonix-connector generate-key makes one)")
		}
		v, err := line(key, ctx, "encryption-key")
		if err != nil {
			return err
		}
		b, e := base64.StdEncoding.DecodeString(v)
		if e != nil || len(b) != 32 {
			return at(key, ctx, "encryption-key must be base64 of exactly 32 bytes (eidonix-connector generate-key makes one)")
		}
		st.EncryptionKey = b
	case "sse-s3", "none":
		if key != nil {
			return at(key, ctx, "encryption-key is used only with encryption: sse-c")
		}
	default:
		return at(f["encryption"], ctx, "encryption must be none, sse-s3 or sse-c")
	}
	s.Storage = &st
	s.IdleTimeout = st.IdleTimeout
	return nil
}

// MaxKeyBytes is the largest key the service accepts.
func (st Storage) MaxKeyBytes() int { return protocol.MaxStorageKeyBytes }
