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

// Storage is a storage service's S3-compatible backend. Fields after
// CACertificate are internal defaults, not settings.
type Storage struct {
	Endpoint       string
	Bucket         string
	Prefix         string
	Region         string
	AccessKey      string
	SecretKey      string
	Encryption     string
	EncryptionKey  []byte
	KMSKeyID       string
	AllowHTTP      bool
	MaxFileSize    int64
	MaxReadPerHour int64
	CACertificate  string

	PathStyle          bool
	PartSize           int64
	MaxOpsPerSecond    float64
	IdleTimeout        time.Duration
	MaxUploadLifetime  time.Duration
	AbandonedUploadAge time.Duration
}

// Storage defaults (v0.1 behaviour, now internal).
const (
	DefaultPartSize           = 16 << 20
	DefaultStorageOps         = 200
	DefaultStorageIdle        = 300 * time.Second
	DefaultUploadLifetime     = 6 * time.Hour
	DefaultAbandonedUploadAge = 24 * time.Hour
)

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// StorageDefaults returns a Storage with every internal default filled in.
func StorageDefaults() Storage {
	return Storage{Region: "us-east-1", Encryption: "sse-c", MaxFileSize: protocol.MaxStorageObjectBytes, PartSize: DefaultPartSize, MaxOpsPerSecond: DefaultStorageOps, IdleTimeout: DefaultStorageIdle, MaxUploadLifetime: DefaultUploadLifetime, AbandonedUploadAge: DefaultAbandonedUploadAge}
}

func parseStorage(s *Service, f map[string]*yaml.Node, n *yaml.Node, ctx string) error {
	st := StorageDefaults()
	for _, req := range []string{"endpoint", "bucket", "prefix", "access_key", "secret_key"} {
		if f[req] == nil {
			return at(n, ctx, "%s is required", req)
		}
	}
	var err error
	strs := map[string]*string{"endpoint": &st.Endpoint, "bucket": &st.Bucket, "prefix": &st.Prefix, "region": &st.Region, "access_key": &st.AccessKey, "secret_key": &st.SecretKey, "encryption": &st.Encryption, "kms_key_id": &st.KMSKeyID}
	for field, dst := range strs {
		if v := f[field]; v != nil {
			if *dst, err = line(v, ctx, field); err != nil {
				return err
			}
		}
	}
	if v := f["allow_http"]; v != nil {
		if st.AllowHTTP, err = boolean(v, ctx, "allow_http"); err != nil {
			return err
		}
	}
	if v := f["max_file_size"]; v != nil {
		if st.MaxFileSize, err = size(v, ctx, "max_file_size", protocol.MaxStorageObjectBytes); err != nil {
			return err
		}
	}
	if v := f["max_read_per_hour"]; v != nil {
		if st.MaxReadPerHour, err = size(v, ctx, "max_read_per_hour", 1<<42); err != nil {
			return err
		}
	}
	if v := f["ca_certificate"]; v != nil {
		if st.CACertificate, err = certificate(v, ctx); err != nil {
			return err
		}
	}
	u, e := url.Parse(st.Endpoint)
	if e != nil || u.Hostname() == "" || strings.Contains(u.Hostname(), "%") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || !validPort(u.Port()) {
		return at(f["endpoint"], ctx, "endpoint must be https://host or https://host:port, without a path")
	}
	st.Endpoint = strings.TrimSuffix(st.Endpoint, "/")
	if u.Scheme == "http" && !st.AllowHTTP {
		return at(f["endpoint"], ctx, "endpoint uses http; storage needs https (set allow_http: true only for a trusted local network)")
	}
	st.PathStyle = !strings.HasSuffix(strings.ToLower(u.Hostname()), "amazonaws.com")
	if !bucketPattern.MatchString(st.Bucket) || strings.Contains(st.Bucket, "..") {
		return at(f["bucket"], ctx, "bucket %q is not a valid bucket name", st.Bucket)
	}
	if len(st.Prefix) > protocol.MaxStoragePrefixBytes || !strings.HasSuffix(st.Prefix, "/") || protocol.ValidateRelativeKey(st.Prefix, 0, false) != nil {
		return at(f["prefix"], ctx, "prefix must be a folder name ending in / (for example cletus/)")
	}
	key := f["encryption_key"]
	switch st.Encryption {
	case "sse-c":
		if u.Scheme != "https" {
			return at(f["endpoint"], ctx, "encryption sse-c needs an https endpoint (the key is sent with every request)")
		}
		if key == nil {
			return at(n, ctx, "encryption sse-c needs encryption_key (base64 of 32 random bytes; eidonix-connector generate-key makes one)")
		}
		v, err := line(key, ctx, "encryption_key")
		if err != nil {
			return err
		}
		b, e := base64.StdEncoding.DecodeString(v)
		if e != nil || len(b) != 32 {
			return at(key, ctx, "encryption_key must be base64 of exactly 32 bytes (eidonix-connector generate-key makes one)")
		}
		st.EncryptionKey = b
	case "sse-s3", "sse-kms", "none":
		if key != nil {
			return at(key, ctx, "encryption_key is used only with encryption: sse-c")
		}
	default:
		return at(f["encryption"], ctx, "encryption must be sse-c, sse-s3, sse-kms or none")
	}
	if st.KMSKeyID != "" && st.Encryption != "sse-kms" {
		return at(f["kms_key_id"], ctx, "kms_key_id is used only with encryption: sse-kms")
	}
	s.Storage = &st
	s.IdleTimeout = st.IdleTimeout
	return parseLimits(s, map[string]*yaml.Node{"max_concurrent_requests": f["max_concurrent_requests"]}, ctx, 256)
}

// MaxKeyBytes is the largest relative key the service accepts.
func (st Storage) MaxKeyBytes() int { return protocol.MaxStorageFullKeyBytes - len(st.Prefix) }
