// Package storage exposes only confined object operations using local credentials.
package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// AuditEntry is one storage line of the access log. Keys are recorded only as
// keyed hashes; object contents never are.
type AuditEntry struct {
	Time             time.Time `json:"time"`
	Origin           string    `json:"origin"`
	Session          string    `json:"session"`
	Service          string    `json:"service"`
	Type             string    `json:"type"`
	Op               string    `json:"op"`
	KeyHash          string    `json:"key_hash"`
	Status           string    `json:"status"`
	Size             int64     `json:"size"`
	Verified         bool      `json:"sha256_verified"`
	DurationMS       int64     `json:"duration_ms"`
	OrgSlug          string    `json:"org_slug"`
	OrgID            string    `json:"org_id"`
	UserKeyID        string    `json:"user_key_id"`
	RequestID        string    `json:"request_id"`
	UploadAgeSeconds int64     `json:"upload_age_s,omitempty"`
}
type Handler struct {
	name                string
	maxConcurrent       int
	cfg                 config.Storage
	core                *minio.Core
	sse                 encrypt.ServerSide
	policy              *network.Policy
	transport           *http.Transport
	audit               func(AuditEntry)
	auditKey            []byte
	max, part           int64
	idle, lifetime, age time.Duration
	mu                  sync.Mutex
	active              map[string]bool
	inflight            int
	tokens              float64
	last                time.Time
	reads               int64
	hour                time.Time
	healthy             bool
	cancel              context.CancelFunc
	start               sync.Once
	warnedCleanup       bool
	maintenance         sync.WaitGroup
}

// New builds a storage handler for a validated storage service. It never
// touches the bucket; Start begins the read-only checks and maintenance.
func New(ctx context.Context, svc config.Service, audit func(AuditEntry)) (*Handler, error) {
	if svc.Storage == nil {
		return nil, errors.New("not a storage service")
	}
	st := *svc.Storage
	return newHandler(ctx, svc.Name, svc.MaxConcurrentRequests, st, st.AccessKey, st.SecretKey, st.EncryptionKey, audit)
}
func newHandler(ctx context.Context, name string, maxConcurrent int, cfg config.Storage, accessKey, secretKey string, key []byte, audit func(AuditEntry)) (*Handler, error) {
	u, e := url.Parse(cfg.Endpoint)
	if e != nil || u.Hostname() == "" || u.Scheme != "https" && u.Scheme != "http" || cfg.Encryption == "sse-c" && u.Scheme != "https" || u.Scheme == "http" && !cfg.AllowHTTP {
		return nil, errors.New("invalid storage endpoint")
	}
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	origins := []string{cfg.Endpoint}
	hosts := map[string]bool{u.Host: true}
	lookup := minio.BucketLookupPath
	if !cfg.PathStyle {
		lookup = minio.BucketLookupDNS
		v := *u
		v.Host = cfg.Bucket + "." + u.Host
		origins = append(origins, v.String())
		hosts[v.Host] = true
	}
	p, e := network.New(ctx, origins)
	if e != nil {
		return nil, e
	}
	ca, e := x509.SystemCertPool()
	if e != nil {
		ca = x509.NewCertPool()
	}
	if cfg.CACertificate != "" && !ca.AppendCertsFromPEM([]byte(cfg.CACertificate)) {
		return nil, errors.New("invalid storage ca_certificate")
	}
	idle := cfg.IdleTimeout
	if idle <= 0 {
		idle = config.DefaultStorageIdle
	}
	t := &http.Transport{Proxy: nil, DialContext: p.DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: ca}, MaxResponseHeaderBytes: 64 << 10, DisableCompression: true, MaxIdleConnsPerHost: maxConcurrent, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: idle}
	rt := &boundedTransport{base: t, hosts: hosts, idle: idle}
	c, e := minio.NewCore(u.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: u.Scheme == "https", Region: cfg.Region, Transport: rt, BucketLookup: lookup, MaxRetries: 1})
	if e != nil {
		return nil, e
	}
	var sse encrypt.ServerSide
	switch cfg.Encryption {
	case "sse-c":
		sse, e = encrypt.NewSSEC(key)
	case "sse-s3":
		sse = encrypt.NewSSE()
	case "sse-kms":
		sse, e = encrypt.NewSSEKMS(cfg.KMSKeyID, nil)
	}
	if e != nil {
		return nil, e
	}
	max, part, life, age := cfg.MaxFileSize, cfg.PartSize, cfg.MaxUploadLifetime, cfg.AbandonedUploadAge
	if max <= 0 || max > protocol.MaxStorageObjectBytes {
		max = protocol.MaxStorageObjectBytes
	}
	if part < protocol.MinStoragePartBytes || part > protocol.MaxStoragePartBytes {
		part = protocol.DefaultStoragePartBytes
	}
	if life <= 0 || life > protocol.MaxUploadLifetime {
		life = protocol.MaxUploadLifetime
	}
	if age < config.DefaultAbandonedUploadAge {
		age = config.DefaultAbandonedUploadAge
	}
	if cfg.MaxOpsPerSecond <= 0 {
		cfg.MaxOpsPerSecond = config.DefaultStorageOps
	}
	auditHash := sha256.Sum256([]byte("cletus-storage-audit-v1\x00" + secretKey))
	h := &Handler{name: name, maxConcurrent: maxConcurrent, cfg: cfg, core: c, sse: sse, policy: p, transport: t, audit: audit, auditKey: auditHash[:], max: max, part: part, idle: idle, lifetime: life, age: age, active: map[string]bool{}, tokens: math.Max(1, cfg.MaxOpsPerSecond), last: time.Now(), hour: time.Now()}
	return h, nil
}
func (h *Handler) Healthy() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.healthy }
func (h *Handler) InFlight() int { h.mu.Lock(); defer h.mu.Unlock(); return h.inflight }
func (h *Handler) Close() error {
	h.mu.Lock()
	if h.cancel != nil {
		h.cancel()
	}
	h.mu.Unlock()
	h.maintenance.Wait()
	h.transport.CloseIdleConnections()
	return nil
}
func (h *Handler) hash(key string) string {
	m := hmac.New(sha256.New, h.auditKey)
	m.Write([]byte(key))
	return hex.EncodeToString(m.Sum(nil))
}
func (h *Handler) record(a AuditEntry) {
	if h.audit != nil {
		a.Time = time.Now().UTC()
		a.Service = h.name
		a.Type = protocol.TypeStorage
		h.audit(a)
	}
}
func (h *Handler) reserve() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.inflight >= h.maxConcurrent {
		return "busy"
	}
	now := time.Now()
	h.tokens = math.Min(math.Max(1, h.cfg.MaxOpsPerSecond), h.tokens+now.Sub(h.last).Seconds()*h.cfg.MaxOpsPerSecond)
	h.last = now
	if h.tokens < 1 {
		return "rate_limited"
	}
	h.tokens--
	h.inflight++
	return ""
}
func (h *Handler) release() { h.mu.Lock(); h.inflight--; h.mu.Unlock() }
func (h *Handler) readBudget(n int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	if now.Sub(h.hour) >= time.Hour {
		h.hour = now
		h.reads = 0
	}
	if h.cfg.MaxReadPerHour > 0 && n > h.cfg.MaxReadPerHour-h.reads {
		return false
	}
	h.reads += n
	return true
}

var errBackendInvalid = errors.New("backend response outside limits")

func backendCode(e error) string {
	var xmlErr *xml.SyntaxError
	if errors.As(e, &xmlErr) {
		return "backend_invalid"
	}
	if errors.Is(e, errBackendInvalid) {
		return "backend_invalid"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "timeout"
	}
	v := minio.ToErrorResponse(e)
	switch v.Code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return "not_found"
	case "PreconditionFailed":
		return "precondition_failed"
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
		return "forbidden"
	}
	if v.StatusCode == 404 {
		return "not_found"
	}
	if v.StatusCode == 403 {
		return "forbidden"
	}
	if v.StatusCode == 412 {
		return "precondition_failed"
	}
	return "backend_unavailable"
}

type boundedTransport struct {
	base  *http.Transport
	hosts map[string]bool
	idle  time.Duration
}

func (t *boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !t.hosts[r.URL.Host] {
		return nil, errors.New("storage destination denied")
	}
	ctx, cancel := context.WithCancelCause(r.Context())
	timer := time.AfterFunc(t.idle, func() { cancel(context.DeadlineExceeded) })
	req := r.Clone(ctx)
	if req.Body != nil {
		req.Body = &activityBody{ReadCloser: req.Body, touch: func() { timer.Reset(t.idle) }}
	}
	resp, e := t.base.RoundTrip(req)
	if e != nil {
		cause := context.Cause(ctx)
		timer.Stop()
		cancel(context.Canceled)
		if cause != nil {
			return nil, cause
		}
		return nil, e
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		resp.Body.Close()
		timer.Stop()
		cancel(context.Canceled)
		return nil, errors.New("storage redirect denied")
	}
	limit := int64(2 << 20)
	if r.Method == "GET" && r.URL.RawQuery == "" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		limit = -1
	}
	if r.Method != "HEAD" && limit >= 0 && resp.ContentLength > limit {
		resp.Body.Close()
		timer.Stop()
		cancel(context.Canceled)
		return nil, errBackendInvalid
	}
	resp.Body = &limitedBody{ReadCloser: resp.Body, ctx: ctx, left: limit, touch: func() { timer.Reset(t.idle) }, finish: func() { timer.Stop(); cancel(context.Canceled) }}
	return resp, nil
}

type activityBody struct {
	io.ReadCloser
	touch func()
}

func (b *activityBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.touch()
	}
	return n, e
}

type limitedBody struct {
	io.ReadCloser
	ctx           context.Context
	left          int64
	touch, finish func()
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.left == 0 {
		var q [1]byte
		n, e := b.ReadCloser.Read(q[:])
		if n > 0 {
			return 0, errBackendInvalid
		}
		return 0, e
	}
	if b.left > 0 && int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, e := b.ReadCloser.Read(p)
	if e != nil && context.Cause(b.ctx) != nil {
		e = context.Cause(b.ctx)
	}
	if b.left >= 0 {
		b.left -= int64(n)
	}
	if n > 0 {
		b.touch()
	}
	return n, e
}
func (b *limitedBody) Close() error { b.finish(); return b.ReadCloser.Close() }
func (h *Handler) warnings() {
	if strings.HasPrefix(h.cfg.Endpoint, "http:") {
		log.Printf("storage service %s uses http (allow_http is set)", h.name)
	}
	if h.cfg.Encryption == "none" {
		log.Printf("storage service %s stores files without encryption (encryption: none)", h.name)
	}
}
