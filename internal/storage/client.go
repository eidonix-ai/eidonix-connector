// Package storage exposes only confined object operations using local credentials.
package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type AuditEntry struct {
	Timestamp        time.Time `json:"timestamp"`
	Origin           string    `json:"origin"`
	Session          string    `json:"session"`
	Route            string    `json:"route"`
	RequestID        string    `json:"request_id"`
	Op               string    `json:"op"`
	KeyHash          string    `json:"key_hash"`
	Status           string    `json:"status"`
	Size             int64     `json:"size"`
	Verified         bool      `json:"sha256_verified"`
	DurationMS       int64     `json:"duration_ms"`
	UploadAgeSeconds int64     `json:"upload_age_s,omitempty"`
}
type Handler struct {
	cfg                 config.Route
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

func New(ctx context.Context, cfg config.Route, audit func(AuditEntry)) (*Handler, error) {
	if e := config.ValidateStorageRoute(&cfg); e != nil {
		return nil, e
	}
	b, e := config.ReadSecret(cfg.CredentialsFile)
	if e != nil {
		return nil, e
	}
	lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		return nil, errors.New("invalid storage credentials file")
	}
	var key []byte
	if cfg.Encryption.Mode == "sse-c" {
		b, e = config.ReadSecret(cfg.Encryption.KeyFile)
		if e != nil {
			return nil, e
		}
		key, e = base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
		if e != nil {
			return nil, errors.New("invalid encryption key")
		}
	}
	return newHandler(ctx, cfg, lines[0], lines[1], key, audit)
}
func newHandler(ctx context.Context, cfg config.Route, accessKey, secretKey string, key []byte, audit func(AuditEntry)) (*Handler, error) {
	u, e := url.Parse(cfg.Endpoint)
	if e != nil || u.Hostname() == "" || u.Scheme != "https" && u.Scheme != "http" || cfg.Encryption.Mode == "sse-c" && u.Scheme != "https" || u.Scheme == "http" && !cfg.AllowInsecureHTTP {
		return nil, errors.New("invalid storage endpoint")
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
	p, e := network.New(ctx, origins, cfg.AllowCIDRs)
	if e != nil {
		return nil, e
	}
	ca, e := x509.SystemCertPool()
	if e != nil {
		ca = x509.NewCertPool()
	}
	if cfg.TLSCAFile != "" {
		b, e := os.ReadFile(cfg.TLSCAFile)
		if e != nil {
			return nil, e
		}
		if !ca.AppendCertsFromPEM(b) {
			return nil, errors.New("invalid storage CA file")
		}
	}
	idle, e := time.ParseDuration(cfg.IdleTimeout)
	if e != nil || idle <= 0 {
		idle = 300 * time.Second
	}
	t := &http.Transport{Proxy: nil, DialContext: p.DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: ca}, MaxResponseHeaderBytes: 64 << 10, DisableCompression: true, MaxIdleConnsPerHost: cfg.MaxConcurrency, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: idle}
	rt := &boundedTransport{base: t, hosts: hosts, idle: idle}
	c, e := minio.NewCore(u.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: u.Scheme == "https", Region: cfg.Region, Transport: rt, BucketLookup: lookup, MaxRetries: 1})
	if e != nil {
		return nil, e
	}
	var sse encrypt.ServerSide
	switch cfg.Encryption.Mode {
	case "sse-c":
		sse, e = encrypt.NewSSEC(key)
	case "sse-s3":
		sse = encrypt.NewSSE()
	case "sse-kms":
		sse, e = encrypt.NewSSEKMS(cfg.Encryption.KMSKeyID, nil)
	}
	if e != nil {
		return nil, e
	}
	max, e := config.Bytes(cfg.MaxObjectBytes)
	if e != nil {
		return nil, e
	}
	part, e := config.Bytes(cfg.PartSize)
	if e != nil {
		return nil, e
	}
	life, e := time.ParseDuration(cfg.MaxUploadLifetime)
	if e != nil || life <= 0 {
		life = 6 * time.Hour
	}
	age, e := time.ParseDuration(cfg.AbandonedUploadAge)
	if e != nil {
		age = 24 * time.Hour
	}
	auditHash := sha256.Sum256([]byte("cletus-storage-audit-v1\x00" + secretKey))
	h := &Handler{cfg: cfg, core: c, sse: sse, policy: p, transport: t, audit: audit, auditKey: auditHash[:], max: max, part: part, idle: idle, lifetime: life, age: age, active: map[string]bool{}, tokens: math.Max(1, cfg.MaxOpsPerSecond), last: time.Now(), hour: time.Now()}
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
		a.Timestamp = time.Now().UTC()
		a.Route = h.cfg.ID
		h.audit(a)
	}
}
func (h *Handler) reserve() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.inflight >= h.cfg.MaxConcurrency {
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
	if h.cfg.MaxReadBytesPerHour > 0 && n > h.cfg.MaxReadBytesPerHour-h.reads {
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
		log.Printf("storage route %s uses explicitly enabled unencrypted HTTP", h.cfg.ID)
	}
	if h.cfg.Encryption.Mode == "none" {
		log.Printf("storage route %s uses explicitly enabled encryption mode none", h.cfg.ID)
	}
}
