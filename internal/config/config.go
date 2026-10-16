package config

import (
	"encoding/base64"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/eidonix-ai/eidonix-connector/internal/profiles"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

type TLS struct {
	CA     string `toml:"ca,omitempty"`
	CAFile string `toml:"ca_file,omitempty"`
}
type Health struct {
	Path     string `toml:"path,omitempty"`
	Interval string `toml:"interval,omitempty"`
}
type Config struct {
	Server       string            `toml:"server"`
	TokenFile    string            `toml:"token_file"`
	AuditLog     string            `toml:"audit_log,omitempty"`
	HealthListen string            `toml:"health_listen,omitempty"`
	ServerKeys   map[string]string `toml:"server_keys"`
	TLS          TLS               `toml:"tls,omitempty"`
	Routes       []Route           `toml:"routes"`
	normalized   bool
}
type Route struct {
	ID                       string            `toml:"id"`
	Type                     string            `toml:"type"`
	Flavor                   string            `toml:"flavor,omitempty"`
	Upstreams                []string          `toml:"upstreams,omitempty"`
	Balance                  string            `toml:"balance,omitempty"`
	Health                   Health            `toml:"health,omitempty"`
	MaxConcurrency           int               `toml:"max_concurrency,omitempty,omitzero"`
	MaxRequestBytes          string            `toml:"max_request_bytes,omitempty"`
	MaxResponseBytes         string            `toml:"max_response_bytes,omitempty"`
	IdleTimeout              string            `toml:"idle_timeout,omitempty"`
	RateLimit                float64           `toml:"rate_limit,omitempty,omitzero"`
	MaxWebsocketMessageBytes string            `toml:"max_websocket_message_bytes,omitempty"`
	ContentTypes             []string          `toml:"content_types,omitempty"`
	MethodsPaths             [][2]string       `toml:"methods_paths,omitempty"`
	ExtraPaths               [][2]string       `toml:"extra_paths,omitempty"`
	WebsocketPaths           []string          `toml:"websocket_paths,omitempty"`
	AllowCIDRs               []string          `toml:"allow_cidrs,omitempty"`
	UpstreamHeaders          map[string]string `toml:"upstream_headers,omitempty"`
	// Models maps the model name the gateway sends (alias) to the id the upstream serves.
	Models map[string]string `toml:"models,omitempty"`

	// Object storage settings (type = "object_storage" only).
	Endpoint            string     `toml:"endpoint,omitempty"`
	Region              string     `toml:"region,omitempty"`
	Bucket              string     `toml:"bucket,omitempty"`
	Prefix              string     `toml:"prefix,omitempty"`
	PathStyle           bool       `toml:"path_style,omitempty"`
	CredentialsFile     string     `toml:"credentials_file,omitempty"`
	Encryption          Encryption `toml:"encryption,omitempty"`
	TLSCAFile           string     `toml:"tls_ca_file,omitempty"`
	AllowInsecureHTTP   bool       `toml:"allow_insecure_http,omitempty"`
	AllowUnencrypted    bool       `toml:"allow_unencrypted,omitempty"`
	MaxObjectBytes      string     `toml:"max_object_bytes,omitempty"`
	PartSize            string     `toml:"part_size,omitempty"`
	MaxOpsPerSecond     float64    `toml:"max_ops_per_second,omitempty,omitzero"`
	MaxReadBytesPerHour int64      `toml:"max_read_bytes_per_hour,omitempty,omitzero"`
	AbandonedUploadAge  string     `toml:"abandoned_upload_age,omitempty"`
	MaxUploadLifetime   string     `toml:"max_upload_lifetime,omitempty"`
	ReportLocation      bool       `toml:"report_location,omitempty"`
}

// DefaultPath is the canonical configuration location.
const DefaultPath = "/etc/eidonix-connector/config.toml"

func Load(path string) (*Config, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || (st.Uid != 0 && int(st.Uid) != os.Getuid()) || !fi.Mode().IsRegular() || (fi.Mode().Perm() != 0600 && fi.Mode().Perm() != 0640) {
		return nil, fmt.Errorf("config must be a regular file owned by root or the current user, mode 0600/0640")
	}
	if fi.Size() > 1<<20 {
		return nil, fmt.Errorf("config exceeds 1 MiB")
	}
	c, e := Decode(io.LimitReader(f, 1<<20))
	if e != nil {
		return nil, e
	}
	if c.AuditLog == "" {
		c.AuditLog = "/var/log/eidonix-connector/audit.jsonl"
	}
	return c.Normalized()
}

// Decode parses TOML strictly: unknown keys and type mismatches are errors.
func Decode(r io.Reader) (*Config, error) {
	var c Config
	md, e := toml.NewDecoder(r).Decode(&c)
	if e != nil {
		return nil, fmt.Errorf("config: %w", e)
	}
	if u := md.Undecoded(); len(u) > 0 {
		keys := make([]string, 0, len(u))
		for _, k := range u {
			keys = append(keys, k.String())
		}
		return nil, fmt.Errorf("config: unknown key(s): %s", strings.Join(keys, ", "))
	}
	return &c, nil
}

// Encode writes c as TOML (used for starter configurations).
func Encode(w io.Writer, c *Config) error {
	enc := toml.NewEncoder(w)
	enc.Indent = ""
	return enc.Encode(c)
}
func Bytes(s string) (int64, error) {
	mult := int64(1)
	for _, x := range []struct {
		s string
		n int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if strings.HasSuffix(s, x.s) {
			s = strings.TrimSuffix(s, x.s)
			mult = x.n
			break
		}
	}
	v, e := strconv.ParseInt(s, 10, 64)
	if e != nil || v < 1 || v > (1<<40)/mult {
		return 0, fmt.Errorf("invalid byte limit")
	}
	return v * mult, nil
}
func (c *Config) Validate() error { _, err := c.Normalized(); return err }

// Normalized returns an independent configuration with defaults and local secrets resolved.
func (c *Config) Normalized() (*Config, error) {
	copy := *c
	copy.ServerKeys = make(map[string]string, len(c.ServerKeys))
	for k, v := range c.ServerKeys {
		copy.ServerKeys[k] = v
	}
	copy.Routes = append([]Route(nil), c.Routes...)
	for i := range copy.Routes {
		r := &copy.Routes[i]
		r.Upstreams = append([]string(nil), r.Upstreams...)
		r.MethodsPaths = append([][2]string(nil), r.MethodsPaths...)
		r.ExtraPaths = append([][2]string(nil), r.ExtraPaths...)
		r.WebsocketPaths = append([]string(nil), r.WebsocketPaths...)
		r.AllowCIDRs = append([]string(nil), r.AllowCIDRs...)
		r.ContentTypes = append([]string(nil), r.ContentTypes...)
		r.UpstreamHeaders = make(map[string]string, len(r.UpstreamHeaders))
		for k, v := range c.Routes[i].UpstreamHeaders {
			r.UpstreamHeaders[k] = v
		}
		if c.Routes[i].Models != nil {
			r.Models = make(map[string]string, len(c.Routes[i].Models))
			for k, v := range c.Routes[i].Models {
				r.Models[k] = v
			}
		}
	}
	if err := copy.normalize(); err != nil {
		return nil, err
	}
	copy.normalized = true
	return &copy, nil
}
func (c *Config) normalize() error {
	u, e := url.Parse(c.Server)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || strings.Contains(u.Hostname(), "%") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Port() != "" && u.Port() != "443") {
		return fmt.Errorf("server must be HTTPS origin on port 443")
	}
	if c.TokenFile == "" || len(c.ServerKeys) < 1 || len(c.ServerKeys) > 2 {
		return fmt.Errorf("token_file and one or two server keys required")
	}
	for _, k := range c.ServerKeys {
		b, e := base64.StdEncoding.DecodeString(k)
		if e != nil || len(b) != 32 {
			return fmt.Errorf("server key must be base64 Ed25519 public key")
		}
	}
	if c.TLS.CA == "" {
		c.TLS.CA = "system"
	}
	if c.TLS.CA != "system" && c.TLS.CA != "file" || c.TLS.CA == "file" && c.TLS.CAFile == "" {
		return fmt.Errorf("invalid tls CA")
	}
	if c.HealthListen != "" {
		h, _, e := net.SplitHostPort(c.HealthListen)
		ip := net.ParseIP(h)
		if e != nil || ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("health_listen must be loopback IP")
		}
	}
	if len(c.Routes) > 256 {
		return fmt.Errorf("too many routes")
	}
	seen := map[string]bool{}
	for i := range c.Routes {
		r := &c.Routes[i]
		if !regexp.MustCompile(`^[a-z0-9-]{1,40}$`).MatchString(r.ID) || seen[r.ID] {
			return fmt.Errorf("invalid or duplicate route id")
		}
		seen[r.ID] = true
		if err := ValidateModels(r.Type, r.Models); err != nil {
			return fmt.Errorf("route %s: %w", r.ID, err)
		}
		if r.Type == "object_storage" {
			if err := ValidateStorageRoute(r); err != nil {
				return err
			}
			continue
		}
		if len(r.Upstreams) == 0 || len(r.Upstreams) > 64 {
			return fmt.Errorf("route needs upstreams")
		}
		for _, s := range r.Upstreams {
			u, e := url.Parse(s)
			if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || strings.Contains(u.Hostname(), "%") || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
				return fmt.Errorf("upstream must be HTTP origin with explicit port")
			}
			p, e := strconv.Atoi(u.Port())
			if e != nil || p < 1 || p > 65535 {
				return fmt.Errorf("invalid port")
			}
		}
		paths, e := profiles.Effective(r.Type, r.Flavor, r.MethodsPaths, r.ExtraPaths)
		if e != nil {
			return e
		}
		r.MethodsPaths = paths
		r.ExtraPaths = nil
		if r.Type == "realtime_ws" && len(r.WebsocketPaths) == 0 {
			return fmt.Errorf("websocket paths required")
		}
		for _, p := range r.WebsocketPaths {
			if e := profiles.ValidatePath(p); e != nil {
				return e
			}
		}
		if r.Balance == "" {
			r.Balance = "least_inflight"
		}
		if r.Balance != "least_inflight" && r.Balance != "round_robin" {
			return fmt.Errorf("invalid balance")
		}
		if r.MaxConcurrency == 0 {
			r.MaxConcurrency = 32
		}
		if r.MaxConcurrency < 1 || r.MaxConcurrency > 10000 || r.RateLimit < 0 || math.IsNaN(r.RateLimit) || math.IsInf(r.RateLimit, 0) {
			return fmt.Errorf("invalid concurrency/rate")
		}
		if r.MaxRequestBytes == "" {
			r.MaxRequestBytes = "16MiB"
			switch r.Type {
			case "audio_transcription":
				r.MaxRequestBytes = "50MiB"
			case "document_ocr":
				r.MaxRequestBytes = "64MiB"
			case "document_conversion":
				r.MaxRequestBytes = "100MiB"
			}
		}
		if r.MaxResponseBytes == "" {
			r.MaxResponseBytes = "256MiB"
		}
		if r.MaxWebsocketMessageBytes == "" {
			r.MaxWebsocketMessageBytes = "4MiB"
		}
		for _, s := range []string{r.MaxRequestBytes, r.MaxResponseBytes, r.MaxWebsocketMessageBytes} {
			if _, e := Bytes(s); e != nil {
				return e
			}
		}
		if r.IdleTimeout == "" {
			r.IdleTimeout = "120s"
			if r.Type == "realtime_ws" {
				r.IdleTimeout = "300s"
			}
		}
		if v, e := time.ParseDuration(r.IdleTimeout); e != nil || v <= 0 {
			return fmt.Errorf("invalid idle_timeout")
		}
		if r.Health.Path == "" {
			r.Health.Path = "/v1/models"
			if r.Flavor != "" {
				r.Health.Path = profiles.Flavors[r.Flavor].Health
			}
		}
		if e := profiles.ValidatePath(r.Health.Path); e != nil {
			return e
		}
		if _, e := profiles.Effective(r.Type, r.Flavor, r.MethodsPaths, [][2]string{{"GET", r.Health.Path}}); e != nil {
			return fmt.Errorf("unsafe health path: %w", e)
		}
		if r.Health.Interval == "" {
			r.Health.Interval = "15s"
		}
		if v, e := time.ParseDuration(r.Health.Interval); e != nil || v < time.Second {
			return fmt.Errorf("invalid health interval")
		}
		for _, cidr := range r.AllowCIDRs {
			if _, _, e := net.ParseCIDR(cidr); e != nil {
				return e
			}
		}
		for k, v := range r.UpstreamHeaders {
			for _, ch := range k {
				if !(ch >= 65 && ch <= 90 || ch >= 97 && ch <= 122 || ch >= 48 && ch <= 57 || strings.ContainsRune("!#$%&'*+-.^_`|~", ch)) {
					return fmt.Errorf("invalid header name")
				}
			}
			if strings.HasPrefix(strings.ToLower(k), "gotenberg-") {
				return fmt.Errorf("converter control header denied")
			}
			if strings.ContainsAny(k, "\r\n :") || k == "" || strings.ContainsAny(v, "\r\n") {
				return fmt.Errorf("invalid local header")
			}
			switch strings.ToLower(k) {
			case "host", "content-length", "connection", "transfer-encoding", "upgrade", "proxy-authorization", "fileurl":
				return fmt.Errorf("unsafe local header %s", k)
			}
			if !c.normalized && strings.Contains(v, "${file:") {
				start := strings.Index(v, "${file:")
				end := strings.Index(v[start:], "}")
				if end < 0 {
					return fmt.Errorf("invalid secret reference")
				}
				path := v[start+7 : start+end]
				b, e := ReadSecret(path)
				if e != nil {
					return e
				}
				expanded := v[:start] + strings.TrimSpace(string(b)) + v[start+end+1:]
				if strings.ContainsAny(expanded, "\r\n") {
					return fmt.Errorf("secret contains newline")
				}
				r.UpstreamHeaders[k] = expanded
			}
		}
	}
	return nil
}

// MaxModelAliases bounds the per-route alias table.
const MaxModelAliases = 64

// MaxModelNameBytes bounds alias names and upstream model ids.
const MaxModelNameBytes = 200

// ValidateModels checks a route's model alias table (alias -> upstream id).
func ValidateModels(kind string, models map[string]string) error {
	if len(models) == 0 {
		return nil
	}
	switch kind {
	case "openai_chat", "openai_embeddings", "anthropic_messages", "rerank", "audio_transcription", "audio_speech", "image", "realtime_ws", "http":
	default:
		return fmt.Errorf("model aliases are not supported on %s routes", kind)
	}
	if len(models) > MaxModelAliases {
		return fmt.Errorf("at most %d model aliases per route", MaxModelAliases)
	}
	seen := make(map[string]bool, len(models))
	for alias, target := range models {
		for _, v := range []string{alias, target} {
			if !validModelName(v) {
				return fmt.Errorf("model alias names and targets must be 1..%d printable characters without control characters: %q", MaxModelNameBytes, v)
			}
		}
		if alias == target {
			return fmt.Errorf("model alias %q maps to itself", alias)
		}
		if seen[alias] {
			return fmt.Errorf("duplicate model alias %q", alias)
		}
		seen[alias] = true
	}
	return nil
}
func validModelName(s string) bool {
	if len(s) == 0 || len(s) > MaxModelNameBytes || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
func ReadSecret(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	fi, e := f.Stat()
	if e != nil {
		return nil, e
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 || !fi.Mode().IsRegular() || (fi.Mode().Perm() != 0600 && fi.Mode().Perm() != 0640) {
		return nil, fmt.Errorf("secret must be root-owned regular file mode 0600/0640")
	}
	if fi.Size() > 65536 {
		return nil, fmt.Errorf("secret too large")
	}
	b, e := io.ReadAll(io.LimitReader(f, 65537))
	if len(b) > 65536 {
		return nil, fmt.Errorf("secret too large")
	}
	return b, e
}
