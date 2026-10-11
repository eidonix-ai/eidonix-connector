package config

import (
	"encoding/base64"
	"fmt"
	"github.com/eidonix-ai/cletus-connector/internal/profiles"
	"go.yaml.in/yaml/v3"
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
)

type TLS struct {
	CA     string `yaml:"ca"`
	CAFile string `yaml:"ca_file"`
}
type Health struct {
	Path     string `yaml:"path"`
	Interval string `yaml:"interval"`
}
type Config struct {
	Server       string            `yaml:"server"`
	TokenFile    string            `yaml:"token_file"`
	ServerKeys   map[string]string `yaml:"server_keys"`
	TLS          TLS               `yaml:"tls"`
	AuditLog     string            `yaml:"audit_log"`
	HealthListen string            `yaml:"health_listen"`
	Routes       []Route           `yaml:"routes"`
}
type Route struct {
	ID                       string            `yaml:"id"`
	Type                     string            `yaml:"type"`
	Flavor                   string            `yaml:"flavor"`
	Upstreams                []string          `yaml:"upstreams"`
	Balance                  string            `yaml:"balance"`
	Health                   Health            `yaml:"health"`
	MaxConcurrency           int               `yaml:"max_concurrency"`
	MaxRequestBytes          string            `yaml:"max_request_bytes"`
	MaxResponseBytes         string            `yaml:"max_response_bytes"`
	IdleTimeout              string            `yaml:"idle_timeout"`
	UpstreamHeaders          map[string]string `yaml:"upstream_headers"`
	MethodsPaths             [][2]string       `yaml:"methods_paths"`
	ExtraPaths               [][2]string       `yaml:"extra_paths"`
	WebsocketPaths           []string          `yaml:"websocket_paths"`
	AllowCIDRs               []string          `yaml:"allow_cidrs"`
	RateLimit                float64           `yaml:"rate_limit"`
	MaxWebsocketMessageBytes string            `yaml:"max_websocket_message_bytes"`
	ContentTypes             []string          `yaml:"content_types"`
}

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
	if fi.Size() > 1<<20 {
		return nil, fmt.Errorf("config exceeds 1 MiB")
	}
	d := yaml.NewDecoder(io.LimitReader(f, 1<<20))
	d.KnownFields(true)
	var c Config
	if e = d.Decode(&c); e != nil {
		return nil, e
	}
	var extra any
	if e = d.Decode(&extra); e != io.EOF {
		return nil, fmt.Errorf("config must contain one YAML document")
	}
	if c.AuditLog == "" {
		c.AuditLog = "/var/log/cletus-connector/audit.jsonl"
	}
	if e = c.Validate(); e != nil {
		return nil, e
	}
	return &c, nil
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
func (c *Config) Validate() error {
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
			if strings.Contains(v, "${file:") {
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
