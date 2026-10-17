package config

import (
	"bytes"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

const (
	MaxServices       = 256
	MaxEndpoints      = 64
	MaxModels         = 64
	MaxModelNameBytes = 200
)

var (
	topFields      = []string{"site", "upstream", "access-log", "services"}
	upstreamFields = []string{"url", "token", "public-keys", "proxy", "ca-certificate"}
	httpFields     = []string{"kind", "address", "allowed-endpoints", "models", "health", "api-key", "max-concurrent-requests", "max-requests-per-minute", "idle-timeout"}
	s3Fields       = []string{"kind", "address", "bucket", "access-key", "secret-key", "region", "encryption", "encryption-key", "ca-certificate", "read-only", "max-file-size", "max-concurrent-requests"}
	// v02Keys are top-level keys only a v0.2 file has.
	v02Keys = []string{"name", "access_log", "chat", "anthropic", "embeddings", "rerank", "transcription", "speech", "image", "document", "conversion", "realtime", "storage", "custom"}
)

// lineError is a validation error tied to a line and, when known, a service.
type lineError struct {
	line int
	msg  string
}

func (e *lineError) Error() string { return fmt.Sprintf("line %d: %s", e.line, e.msg) }

// at reports a problem with node n in context ctx ("" at the top level).
func at(n *yaml.Node, ctx, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if ctx != "" {
		msg = ctx + ": " + msg
	}
	return &lineError{n.Line, msg}
}

// isOldFormat reports whether a top-level mapping is a v0.2 file.
func isOldFormat(root *yaml.Node) bool {
	for i := 0; i+1 < len(root.Content); i += 2 {
		k := root.Content[i].Value
		for _, old := range v02Keys {
			if k == old {
				return true
			}
		}
		if k == "upstream" && root.Content[i+1].Kind == yaml.MappingNode {
			up := root.Content[i+1]
			for j := 0; j+1 < len(up.Content); j += 2 {
				switch up.Content[j].Value {
				case "public_keys", "proxy_url", "ca_certificate":
					return true
				}
			}
		}
	}
	return false
}

// Parse decodes and validates a configuration file's bytes. It never reads
// other files: every secret is in the file itself.
func Parse(data []byte) (*Config, error) {
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("the file is empty; it needs at least site and upstream")
		}
		return nil, fmt.Errorf("not valid YAML: %v", strings.TrimPrefix(err.Error(), "yaml: "))
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("the file must contain exactly one YAML document")
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, &lineError{doc.Line, "the file must be a set of settings (site, upstream, services, …)"}
	}
	root := doc.Content[0]
	if isOldFormat(root) {
		return nil, ErrOldFormat
	}
	fields, err := mapping(root, "", topFields)
	if err != nil {
		return nil, err
	}
	c := &Config{AccessLog: DefaultAccessLog}
	site := fields["site"]
	if site == nil {
		return nil, &lineError{root.Line, "site is required: the connector's name in Eidonix Setup"}
	}
	if c.Site, err = line(site, "", "site"); err != nil {
		return nil, err
	}
	if !protocol.ValidSiteName(c.Site) {
		return nil, at(site, "", "site %q must be the connector's name in Eidonix Setup: 1-40 lowercase letters, digits or -, starting with a letter or digit", c.Site)
	}
	up := fields["upstream"]
	if up == nil {
		return nil, &lineError{root.Line, "upstream is required (url, token and public-keys from Eidonix Setup)"}
	}
	if c.Upstream, err = parseUpstream(up); err != nil {
		return nil, err
	}
	if n := fields["access-log"]; n != nil {
		if c.AccessLog, err = text(n, "", "access-log"); err != nil {
			return nil, err
		}
		if !filepath.IsAbs(c.AccessLog) {
			return nil, at(n, "", "access-log must be an absolute path")
		}
	}
	section := fields["services"]
	if section == nil || section.Tag == "!!null" {
		return c, nil
	}
	if section.Kind != yaml.MappingNode {
		return nil, at(section, "", "services must be a set of services by name (name: then its settings)")
	}
	lines := map[string]int{}
	for i := 0; i+1 < len(section.Content); i += 2 {
		k, v := section.Content[i], section.Content[i+1]
		if k.Kind != yaml.ScalarNode || !protocol.ValidServiceName(k.Value) {
			return nil, at(k, "", "service name %q must be 1-40 lowercase letters, digits or -, starting with a letter or digit", k.Value)
		}
		if prior, dup := lines[k.Value]; dup {
			return nil, at(k, fmt.Sprintf("service %q", k.Value), "name already used on line %d; names must be unique in the file", prior)
		}
		lines[k.Value] = k.Line
		s, err := parseService(k, v)
		if err != nil {
			return nil, err
		}
		c.Services = append(c.Services, s)
	}
	if len(c.Services) > MaxServices {
		return nil, fmt.Errorf("at most %d services", MaxServices)
	}
	return c, nil
}

// mapping checks n is a mapping whose keys are all in allowed, without
// duplicates, and returns the value nodes by key.
func mapping(n *yaml.Node, ctx string, allowed []string) (map[string]*yaml.Node, error) {
	if n.Kind != yaml.MappingNode {
		return nil, at(n, ctx, "expected settings (key: value), found %s", describe(n))
	}
	known := map[string]bool{}
	for _, k := range allowed {
		known[k] = true
	}
	out := map[string]*yaml.Node{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind != yaml.ScalarNode || !known[k.Value] {
			where := ""
			if ctx != "" {
				where = " in " + ctx
			}
			return nil, &lineError{k.Line, fmt.Sprintf("unknown setting %q%s (expected one of: %s)", k.Value, where, strings.Join(allowed, ", "))}
		}
		if _, dup := out[k.Value]; dup {
			return nil, at(k, ctx, "%s is set twice", k.Value)
		}
		out[k.Value] = v
	}
	return out, nil
}
func describe(n *yaml.Node) string {
	switch n.Kind {
	case yaml.SequenceNode:
		return "a list"
	case yaml.MappingNode:
		return "settings"
	}
	if n.Tag == "!!null" {
		return "nothing"
	}
	return fmt.Sprintf("%q", n.Value)
}

// text returns a non-empty scalar value without control characters other than
// newlines (which only multi-line values such as certificates may contain).
func text(n *yaml.Node, ctx, field string) (string, error) {
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" || n.Value == "" {
		return "", at(n, ctx, "%s needs a value", field)
	}
	for _, r := range n.Value {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return "", at(n, ctx, "%s contains a control character", field)
		}
	}
	return n.Value, nil
}

// line returns a single-line text value.
func line(n *yaml.Node, ctx, field string) (string, error) {
	v, err := text(n, ctx, field)
	if err == nil && strings.ContainsAny(v, "\n\t") {
		err = at(n, ctx, "%s must be on one line", field)
	}
	return v, err
}
func integer(n *yaml.Node, ctx, field string, min, max int) (int, error) {
	var v int
	if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || n.Decode(&v) != nil || v < min || v > max {
		return 0, at(n, ctx, "%s must be a whole number from %d to %d", field, min, max)
	}
	return v, nil
}
func boolean(n *yaml.Node, ctx, field string) (bool, error) {
	var v bool
	if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" || n.Decode(&v) != nil {
		return false, at(n, ctx, "%s must be true or false", field)
	}
	return v, nil
}
func duration(n *yaml.Node, ctx, field string) (time.Duration, error) {
	v, err := line(n, ctx, field)
	if err != nil {
		return 0, err
	}
	d, e := time.ParseDuration(v)
	if e != nil || d <= 0 || d > 24*time.Hour {
		return 0, at(n, ctx, "%s must be a duration such as 90s, 15m or 1h (at most 24h)", field)
	}
	return d, nil
}
func size(n *yaml.Node, ctx, field string, max int64) (int64, error) {
	v := n.Value
	if n.Kind != yaml.ScalarNode {
		v = ""
	}
	b, err := ParseSize(v)
	if err != nil || b > max {
		return 0, at(n, ctx, "%s must be a size such as 500MB, 5GB or 2GiB (at most %s)", field, FormatSize(max))
	}
	return b, nil
}

// ParseSize reads a byte size: a whole number of bytes, or with KB/MB/GB
// (×1000) or KiB/MiB/GiB (×1024).
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		n      int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}} {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.n
			break
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 1 || v > (1<<42)/mult {
		return 0, errors.New("invalid size")
	}
	return v * mult, nil
}

// FormatSize renders bytes using the largest exact unit.
func FormatSize(n int64) string {
	for _, u := range []struct {
		suffix string
		n      int64
	}{{"GiB", 1 << 30}, {"GB", 1e9}, {"MiB", 1 << 20}, {"MB", 1e6}, {"KiB", 1 << 10}, {"KB", 1e3}} {
		if n >= u.n && n%u.n == 0 {
			return strconv.FormatInt(n/u.n, 10) + u.suffix
		}
	}
	return strconv.FormatInt(n, 10)
}

func parseUpstream(n *yaml.Node) (Upstream, error) {
	const ctx = "upstream"
	var u Upstream
	f, err := mapping(n, ctx, upstreamFields)
	if err != nil {
		return u, err
	}
	for _, req := range []string{"url", "token", "public-keys"} {
		if f[req] == nil {
			return u, at(n, ctx, "%s is required", req)
		}
	}
	if u.URL, err = line(f["url"], ctx, "url"); err != nil {
		return u, err
	}
	if p, e := url.Parse(u.URL); e != nil || p.Scheme != "https" || p.Hostname() == "" || p.User != nil || p.RawQuery != "" || p.Fragment != "" || (p.Path != "" && p.Path != "/") || !validPort(p.Port()) || strings.Contains(p.Hostname(), "%") {
		return u, at(f["url"], ctx, "url must be https://host or https://host:port, without a path")
	}
	if u.Token, err = line(f["token"], ctx, "token"); err != nil {
		return u, err
	}
	if _, _, e := protocol.ParseToken(u.Token); e != nil {
		return u, at(f["token"], ctx, "token is not a connection token (cxn_…); paste the token shown in Eidonix Setup")
	}
	keys := f["public-keys"]
	if keys.Kind != yaml.SequenceNode || len(keys.Content) < 1 || len(keys.Content) > 2 {
		return u, at(keys, ctx, "public-keys must list one or two keys (two only during a key rotation)")
	}
	for _, k := range keys.Content {
		v, err := line(k, ctx, "public key")
		if err != nil {
			return u, err
		}
		if _, e := protocol.ParsePublicKey(v); e != nil {
			return u, at(k, ctx, "public key %q is not a base64 Ed25519 public key; paste the key shown in Eidonix Setup", v)
		}
		u.PublicKeys = append(u.PublicKeys, v)
	}
	if n := f["proxy"]; n != nil {
		if u.Proxy, err = line(n, ctx, "proxy"); err != nil {
			return u, err
		}
		p, e := url.Parse(u.Proxy)
		if e != nil || (p.Scheme != "http" && p.Scheme != "https") || p.Hostname() == "" || (p.Path != "" && p.Path != "/") || p.RawQuery != "" || p.Fragment != "" || !validPort(p.Port()) {
			return u, at(n, ctx, "proxy must be http://host:port or https://host:port")
		}
	}
	if n := f["ca-certificate"]; n != nil {
		if u.CACertificate, err = certificate(n, ctx); err != nil {
			return u, err
		}
	}
	return u, nil
}
func certificate(n *yaml.Node, ctx string) (string, error) {
	v, err := text(n, ctx, "ca-certificate")
	if err != nil {
		return "", err
	}
	if !x509.NewCertPool().AppendCertsFromPEM([]byte(v)) {
		return "", at(n, ctx, "ca-certificate must be a PEM certificate (-----BEGIN CERTIFICATE----- …)")
	}
	return v, nil
}
func validPort(p string) bool {
	if p == "" {
		return true
	}
	v, err := strconv.Atoi(p)
	return err == nil && v >= 1 && v <= 65535 && strconv.Itoa(v) == p
}

// parseAddress reads a service address: host:port for plain HTTP, or
// https://host[:port]. It returns the address as written and the URL dialed.
func parseAddress(n *yaml.Node, ctx string) (string, string, error) {
	v, err := line(n, ctx, "address")
	if err != nil {
		return "", "", err
	}
	bad := at(n, ctx, "address must be host:port (plain HTTP) or https://host:port, without a path")
	if rest, ok := strings.CutPrefix(v, "https://"); ok {
		p, e := url.Parse(v)
		if e != nil || rest == "" || p.Hostname() == "" || strings.Contains(p.Hostname(), "%") || p.User != nil || p.RawQuery != "" || p.Fragment != "" || p.Opaque != "" || (p.Path != "" && p.Path != "/") || !validPort(p.Port()) || strings.HasSuffix(p.Host, ":") {
			return "", "", bad
		}
		v = strings.TrimSuffix(v, "/")
		return v, v, nil
	}
	if strings.Contains(v, "://") {
		return "", "", at(n, ctx, "address must be host:port (plain HTTP) or https://host:port; write %q without http://", strings.TrimPrefix(v, "http://"))
	}
	host, port, e := net.SplitHostPort(v)
	if e != nil || host == "" || port == "" || !validPort(port) || strings.ContainsAny(host, "%/@?#[] ") {
		return "", "", bad
	}
	return v, "http://" + v, nil
}

// fieldsOf returns the settings a service of kind accepts.
func fieldsOf(kind string) []string {
	if kind == KindS3 {
		return s3Fields
	}
	return httpFields
}

func parseService(k, n *yaml.Node) (Service, error) {
	s := Service{Name: k.Value, Kind: KindHTTP, Line: k.Line, MaxConcurrentRequests: DefaultMaxConcurrentRequests, IdleTimeout: DefaultIdleTimeout}
	ctx := fmt.Sprintf("service %q", s.Name)
	if n.Kind != yaml.MappingNode {
		return s, at(k, ctx, "needs settings (address, allowed-endpoints, …)")
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == "kind" {
			v, err := line(n.Content[i+1], ctx, "kind")
			if err != nil {
				return s, err
			}
			if v != KindHTTP && v != KindS3 {
				return s, at(n.Content[i+1], ctx, "kind must be http (the default) or s3")
			}
			s.Kind = v
		}
	}
	// A setting of the other kind gets a message that says so.
	other := map[string]bool{}
	for _, f := range fieldsOf(map[string]string{KindHTTP: KindS3, KindS3: KindHTTP}[s.Kind]) {
		other[f] = true
	}
	for _, f := range fieldsOf(s.Kind) {
		delete(other, f)
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		key := n.Content[i]
		if other[key.Value] {
			if s.Kind == KindHTTP {
				return s, at(key, ctx, "%s is a setting of s3 services; add kind: s3 for a storage service", key.Value)
			}
			return s, at(key, ctx, "%s is not used by s3 services", key.Value)
		}
	}
	f, err := mapping(n, ctx, fieldsOf(s.Kind))
	if err != nil {
		return s, err
	}
	if f["address"] == nil {
		return s, at(k, ctx, "address is required (where the service runs, e.g. 10.0.0.5:8000 or https://host:8443)")
	}
	if s.Address, s.URL, err = parseAddress(f["address"], ctx); err != nil {
		return s, err
	}
	if v := f["max-concurrent-requests"]; v != nil {
		limit := 10000
		if s.Kind == KindS3 {
			limit = 256
		}
		if s.MaxConcurrentRequests, err = integer(v, ctx, "max-concurrent-requests", 1, limit); err != nil {
			return s, err
		}
	}
	if s.Kind == KindS3 {
		return s, parseStorage(&s, f, k, ctx)
	}
	list := f["allowed-endpoints"]
	if list == nil {
		return s, at(k, ctx, "allowed-endpoints is required: list the requests allowed, e.g. [POST /v1/*, GET /v1/models]")
	}
	if list.Kind != yaml.SequenceNode || len(list.Content) == 0 || len(list.Content) > MaxEndpoints {
		return s, at(list, ctx, "allowed-endpoints must list 1 to %d requests, e.g. [POST /v1/*, GET /v1/models]", MaxEndpoints)
	}
	seen := map[Endpoint]bool{}
	for _, item := range list.Content {
		entry, err := line(item, ctx, "allowed-endpoints entry")
		if err != nil {
			return s, err
		}
		e, perr := ParseEndpoint(entry)
		if perr != nil {
			return s, at(item, ctx, "allowed-endpoints entry %v", perr)
		}
		if seen[e] {
			return s, at(item, ctx, "allowed-endpoints lists %s twice", e)
		}
		seen[e] = true
		s.Endpoints = append(s.Endpoints, e)
	}
	if v := f["models"]; v != nil {
		if s.Models, err = parseModels(v, ctx); err != nil {
			return s, err
		}
	}
	if v := f["health"]; v != nil {
		if s.Health, err = line(v, ctx, "health"); err != nil {
			return s, err
		}
		if ValidatePath(s.Health) != nil {
			return s, at(v, ctx, "health must be a path such as /health")
		}
	}
	if v := f["max-requests-per-minute"]; v != nil {
		if s.MaxRequestsPerMinute, err = integer(v, ctx, "max-requests-per-minute", 1, 1000000); err != nil {
			return s, err
		}
	}
	if v := f["idle-timeout"]; v != nil {
		if s.IdleTimeout, err = duration(v, ctx, "idle-timeout"); err != nil {
			return s, err
		}
	}
	if v := f["api-key"]; v != nil {
		if s.APIKey, err = line(v, ctx, "api-key"); err != nil {
			return s, err
		}
	}
	return s, nil
}

func modelName(n *yaml.Node, ctx string) (string, error) {
	v, err := line(n, ctx, "model name")
	if err != nil {
		return "", err
	}
	if len(v) > MaxModelNameBytes || !utf8.ValidString(v) {
		return "", at(n, ctx, "model name %q is longer than %d bytes", v, MaxModelNameBytes)
	}
	for _, r := range v {
		if !unicode.IsPrint(r) {
			return "", at(n, ctx, "model name %q contains a non-printable character", v)
		}
	}
	return v, nil
}

// parseModels reads a list of names, or a map of <name on the server>:
// {alias: <name Cletus uses>} (alias optional).
func parseModels(n *yaml.Node, ctx string) (Models, error) {
	m := Models{Target: map[string]string{}}
	add := func(item *yaml.Node, name, target string) error {
		if _, dup := m.Target[name]; dup {
			return at(item, ctx, "models uses the name %q twice", name)
		}
		m.Names = append(m.Names, name)
		m.Target[name] = target
		return nil
	}
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Value == "any" {
			return m, at(n, ctx, "models: any is gone in v0.3; leave models out and request bodies are not inspected")
		}
	case yaml.SequenceNode:
		for _, item := range n.Content {
			name, err := modelName(item, ctx)
			if err != nil {
				return m, err
			}
			if err := add(item, name, name); err != nil {
				return m, err
			}
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			server, err := modelName(n.Content[i], ctx)
			if err != nil {
				return m, err
			}
			name := server
			v := n.Content[i+1]
			switch {
			case v.Kind == yaml.ScalarNode && v.Tag == "!!null":
			case v.Kind == yaml.MappingNode:
				f, err := mapping(v, ctx+" model "+strconv.Quote(server), []string{"alias"})
				if err != nil {
					return m, err
				}
				if a := f["alias"]; a != nil {
					if name, err = modelName(a, ctx); err != nil {
						return m, err
					}
				}
			default:
				return m, at(v, ctx, "model %q: expected {alias: <name Cletus uses>}, found %s", server, describe(v))
			}
			if err := add(n.Content[i], name, server); err != nil {
				return m, err
			}
		}
	}
	if len(m.Names) == 0 || len(m.Names) > MaxModels {
		return m, at(n, ctx, "models must be a list of 1 to %d names ([a, b]) or <name on the server>: {alias: <name Cletus uses>} entries", MaxModels)
	}
	return m, nil
}

// HostPort returns host:port for an http(s) URL, filling in the scheme's default port.
func HostPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port)
}
