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

	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

const (
	MaxServices       = 256
	MaxURLs           = 64
	MaxModels         = 64
	MaxModelNameBytes = 200
	MaxPaths          = 64
)

var (
	topFields      = append(append([]string{"name", "upstream"}, protocol.ServiceTypes...), "access_log")
	upstreamFields = []string{"url", "token", "public_keys", "proxy_url", "ca_certificate"}
	limitFields    = []string{"max_concurrent_requests", "max_requests_per_minute", "idle_timeout", "api_key"}
	storageFields  = []string{"name", "endpoint", "bucket", "prefix", "region", "access_key", "secret_key", "encryption", "encryption_key", "kms_key_id", "allow_http", "max_file_size", "max_read_per_hour", "max_concurrent_requests", "ca_certificate"}
)

// serviceFields lists the settings a service of type t accepts.
func serviceFields(t servicetype.Type) []string {
	switch t.Name {
	case protocol.TypeStorage:
		return storageFields
	case protocol.TypeCustom:
		return append([]string{"name", "url", "urls", "paths"}, limitFields...)
	}
	f := []string{"name", "url", "urls", "api_path"}
	if t.Models {
		f = append(f, "models")
	}
	return append(f, limitFields...)
}

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

// Parse decodes and validates a configuration file's bytes. It never reads
// other files: every secret is in the file itself.
func Parse(data []byte) (*Config, error) {
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("the file is empty; it needs at least an upstream section")
		}
		return nil, fmt.Errorf("not valid YAML: %v", strings.TrimPrefix(err.Error(), "yaml: "))
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("the file must contain exactly one YAML document")
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, &lineError{doc.Line, "the file must be a set of settings (name, upstream, chat, …)"}
	}
	fields, err := mapping(doc.Content[0], "", topFields)
	if err != nil {
		return nil, err
	}
	c := &Config{AccessLog: DefaultAccessLog}
	if n := fields["name"]; n != nil {
		if c.Name, err = text(n, "", "name"); err != nil {
			return nil, err
		}
	}
	up := fields["upstream"]
	if up == nil {
		return nil, &lineError{doc.Content[0].Line, "upstream is required (url, token and public_keys from Eidonix Setup)"}
	}
	if c.Upstream, err = parseUpstream(up); err != nil {
		return nil, err
	}
	if n := fields["access_log"]; n != nil {
		if c.AccessLog, err = text(n, "", "access_log"); err != nil {
			return nil, err
		}
		if !filepath.IsAbs(c.AccessLog) {
			return nil, at(n, "", "access_log must be an absolute path")
		}
	}
	names := map[string]Service{}
	for _, typ := range servicetype.All() {
		section := fields[typ.Name]
		if section == nil || section.Tag == "!!null" {
			continue
		}
		if section.Kind != yaml.SequenceNode {
			return nil, at(section, "", "%s must be a list of services (each starting with \"- name: …\")", typ.Name)
		}
		for i, item := range section.Content {
			s, err := parseService(typ, item, i+1)
			if err != nil {
				return nil, err
			}
			if prior, ok := names[s.Name]; ok {
				return nil, at(item, fmt.Sprintf("%s service %q", typ.Name, s.Name), "name already used by the %s service on line %d; names must be unique in the file", prior.Type, prior.Line)
			}
			names[s.Name] = s
			c.Services = append(c.Services, s)
		}
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
	for _, req := range []string{"url", "token", "public_keys"} {
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
	keys := f["public_keys"]
	if keys.Kind != yaml.SequenceNode || len(keys.Content) < 1 || len(keys.Content) > 2 {
		return u, at(keys, ctx, "public_keys must list one or two keys (two only during a key rotation)")
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
	if n := f["proxy_url"]; n != nil {
		if u.ProxyURL, err = line(n, ctx, "proxy_url"); err != nil {
			return u, err
		}
		p, e := url.Parse(u.ProxyURL)
		if e != nil || (p.Scheme != "http" && p.Scheme != "https") || p.Hostname() == "" || (p.Path != "" && p.Path != "/") || p.RawQuery != "" || p.Fragment != "" || !validPort(p.Port()) {
			return u, at(n, ctx, "proxy_url must be http://host:port or https://host:port")
		}
	}
	if n := f["ca_certificate"]; n != nil {
		if u.CACertificate, err = certificate(n, ctx); err != nil {
			return u, err
		}
	}
	return u, nil
}
func certificate(n *yaml.Node, ctx string) (string, error) {
	v, err := text(n, ctx, "ca_certificate")
	if err != nil {
		return "", err
	}
	if !x509.NewCertPool().AppendCertsFromPEM([]byte(v)) {
		return "", at(n, ctx, "ca_certificate must be a PEM certificate (-----BEGIN CERTIFICATE----- …)")
	}
	return v, nil
}
func validPort(p string) bool {
	if p == "" {
		return true
	}
	v, err := strconv.Atoi(p)
	return err == nil && v >= 1 && v <= 65535
}

// serviceURL validates a service address: http or https, a host, an optional
// port, and no path, query, fragment or credentials.
func serviceURL(n *yaml.Node, ctx, field string) (string, error) {
	v, err := line(n, ctx, field)
	if err != nil {
		return "", err
	}
	p, e := url.Parse(v)
	if e != nil || (p.Scheme != "http" && p.Scheme != "https") || p.Hostname() == "" || strings.Contains(p.Hostname(), "%") || p.User != nil || p.RawQuery != "" || p.Fragment != "" || p.Opaque != "" || !validPort(p.Port()) {
		return "", at(n, ctx, "%s must be http://host:port or https://host:port", field)
	}
	if p.Path != "" && p.Path != "/" {
		return "", at(n, ctx, "%s must not include a path; use api_path for a path prefix", field)
	}
	return strings.TrimSuffix(v, "/"), nil
}

func parseService(t servicetype.Type, n *yaml.Node, index int) (Service, error) {
	s := Service{Type: t.Name, Line: n.Line}
	ctx := fmt.Sprintf("%s service #%d", t.Name, index)
	if n.Kind != yaml.MappingNode {
		return s, at(n, ctx, "each service needs settings (name, url, …)")
	}
	// Read the name first so every later message names the service.
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == "name" {
			v := n.Content[i+1]
			name, err := line(v, ctx, "name")
			if err != nil {
				return s, err
			}
			if !protocol.ValidServiceName(name) {
				return s, at(v, ctx, "name %q must be 1-40 lowercase letters, digits or -, starting with a letter or digit", name)
			}
			s.Name = name
			ctx = fmt.Sprintf("%s service %q", t.Name, name)
		}
	}
	f, err := mapping(n, ctx, serviceFields(t))
	if err != nil {
		return s, err
	}
	if s.Name == "" {
		return s, at(n, ctx, "name is required")
	}
	s.MaxConcurrentRequests = t.MaxConcurrent
	s.IdleTimeout = t.Idle
	if t.Name == protocol.TypeStorage {
		return s, parseStorage(&s, f, n, ctx)
	}
	switch {
	case f["url"] != nil && f["urls"] != nil:
		return s, at(f["urls"], ctx, "set url or urls, not both")
	case f["url"] != nil:
		v, err := serviceURL(f["url"], ctx, "url")
		if err != nil {
			return s, err
		}
		s.URLs = []string{v}
	case f["urls"] != nil:
		list := f["urls"]
		if list.Kind != yaml.SequenceNode || len(list.Content) == 0 || len(list.Content) > MaxURLs {
			return s, at(list, ctx, "urls must list 1 to %d addresses", MaxURLs)
		}
		for _, item := range list.Content {
			v, err := serviceURL(item, ctx, "urls entry")
			if err != nil {
				return s, err
			}
			s.URLs = append(s.URLs, v)
		}
	default:
		return s, at(n, ctx, "url is required (where the service runs, e.g. http://10.0.0.5:8000)")
	}
	if v := f["api_path"]; v != nil {
		p, err := line(v, ctx, "api_path")
		if err != nil {
			return s, err
		}
		if p != "/" && (servicetype.ValidatePath(p) != nil || strings.HasSuffix(p, "/")) {
			return s, at(v, ctx, "api_path must be / or a path such as /api/v1 (no trailing /)")
		}
		s.APIPath = p
	}
	if t.Models {
		v := f["models"]
		if v == nil {
			return s, at(n, ctx, "models is required: list the model names that may be used, or write models: any")
		}
		if s.Models, err = parseModels(v, ctx); err != nil {
			return s, err
		}
	}
	if t.Name == protocol.TypeCustom {
		v := f["paths"]
		if v == nil || v.Kind != yaml.SequenceNode || len(v.Content) == 0 || len(v.Content) > MaxPaths {
			node := n
			if v != nil {
				node = v
			}
			return s, at(node, ctx, "paths is required: list the exact requests allowed, e.g. [\"POST /v1/run\"]")
		}
		seen := map[servicetype.Request]bool{}
		for _, item := range v.Content {
			entry, err := line(item, ctx, "paths entry")
			if err != nil {
				return s, err
			}
			r, e := servicetype.ParseRequest(entry)
			if e != nil {
				return s, at(item, ctx, "paths entry %v", e)
			}
			if seen[r] {
				return s, at(item, ctx, "paths lists %s twice", r)
			}
			seen[r] = true
			s.Paths = append(s.Paths, r)
		}
	}
	return s, parseLimits(&s, f, ctx, 10000)
}

func parseLimits(s *Service, f map[string]*yaml.Node, ctx string, maxConcurrent int) error {
	var err error
	if v := f["max_concurrent_requests"]; v != nil {
		if s.MaxConcurrentRequests, err = integer(v, ctx, "max_concurrent_requests", 1, maxConcurrent); err != nil {
			return err
		}
	}
	if v := f["max_requests_per_minute"]; v != nil {
		if s.MaxRequestsPerMinute, err = integer(v, ctx, "max_requests_per_minute", 0, 1000000); err != nil {
			return err
		}
	}
	if v := f["idle_timeout"]; v != nil {
		if s.IdleTimeout, err = duration(v, ctx, "idle_timeout"); err != nil {
			return err
		}
	}
	if v := f["api_key"]; v != nil {
		if s.APIKey, err = line(v, ctx, "api_key"); err != nil {
			return err
		}
	}
	return nil
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

// parseModels reads `any`, a list of names, or a map of name: server-name.
func parseModels(n *yaml.Node, ctx string) (Models, error) {
	m := Models{Target: map[string]string{}}
	add := func(item *yaml.Node, name, target string) error {
		if _, dup := m.Target[name]; dup {
			return at(item, ctx, "models lists %q twice", name)
		}
		m.Names = append(m.Names, name)
		m.Target[name] = target
		return nil
	}
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Value == "any" && n.Tag == "!!str" {
			return Models{Any: true}, nil
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
			name, err := modelName(n.Content[i], ctx)
			if err != nil {
				return m, err
			}
			target, err := modelName(n.Content[i+1], ctx)
			if err != nil {
				return m, err
			}
			if err := add(n.Content[i], name, target); err != nil {
				return m, err
			}
		}
	}
	if len(m.Names) == 0 || len(m.Names) > MaxModels {
		return m, at(n, ctx, "models must be a list of 1 to %d names ([a, b]), name: server-name pairs, or any", MaxModels)
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
