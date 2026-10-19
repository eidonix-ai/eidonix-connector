// Package hub is eidonix-connect: it accepts connector sessions on its public
// port and makes every connected service reachable on its local port, by
// site and service name. Its whole configuration is one file,
// /etc/eidonix-connect/connect.yaml (docs/HUB.md).
package hub

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultPath is where serve, check and status read the file.
	DefaultPath = "/etc/eidonix-connect/connect.yaml"
	// DefaultAccessLog is the access log unless the file names another.
	DefaultAccessLog = "/var/log/eidonix-connect/access.log"
	// MaxSites bounds the sites one file may list.
	MaxSites = 1000
	// MaxApplications bounds a site's applications list.
	MaxApplications = 64
	// MaxLabelBytes bounds an application name (and the For label).
	MaxLabelBytes = protocol.MaxStreamMetadataBytes
	maxFileBytes  = 1 << 20
)

// reservedSites are site names that would collide with the local port's own
// paths (/v1/catalog, /healthz).
var reservedSites = map[string]bool{"v1": true, "healthz": true}

var (
	topFields     = []string{"public", "local", "server-key", "access-log", "sites"}
	siteFields    = []string{"token", "next-token", "disabled", "applications", "services"}
	serviceFields = []string{"applications"}
)

// Config is a validated hub file.
type Config struct {
	// Public is the address connectors' sessions arrive on (behind TLS).
	Public string
	// Local is the address applications on this host use.
	Local string
	// ServerKey signs the identity proof; KeyID and PublicKey are derived.
	ServerKey ed25519.PrivateKey
	KeyID     string
	PublicKey string
	// AccessLog is a file path, or "-" for standard output.
	AccessLog string
	// Sites by name; Order is file order.
	Sites map[string]*Site
	Order []string
}

// Site is one sites entry.
type Site struct {
	Name      string
	Line      int
	Token     string
	NextToken string
	Disabled  bool
	// Applications, when non-nil, lists the only application labels that
	// may use the site.
	Applications []string
	// Services holds per-service settings by service name. A service named
	// here need not be offered by the site (yet).
	Services map[string]*SiteService
}

// SiteService is a site's per-service settings.
type SiteService struct {
	Name string
	Line int
	// Applications, when non-nil, replaces the site's list for this service.
	Applications []string
}

// Accepts reports whether token is one of the site's tokens (constant time).
func (s *Site) Accepts(token string) bool {
	return constantEqual(token, s.Token) || s.NextToken != "" && constantEqual(token, s.NextToken)
}

// Allows reports whether an application label may use one of the site's
// services: the service's own applications list when it has one, otherwise
// the site's; without either, every application may.
func (s *Site) Allows(service, application string) bool {
	list := s.Applications
	if svc := s.Services[service]; svc != nil && svc.Applications != nil {
		list = svc.Applications
	}
	if list == nil {
		return true
	}
	for _, a := range list {
		if a == application {
			return true
		}
	}
	return false
}

// lineError is a validation error tied to a line of the file.
type lineError struct {
	line int
	msg  string
}

func (e *lineError) Error() string { return fmt.Sprintf("line %d: %s", e.line, e.msg) }

func at(n *yaml.Node, ctx, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if ctx != "" {
		msg = ctx + ": " + msg
	}
	return &lineError{n.Line, msg}
}

// Load reads path through the permission gate and parses it.
func Load(path string) (*Config, error) {
	data, err := ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// ReadFile applies the permission gate and returns the file's bytes.
func ReadFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := CheckPermissions(path, fi, os.Geteuid()); err != nil {
		return nil, err
	}
	if fi.Size() > maxFileBytes {
		return nil, fmt.Errorf("%s is larger than 1 MiB", path)
	}
	return io.ReadAll(io.LimitReader(f, maxFileBytes+1))
}

// CheckPermissions is the permission gate: the file holds the server key and
// every site token, so it must be a regular file owned by root (or by the
// user running the hub), with no access for other users and no group write.
func CheckPermissions(path string, fi os.FileInfo, euid int) error {
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: cannot read file ownership", path)
	}
	uid := int(st.Uid)
	if uid != 0 && uid != euid {
		return fmt.Errorf("%s is owned by uid %d; it must be owned by root or by the user running eidonix-connect. Fix: chown root %s", path, uid, path)
	}
	if mode := fi.Mode().Perm(); mode&0o027 != 0 {
		return fmt.Errorf("%s has mode %04o; it holds the server key and site tokens, so other users must have no access and its group no write. Fix: chmod 0600 %s (or 0640 with the hub's group)", path, uint32(mode), path)
	}
	return nil
}

// Parse decodes and validates the file's bytes.
func Parse(data []byte) (*Config, error) {
	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("the file is empty; it needs public, local, server-key and sites")
		}
		return nil, fmt.Errorf("not valid YAML: %v", strings.TrimPrefix(err.Error(), "yaml: "))
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("the file must contain exactly one YAML document")
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, &lineError{doc.Line, "the file must be a set of settings (public, local, server-key, sites)"}
	}
	root := doc.Content[0]
	f, err := mapping(root, "", topFields)
	if err != nil {
		return nil, err
	}
	c := &Config{AccessLog: DefaultAccessLog, Sites: map[string]*Site{}}
	for _, req := range []string{"public", "local", "server-key"} {
		if f[req] == nil {
			return nil, &lineError{root.Line, req + " is required"}
		}
	}
	if c.Public, err = address(f["public"], "public"); err != nil {
		return nil, err
	}
	if c.Local, err = address(f["local"], "local"); err != nil {
		return nil, err
	}
	if samePort(c.Public, c.Local) {
		return nil, at(f["local"], "", "local must not be the same address as public")
	}
	key, err := line(f["server-key"], "", "server-key")
	if err != nil {
		return nil, err
	}
	if c.ServerKey, err = ParseServerKey(key); err != nil {
		return nil, at(f["server-key"], "", "%v", err)
	}
	pub := c.ServerKey.Public().(ed25519.PublicKey)
	c.KeyID, c.PublicKey = protocol.KeyID(pub), base64.StdEncoding.EncodeToString(pub)
	if n := f["access-log"]; n != nil {
		if c.AccessLog, err = line(n, "", "access-log"); err != nil {
			return nil, err
		}
		if c.AccessLog != "-" && !filepath.IsAbs(c.AccessLog) {
			return nil, at(n, "", "access-log must be an absolute path, or - for standard output")
		}
	}
	section := f["sites"]
	if section == nil || section.Tag == "!!null" {
		return c, nil
	}
	if section.Kind != yaml.MappingNode {
		return nil, at(section, "", "sites must be a set of sites by name (name: then its settings)")
	}
	tokens := map[string]string{} // token id → site that uses it
	for i := 0; i+1 < len(section.Content); i += 2 {
		k, v := section.Content[i], section.Content[i+1]
		if k.Kind != yaml.ScalarNode || !protocol.ValidSiteName(k.Value) {
			return nil, at(k, "", "site name %q must be the connector's site: 1-40 lowercase letters, digits or -, starting with a letter or digit", k.Value)
		}
		if reservedSites[k.Value] {
			return nil, at(k, "", "site name %q is reserved (the local port uses /%s/)", k.Value, k.Value)
		}
		if prior, dup := c.Sites[k.Value]; dup {
			return nil, at(k, fmt.Sprintf("site %q", k.Value), "name already used on line %d", prior.Line)
		}
		s, err := parseSite(k, v)
		if err != nil {
			return nil, err
		}
		for _, t := range []string{s.Token, s.NextToken} {
			if t == "" {
				continue
			}
			id, _, _ := protocol.ParseToken(t)
			if other, dup := tokens[id]; dup {
				return nil, at(k, fmt.Sprintf("site %q", k.Value), "a token with id %s is already used by site %q; every token must be unique (eidonix-connect token prints a new one)", id, other)
			}
			tokens[id] = s.Name
		}
		c.Sites[s.Name] = s
		c.Order = append(c.Order, s.Name)
	}
	if len(c.Order) > MaxSites {
		return nil, fmt.Errorf("at most %d sites", MaxSites)
	}
	return c, nil
}

func parseSite(k, v *yaml.Node) (*Site, error) {
	ctx := fmt.Sprintf("site %q", k.Value)
	s := &Site{Name: k.Value, Line: k.Line}
	f, err := mapping(v, ctx, siteFields)
	if err != nil {
		return nil, err
	}
	if f["token"] == nil {
		return nil, at(k, ctx, "token is required (eidonix-connect token prints a new one)")
	}
	if s.Token, err = token(f["token"], ctx, "token"); err != nil {
		return nil, err
	}
	if n := f["next-token"]; n != nil {
		if s.NextToken, err = token(n, ctx, "next-token"); err != nil {
			return nil, err
		}
	}
	if n := f["disabled"]; n != nil {
		if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" || n.Decode(&s.Disabled) != nil {
			return nil, at(n, ctx, "disabled must be true or false")
		}
	}
	if n := f["applications"]; n != nil {
		if s.Applications, err = applications(n, ctx); err != nil {
			return nil, err
		}
	}
	if n := f["services"]; n != nil && n.Tag != "!!null" {
		if n.Kind != yaml.MappingNode {
			return nil, at(n, ctx, "services must be a set of services by name (name: then its settings)")
		}
		s.Services = map[string]*SiteService{}
		for i := 0; i+1 < len(n.Content); i += 2 {
			sk, sv := n.Content[i], n.Content[i+1]
			if sk.Kind != yaml.ScalarNode || !protocol.ValidServiceName(sk.Value) {
				return nil, at(sk, ctx, "service name %q must be 1-40 lowercase letters, digits or -, starting with a letter or digit", sk.Value)
			}
			sctx := fmt.Sprintf("site %q service %q", s.Name, sk.Value)
			if prior, dup := s.Services[sk.Value]; dup {
				return nil, at(sk, sctx, "name already used on line %d", prior.Line)
			}
			svc := &SiteService{Name: sk.Value, Line: sk.Line}
			sf, err := mapping(sv, sctx, serviceFields)
			if err != nil {
				return nil, err
			}
			if a := sf["applications"]; a != nil {
				if svc.Applications, err = applications(a, sctx); err != nil {
					return nil, err
				}
			}
			s.Services[svc.Name] = svc
		}
	}
	return s, nil
}

func applications(n *yaml.Node, ctx string) ([]string, error) {
	if n.Kind != yaml.SequenceNode || len(n.Content) == 0 || len(n.Content) > MaxApplications {
		return nil, at(n, ctx, "applications must list 1 to %d application names (leave it out to let every application use it)", MaxApplications)
	}
	out := []string{}
	for _, a := range n.Content {
		name, err := line(a, ctx, "application name")
		if err != nil {
			return nil, err
		}
		if !validLabel(name) || strings.Contains(name, " ") {
			return nil, at(a, ctx, "application name %q must be at most %d printable characters without spaces", name, MaxLabelBytes)
		}
		for _, prior := range out {
			if prior == name {
				return nil, at(a, ctx, "application %q is listed twice", name)
			}
		}
		out = append(out, name)
	}
	return out, nil
}

func token(n *yaml.Node, ctx, field string) (string, error) {
	v, err := line(n, ctx, field)
	if err != nil {
		return "", err
	}
	if _, _, e := protocol.ParseToken(v); e != nil {
		return "", at(n, ctx, "%s is not a connection token (cxn_<16 characters>_<43 characters>); eidonix-connect token prints a new one", field)
	}
	return v, nil
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

// line returns a non-empty single-line scalar without control characters.
func line(n *yaml.Node, ctx, field string) (string, error) {
	if n.Kind != yaml.ScalarNode || n.Tag == "!!null" || n.Value == "" {
		return "", at(n, ctx, "%s needs a value", field)
	}
	for _, r := range n.Value {
		if unicode.IsControl(r) {
			return "", at(n, ctx, "%s must be one line without control characters", field)
		}
	}
	return n.Value, nil
}

// address validates a listen address: [host]:port.
func address(n *yaml.Node, field string) (string, error) {
	v, err := line(n, "", field)
	if err != nil {
		return "", err
	}
	host, port, e := net.SplitHostPort(v)
	p, pe := strconv.Atoi(port)
	if e != nil || pe != nil || p < 0 || p > 65535 || strings.ContainsAny(host, " /") {
		return "", at(n, "", "%s must be an address to listen on, such as :8443 or 127.0.0.1:8080", field)
	}
	return v, nil
}

// samePort reports whether two listen addresses would collide.
func samePort(a, b string) bool {
	ah, ap, _ := net.SplitHostPort(a)
	bh, bp, _ := net.SplitHostPort(b)
	if ap != bp || ap == "0" { // port 0: the system picks distinct ports
		return false
	}
	return ah == bh || ah == "" || bh == "" || ah == "0.0.0.0" || bh == "0.0.0.0" || ah == "::" || bh == "::"
}

// ParseServerKey decodes the hub's Ed25519 identity: standard base64 of a
// 32-byte seed or a 64-byte private key (seed followed by its public key).
func ParseServerKey(s string) (ed25519.PrivateKey, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, errors.New("server-key must be base64 (eidonix-connect server-key prints a new one)")
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		key := ed25519.NewKeyFromSeed(raw[:ed25519.SeedSize])
		if !bytes.Equal(key[ed25519.SeedSize:], raw[ed25519.SeedSize:]) {
			return nil, errors.New("server-key: the 64-byte key's public half does not match its seed")
		}
		return key, nil
	}
	return nil, fmt.Errorf("server-key must be a 32-byte Ed25519 seed or 64-byte private key, found %d bytes", len(raw))
}

// validLabel reports whether s is printable ASCII of 1..MaxLabelBytes.
func validLabel(s string) bool {
	if s == "" || len(s) > MaxLabelBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 32 || s[i] > 126 {
			return false
		}
	}
	return true
}

// constantEqual compares two strings in time that depends only on their
// lengths (hashing first makes even that constant).
func constantEqual(a, b string) bool {
	ha, hb := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	var diff byte
	for i := range ha {
		diff |= ha[i] ^ hb[i]
	}
	return diff == 0
}
