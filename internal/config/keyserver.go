package config

import (
	"errors"
	"fmt"
	"net"
	"net/mail"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults of the key-server block.
const (
	DefaultKeyStateFile = "/var/lib/eidonix-connector/keys.json"
	DefaultClaimTTL     = 24 * time.Hour
	DefaultClaimLink    = "cletus://claim"
)

var (
	keyServerFields = []string{"allowed-domains", "claim-ttl", "state-file", "link", "smtp"}
	smtpFields      = []string{"address", "username", "password-file", "from"}
	domainPattern   = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$`)
	linkPattern     = regexp.MustCompile(`^[a-z][a-z0-9+.-]*://[a-z0-9.-]+$`)
)

// KeyServer is the key-server block: the connector issues, revokes and
// hands over account keys for the people of AllowedDomains.
type KeyServer struct {
	AllowedDomains []string
	ClaimTTL       time.Duration
	StateFile      string
	// Link is the claim link without its query, e.g. cletus://claim.
	Link string
	SMTP SMTP
}

// SMTP is the mail server claim links are sent through. Port 465 is implicit
// TLS; port 587 is STARTTLS.
type SMTP struct {
	Address      string
	Username     string
	PasswordFile string
	From         string
}

func parseKeyServer(n *yaml.Node) (*KeyServer, error) {
	const ctx = "key-server"
	k := &KeyServer{ClaimTTL: DefaultClaimTTL, StateFile: DefaultKeyStateFile, Link: DefaultClaimLink}
	f, err := mapping(n, ctx, keyServerFields)
	if err != nil {
		return nil, err
	}
	for _, req := range []string{"allowed-domains", "smtp"} {
		if f[req] == nil {
			return nil, at(n, ctx, "%s is required", req)
		}
	}
	d := f["allowed-domains"]
	if d.Kind != yaml.SequenceNode || len(d.Content) < 1 || len(d.Content) > 64 {
		return nil, at(d, ctx, "allowed-domains must list 1 to 64 e-mail domains, e.g. [example.com]")
	}
	for _, c := range d.Content {
		v, err := line(c, ctx, "domain")
		if err != nil {
			return nil, err
		}
		if !domainPattern.MatchString(v) {
			return nil, at(c, ctx, "domain %q must be lowercase, like example.com", v)
		}
		k.AllowedDomains = append(k.AllowedDomains, v)
	}
	if c := f["claim-ttl"]; c != nil {
		if k.ClaimTTL, err = duration(c, ctx, "claim-ttl"); err != nil {
			return nil, err
		}
	}
	if c := f["state-file"]; c != nil {
		if k.StateFile, err = text(c, ctx, "state-file"); err != nil {
			return nil, err
		}
		if !filepath.IsAbs(k.StateFile) {
			return nil, at(c, ctx, "state-file must be an absolute path")
		}
	}
	if c := f["link"]; c != nil {
		if k.Link, err = line(c, ctx, "link"); err != nil {
			return nil, err
		}
		if !linkPattern.MatchString(k.Link) {
			return nil, at(c, ctx, "link must be a URL without a query, like cletus://claim")
		}
	}
	if k.SMTP, err = parseSMTP(f["smtp"]); err != nil {
		return nil, err
	}
	return k, nil
}

func parseSMTP(n *yaml.Node) (SMTP, error) {
	const ctx = "key-server smtp"
	var s SMTP
	f, err := mapping(n, ctx, smtpFields)
	if err != nil {
		return s, err
	}
	for _, req := range smtpFields {
		if f[req] == nil {
			return s, at(n, ctx, "%s is required", req)
		}
	}
	if s.Address, err = line(f["address"], ctx, "address"); err != nil {
		return s, err
	}
	if host, port, e := net.SplitHostPort(s.Address); e != nil || host == "" || (port != "465" && port != "587") {
		return s, at(f["address"], ctx, "address must be host:465 (TLS from the start) or host:587 (STARTTLS)")
	}
	if s.Username, err = line(f["username"], ctx, "username"); err != nil {
		return s, err
	}
	if s.PasswordFile, err = line(f["password-file"], ctx, "password-file"); err != nil {
		return s, err
	}
	if !filepath.IsAbs(s.PasswordFile) {
		return s, at(f["password-file"], ctx, "password-file must be an absolute path")
	}
	if s.From, err = line(f["from"], ctx, "from"); err != nil {
		return s, err
	}
	if a, e := mail.ParseAddress(s.From); e != nil || a.Address != s.From {
		return s, at(f["from"], ctx, "from must be an e-mail address like keys@example.com")
	}
	return s, nil
}

// ReadSecretFile returns the first line of a file holding a secret (the SMTP
// password) after the permission gate.
func ReadSecretFile(path string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if _, err = CheckPermissions(path, fi, os.Geteuid()); err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	v, _, _ := strings.Cut(strings.TrimSpace(string(data)), "\n")
	if v = strings.TrimSpace(v); v == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return v, nil
}

// checkKeyServerFiles applies the permission gate to the password file and,
// when it exists, the state file.
func (c *Config) checkKeyServerFiles() error {
	k := c.KeyServer
	if k == nil {
		return nil
	}
	if _, err := ReadSecretFile(k.SMTP.PasswordFile); err != nil {
		return fmt.Errorf("key-server smtp password-file: %w", err)
	}
	if fi, err := os.Stat(k.StateFile); err == nil {
		if _, err = CheckPermissions(k.StateFile, fi, os.Geteuid()); err != nil {
			return fmt.Errorf("key-server state-file: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("key-server state-file: %w", err)
	}
	return nil
}

func (k *KeyServer) describe() string {
	return fmt.Sprintf("Issues account keys for %s and mails claim links (valid %s) from %s through %s; keeps its key state in %s.\n",
		strings.Join(k.AllowedDomains, ", "), ShortDuration(k.ClaimTTL), k.SMTP.From, k.SMTP.Address, k.StateFile)
}
