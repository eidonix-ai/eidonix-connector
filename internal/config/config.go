// Package config reads and validates the connector's single YAML file,
// /etc/eidonix-connector/connector.yaml: the upstream it connects out to, the
// services it shares (grouped by type), and where it logs. Secrets live in the
// same file, so the file itself is protected by a permission gate.
package config

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
)

const (
	DefaultPath      = "/etc/eidonix-connector/connector.yaml"
	DefaultAccessLog = "/var/log/eidonix-connector/access.log"
	// ServiceGroup is the packaged service account's group.
	ServiceGroup = "eidonix-connector"
	maxFileBytes = 1 << 20
)

// Config is a validated configuration. Services are in canonical type order,
// then file order.
type Config struct {
	Name      string
	Upstream  Upstream
	Services  []Service
	AccessLog string
	// File describes the file Load read (zero for Parse).
	File FileInfo
}

type Upstream struct {
	URL           string
	Token         string
	PublicKeys    []string
	ProxyURL      string
	CACertificate string
}

// Service is one shared service. Storage is set only for type storage; Paths
// only for type custom.
type Service struct {
	Name                  string
	Type                  string
	Line                  int
	URLs                  []string
	APIPath               string
	Models                Models
	MaxConcurrentRequests int
	MaxRequestsPerMinute  int
	IdleTimeout           time.Duration
	APIKey                string
	Paths                 []servicetype.Request
	Storage               *Storage
}

// Models is a service's model policy. With Any, any model passes through;
// otherwise Names (as callers send them, in file order) is the allowlist and
// Target maps each name to the name the server uses.
type Models struct {
	Any    bool
	Names  []string
	Target map[string]string
}

// Allowlist reports whether the service restricts models to Names.
func (m Models) Allowlist() bool { return !m.Any && len(m.Names) > 0 }

// Renamed reports whether any name differs from its server name.
func (m Models) Renamed() bool {
	for _, n := range m.Names {
		if m.Target[n] != n {
			return true
		}
	}
	return false
}

// Contract returns the service's type contract.
func (s Service) Contract() servicetype.Type { t, _ := servicetype.Lookup(s.Type); return t }

// FileInfo is what the permission gate saw.
type FileInfo struct {
	Path  string
	Mode  os.FileMode
	Owner string
	Group string
}

// Load opens path, applies the permission gate and parses the file.
func Load(path string) (*Config, error) {
	data, info, err := ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	c.File = info
	return c, nil
}

// ReadFile applies the permission gate and returns the file's bytes.
func ReadFile(path string) ([]byte, FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, FileInfo{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, FileInfo{}, err
	}
	info, err := CheckPermissions(path, fi, os.Geteuid())
	if err != nil {
		return nil, info, err
	}
	if fi.Size() > maxFileBytes {
		return nil, info, fmt.Errorf("config %s is larger than 1 MiB", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	return data, info, err
}

// CheckPermissions is the file permission gate: a regular file owned by root
// (or, when not running as root, by the current user), with no permission
// bits for other users and no group write. The error says exactly how to fix it.
func CheckPermissions(path string, fi os.FileInfo, euid int) (FileInfo, error) {
	info := FileInfo{Path: path, Mode: fi.Mode().Perm()}
	if !fi.Mode().IsRegular() {
		return info, fmt.Errorf("config %s is not a regular file", path)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return info, fmt.Errorf("config %s: cannot read file ownership", path)
	}
	uid, gid := int(st.Uid), int(st.Gid)
	info.Owner, info.Group = userName(uid), groupName(gid)
	fix := fmt.Sprintf("chown root:%s %s && chmod 0640 %s", ServiceGroup, path, path)
	if euid != 0 && uid == euid {
		fix = fmt.Sprintf("chmod 0600 %s", path)
	}
	if uid != 0 && uid != euid {
		return info, fmt.Errorf("config %s is owned by %s; it must be owned by root. Fix: %s", path, info.Owner, fix)
	}
	if info.Mode&0o027 != 0 {
		return info, fmt.Errorf("config %s has mode %04o; it holds secrets, so other users must have no access and its group no write. Fix: %s", path, uint32(info.Mode), fix)
	}
	return info, nil
}
func userName(uid int) string {
	if u, err := user.LookupId(strconv.Itoa(uid)); err == nil {
		return u.Username
	}
	return strconv.Itoa(uid)
}
func groupName(gid int) string {
	if g, err := user.LookupGroupId(strconv.Itoa(gid)); err == nil {
		return g.Name
	}
	return strconv.Itoa(gid)
}

// Secrets lists the secret settings that are set, for check output, e.g.
// "token", "api_key (gpu1)".
func (c *Config) Secrets() []string {
	out := []string{}
	if c.Upstream.Token != "" {
		out = append(out, "token")
	}
	for _, s := range c.Services {
		if s.APIKey != "" {
			out = append(out, "api_key ("+s.Name+")")
		}
		if st := s.Storage; st != nil {
			if st.AccessKey != "" {
				out = append(out, "access_key ("+s.Name+")")
			}
			if st.SecretKey != "" {
				out = append(out, "secret_key ("+s.Name+")")
			}
			if len(st.EncryptionKey) > 0 {
				out = append(out, "encryption_key ("+s.Name+")")
			}
		}
	}
	return out
}

// SecretFields are the setting names whose values are secret.
var SecretFields = map[string]bool{"token": true, "api_key": true, "access_key": true, "secret_key": true, "encryption_key": true}
