// Package config reads and validates the connector's single YAML file,
// /etc/eidonix-connector/connector.yaml: the site it belongs to, the upstream
// it connects out to, the services it shares (by name) and where it logs.
// Secrets live in the same file, so the file itself is protected by a
// permission gate.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

const (
	DefaultPath      = "/etc/eidonix-connector/connector.yaml"
	DefaultAccessLog = "/var/log/eidonix-connector/access.log"
	// ServiceGroup is the packaged service account's group.
	ServiceGroup = "eidonix-connector"
	maxFileBytes = 1 << 20

	// Service kinds.
	KindHTTP = protocol.KindHTTP
	KindS3   = protocol.KindS3

	// Defaults for settings the file may leave out.
	DefaultMaxConcurrentRequests = 16
	DefaultIdleTimeout           = 5 * time.Minute
)

// ErrOldFormat reports a v0.2 file (services grouped by type).
var ErrOldFormat = errors.New("this is a v0.2 configuration (services grouped by type); v0.3 lists services by name, under services")

// Config is a validated configuration. Services are in file order.
type Config struct {
	Site      string
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
	Proxy         string
	CACertificate string
}

// Service is one shared service. An http service has Endpoints (and
// optionally Models, Health, APIKey); an s3 service has Storage.
type Service struct {
	Name string
	Kind string
	Line int
	// Address is as written in the file; URL is what the connector dials:
	// http://host:port for host:port, or the https:// address itself.
	Address               string
	URL                   string
	Endpoints             []Endpoint
	Models                Models
	Health                string
	APIKey                string
	MaxConcurrentRequests int
	MaxRequestsPerMinute  int
	IdleTimeout           time.Duration
	Storage               *Storage
}

// Models is a service's model list. Names are the names applications use, in file
// order; Target maps each to the model's name on the server. Without names
// the connector does not inspect request bodies.
type Models struct {
	Names  []string
	Target map[string]string
}

// Set reports whether the service checks models.
func (m Models) Set() bool { return len(m.Names) > 0 }

// Renamed reports whether any name differs from its server name.
func (m Models) Renamed() bool {
	for _, n := range m.Names {
		if m.Target[n] != n {
			return true
		}
	}
	return false
}

// Allows reports whether the service accepts method (WEBSOCKET for an
// upgrade) on an already canonical path.
func (s Service) Allows(method, path string) bool {
	for _, e := range s.Endpoints {
		if e.Matches(method, path) {
			return true
		}
	}
	return false
}

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
	if errors.Is(err, ErrOldFormat) {
		return nil, fmt.Errorf("%s: %w\nConvert it: eidonix-connector convert --site <connector name in Setup> %s > %s.new\nthen review the notes, run eidonix-connector check --config %s.new and replace the file", path, err, path, path, path)
	}
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
// "token", "api-key (gpu1)".
func (c *Config) Secrets() []string {
	out := []string{}
	if c.Upstream.Token != "" {
		out = append(out, "token")
	}
	for _, s := range c.Services {
		if s.APIKey != "" {
			out = append(out, "api-key ("+s.Name+")")
		}
		if st := s.Storage; st != nil {
			if st.AccessKey != "" {
				out = append(out, "access-key ("+s.Name+")")
			}
			if st.SecretKey != "" {
				out = append(out, "secret-key ("+s.Name+")")
			}
			if len(st.EncryptionKey) > 0 {
				out = append(out, "encryption-key ("+s.Name+")")
			}
		}
	}
	return out
}

// SecretFields are the setting names whose values are secret.
var SecretFields = map[string]bool{"token": true, "api-key": true, "access-key": true, "secret-key": true, "encryption-key": true}
