package config

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

// Describe explains the configuration back in sentences, as `check` prints it.
func (c *Config) Describe() string {
	var b strings.Builder
	keys := "1 public key"
	if len(c.Upstream.PublicKeys) != 1 {
		keys = fmt.Sprintf("%d public keys", len(c.Upstream.PublicKeys))
	}
	host := upstreamHost(c.Upstream.URL)
	if c.Upstream.ProxyURL != "" {
		fmt.Fprintf(&b, "Connects to upstream %s through the web proxy %s (%s).\n", host, HostPort(c.Upstream.ProxyURL), keys)
	} else {
		fmt.Fprintf(&b, "Connects to upstream %s (%s).\n", host, keys)
	}
	if c.Upstream.CACertificate != "" {
		b.WriteString("Also trusts the configured CA certificate for the upstream connection.\n")
	}
	switch len(c.Services) {
	case 0:
		b.WriteString("Shares no services, and reaches no other machine.\n")
	case 1:
		b.WriteString("Shares 1 service and can reach only this machine:\n")
	default:
		fmt.Fprintf(&b, "Shares %d services and can reach only these machines:\n", len(c.Services))
	}
	var table bytes.Buffer
	w := tabwriter.NewWriter(&table, 0, 0, 2, ' ', 0)
	for _, s := range c.Services {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", s.Type, s.Name, strings.Join(s.Hosts(), ", "), s.details())
	}
	w.Flush()
	b.Write(table.Bytes())
	used := map[string]bool{}
	for _, s := range c.Services {
		used[s.Type] = true
	}
	unused := []string{}
	for _, t := range protocol.ServiceTypes {
		if !used[t] {
			unused = append(unused, t)
		}
	}
	if len(unused) > 0 {
		fmt.Fprintf(&b, "Not shared: %s.\n", strings.Join(unused, ", "))
	}
	fmt.Fprintf(&b, "Logs every request to %s (never contents).\n", c.AccessLog)
	if c.File.Path != "" {
		fmt.Fprintf(&b, "File permissions: ok (%04o %s:%s). ", uint32(c.File.Mode), c.File.Owner, c.File.Group)
	}
	if secrets := c.Secrets(); len(secrets) > 0 {
		fmt.Fprintf(&b, "Secrets set: %s.\n", strings.Join(secrets, ", "))
	} else {
		b.WriteString("No secrets set.\n")
	}
	return b.String()
}

func upstreamHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Port() == "" || u.Port() == "443" {
		if err == nil {
			return u.Hostname()
		}
		return raw
	}
	return u.Host
}

// Hosts lists the host:port pairs the service may reach.
func (s Service) Hosts() []string {
	if s.Storage != nil {
		return []string{HostPort(s.Storage.Endpoint)}
	}
	out := []string{}
	for _, u := range s.URLs {
		out = append(out, HostPort(u))
	}
	return out
}

func (s Service) details() string {
	parts := []string{}
	if st := s.Storage; st != nil {
		parts = append(parts, fmt.Sprintf("bucket %s, folder %s; %s", st.Bucket, st.Prefix, encryptionSentence(st.Encryption)))
		if st.MaxReadPerHour > 0 {
			parts = append(parts, "max read per hour: "+FormatSize(st.MaxReadPerHour))
		}
	}
	if s.Contract().Models {
		parts = append(parts, "models: "+s.Models.describe())
	}
	if s.Type == protocol.TypeCustom {
		reqs := []string{}
		for _, r := range s.Paths {
			reqs = append(reqs, r.String())
		}
		parts = append(parts, "requests: "+strings.Join(reqs, ", "))
	}
	if s.APIPath != "" {
		parts = append(parts, "api path: "+s.APIPath)
	}
	parts = append(parts, fmt.Sprintf("max concurrent: %d", s.MaxConcurrentRequests))
	if s.MaxRequestsPerMinute > 0 {
		parts = append(parts, fmt.Sprintf("max per minute: %d", s.MaxRequestsPerMinute))
	}
	if s.Type != protocol.TypeStorage && s.IdleTimeout != s.Contract().Idle {
		parts = append(parts, "idle timeout: "+shortDuration(s.IdleTimeout))
	}
	return strings.Join(parts, "   ")
}

// shortDuration renders 5m0s as 5m and 1h0m0s as 1h.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

func (m Models) describe() string {
	if m.Any {
		return "any (whatever the server offers)"
	}
	out := []string{}
	for _, n := range m.Names {
		if t := m.Target[n]; t != n {
			out = append(out, n+" → "+t)
		} else {
			out = append(out, n)
		}
	}
	return strings.Join(out, ", ")
}

func encryptionSentence(mode string) string {
	switch mode {
	case "sse-c":
		return "files are encrypted with a key kept only on this machine (sse-c)"
	case "sse-s3":
		return "files are encrypted by the storage (sse-s3)"
	case "sse-kms":
		return "files are encrypted by the storage with a KMS key (sse-kms)"
	}
	return "files are stored without encryption"
}

// Redacted is what a secret value becomes in `check --redacted`.
const Redacted = "•••• (set)"

// Redact returns the file with every secret value replaced by Redacted,
// keeping comments and layout as far as a YAML round-trip allows.
func Redact(data []byte) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("not valid YAML: %v", strings.TrimPrefix(err.Error(), "yaml: "))
	}
	redactNode(&doc)
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	enc.Close()
	return out.Bytes(), nil
}
func redactNode(n *yaml.Node) {
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			// A proxy URL with a password is secret too.
			proxyCredentials := k.Value == "proxy_url" && strings.Contains(v.Value, "@")
			if (SecretFields[k.Value] || proxyCredentials) && v.Kind == yaml.ScalarNode && v.Value != "" {
				v.Value, v.Tag, v.Style = Redacted, "!!str", 0
				continue
			}
			redactNode(v)
		}
		return
	}
	for _, c := range n.Content {
		redactNode(c)
	}
}
