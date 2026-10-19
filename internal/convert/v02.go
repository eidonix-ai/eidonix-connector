package convert

// v0.2 → v0.3: services grouped by type become services by name, each type's
// fixed request list becomes allowed-endpoints (through api_path), model
// aliases turn around (<name on the server>: {alias: <name applications use>}),
// and limits the old type implied are written out where they differ from the
// v0.3 defaults, so the converted file behaves as the old one did.

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"gopkg.in/yaml.v3"
)

// v03Endpoints is what each v0.2 type becomes, written with the type's
// default prefix; api_path maps it as v0.2 did.
var v03Endpoints = map[string][]string{
	"chat":          {"POST /v1/*", "GET /v1/models"},
	"anthropic":     {"POST /v1/messages", "POST /v1/messages/count_tokens"},
	"embeddings":    {"POST /v1/embeddings"},
	"rerank":        {"POST /v1/rerank"},
	"transcription": {"POST /v1/audio/transcriptions"},
	"speech":        {"POST /v1/audio/speech", "GET /v1/audio/voices"},
	"image":         {"POST /v1/images/*"},
	"document":      {"POST /layout-parsing"},
	"conversion":    {"POST /convert", "POST /extract"},
	"realtime":      {"WEBSOCKET /v1/stream", "GET /v1/config"},
}

var (
	v02Upstream = []string{"url", "token", "public_keys", "proxy_url", "ca_certificate"}
	v02HTTP     = []string{"name", "url", "urls", "api_path", "models", "paths", "max_concurrent_requests", "max_requests_per_minute", "idle_timeout", "api_key"}
	v02Storage  = []string{"name", "endpoint", "bucket", "prefix", "region", "access_key", "secret_key", "encryption", "encryption_key", "kms_key_id", "allow_http", "max_file_size", "max_read_per_hour", "max_concurrent_requests", "ca_certificate"}
)

type v02converter struct{ notes []string }

func (c *v02converter) note(format string, args ...any) {
	c.notes = append(c.notes, fmt.Sprintf(format, args...))
}

// fields returns a mapping's values by key, refusing keys outside allowed.
func fields(n *yaml.Node, where string, allowed []string) (map[string]*yaml.Node, error) {
	if n.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: %s: expected settings", n.Line, where)
	}
	known := map[string]bool{}
	for _, k := range allowed {
		known[k] = true
	}
	out := map[string]*yaml.Node{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i]
		if !known[k.Value] {
			return nil, fmt.Errorf("line %d: %s: unknown v0.2 setting %q", k.Line, where, k.Value)
		}
		out[k.Value] = n.Content[i+1]
	}
	return out, nil
}

// address turns a v0.2 http(s) URL into a v0.3 address.
func address(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSuffix(raw, "/"))
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("address %q is not an http or https URL", raw)
	}
	if u.Scheme == "https" {
		return u.String(), nil
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	return net.JoinHostPort(u.Hostname(), port), nil
}

func fromV02(data []byte, site string) ([]byte, []string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, nil, fmt.Errorf("not valid YAML: %v", err)
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, nil, errors.New("not a v0.2 configuration")
	}
	c := &v02converter{}
	root := &yaml.Node{Kind: yaml.MappingNode}
	set(root, "site", str(site))
	services := &yaml.Node{Kind: yaml.MappingNode}
	var upstream, accessLog *yaml.Node
	names := map[string]bool{}
	in := doc.Content[0]
	for i := 0; i+1 < len(in.Content); i += 2 {
		k, v := in.Content[i], in.Content[i+1]
		switch k.Value {
		case "name":
			c.note("name %q not carried; the connector is now known by its site, %s", v.Value, site)
		case "upstream":
			var err error
			if upstream, err = c.upstream(v); err != nil {
				return nil, nil, err
			}
		case "access_log":
			if v.Value != config.DefaultAccessLog {
				accessLog = str(v.Value)
			}
		default:
			t, ok := v02Lookup(k.Value)
			if !ok {
				return nil, nil, fmt.Errorf("line %d: unknown v0.2 setting %q", k.Line, k.Value)
			}
			if v.Tag == "!!null" {
				continue
			}
			if v.Kind != yaml.SequenceNode {
				return nil, nil, fmt.Errorf("line %d: %s must be a list of services", v.Line, t.Name)
			}
			for _, item := range v.Content {
				name, svc, err := c.service(t, item)
				if err != nil {
					return nil, nil, err
				}
				if names[name] {
					return nil, nil, fmt.Errorf("line %d: service name %q is used twice", item.Line, name)
				}
				names[name] = true
				key := str(name)
				key.LineComment = "was " + t.Name
				services.Content = append(services.Content, key, svc)
			}
		}
	}
	if upstream == nil {
		return nil, nil, errors.New("the v0.2 file has no upstream section")
	}
	set(root, "upstream", upstream)
	if accessLog != nil {
		set(root, "access-log", accessLog)
	}
	set(root, "services", services)
	out := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}, HeadComment: "Converted to v0.3 by eidonix-connector convert.\nReview the notes it printed, then run: eidonix-connector check"}
	var b bytes.Buffer
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(out); err != nil {
		return nil, nil, err
	}
	enc.Close()
	return b.Bytes(), c.notes, nil
}

func (c *v02converter) upstream(n *yaml.Node) (*yaml.Node, error) {
	f, err := fields(n, "upstream", v02Upstream)
	if err != nil {
		return nil, err
	}
	up := &yaml.Node{Kind: yaml.MappingNode}
	for _, k := range v02Upstream {
		v := f[k]
		if v == nil {
			continue
		}
		switch k {
		case "public_keys":
			keys := []string{}
			for _, item := range v.Content {
				keys = append(keys, item.Value)
			}
			set(up, "public-keys", flowList(keys))
		case "proxy_url":
			set(up, "proxy", str(v.Value))
		case "ca_certificate":
			set(up, "ca-certificate", literal(v.Value))
		default:
			set(up, k, str(v.Value))
		}
	}
	return up, nil
}

// flowList is a [a, b] list with plain scalars.
func flowList(items []string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, s := range items {
		n.Content = append(n.Content, str(s))
	}
	return n
}

func (c *v02converter) service(t v02Type, n *yaml.Node) (string, *yaml.Node, error) {
	allowed := v02HTTP
	if t.Name == "storage" {
		allowed = v02Storage
	}
	f, err := fields(n, t.Name+" service", allowed)
	if err != nil {
		return "", nil, err
	}
	if f["name"] == nil {
		return "", nil, fmt.Errorf("line %d: %s service without a name", n.Line, t.Name)
	}
	name := f["name"].Value
	if t.Name == "storage" {
		svc, err := c.storage(name, f)
		return name, svc, err
	}
	svc := &yaml.Node{Kind: yaml.MappingNode}
	// Address: v0.3 dials exactly one.
	var addr string
	switch {
	case f["url"] != nil:
		addr = f["url"].Value
	case f["urls"] != nil && len(f["urls"].Content) > 0:
		addr = f["urls"].Content[0].Value
		if len(f["urls"].Content) > 1 {
			c.note("%s: urls listed %d addresses; v0.3 dials one, so only %s is kept (put a load balancer in front of the others)", name, len(f["urls"].Content), addr)
		}
	default:
		return "", nil, fmt.Errorf("line %d: %s service %q has no url", n.Line, t.Name, name)
	}
	a, err := address(addr)
	if err != nil {
		return "", nil, fmt.Errorf("%s service %q: %w", t.Name, name, err)
	}
	set(svc, "address", str(a))
	// Requests.
	var endpoints []string
	apiPath := ""
	if v := f["api_path"]; v != nil {
		apiPath = v.Value
	}
	if t.Name == "custom" {
		if p := f["paths"]; p != nil {
			for _, item := range p.Content {
				endpoints = append(endpoints, strings.Join(strings.Fields(item.Value), " "))
			}
		}
	} else {
		for _, e := range v03Endpoints[t.Name] {
			method, path, _ := strings.Cut(e, " ")
			endpoints = append(endpoints, method+" "+t.UpstreamPath(apiPath, path))
		}
	}
	if apiPath != "" {
		c.note("%s: api_path %s is now part of the paths (%s); point Eidonix at the new path", name, apiPath, strings.Join(endpoints, ", "))
	}
	if t.Name == "chat" {
		c.note("%s: %s allows every POST under %s (v0.2 allowed only chat/completions and responses)", name, endpoints[0], strings.TrimSuffix(strings.TrimPrefix(endpoints[0], "POST "), "/*"))
	}
	set(svc, "allowed-endpoints", flowList(endpoints))
	// Models.
	if v := f["models"]; v != nil {
		listed := ""
		for _, e := range endpoints {
			if strings.HasPrefix(e, "GET ") && strings.HasSuffix(e, "/models") {
				listed = e
			}
		}
		models, err := c.models(name, v, listed)
		if err != nil {
			return "", nil, err
		}
		if models != nil {
			set(svc, "models", models)
		}
	}
	// v0.2 probed /health on every service and counted any answer below 500.
	set(svc, "health", str(v02HealthPath))
	if v := f["api_key"]; v != nil {
		set(svc, "api-key", str(v.Value))
	}
	// Limits: the old type's defaults are written out where v0.3's differ.
	concurrent := t.MaxConcurrent
	if v := f["max_concurrent_requests"]; v != nil {
		fmt.Sscan(v.Value, &concurrent)
	}
	if concurrent != config.DefaultMaxConcurrentRequests {
		set(svc, "max-concurrent-requests", num(concurrent))
	}
	if v := f["max_requests_per_minute"]; v != nil && v.Value != "0" {
		set(svc, "max-requests-per-minute", str(v.Value))
		svc.Content[len(svc.Content)-1].Tag = "!!int"
	}
	idle := t.Idle
	if v := f["idle_timeout"]; v != nil {
		d, err := time.ParseDuration(v.Value)
		if err != nil {
			return "", nil, fmt.Errorf("%s service %q: idle_timeout %q", t.Name, name, v.Value)
		}
		idle = d
	}
	if idle != config.DefaultIdleTimeout {
		set(svc, "idle-timeout", str(config.ShortDuration(idle)))
	}
	return name, svc, nil
}

// models turns a v0.2 list or alias: server-name map into v0.3. It returns
// nil for models: any. listed is the service's model-list request, if any:
// v0.2 answered it itself, v0.3 passes it to the server.
func (c *v02converter) models(name string, v *yaml.Node, listed string) (*yaml.Node, error) {
	switch v.Kind {
	case yaml.ScalarNode:
		if v.Value == "any" {
			c.note("%s: models: any left out; the connector does not inspect this service's requests", name)
			return nil, nil
		}
		return nil, fmt.Errorf("line %d: %s: models %q", v.Line, name, v.Value)
	case yaml.SequenceNode:
		names := []string{}
		for _, item := range v.Content {
			names = append(names, item.Value)
		}
		return flowList(names), nil
	case yaml.MappingNode:
		renamed := false
		for i := 0; i+1 < len(v.Content); i += 2 {
			renamed = renamed || v.Content[i].Value != v.Content[i+1].Value
		}
		if !renamed {
			names := []string{}
			for i := 0; i+1 < len(v.Content); i += 2 {
				names = append(names, v.Content[i].Value)
			}
			return flowList(names), nil
		}
		out := &yaml.Node{Kind: yaml.MappingNode}
		seen := map[string]string{}
		for i := 0; i+1 < len(v.Content); i += 2 {
			alias, server := v.Content[i].Value, v.Content[i+1].Value
			if prior, dup := seen[server]; dup {
				c.note("%s: the name %s for %s is dropped; v0.3 gives each server model one name (kept %s)", name, alias, server, prior)
				continue
			}
			seen[server] = alias
			value := &yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle}
			if alias != server {
				set(value, "alias", str(alias))
			}
			set(out, server, value)
		}
		if listed != "" {
			c.note("%s: %s now reaches the server, which lists its own model names (not the aliases)", name, listed)
		}
		return out, nil
	}
	return nil, fmt.Errorf("line %d: %s: models", v.Line, name)
}

func (c *v02converter) storage(name string, f map[string]*yaml.Node) (*yaml.Node, error) {
	svc := &yaml.Node{Kind: yaml.MappingNode}
	set(svc, "kind", str("s3"))
	value := func(k string) string {
		if v := f[k]; v != nil {
			return v.Value
		}
		return ""
	}
	a, err := address(value("endpoint"))
	if err != nil {
		return nil, fmt.Errorf("storage service %q: %w", name, err)
	}
	set(svc, "address", str(a))
	for _, k := range []string{"bucket", "access_key", "secret_key", "region"} {
		if v := value(k); v != "" {
			set(svc, strings.ReplaceAll(k, "_", "-"), str(v))
		}
	}
	// v0.2 defaulted to sse-c; v0.3 defaults to none, so it is always written.
	encryption := value("encryption")
	if encryption == "" {
		encryption = "sse-c"
	}
	set(svc, "encryption", str(encryption))
	if encryption == "sse-kms" {
		c.note("%s: encryption sse-kms is not supported in v0.3; choose none, sse-s3 or sse-c", name)
	}
	if v := value("encryption_key"); v != "" {
		set(svc, "encryption-key", str(v))
	}
	if v := f["ca_certificate"]; v != nil {
		set(svc, "ca-certificate", literal(v.Value))
	}
	if v := value("max_file_size"); v != "" {
		set(svc, "max-file-size", str(v))
	}
	if v := value("max_concurrent_requests"); v != "" && v != fmt.Sprint(config.DefaultMaxConcurrentRequests) {
		set(svc, "max-concurrent-requests", str(v))
		svc.Content[len(svc.Content)-1].Tag = "!!int"
	}
	if value("kms_key_id") != "" {
		c.note("%s: kms_key_id not carried (sse-kms is gone)", name)
	}
	if value("max_read_per_hour") != "" {
		c.note("%s: max_read_per_hour not carried (gone in v0.3)", name)
	}
	if prefix := value("prefix"); prefix != "" {
		c.note("%s: v0.3 has no storage folder; Eidonix uses the whole bucket %s. The files are under %s: with the connector stopped, move them to the top of the bucket with: eidonix-connector storage move-to-root %s --from %s", name, value("bucket"), prefix, name, prefix)
	}
	return svc, nil
}
