// Package convert turns a v0.1 cletus-connector TOML configuration (and the
// secret files it references) into a v0.2 connector.yaml, listing anything it
// could not carry for the owner to review. It is the only package that reads
// TOML; the v0.1 type and flavor tables below are input-format references.
package convert

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/servicetype"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

// v0.1 /etc/cletus-connector/config.toml schema.
type v1Config struct {
	Server       string            `toml:"server"`
	TokenFile    string            `toml:"token_file"`
	AuditLog     string            `toml:"audit_log"`
	HealthListen string            `toml:"health_listen"`
	ServerKeys   map[string]string `toml:"server_keys"`
	TLS          struct {
		CA     string `toml:"ca"`
		CAFile string `toml:"ca_file"`
	} `toml:"tls"`
	Routes []v1Route `toml:"routes"`
}
type v1Route struct {
	ID                       string            `toml:"id"`
	Type                     string            `toml:"type"`
	Flavor                   string            `toml:"flavor"`
	Upstreams                []string          `toml:"upstreams"`
	Balance                  string            `toml:"balance"`
	Health                   v1Health          `toml:"health"`
	MaxConcurrency           int               `toml:"max_concurrency"`
	MaxRequestBytes          string            `toml:"max_request_bytes"`
	MaxResponseBytes         string            `toml:"max_response_bytes"`
	IdleTimeout              string            `toml:"idle_timeout"`
	RateLimit                float64           `toml:"rate_limit"`
	MaxWebsocketMessageBytes string            `toml:"max_websocket_message_bytes"`
	ContentTypes             []string          `toml:"content_types"`
	MethodsPaths             [][2]string       `toml:"methods_paths"`
	ExtraPaths               [][2]string       `toml:"extra_paths"`
	WebsocketPaths           []string          `toml:"websocket_paths"`
	AllowCIDRs               []string          `toml:"allow_cidrs"`
	UpstreamHeaders          map[string]string `toml:"upstream_headers"`
	Models                   map[string]string `toml:"models"`

	Endpoint        string `toml:"endpoint"`
	Region          string `toml:"region"`
	Bucket          string `toml:"bucket"`
	Prefix          string `toml:"prefix"`
	PathStyle       bool   `toml:"path_style"`
	CredentialsFile string `toml:"credentials_file"`
	Encryption      struct {
		Mode     string `toml:"mode"`
		KeyFile  string `toml:"key_file"`
		KMSKeyID string `toml:"kms_key_id"`
	} `toml:"encryption"`
	TLSCAFile           string  `toml:"tls_ca_file"`
	AllowInsecureHTTP   bool    `toml:"allow_insecure_http"`
	AllowUnencrypted    bool    `toml:"allow_unencrypted"`
	MaxObjectBytes      string  `toml:"max_object_bytes"`
	PartSize            string  `toml:"part_size"`
	MaxOpsPerSecond     float64 `toml:"max_ops_per_second"`
	MaxReadBytesPerHour int64   `toml:"max_read_bytes_per_hour"`
	AbandonedUploadAge  string  `toml:"abandoned_upload_age"`
	MaxUploadLifetime   string  `toml:"max_upload_lifetime"`
	ReportLocation      bool    `toml:"report_location"`
}
type v1Health struct {
	Path     string `toml:"path"`
	Interval string `toml:"interval"`
}

type pair = [2]string

// v1Types maps v0.1 route types to v0.2 service types.
var v1Types = map[string]string{
	"openai_chat": protocol.TypeChat, "anthropic_messages": protocol.TypeAnthropic, "openai_embeddings": protocol.TypeEmbeddings,
	"rerank": protocol.TypeRerank, "audio_transcription": protocol.TypeTranscription, "audio_speech": protocol.TypeSpeech,
	"image": protocol.TypeImage, "document_ocr": protocol.TypeDocument, "document_conversion": protocol.TypeConversion,
	"realtime_ws": protocol.TypeRealtime, "object_storage": protocol.TypeStorage, "http": protocol.TypeCustom,
}

// v1Paths and v1Flavors are the v0.1 compiled request tables.
var v1Paths = map[string][]pair{
	"openai_chat":         {{"POST", "/v1/chat/completions"}, {"POST", "/v1/completions"}, {"GET", "/v1/models"}},
	"openai_embeddings":   {{"POST", "/v1/embeddings"}, {"GET", "/v1/models"}},
	"anthropic_messages":  {{"POST", "/v1/messages"}, {"POST", "/v1/messages/count_tokens"}, {"GET", "/v1/models"}},
	"rerank":              {{"POST", "/v1/rerank"}, {"POST", "/rerank"}},
	"audio_transcription": {{"POST", "/v1/audio/transcriptions"}, {"POST", "/v1/audio/translations"}},
	"audio_speech":        {{"POST", "/v1/audio/speech"}},
	"image":               {{"POST", "/v1/images/generations"}, {"POST", "/v1/images/edits"}},
	"document_ocr":        {{"POST", "/layout-parsing"}},
}
var v1Flavors = map[string][]pair{
	"vllm":           {{"POST", "/v1/responses"}, {"POST", "/v1/score"}, {"POST", "/rerank"}, {"POST", "/v1/rerank"}},
	"sglang":         {{"POST", "/generate"}, {"POST", "/v1/rerank"}},
	"ollama":         {{"POST", "/api/chat"}, {"POST", "/api/generate"}, {"POST", "/api/embed"}, {"POST", "/api/embeddings"}, {"GET", "/api/tags"}, {"POST", "/api/show"}},
	"go-llm-proxy":   nil,
	"llamacpp":       {{"POST", "/completion"}, {"POST", "/embedding"}, {"POST", "/rerank"}},
	"tei":            {{"POST", "/embed"}, {"POST", "/rerank"}},
	"infinity":       {{"POST", "/embeddings"}, {"POST", "/rerank"}},
	"litellm":        {{"POST", "/v1/responses"}},
	"openai":         nil,
	"gotenberg":      {{"POST", "/forms/libreoffice/convert"}, {"POST", "/forms/pdfengines/convert"}},
	"tika":           {{"PUT", "/tika"}, {"PUT", "/rmeta/text"}, {"PUT", "/detect/stream"}, {"PUT", "/meta"}},
	"unoserver":      nil,
	"cletus-convert": {{"POST", "/convert"}, {"POST", "/extract"}},
}

const v1DefaultAuditLog = "/var/log/cletus-connector/audit.jsonl"

// Convert reads the v0.1 file at path and the files it references (relative
// references resolve against the file's directory). It returns the v0.2 YAML
// and the review notes.
func Convert(path string) ([]byte, []string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var old v1Config
	md, err := toml.NewDecoder(bytes.NewReader(data)).Decode(&old)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	if u := md.Undecoded(); len(u) > 0 {
		keys := []string{}
		for _, k := range u {
			keys = append(keys, k.String())
		}
		return nil, nil, fmt.Errorf("%s: unknown v0.1 settings: %s", path, strings.Join(keys, ", "))
	}
	c := &converter{dir: filepath.Dir(path)}
	out, err := c.convert(old)
	if err != nil {
		return nil, nil, err
	}
	if _, err := config.Parse(out); err != nil {
		c.note("the converted file does not pass check yet: %v", err)
	}
	return out, c.notes, nil
}

type converter struct {
	dir   string
	notes []string
}

func (c *converter) note(format string, args ...any) {
	c.notes = append(c.notes, fmt.Sprintf(format, args...))
}

// read returns a referenced file's trimmed contents.
func (c *converter) read(ref, what string) (string, error) {
	if !filepath.IsAbs(ref) {
		ref = filepath.Join(c.dir, ref)
	}
	f, err := os.Open(ref)
	if err != nil {
		return "", fmt.Errorf("%s: %w", what, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return "", fmt.Errorf("%s: %w", what, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// expand resolves ${file:PATH} references in a v0.1 header value.
func (c *converter) expand(v, what string) (string, error) {
	for {
		start := strings.Index(v, "${file:")
		if start < 0 {
			return v, nil
		}
		end := strings.Index(v[start:], "}")
		if end < 0 {
			return "", fmt.Errorf("%s: unterminated ${file:…}", what)
		}
		secret, err := c.read(v[start+7:start+end], what)
		if err != nil {
			return "", err
		}
		v = v[:start] + secret + v[start+end+1:]
	}
}

// YAML node helpers.
func str(s string) *yaml.Node { return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s} }
func num(n int) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: fmt.Sprint(n)}
}
func boolean(b bool) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: fmt.Sprint(b)}
}
func literal(s string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: strings.TrimRight(s, "\n") + "\n", Style: yaml.LiteralStyle}
}
func flow(items []string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, s := range items {
		item := str(s)
		item.Style = yaml.DoubleQuotedStyle
		n.Content = append(n.Content, item)
	}
	return n
}
func block(items []string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.SequenceNode}
	for _, s := range items {
		n.Content = append(n.Content, str(s))
	}
	return n
}
func set(m *yaml.Node, key string, v *yaml.Node) { m.Content = append(m.Content, str(key), v) }

func (c *converter) convert(old v1Config) ([]byte, error) {
	root := &yaml.Node{Kind: yaml.MappingNode}
	up := &yaml.Node{Kind: yaml.MappingNode}
	set(up, "url", str(strings.TrimSuffix(old.Server, "/")))
	if old.TokenFile == "" {
		return nil, fmt.Errorf("v0.1 file has no token_file")
	}
	token, err := c.read(old.TokenFile, "token_file")
	if err != nil {
		return nil, err
	}
	set(up, "token", str(token))
	ids := make([]string, 0, len(old.ServerKeys))
	for id := range old.ServerKeys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	keys := []string{}
	for _, id := range ids {
		keys = append(keys, old.ServerKeys[id])
	}
	set(up, "public_keys", block(keys))
	if old.TLS.CA == "file" {
		pem, err := c.read(old.TLS.CAFile, "tls.ca_file")
		if err != nil {
			return nil, err
		}
		set(up, "ca_certificate", literal(pem))
	}
	set(root, "upstream", up)
	if old.HealthListen != "" {
		c.note("health_listen %s not carried: v0.2 has no local health listener (use `eidonix-connector check` and the access log)", old.HealthListen)
	}
	if old.AuditLog != "" && old.AuditLog != v1DefaultAuditLog {
		c.note("audit_log %s not carried: v0.2 logs to %s unless you set access_log", old.AuditLog, config.DefaultAccessLog)
	}
	c.note("the web proxy came from HTTPS_PROXY in v0.1; if this host needs one, set upstream.proxy_url")

	sections := map[string]*yaml.Node{}
	for _, r := range old.Routes {
		typ, ok := v1Types[r.Type]
		if !ok {
			return nil, fmt.Errorf("route %s: unknown v0.1 type %q", r.ID, r.Type)
		}
		var svc *yaml.Node
		if typ == protocol.TypeStorage {
			svc, err = c.storage(r)
		} else {
			svc, err = c.service(r, typ)
		}
		if err != nil {
			return nil, err
		}
		if sections[typ] == nil {
			sections[typ] = &yaml.Node{Kind: yaml.SequenceNode}
		}
		sections[typ].Content = append(sections[typ].Content, svc)
	}
	for _, typ := range protocol.ServiceTypes {
		if s := sections[typ]; s != nil {
			set(root, typ, s)
		}
	}
	doc := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}, HeadComment: "Converted from a v0.1 cletus-connector configuration by eidonix-connector convert.\nReview the notes it printed, then run: eidonix-connector check"}
	var b bytes.Buffer
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	enc.Close()
	return b.Bytes(), nil
}

func (c *converter) service(r v1Route, typ string) (*yaml.Node, error) {
	t, _ := servicetype.Lookup(typ)
	n := &yaml.Node{Kind: yaml.MappingNode}
	set(n, "name", str(r.ID))
	switch len(r.Upstreams) {
	case 0:
		return nil, fmt.Errorf("route %s has no upstreams", r.ID)
	case 1:
		set(n, "url", str(r.Upstreams[0]))
	default:
		set(n, "urls", flow(r.Upstreams))
	}
	apiPath := ""
	if (r.Flavor == "infinity" || r.Flavor == "tei") && (typ == protocol.TypeEmbeddings || typ == protocol.TypeRerank) {
		apiPath = "/"
		set(n, "api_path", str(apiPath))
		c.note("%s: flavor %s became api_path: / (requests to /v1/… go to /… on the server)", r.ID, r.Flavor)
	} else if r.Flavor != "" {
		c.note("%s: flavor %s not carried; the %s type accepts a fixed request list", r.ID, r.Flavor, typ)
	}
	// Every request v0.1 allowed, as the gateway sent it.
	allowed := append(append(append(append([]pair{}, v1Paths[r.Type]...), r.MethodsPaths...), v1Flavors[r.Flavor]...), r.ExtraPaths...)
	for _, p := range r.WebsocketPaths {
		allowed = append(allowed, pair{servicetype.WebSocket, p})
	}
	if typ == protocol.TypeCustom {
		paths := []string{}
		seen := map[pair]bool{}
		for _, p := range allowed {
			if !seen[p] {
				seen[p] = true
				paths = append(paths, p[0]+" "+p[1])
			}
		}
		set(n, "paths", flow(paths))
		c.note("%s: custom service; check that paths lists exactly what it should allow", r.ID)
	} else {
		reported := map[pair]bool{}
		for _, p := range allowed {
			if t.Allows(p[0], p[1]) || reported[p] || (apiPath != "" && mapsTo(t, apiPath, p)) {
				continue
			}
			reported[p] = true
			c.note("%s: not carried: %s %s", r.ID, p[0], p[1])
		}
	}
	if t.Models {
		if len(r.Models) == 0 {
			set(n, "models", str("any"))
			c.note("%s: models: any; list the models this service may use", r.ID)
		} else {
			aliases := make([]string, 0, len(r.Models))
			for a := range r.Models {
				aliases = append(aliases, a)
			}
			sort.Strings(aliases)
			m := &yaml.Node{Kind: yaml.MappingNode}
			for _, a := range aliases {
				set(m, a, str(r.Models[a]))
			}
			set(n, "models", m)
			c.note("%s: models now restrict as well as rename: only %s may be used", r.ID, strings.Join(aliases, ", "))
		}
	} else if len(r.Models) > 0 {
		c.note("%s: model aliases not carried: %s services have no models", r.ID, typ)
	}
	if r.MaxConcurrency != 0 {
		set(n, "max_concurrent_requests", num(r.MaxConcurrency))
	}
	if r.RateLimit > 0 {
		set(n, "max_requests_per_minute", num(int(math.Round(r.RateLimit*60))))
	}
	if r.IdleTimeout != "" {
		if _, err := time.ParseDuration(r.IdleTimeout); err != nil {
			return nil, fmt.Errorf("route %s: idle_timeout %q", r.ID, r.IdleTimeout)
		}
		set(n, "idle_timeout", str(r.IdleTimeout))
	}
	headers := make([]string, 0, len(r.UpstreamHeaders))
	for h := range r.UpstreamHeaders {
		headers = append(headers, h)
	}
	sort.Strings(headers)
	for _, h := range headers {
		v, err := c.expand(r.UpstreamHeaders[h], r.ID+" upstream_headers")
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(h, "Authorization") && strings.HasPrefix(v, "Bearer ") {
			set(n, "api_key", str(strings.TrimPrefix(v, "Bearer ")))
			continue
		}
		c.note("%s: upstream header %s not carried; v0.2 sends only api_key (as Authorization: Bearer)", r.ID, h)
	}
	c.removed(r.ID, []removedSetting{{"balance", r.Balance != ""}, {"health", r.Health != (v1Health{})}, {"content_types", len(r.ContentTypes) > 0}, {"allow_cidrs", len(r.AllowCIDRs) > 0}, {"max_request_bytes", r.MaxRequestBytes != ""}, {"max_response_bytes", r.MaxResponseBytes != ""}, {"max_websocket_message_bytes", r.MaxWebsocketMessageBytes != ""}})
	return n, nil
}

type removedSetting struct {
	name  string
	isSet bool
}

// removed notes v0.1 settings that v0.2 fixes internally.
func (c *converter) removed(id string, settings []removedSetting) {
	for _, s := range settings {
		if s.isSet {
			c.note("%s: %s not carried (fixed internally in v0.2)", id, s.name)
		}
	}
}

// mapsTo reports whether v0.1 request p is what a contract request becomes upstream under apiPath.
func mapsTo(t servicetype.Type, apiPath string, p pair) bool {
	for _, r := range t.Requests {
		if r.Method == p[0] && t.UpstreamPath(apiPath, r.Path) == p[1] {
			return true
		}
	}
	return false
}

func (c *converter) storage(r v1Route) (*yaml.Node, error) {
	n := &yaml.Node{Kind: yaml.MappingNode}
	set(n, "name", str(r.ID))
	set(n, "endpoint", str(r.Endpoint))
	set(n, "bucket", str(r.Bucket))
	set(n, "prefix", str(r.Prefix))
	if r.Region != "" {
		set(n, "region", str(r.Region))
	}
	creds, err := c.read(r.CredentialsFile, r.ID+" credentials_file")
	if err != nil {
		return nil, err
	}
	lines := strings.Split(creds, "\n")
	if len(lines) != 2 {
		return nil, fmt.Errorf("%s credentials_file: expected access key and secret on two lines", r.ID)
	}
	set(n, "access_key", str(strings.TrimSpace(lines[0])))
	set(n, "secret_key", str(strings.TrimSpace(lines[1])))
	mode := r.Encryption.Mode
	if mode == "" {
		mode = "none"
	}
	set(n, "encryption", str(mode))
	if mode == "none" {
		c.note("%s: encryption: none; files are stored without encryption", r.ID)
	}
	if r.Encryption.KeyFile != "" {
		key, err := c.read(r.Encryption.KeyFile, r.ID+" encryption.key_file")
		if err != nil {
			return nil, err
		}
		set(n, "encryption_key", str(key))
		c.note("%s: encryption_key copied into the file; keep your backup of it", r.ID)
	}
	if r.Encryption.KMSKeyID != "" {
		set(n, "kms_key_id", str(r.Encryption.KMSKeyID))
	}
	if r.AllowInsecureHTTP {
		set(n, "allow_http", boolean(true))
	}
	if r.MaxObjectBytes != "" {
		set(n, "max_file_size", str(r.MaxObjectBytes))
	}
	if r.MaxReadBytesPerHour > 0 {
		set(n, "max_read_per_hour", str(config.FormatSize(r.MaxReadBytesPerHour)))
	}
	if r.MaxConcurrency != 0 {
		set(n, "max_concurrent_requests", num(r.MaxConcurrency))
	}
	if r.TLSCAFile != "" {
		pem, err := c.read(r.TLSCAFile, r.ID+" tls_ca_file")
		if err != nil {
			return nil, err
		}
		set(n, "ca_certificate", literal(pem))
	}
	auto := !strings.HasSuffix(strings.ToLower(hostOf(r.Endpoint)), "amazonaws.com")
	if r.PathStyle != auto {
		c.note("%s: path_style %v differs from v0.2's automatic choice (%v for this endpoint)", r.ID, r.PathStyle, auto)
	}
	c.removed(r.ID, []removedSetting{{"part_size", r.PartSize != ""}, {"max_ops_per_second", r.MaxOpsPerSecond != 0}, {"abandoned_upload_age", r.AbandonedUploadAge != ""}, {"max_upload_lifetime", r.MaxUploadLifetime != ""}, {"report_location", r.ReportLocation}, {"idle_timeout", r.IdleTimeout != ""}, {"allow_cidrs", len(r.AllowCIDRs) > 0}})
	return n, nil
}
func hostOf(endpoint string) string {
	s := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	if i := strings.IndexAny(s, ":/"); i >= 0 {
		s = s[:i]
	}
	return s
}
