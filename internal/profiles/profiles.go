// Package profiles defines the compiled, default-deny inference surface.
package profiles

import (
	"fmt"
	"strings"
)

type Pair = [2]string
type Profile struct {
	Paths        []Pair
	ContentTypes []string
}

var Table = map[string]Profile{
	"openai_chat":         {[]Pair{{"POST", "/v1/chat/completions"}, {"POST", "/v1/completions"}, {"GET", "/v1/models"}}, []string{"application/json"}},
	"openai_embeddings":   {[]Pair{{"POST", "/v1/embeddings"}, {"GET", "/v1/models"}}, []string{"application/json"}},
	"anthropic_messages":  {[]Pair{{"POST", "/v1/messages"}, {"POST", "/v1/messages/count_tokens"}, {"GET", "/v1/models"}}, []string{"application/json"}},
	"rerank":              {[]Pair{{"POST", "/v1/rerank"}, {"POST", "/rerank"}}, []string{"application/json"}},
	"audio_transcription": {[]Pair{{"POST", "/v1/audio/transcriptions"}, {"POST", "/v1/audio/translations"}}, []string{"multipart/form-data"}},
	"audio_speech":        {[]Pair{{"POST", "/v1/audio/speech"}}, []string{"application/json"}},
	"image":               {[]Pair{{"POST", "/v1/images/generations"}, {"POST", "/v1/images/edits"}}, []string{"application/json", "multipart/form-data"}},
	"document_ocr":        {[]Pair{{"POST", "/layout-parsing"}}, []string{"application/json"}},
	"realtime_ws":         {}, "http": {}, "document_conversion": {nil, []string{"multipart/form-data", "application/octet-stream", "application/pdf", "text/plain"}},
}

type Flavor struct {
	Profiles string
	Paths    []Pair
	Health   string
	Denied   []string
}

var Flavors = map[string]Flavor{
	"vllm":           {"openai_chat openai_embeddings rerank audio_transcription", []Pair{{"POST", "/v1/responses"}, {"POST", "/v1/score"}, {"POST", "/rerank"}, {"POST", "/v1/rerank"}}, "/health", strings.Fields("/v1/load_lora_adapter /v1/unload_lora_adapter /reset_prefix_cache /sleep /wake_up /collective_rpc /start_profile /stop_profile /metrics /scale_elastic_ep")},
	"sglang":         {"openai_chat openai_embeddings rerank", []Pair{{"POST", "/generate"}, {"POST", "/v1/rerank"}}, "/health", strings.Fields("/flush_cache /update_weights_from_disk /update_weights_from_tensor /update_weights_from_distributed /init_weights_update_group /start_profile /stop_profile /release_memory_occupation /resume_memory_occupation /configure_logging /abort_request /metrics")},
	"ollama":         {"openai_chat openai_embeddings", []Pair{{"POST", "/api/chat"}, {"POST", "/api/generate"}, {"POST", "/api/embed"}, {"POST", "/api/embeddings"}, {"GET", "/api/tags"}, {"POST", "/api/show"}}, "/api/version", strings.Fields("/api/pull /api/push /api/create /api/copy /api/delete /api/blobs* /api/ps")},
	"go-llm-proxy":   {"openai_chat anthropic_messages openai_embeddings", nil, "/health", nil},
	"llamacpp":       {"openai_chat openai_embeddings rerank", []Pair{{"POST", "/completion"}, {"POST", "/embedding"}, {"POST", "/rerank"}}, "/health", strings.Fields("/slots* /lora-adapters /props /metrics")},
	"tei":            {"openai_embeddings rerank", []Pair{{"POST", "/embed"}, {"POST", "/rerank"}}, "/health", []string{"/metrics"}},
	"infinity":       {"openai_embeddings rerank", []Pair{{"POST", "/embeddings"}, {"POST", "/rerank"}}, "/health", []string{"/metrics"}},
	"litellm":        {"openai_chat openai_embeddings rerank audio_transcription audio_speech image", []Pair{{"POST", "/v1/responses"}}, "/health/liveliness", strings.Fields("/key* /user* /team* /model* /config* /spend* /global* /sso* /ui*")},
	"openai":         {"audio_transcription audio_speech", nil, "/health", nil},
	"gotenberg":      {"document_conversion", []Pair{{"POST", "/forms/libreoffice/convert"}, {"POST", "/forms/pdfengines/convert"}}, "/health", strings.Fields("/forms/chromium* /forms/pdfengines/metadata/write /prometheus/metrics /debug*")},
	"tika":           {"document_conversion", []Pair{{"PUT", "/tika"}, {"PUT", "/rmeta/text"}, {"PUT", "/detect/stream"}, {"PUT", "/meta"}}, "/", strings.Fields("/tika/form /config /version")},
	"unoserver":      {"document_conversion", nil, "/health", nil},
	"cletus-convert": {"document_conversion", []Pair{{"POST", "/convert"}, {"POST", "/extract"}}, "/health", nil},
}

func ValidatePath(p string) error {
	if len(p) == 0 || len(p) > 256 || p[0] != '/' || strings.ContainsAny(p, "%\\\x00\r\n?#") || strings.Contains(p, "//") {
		return fmt.Errorf("unsafe path %q", p)
	}
	for _, s := range strings.Split(p, "/") {
		if s == "." || s == ".." {
			return fmt.Errorf("unsafe path %q", p)
		}
	}
	return nil
}
func Effective(kind, flavor string, explicit, extra []Pair) ([]Pair, error) {
	p, ok := Table[kind]
	if !ok {
		return nil, fmt.Errorf("unknown profile %q", kind)
	}
	paths := append([]Pair{}, p.Paths...)
	paths = append(paths, explicit...)
	var f Flavor
	if flavor != "" {
		var found bool
		f, found = Flavors[flavor]
		if !found || !strings.Contains(" "+f.Profiles+" ", " "+kind+" ") {
			return nil, fmt.Errorf("incompatible flavor %q", flavor)
		}
		paths = append(paths, f.Paths...)
	}
	paths = append(paths, extra...)
	seen := map[Pair]bool{}
	result := []Pair{}
	for _, v := range paths {
		if err := ValidatePath(v[1]); err != nil {
			return nil, err
		}
		if v[0] != "GET" && v[0] != "POST" && v[0] != "PUT" {
			return nil, fmt.Errorf("unsupported method")
		}
		for _, deny := range f.Denied {
			if v[1] == deny || strings.HasSuffix(deny, "*") && strings.HasPrefix(v[1], strings.TrimSuffix(deny, "*")) {
				return nil, fmt.Errorf("flavor denies %s", v[1])
			}
		}
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	if kind == "document_conversion" && flavor == "" {
		return nil, fmt.Errorf("conversion needs flavor")
	}
	if kind == "http" && len(result) == 0 {
		return nil, fmt.Errorf("http needs explicit paths")
	}
	return result, nil
}
