package servicetype

import (
	"testing"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func TestEveryProtocolTypeHasAContract(t *testing.T) {
	for _, name := range protocol.ServiceTypes {
		typ, ok := Lookup(name)
		if !ok || typ.MaxConcurrent < 1 || typ.Idle <= 0 {
			t.Fatalf("%s: %+v", name, typ)
		}
	}
	if len(All()) != len(protocol.ServiceTypes) {
		t.Fatal("table and protocol type list differ")
	}
}
func TestContractsAreExact(t *testing.T) {
	chat, _ := Lookup(protocol.TypeChat)
	for _, r := range []Request{{"POST", "/v1/chat/completions"}, {"POST", "/v1/responses"}, {"GET", "/v1/models"}} {
		if !chat.Allows(r.Method, r.Path) {
			t.Fatal(r)
		}
	}
	for _, r := range []Request{{"POST", "/v1/completions"}, {"POST", "/v1/score"}, {"GET", "/metrics"}, {"POST", "/v1/models"}, {"GET", "/v1/chat/completions"}} {
		if chat.Allows(r.Method, r.Path) {
			t.Fatalf("chat allows %s", r)
		}
	}
	rt, _ := Lookup(protocol.TypeRealtime)
	if !rt.Allows(WebSocket, "/v1/stream") || rt.Allows("GET", "/v1/stream") || !rt.Allows("GET", "/v1/config") {
		t.Fatal("realtime contract")
	}
	speech, _ := Lookup(protocol.TypeSpeech)
	if !speech.Allows("GET", "/v1/audio/voices") {
		t.Fatal("speech voices")
	}
}
func TestUpstreamPath(t *testing.T) {
	emb, _ := Lookup(protocol.TypeEmbeddings)
	doc, _ := Lookup(protocol.TypeDocument)
	for _, tc := range []struct {
		typ          Type
		api, in, out string
	}{{emb, "", "/v1/embeddings", "/v1/embeddings"}, {emb, "/", "/v1/embeddings", "/embeddings"}, {emb, "/api/v1", "/v1/models", "/api/v1/models"}, {doc, "/ocr", "/layout-parsing", "/ocr/layout-parsing"}, {doc, "/", "/layout-parsing", "/layout-parsing"}} {
		if got := tc.typ.UpstreamPath(tc.api, tc.in); got != tc.out {
			t.Fatalf("%s %q %q: %q", tc.typ.Name, tc.api, tc.in, got)
		}
	}
}
func TestUnsafePaths(t *testing.T) {
	for _, p := range []string{"", "x", "/a/../b", "/a//b", "/a%2fb", "/a?b", "/a#b", "/a\\b", "/./a"} {
		if ValidatePath(p) == nil {
			t.Fatalf("accepted %q", p)
		}
	}
}
func TestParseRequest(t *testing.T) {
	if r, e := ParseRequest("WEBSOCKET /v1/stream"); e != nil || r.Method != WebSocket {
		t.Fatal(r, e)
	}
	for _, s := range []string{"/x", "DELETE /x", "POST x", "POST /a/../b"} {
		if _, e := ParseRequest(s); e == nil {
			t.Fatal(s)
		}
	}
}
