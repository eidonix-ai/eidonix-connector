package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// Synthetic placeholder credentials used only by tests.
const upstreamYAML = `site: test-site
upstream:
  url: https://connect.example.com
  token: cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
  public-keys: [AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=]
services:
`

// testConfig parses services YAML (with %s replaced by origin as an address:
// host:port for http://, unchanged for https://) and points the access log at
// a temporary file.
func testConfig(t testing.TB, services, origin string) *config.Config {
	t.Helper()
	c, err := config.Parse([]byte(upstreamYAML + fmt.Sprintf(services, strings.TrimPrefix(origin, "http://"))))
	if err != nil {
		t.Fatal(err)
	}
	c.AccessLog = filepath.Join(t.TempDir(), "access.log")
	return c
}
// chatEndpoints is what Eidonix uses of an OpenAI-compatible chat server.
const chatEndpoints = "[POST /v1/chat/completions, POST /v1/responses, GET /v1/models]"

func chatConfig(t testing.TB, origin string) *config.Config {
	return testConfig(t, "  chat:\n    address: %s\n    allowed-endpoints: "+chatEndpoints+"\n", origin)
}
func newConnector(t testing.TB, c *config.Config) *Connector {
	t.Helper()
	x, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { x.Close() })
	return x
}

// exchange sends one raw HTTP request on a stream to service and reads the response.
func exchange(t testing.TB, x *Connector, service, request string) (*http.Response, []byte) {
	t.Helper()
	a, b := net.Pipe()
	defer b.Close()
	b.SetDeadline(time.Now().Add(3 * time.Second))
	done := make(chan struct{})
	go func() { defer close(done); x.ServeStream(context.Background(), a) }()
	go func() {
		protocol.WriteStreamHeader(b, protocol.StreamHeader{V: protocol.Version, Service: service, Kind: "http", RequestID: "req-1", OrgID: "7", OrgSlug: "example-org", UserKeyID: "42"})
		io.WriteString(b, request)
	}()
	resp, err := http.ReadResponse(bufio.NewReader(b), nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	b.Close()
	<-done
	return resp, data
}
func post(path, ct, body string) string {
	return fmt.Sprintf("POST %s HTTP/1.1\r\nHost: ignored\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s", path, ct, len(body), body)
}
func get(path string) string { return "GET " + path + " HTTP/1.1\r\nHost: ignored\r\n\r\n" }

func accessLines(t testing.TB, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		out = append(out, record)
	}
	return out
}
func lastAccess(t testing.TB, path string) map[string]any {
	t.Helper()
	lines := accessLines(t, path)
	if len(lines) == 0 {
		t.Fatal("no access log line")
	}
	return lines[len(lines)-1]
}
