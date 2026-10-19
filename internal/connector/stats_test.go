package connector

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

func TestRollingWindow(t *testing.T) {
	var r rolling
	now := time.Unix(1_800_000_000, 0)
	r.add(now.Add(-61*time.Minute), true) // outside the hour
	r.add(now.Add(-59*time.Minute), true)
	r.add(now, false)
	r.add(now, true)
	if req, errs := r.totals(now); req != 3 || errs != 2 {
		t.Fatalf("%d requests %d errors", req, errs)
	}
	if req, errs := r.totals(now.Add(2 * time.Minute)); req != 2 || errs != 1 {
		t.Fatalf("after two minutes: %d requests %d errors", req, errs)
	}
	if req, _ := r.totals(now.Add(2 * time.Hour)); req != 0 {
		t.Fatal("old requests still counted")
	}
}

// TestSnapshotCountsRequestsAndErrors: served and refused requests count per
// service; a reload's new connector inherits the totals.
func TestSnapshotCountsRequestsAndErrors(t *testing.T) {
	f := &fakeServer{}
	cfg := chatConfig(t, f.start(t).URL)
	x := newConnector(t, cfg)
	exchange(t, x, "chat", get("/v1/models"))
	exchange(t, x, "chat", get("/admin")) // refused: path_not_allowed
	snap := x.Snapshot()
	if len(snap) != 1 || snap[0].Name != "chat" || snap[0].Kind != "http" || !snap[0].Healthy || snap[0].MaxConcurrent != 16 || snap[0].InFlight != 0 || snap[0].Requests != 2 || snap[0].Errors != 1 {
		t.Fatalf("%+v", snap)
	}
	next := newConnector(t, chatConfig(t, f.start(t).URL))
	next.InheritStats(x)
	exchange(t, next, "chat", get("/v1/models"))
	if s := next.Snapshot()[0]; s.Requests != 3 || s.Errors != 1 {
		t.Fatalf("inherited %+v", s)
	}
	if s := x.Snapshot()[0]; s.Requests != 3 {
		t.Fatalf("old connector's totals diverged: %+v", s)
	}
}

func TestDrainBound(t *testing.T) {
	c := chatConfig(t, "http://127.0.0.1:1")
	if got := DrainBound(c); got != c.Services[0].IdleTimeout {
		t.Fatalf("%s", got)
	}
	c.Services[0].IdleTimeout = 2 * time.Hour
	if got := DrainBound(c); got != MaxDrain {
		t.Fatalf("%s", got)
	}
}

// TestRefuseStream: a refused stream gets the closed error code (HTTP or
// storage form), nothing reaches the service, and the access log records it.
func TestRefuseStream(t *testing.T) {
	cfg := chatConfig(t, "http://127.0.0.1:1")
	x := newConnector(t, cfg)
	refuse := func(h protocol.StreamHeader) []byte {
		a, b := net.Pipe()
		b.SetDeadline(time.Now().Add(3 * time.Second))
		go func() { protocol.WriteStreamHeader(b, h) }()
		done := make(chan struct{})
		go func() { defer close(done); x.RefuseStream(context.Background(), a, "busy") }()
		data, _ := io.ReadAll(b)
		<-done
		return data
	}
	h := protocol.StreamHeader{V: protocol.Version, Service: "chat", Kind: "http", RequestID: "r1", Application: "app-one", For: "acct-9"}
	resp, err := http.ReadResponse(bufio.NewReader(strings.NewReader(string(refuse(h)))), nil)
	if err != nil || resp.StatusCode != 429 || resp.Header.Get(protocol.HeaderError) != "busy" {
		t.Fatalf("%v %v", resp, err)
	}
	h.Kind, h.Op = "storage", "head"
	if out := string(refuse(h)); !strings.Contains(out, `"code":"busy"`) {
		t.Fatalf("storage refusal %q", out)
	}
	if out := refuse(protocol.StreamHeader{V: 9}); len(out) != 0 {
		t.Fatalf("malformed header answered: %q", out)
	}
	r := lastAccess(t, cfg.AccessLog)
	if r["error"] != "busy" || r["service"] != "chat" || r["application"] != "app-one" || r["for"] != "acct-9" || r["request_id"] != "r1" {
		t.Fatal(r)
	}
	if s := x.Snapshot()[0]; s.Requests != 0 {
		t.Fatalf("refusals counted against the service: %+v", s)
	}
}
