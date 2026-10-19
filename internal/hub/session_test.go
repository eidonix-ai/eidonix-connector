package hub

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

// Session-level behaviour with a scripted connector (ported from the
// production gateway's session tests).

func dialRaw(r *rig, subprotocol string, h http.Header) (*http.Response, error) {
	nonce, _ := protocol.NewNonce()
	instance, _ := protocol.NewInstanceID()
	if h == nil {
		h = http.Header{}
	}
	h.Set(protocol.HeaderNonce, nonce)
	h.Set(protocol.HeaderInstance, instance)
	d := websocket.Dialer{Subprotocols: []string{subprotocol}}
	ws, resp, err := d.Dial("ws://"+r.srv.PublicAddr.String()+protocol.SessionPath, h)
	if ws != nil {
		ws.Close()
	}
	return resp, err
}

func TestSessionUpgradeRefusals(t *testing.T) {
	r := newRig(t, siteA)
	// A credential on the upgrade is refused unread.
	resp, err := dialRaw(r, protocol.Subprotocol, http.Header{"Authorization": {"Bearer " + tokenA}})
	if err == nil || resp == nil || resp.StatusCode != 400 {
		t.Fatalf("credential on upgrade: %v %v", err, resp)
	}
	// Older protocols are refused at the upgrade.
	for _, sub := range []string{"eidonix-connector.v2", "example-connector.v1"} {
		resp, err := dialRaw(r, sub, nil)
		if err == nil || resp == nil || resp.StatusCode != http.StatusUpgradeRequired {
			t.Fatalf("%s: %v %v", sub, err, resp)
		}
	}
	if n := r.hub().authAttempts.Load(); n != 0 {
		t.Fatalf("%d tokens read", n)
	}
	// Any other path on the public port is not found.
	resp2, err := http.Get("http://" + r.srv.PublicAddr.String() + "/v1/catalog")
	if err != nil || resp2.StatusCode != 404 {
		t.Fatalf("catalog on the public port: %v %v", err, resp2)
	}
	resp2.Body.Close()
}

func TestSessionAuthRefusals(t *testing.T) {
	r := newRig(t, siteA+"  site-b:\n    token: "+tokenB+"\n    disabled: true\n")
	for token, want := range map[string]string{
		"not-a-token": "invalid token",
		"cxn_dddddddddddddddd_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA": "invalid token",
		tokenB: "site disabled",
	} {
		_, err := r.fakeConnect(fakeOptions{site: "site-b", token: token})
		if !errors.Is(err, errAuthFailed) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", token, err)
		}
	}
	// A connector pinned to another key never sends its token.
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	before := r.hub().authAttempts.Load()
	if _, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA, key: other}); err == nil || !strings.Contains(err.Error(), "server identity") {
		t.Fatal(err)
	}
	if r.hub().authAttempts.Load() != before {
		t.Fatal("token sent to an unproven server")
	}
}

func TestSessionRateLimits(t *testing.T) {
	r := newRig(t, siteA)
	// Per IP before authentication.
	limited := false
	for i := 0; i < upgradeBurst+2; i++ {
		resp, _ := dialRaw(r, "nothing", http.Header{"X-Forwarded-For": {"203.0.113.9"}})
		if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			limited = true
		}
	}
	if !limited {
		t.Fatal("no per-IP limit")
	}
	// Per token after authentication, even from many addresses.
	var last error
	for i := 0; i < upgradeBurst+2; i++ {
		f, err := r.fakeConnectFrom(fmt.Sprintf("203.0.113.%d", 20+i), fakeOptions{site: "site-a", token: tokenA})
		if err == nil {
			f.mux.Close()
		}
		last = err
	}
	if last == nil || !strings.Contains(last.Error(), "too many connection attempts") {
		t.Fatalf("no per-token limit: %v", last)
	}
}

func TestSessionHelloAdmission(t *testing.T) {
	r := newRig(t, siteA)
	f, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA, caps: []string{protocol.CapabilityInference}, services: []protocol.Service{
		{Name: "chat", Kind: "http", MaxConcurrentRequests: 1000, Healthy: true, Models: []string{"m"}},
		{Name: "chat", Kind: "http"},
		{Name: "Bad Name", Kind: "http"},
		{Name: "odd", Kind: "ftp"},
		{Name: "files", Kind: "s3", Storage: &protocol.StorageService{Encryption: "none"}},
		{Name: "tiny", Kind: "http"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.ack.Accepted, ",") != "chat,tiny" || f.ack.Limits.MaxStreams != maxStreamsPerSession {
		t.Fatalf("%+v", f.ack)
	}
	reasons := map[string]string{}
	for _, rj := range f.ack.Rejected {
		reasons[rj.Name] = rj.Reason
	}
	if reasons["chat"] != "duplicate service name" || reasons["Bad Name"] != "invalid service name" || reasons["odd"] != "unknown_kind" || reasons["files"] != "storage_capability_missing" {
		t.Fatalf("%v", reasons)
	}
	// Status changes health and models; an empty list clears them.
	f.send(protocol.Status{V: protocol.Version, Type: protocol.MessageStatus, Services: []protocol.ServiceStatus{{Name: "chat", Healthy: false, Models: []string{}}}})
	waitFor(t, "status applied", func() bool {
		svc := r.hub().Catalog().Sites[0].Services[0]
		return !svc.Healthy && len(svc.Models) == 0
	})
	// Too many services end the session.
	many := make([]protocol.Service, maxServices+1)
	for i := range many {
		many[i] = protocol.Service{Name: fmt.Sprintf("s%d", i), Kind: "http"}
	}
	if _, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA, services: many}); err == nil {
		t.Fatal("hello with too many services accepted")
	}
}

func TestSessionConnectorOpenedStreamEndsSession(t *testing.T) {
	r := newRig(t, siteA)
	f, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA, services: []protocol.Service{{Name: "chat", Kind: "http", Healthy: true}}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "session", func() bool { return r.sessions("site-a") == 1 })
	if st, err := f.mux.OpenStream(); err == nil {
		st.Write([]byte("x"))
	}
	waitFor(t, "session closed", func() bool { return r.sessions("site-a") == 0 })
}

func TestSessionLimitPerSite(t *testing.T) {
	r := newRig(t, siteA)
	for i := 0; i < maxSessionsPerSite; i++ {
		r.resetLimits()
		if _, err := r.fakeConnectFrom(fmt.Sprintf("203.0.113.%d", i+1), fakeOptions{site: "site-a", token: tokenA}); err != nil {
			t.Fatal(i, err)
		}
	}
	waitFor(t, "sessions", func() bool { return r.sessions("site-a") == maxSessionsPerSite })
	r.resetLimits()
	f, err := r.fakeConnectFrom("203.0.113.99", fakeOptions{site: "site-a", token: tokenA})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-f.closed:
		if reason != "too many sessions for this site" {
			t.Fatal(reason)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("17th session not refused")
	}
}

// A connector's own refusal is relayed in the hub's error shape; its busy
// becomes the hub's busy.
func TestConnectorRefusalShape(t *testing.T) {
	r := newRig(t, siteA)
	_, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA, services: []protocol.Service{{Name: "chat", Kind: "http", Healthy: true, MaxConcurrentRequests: 4}},
		serve: func(c net.Conn) {
			defer c.Close()
			br := bufio.NewReader(c)
			protocol.ReadStreamHeader(br)
			req, err := http.ReadRequest(br)
			if err != nil {
				return
			}
			code := strings.TrimPrefix(req.URL.Path, "/")
			fmt.Fprintf(c, "HTTP/1.1 %d X\r\n%s: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s\n", protocol.HTTPStatus(code), protocol.HeaderError, code, len(code)+1, code)
		}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "session", func() bool { return r.sessions("site-a") == 1 })
	for code, want := range map[string]int{"busy": 503, "model_not_allowed": 403, "upstream_timeout": 504, "request_too_large": 413} {
		resp, body := r.do("POST", "/site-a/chat/"+code, strings.NewReader("{}"), nil)
		if got := errorCode(t, resp, body); got != code || resp.StatusCode != want {
			t.Errorf("%s: %d %s", code, resp.StatusCode, got)
		}
	}
}

func (r *rig) fakeConnectFrom(ip string, o fakeOptions) (*fake, error) {
	o.xff = ip
	return r.fakeConnect(o)
}
