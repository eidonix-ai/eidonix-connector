package hub

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/client"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

// End-to-end: the real connector (internal/connector + internal/session)
// against the real hub, with fake HTTP, SSE, WebSocket and S3 services.

const siteA = "  site-a:\n    token: " + tokenA + "\n"

// echo answers "<METHOD> <path> <body>" and records the headers it saw.
type echo struct {
	mu   sync.Mutex
	seen []http.Header
}

func (e *echo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	e.mu.Lock()
	e.seen = append(e.seen, r.Header.Clone())
	e.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Cache-Control", "no-cache")
	fmt.Fprintf(w, "%s %s %s", r.Method, r.URL.Path, b)
}

func (e *echo) last() http.Header {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seen[len(e.seen)-1]
}

func TestE2EHTTPLabelsAndHeaders(t *testing.T) {
	up := &echo{}
	srv := httptest.NewServer(up)
	defer srv.Close()
	r := newRig(t, siteA)
	s := r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*, GET /v1/models]", ""), nil)
	r.waitService("site-a", "chat", true)

	h := http.Header{"Content-Type": {"application/json"}, HeaderApplication: {"app-one"}, HeaderFor: {"account 42"}, HeaderRequestID: {"req-1"},
		"Authorization": {"Bearer app-secret"}, "Cookie": {"c=1"}, "X-Eidonix-Org-Id": {"7"}, "Connection": {"X-Hop"}, "X-Hop": {"1"}}
	resp, body := r.do("POST", "/site-a/chat/v1/chat/completions", strings.NewReader(`{"x":1}`), h)
	if resp.StatusCode != 200 || string(body) != `POST /v1/chat/completions {"x":1}` {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if resp.Header.Get(HeaderRequestID) != "req-1" || resp.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("response headers %v", resp.Header)
	}
	seen := up.last()
	if seen.Get("Authorization") != "" || seen.Get("Cookie") != "" || seen.Get("X-Eidonix-Org-Id") != "" || seen.Get("X-Hop") != "" || seen.Get("X-Request-Id") != "req-1" {
		t.Fatalf("service saw %v", seen)
	}
	// The labels reached the connector in the stream header.
	hd := s.lastHeader()
	if hd.Application != "app-one" || hd.For != "account 42" || hd.RequestID != "req-1" || hd.Service != "chat" || hd.Kind != "http" || hd.OrgID != "" {
		t.Fatalf("stream header %+v", hd)
	}
	// Connectors that log the labels (v0.4) show them in their access log.
	waitFor(t, "connector access log", func() bool { return len(s.connectorAccess(t)) > 0 })
	for _, line := range s.connectorAccess(t) {
		if line["request_id"] != "req-1" {
			continue
		}
		if app, ok := line["application"]; ok && app != "app-one" {
			t.Fatalf("connector logged application %v", app)
		}
	}
	// An invalid request id or label is replaced / dropped, not forwarded.
	resp, _ = r.do("GET", "/site-a/chat/v1/models", nil, http.Header{HeaderRequestID: {"bad id!"}, HeaderApplication: {strings.Repeat("a", 65)}})
	if id := resp.Header.Get(HeaderRequestID); resp.StatusCode != 200 || id == "bad id!" || len(id) != 36 {
		t.Fatalf("%d request id %q", resp.StatusCode, id)
	}
	if hd := s.lastHeader(); hd.Application != "" || hd.RequestID != resp.Header.Get(HeaderRequestID) {
		t.Fatalf("%+v", hd)
	}
	// The hub's access log has one line per request with the labels.
	waitFor(t, "hub access log", func() bool { return len(r.accessEntries()) >= 2 })
	e := r.accessEntries()[0]
	if e.Application != "app-one" || e.For != "account 42" || e.Site != "site-a" || e.Service != "chat" || e.Kind != "http" || e.Method != "POST" ||
		e.Path != "/v1/chat/completions" || e.Status != 200 || e.BytesIn != 7 || e.BytesOut == 0 || e.RequestID != "req-1" {
		t.Fatalf("%+v", e)
	}
}

func TestE2ESSEArrivesBeforeTheEnd(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		fmt.Fprint(w, "data: last\n\n")
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", ""), nil)
	r.waitService("site-a", "chat", true)
	resp, err := http.Post(r.local+"/site-a/chat/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(resp.Header)
	}
	buf := make([]byte, 64)
	got := make(chan string, 1)
	go func() { n, _ := io.ReadAtLeast(resp.Body, buf, len("data: first\n\n")); got <- string(buf[:n]) }()
	select {
	case s := <-got:
		if s != "data: first\n\n" {
			t.Fatalf("%q", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the first event did not arrive before the response ended")
	}
	close(release)
	rest, _ := io.ReadAll(resp.Body)
	if string(rest) != "data: last\n\n" {
		t.Fatalf("%q", rest)
	}
}

func TestE2EClientCancelReachesService(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", ""), nil)
	r.waitService("site-a", "chat", true)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", r.local+"/site-a/chat/v1/chat/completions", strings.NewReader("{}"))
	go func() {
		if resp, err := http.DefaultClient.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request never reached the service")
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelling at the application did not cancel the service request")
	}
	// The slot is released.
	waitFor(t, "in_flight 0", func() bool { return r.hub().Catalog().Sites[0].Services[0].InFlight == 0 })
}

func TestE2EWebSocketEcho(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			typ, b, err := ws.ReadMessage()
			if err != nil || ws.WriteMessage(typ, append([]byte(r.URL.Path+":"), b...)) != nil {
				return
			}
		}
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	s := r.connect("site-a", tokenA, httpService("voice", srv.URL, "[WEBSOCKET /v1/stream]", ""), nil)
	r.waitService("site-a", "voice", true)
	ws, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(r.local, "http")+"/site-a/voice/v1/stream", http.Header{HeaderApplication: {"app-one"}})
	if err != nil {
		t.Fatal(err, resp)
	}
	defer ws.Close()
	for i, msg := range []string{"hello", strings.Repeat("x", 300<<10)} {
		typ := websocket.TextMessage
		if i == 1 {
			typ = websocket.BinaryMessage
		}
		if err := ws.WriteMessage(typ, []byte(msg)); err != nil {
			t.Fatal(err)
		}
		ws.SetReadDeadline(time.Now().Add(3 * time.Second))
		gt, b, err := ws.ReadMessage()
		if err != nil || gt != typ || string(b) != "/v1/stream:"+msg {
			t.Fatalf("message %d: %v %d %d bytes", i, err, gt, len(b))
		}
	}
	if hd := s.lastHeader(); hd.Kind != "websocket" || hd.Application != "app-one" {
		t.Fatalf("%+v", hd)
	}
	ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := ws.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("close: %v", err)
	}
	// A WebSocket to a path the service does not allow is refused in JSON.
	_, resp, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(r.local, "http")+"/site-a/voice/v1/other", nil)
	if err == nil || resp == nil || resp.StatusCode != 403 || resp.Header.Get(protocol.HeaderError) != "path_not_allowed" {
		t.Fatalf("%v %v", err, resp)
	}
}

func TestE2EStorage(t *testing.T) {
	_, files := s3Service(t, "files", "")
	r := newRig(t, siteA)
	s := r.connect("site-a", tokenA, files, nil)
	r.waitService("site-a", "files", true)
	ctx := client.WithFor(context.Background(), "user-7")
	st := r.client("app-one").Storage("site-a", "files")

	key := "reports/q3 100%.txt"
	etag, err := st.Put(ctx, key, strings.NewReader("hello world"), 11, "text/plain")
	if err != nil || etag == "" {
		t.Fatal(etag, err)
	}
	if hd := s.lastHeader(); hd.Kind != "storage" || hd.Op != "put" || hd.Application != "app-one" || hd.For != "user-7" {
		t.Fatalf("%+v", hd)
	}
	// Unknown size (chunked).
	if _, err := st.Put(ctx, "reports/chunked.bin", io.MultiReader(strings.NewReader("ab"), strings.NewReader("cd")), -1, ""); err != nil {
		t.Fatal(err)
	}
	body, info, err := st.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(b) != "hello world" || info.Size != 11 || info.ETag != etag || info.ContentType != "text/plain" || info.LastModified.IsZero() {
		t.Fatalf("%q %+v %v", b, info, err)
	}
	body, info, err = st.GetRange(ctx, key, 6, 10, etag)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(body)
	body.Close()
	if string(b) != "world" || !info.Partial || info.Start != 6 || info.End != 10 || info.Size != 11 {
		t.Fatalf("%q %+v", b, info)
	}
	body, _, err = st.GetRange(ctx, key, 6, -1, "")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(body)
	body.Close()
	if string(b) != "world" {
		t.Fatalf("open range %q", b)
	}
	if _, _, err := st.GetRange(ctx, key, 0, 1, "other-etag"); !client.IsCode(err, "precondition_failed") || err.(*client.Error).Status != 412 {
		t.Fatalf("if-match: %v", err)
	}
	hi, err := st.Head(ctx, key)
	if err != nil || hi.Size != 11 || hi.ETag != etag {
		t.Fatalf("%+v %v", hi, err)
	}
	if _, err := st.Head(ctx, "missing"); !client.IsCode(err, "not_found") {
		t.Fatalf("head missing: %v", err)
	}
	if _, _, err := st.Get(ctx, "missing"); !client.IsCode(err, "not_found") || err.(*client.Error).Status != 404 {
		t.Fatalf("get missing: %v", err)
	}
	if _, err := st.Copy(ctx, key, "archive/q3.txt"); err != nil {
		t.Fatal(err)
	}
	page, err := st.List(ctx, "", false, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 2 || page.Objects[0].Key != "archive/" || !page.Objects[0].Dir || page.Objects[1].Key != "reports/" || page.Truncated {
		t.Fatalf("%+v", page)
	}
	page, err = st.List(ctx, "reports/", true, "", 1)
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Key != "reports/chunked.bin" || page.Objects[0].Size != 4 || !page.Truncated || page.NextToken == "" {
		t.Fatalf("%+v %v", page, err)
	}
	page, err = st.List(ctx, "reports/", true, page.NextToken, 1)
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Key != key || page.Truncated {
		t.Fatalf("page 2 %+v %v", page, err)
	}
	if err := st.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Head(ctx, key); !client.IsCode(err, "not_found") {
		t.Fatalf("after delete: %v", err)
	}
	// Invalid keys never reach the site.
	for _, bad := range []string{"/site-a/files/a//b", "/site-a/files/..%2Fx", "/site-a/files/.eidonix-connector/x"} {
		resp, raw := r.do("GET", bad, nil, nil)
		if code := errorCode(t, resp, raw); resp.StatusCode != 400 || code != "invalid_key" {
			t.Errorf("%s: %d %s", bad, resp.StatusCode, code)
		}
	}
	resp, raw := r.do("PATCH", "/site-a/files/x", nil, nil)
	if code := errorCode(t, resp, raw); resp.StatusCode != 405 || code != "method_not_allowed" {
		t.Fatalf("%d %s", resp.StatusCode, code)
	}
	resp, raw = r.do("POST", "/site-a/files/x", nil, nil)
	if code := errorCode(t, resp, raw); resp.StatusCode != 400 || code != "invalid_request" {
		t.Fatalf("post without copy-from %d %s", resp.StatusCode, code)
	}
	// Storage requests log the operation, never the key.
	for _, e := range r.accessEntries() {
		if e.Kind == "storage" && (e.Path != "" || e.Op == "" && e.Error == "") {
			t.Fatalf("%+v", e)
		}
	}
}

func TestE2EStorageReadOnlyAndLimits(t *testing.T) {
	_, files := s3Service(t, "files", "    read-only: true\n    max-file-size: 10\n")
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, files, nil)
	r.waitService("site-a", "files", true)
	st := r.client("").Storage("site-a", "files")
	if _, err := st.Put(context.Background(), "x", strings.NewReader("abc"), 3, ""); !client.IsCode(err, "read_only") || err.(*client.Error).Status != 403 {
		t.Fatalf("read-only put: %v", err)
	}
	if err := st.Delete(context.Background(), "x"); !client.IsCode(err, "read_only") {
		t.Fatalf("read-only delete: %v", err)
	}
	cat, _ := r.client("").Site(context.Background(), "site-a")
	if f := cat.Services[0]; f.Storage == nil || !f.Storage.ReadOnly || f.Storage.MaxObjectBytes != 10 || f.Storage.Encryption != "sse-c" {
		t.Fatalf("%+v", f)
	}
}

func TestE2ECatalog(t *testing.T) {
	srv := httptest.NewServer(&echo{})
	defer srv.Close()
	_, files := s3Service(t, "files", "")
	r := newRig(t, siteA+"  site-b:\n    token: "+tokenB+"\n    disabled: true\n")
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*, GET /v1/models]", "    models: [model-a]\n    max-concurrent-requests: 32\n")+files, nil)
	r.waitService("site-a", "files", true)
	cat, err := r.client("").Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Sites) != 2 {
		t.Fatalf("%+v", cat)
	}
	a, b := cat.Sites[0], cat.Sites[1]
	if a.Name != "site-a" || !a.Online || len(a.Sessions) != 1 || a.Sessions[0].Version == "" || a.Sessions[0].RemoteIP != "127.0.0.1" || a.Sessions[0].ConnectedAt.IsZero() {
		t.Fatalf("%+v", a)
	}
	if b.Name != "site-b" || b.Online || !b.Disabled || len(b.Sessions) != 0 || len(b.Services) != 0 {
		t.Fatalf("%+v", b)
	}
	chat, fs := a.Services[0], a.Services[1]
	if chat.Name != "chat" || chat.Kind != "http" || !chat.Healthy || strings.Join(chat.Endpoints, ",") != "POST /v1/*,GET /v1/models" ||
		strings.Join(chat.Models, ",") != "model-a" || chat.MaxConcurrentRequests != 32 || chat.Storage != nil {
		t.Fatalf("%+v", chat)
	}
	if fs.Name != "files" || fs.Kind != "s3" || fs.Storage == nil || fs.Storage.Encryption != "sse-c" || fs.Storage.MaxKeyBytes != 1023 || fs.Storage.MaxObjectBytes != protocol.MaxStorageObjectBytes {
		t.Fatalf("%+v", fs)
	}
	if _, err := r.client("").Site(context.Background(), "site-z"); !client.IsCode(err, "unknown_site") {
		t.Fatal(err)
	}
	resp, _ := r.do("GET", "/healthz", nil, nil)
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
}

func TestE2EErrorCodes(t *testing.T) {
	block := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(block) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/block" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-block
		}
	}))
	defer srv.Close()
	_, files := s3Service(t, "files", "")
	r := newRig(t, siteA+`  site-b:
    token: `+tokenB+`
  site-c:
    token: `+tokenC+`
    applications: [app-one]
    services:
      files:
        applications: [app-two]
`)
	services := httpService("chat", srv.URL, "[POST /v1/*]", "    max-concurrent-requests: 1\n") +
		"  broken:\n    kind: s3\n    address: 127.0.0.1:1\n    bucket: files-bucket\n    access-key: a\n    secret-key: b\n" + files
	r.connect("site-a", tokenA, services, nil)
	r.connect("site-c", tokenC, services, nil)
	r.waitService("site-a", "files", true)
	r.waitService("site-c", "files", true)
	r.waitService("site-a", "broken", false)

	check := func(method, path, app string, wantStatus int, wantCode string) {
		t.Helper()
		h := http.Header{}
		if app != "" {
			h.Set(HeaderApplication, app)
		}
		resp, body := r.do(method, path, strings.NewReader("{}"), h)
		if code := errorCode(t, resp, body); resp.StatusCode != wantStatus || code != wantCode {
			t.Errorf("%s %s (%s): %d %s, want %d %s", method, path, app, resp.StatusCode, code, wantStatus, wantCode)
		}
	}
	check("POST", "/site-z/chat/v1/x", "", 404, "unknown_site")
	check("POST", "/Site-A/chat/v1/x", "", 404, "unknown_site")
	check("POST", "/site-a/nope/v1/x", "", 404, "unknown_service")
	check("POST", "/site-b/chat/v1/x", "", 503, "site_offline")
	check("GET", "/site-a/broken/x", "", 503, "service_unhealthy")
	check("POST", "/site-c/chat/v1/x", "", 403, "not_allowed")
	check("POST", "/site-c/chat/v1/x", "app-two", 403, "not_allowed")
	check("GET", "/site-c/files/x", "app-one", 403, "not_allowed")
	check("GET", "/site-c/files/x", "app-two", 404, "not_found")
	// Refused at the site: the connector's code, in the hub's JSON shape.
	check("GET", "/site-a/chat/admin", "", 403, "path_not_allowed")
	check("POST", "/site-a/chat/v1/%2e%2e/admin", "", 403, "path_not_allowed")
	check("POST", "/site-a/chat//v1/x", "", 403, "path_not_allowed")

	resp, _ := r.do("POST", "/site-c/chat/v1/ok", strings.NewReader("{}"), http.Header{HeaderApplication: {"app-one"}})
	if resp.StatusCode != 200 {
		t.Fatalf("listed application: %d", resp.StatusCode)
	}

	// busy: the service's max-concurrent-requests (1) is held.
	held := make(chan struct{})
	go func() {
		defer close(held)
		resp, err := http.Post(r.local+"/site-a/chat/v1/block", "application/json", strings.NewReader("{}"))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	waitFor(t, "held request", func() bool {
		for _, svc := range r.hub().Catalog().Sites[0].Services {
			if svc.Name == "chat" && svc.InFlight == 1 {
				return true
			}
		}
		return false
	})
	resp, body := r.do("POST", "/site-a/chat/v1/x", strings.NewReader("{}"), nil)
	if code := errorCode(t, resp, body); resp.StatusCode != 503 || code != "busy" || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("busy: %d %s %v", resp.StatusCode, code, resp.Header)
	}
	once.Do(func() { close(block) })
	<-held
}

func TestE2EDraining(t *testing.T) {
	up := &echo{}
	srv := httptest.NewServer(up)
	defer srv.Close()
	r := newRig(t, siteA)
	real := r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", ""), nil)
	r.waitService("site-a", "chat", true)
	var fakeHits atomic.Int64
	f, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA,
		services: []protocol.Service{{Name: "chat", Kind: "http", MaxConcurrentRequests: 4, Healthy: true}},
		serve: httpServe(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fakeHits.Add(1)
			io.WriteString(w, "fake")
		}))})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "two sessions", func() bool { return r.sessions("site-a") == 2 })
	// Load is spread across both sessions.
	for i := 0; i < 20 && fakeHits.Load() == 0; i++ {
		r.do("POST", "/site-a/chat/v1/x", strings.NewReader("{}"), nil)
	}
	if fakeHits.Load() == 0 {
		t.Fatal("the second session never got a request")
	}
	if err := f.drain(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "draining in the catalog", func() bool {
		for _, s := range r.hub().Catalog().Sites[0].Sessions {
			if s.Draining {
				return true
			}
		}
		return false
	})
	before := fakeHits.Load()
	for i := 0; i < 10; i++ {
		resp, body := r.do("POST", "/site-a/chat/v1/x", strings.NewReader("{}"), nil)
		if resp.StatusCode != 200 || !strings.HasPrefix(string(body), "POST /v1/x") {
			t.Fatalf("%d %q", resp.StatusCode, body)
		}
	}
	if fakeHits.Load() != before {
		t.Fatal("a draining session got a new request")
	}
	// With only the draining session left, the site is offline for new
	// requests, but the session stays in the catalog until it closes.
	real.stop()
	waitFor(t, "one session", func() bool { return r.sessions("site-a") == 1 })
	resp, body := r.do("POST", "/site-a/chat/v1/x", strings.NewReader("{}"), nil)
	if code := errorCode(t, resp, body); resp.StatusCode != 503 || code != "site_offline" {
		t.Fatalf("%d %s", resp.StatusCode, code)
	}
	cs := r.hub().Catalog().Sites[0]
	if cs.Online || len(cs.Sessions) != 1 || !cs.Sessions[0].Draining || cs.Services[0].Healthy {
		t.Fatalf("%+v", cs)
	}
	f.mux.Close()
	waitFor(t, "no sessions", func() bool { return r.sessions("site-a") == 0 })
}

func TestE2EReload(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/hold" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			started <- struct{}{}
			<-r.Context().Done()
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	chat := httpService("chat", srv.URL, "[POST /v1/*]", "")
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, chat, nil)
	r.waitService("site-a", "chat", true)

	// Adding a site lets its connector in.
	resp, body := r.do("POST", "/site-b/chat/v1/x", nil, nil)
	if code := errorCode(t, resp, body); code != "unknown_site" {
		t.Fatal(code)
	}
	r.write(siteA + "  site-b:\n    token: " + tokenB + "\n")
	waitFor(t, "site-b in the file", func() bool { return r.hub().config().Sites["site-b"] != nil })
	r.connect("site-b", tokenB, chat, nil)
	r.waitService("site-b", "chat", true)

	// A bad file is refused and the last good one stays in force.
	r.write("  site-a:\n    tokn: x\n")
	waitFor(t, "reload error logged", func() bool { return strings.Contains(r.logBuf.String(), `unknown setting \"tokn\"`) })
	if resp, _ := r.do("POST", "/site-b/chat/v1/x", nil, nil); resp.StatusCode != 200 {
		t.Fatal("a bad file changed the running configuration", resp.StatusCode)
	}

	// Disabling a site ends its session at once, in-flight streams included.
	done := make(chan error, 1)
	go func() {
		resp, err := http.Post(r.local+"/site-b/chat/v1/hold", "application/json", strings.NewReader("{}"))
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		done <- err
	}()
	<-started
	r.write(siteA + "  site-b:\n    token: " + tokenB + "\n    disabled: true\n")
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the in-flight response ended cleanly")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disabling the site did not end its in-flight request")
	}
	waitFor(t, "site-b offline", func() bool { return r.sessions("site-b") == 0 })
	resp, body = r.do("POST", "/site-b/chat/v1/x", nil, nil)
	if code := errorCode(t, resp, body); code != "site_offline" {
		t.Fatal(code)
	}
	// The connector keeps retrying, and is refused while disabled.
	time.Sleep(1500 * time.Millisecond)
	if r.sessions("site-b") != 0 {
		t.Fatal("a disabled site reconnected")
	}

	// Rotating site-a's token away (next-token only) ends its session too.
	r.write("  site-a:\n    token: " + tokenC + "\n")
	waitFor(t, "site-a closed", func() bool { return r.sessions("site-a") == 0 })
	if !strings.Contains(r.logBuf.String(), "token rotated") {
		t.Fatal(r.logBuf.String())
	}
}

func TestE2EIdentityPinned(t *testing.T) {
	r := newRig(t, siteA)
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	r.connect("site-a", tokenA, "", other)
	time.Sleep(1500 * time.Millisecond)
	if n := r.hub().authAttempts.Load(); n != 0 {
		t.Fatalf("a connector pinned to another key sent its token %d times", n)
	}
	if r.sessions("site-a") != 0 {
		t.Fatal("session up")
	}
	// The same connector pinned to this hub's key connects.
	r.connect("site-a", tokenA, "", nil)
	waitFor(t, "session", func() bool { return r.sessions("site-a") == 1 })
}

func TestE2ESiteMismatch(t *testing.T) {
	r := newRig(t, siteA+"  site-b:\n    token: "+tokenB+"\n")
	// site-b's token in a file naming site-a.
	_, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenB})
	if err == nil || !strings.Contains(err.Error(), "site_mismatch") {
		t.Fatal(err)
	}
	// The real connector reports the same refusal.
	s := r.connect("site-a", tokenB, "", nil)
	time.Sleep(300 * time.Millisecond)
	if r.sessions("site-a")+r.sessions("site-b") != 0 {
		t.Fatal("mismatched session registered")
	}
	s.stop()
}

// ---- adversarial (ported from the development gateway's suite) -----------

func TestAdversarialPathsNeverContactUpstream(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/chat/completions, GET /v1/models]", ""), nil)
	r.waitService("site-a", "chat", true)
	for _, path := range []string{"/admin", "/v1/../admin", "/v1//chat/completions", "/v1/%2e%2e/admin", "/v1%2fchat/completions", "/v1/chat/completions?url=http://evil", "/" + strings.Repeat("a", 257)} {
		resp, body := r.do("POST", "/site-a/chat"+path, strings.NewReader("{}"), http.Header{"Content-Type": {"application/json"}})
		if code := errorCode(t, resp, body); resp.StatusCode != 403 || code != "path_not_allowed" {
			t.Errorf("%s: %d %s", path, resp.StatusCode, code)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("denied requests contacted the service %d times", hits.Load())
	}
}

func TestAdversarialRedirectNotFollowed(t *testing.T) {
	var redirected atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", other.URL)
		w.WriteHeader(302)
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", ""), nil)
	r.waitService("site-a", "chat", true)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Post(r.local+"/site-a/chat/v1/x", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// The connector strips Location, and neither end follows it.
	if resp.StatusCode != 302 || resp.Header.Get("Location") != "" || redirected.Load() != 0 {
		t.Fatalf("%d %v %d", resp.StatusCode, resp.Header, redirected.Load())
	}
}

func TestAdversarialActiveSSEHasNoTotalCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 12; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", "    idle-timeout: 120ms\n"), nil)
	r.waitService("site-a", "chat", true)
	resp, b := r.do("POST", "/site-a/chat/v1/x", strings.NewReader("{}"), nil)
	if resp.StatusCode != 200 || !strings.Contains(string(b), "data: 11") {
		t.Fatalf("stream cut: %d %q", resp.StatusCode, b)
	}
}

func TestAdversarialSilentResponseHitsIdle(t *testing.T) {
	stopped := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		close(stopped)
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", "    idle-timeout: 150ms\n"), nil)
	r.waitService("site-a", "chat", true)
	start := time.Now()
	resp, body := r.do("POST", "/site-a/chat/v1/x", strings.NewReader("{}"), nil)
	if code := errorCode(t, resp, body); resp.StatusCode != 504 || code != "upstream_timeout" {
		t.Fatalf("%d %s", resp.StatusCode, code)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("idle timeout not enforced")
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("idle did not cancel the service request")
	}
}

func TestAdversarialLargeUploadStreams(t *testing.T) {
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		got.Store(n)
		fmt.Fprint(w, n)
	}))
	defer srv.Close()
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("ocr", srv.URL, "[POST /layout-parsing]", ""), nil)
	r.waitService("site-a", "ocr", true)
	payload := bytes.Repeat([]byte("x"), 20<<20)
	// Chunked (unknown length) and with Content-Length.
	for _, body := range []io.Reader{io.MultiReader(bytes.NewReader(payload)), bytes.NewReader(payload)} {
		resp, b := r.do("POST", "/site-a/ocr/layout-parsing", body, http.Header{"Content-Type": {"application/pdf"}})
		if resp.StatusCode != 200 || string(b) != fmt.Sprint(len(payload)) {
			t.Fatalf("%d %q", resp.StatusCode, b)
		}
	}
}

// A session that is draining but not yet marked so (its draining message
// is in flight) refuses new streams with busy without contacting the
// service; the hub sends the request once more to another session.
func TestE2EBusyFromDrainingSessionIsRetried(t *testing.T) {
	ec := &echo{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			if ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil); err == nil {
				ws.ReadMessage()
				ws.Close()
			}
			return
		}
		ec.ServeHTTP(w, r)
	}))
	defer srv.Close()
	_, files := s3Service(t, "files", "")
	r := newRig(t, siteA)
	r.connect("site-a", tokenA, httpService("chat", srv.URL, "[POST /v1/*]", "")+httpService("voice", srv.URL, "[WEBSOCKET /v1/stream]", "")+files, nil)
	r.waitService("site-a", "files", true)
	var refused atomic.Int64
	_, err := r.fakeConnect(fakeOptions{site: "site-a", token: tokenA,
		services: []protocol.Service{{Name: "chat", Kind: "http", Healthy: true, MaxConcurrentRequests: 8},
			{Name: "voice", Kind: "http", Healthy: true, MaxConcurrentRequests: 8},
			{Name: "files", Kind: "s3", Healthy: true, MaxConcurrentRequests: 8, Storage: &protocol.StorageService{Encryption: "sse-c", MaxKeyBytes: 1023, MaxObjectBytes: protocol.MaxStorageObjectBytes}}},
		serve: func(c net.Conn) {
			defer c.Close()
			refused.Add(1)
			h, err := protocol.ReadStreamHeader(bufio.NewReader(c))
			if err != nil {
				return
			}
			if h.Kind == "storage" {
				protocol.WriteStorageResult(c, protocol.StorageErrorResult("busy"), h.Op)
				return
			}
			fmt.Fprintf(c, "HTTP/1.1 429 Too Many Requests\r\n%s: busy\r\nContent-Length: 5\r\nConnection: close\r\n\r\nbusy\n", protocol.HeaderError)
		}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "two sessions", func() bool { return r.sessions("site-a") == 2 })
	payload := bytes.Repeat([]byte("p"), 300<<10)
	st := r.client("app-one").Storage("site-a", "files")
	for i := 0; refused.Load() < 6 && i < 60; i++ {
		resp, body := r.do("POST", "/site-a/chat/v1/x", bytes.NewReader(payload), nil)
		if resp.StatusCode != 200 || !bytes.Equal(body, append([]byte("POST /v1/x "), payload...)) {
			t.Fatalf("http %d: %d bytes", resp.StatusCode, len(body))
		}
		key := fmt.Sprintf("k%d", i)
		if _, err := st.Put(context.Background(), key, bytes.NewReader(payload), int64(len(payload)), ""); err != nil {
			t.Fatalf("put: %v", err)
		}
		rc, _, err := st.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, payload) {
			t.Fatal("get returned other bytes")
		}
		ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(r.local, "http")+"/site-a/voice/v1/stream", nil)
		if err != nil {
			t.Fatalf("websocket: %v", err)
		}
		ws.Close()
	}
	if refused.Load() < 6 {
		t.Fatalf("the draining session was tried only %d times", refused.Load())
	}
}
