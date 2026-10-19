package hub

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/client"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/connector"
	csession "github.com/eidonix-ai/eidonix-connector/internal/session"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// ---- the hub ---------------------------------------------------------------

// rig is a running hub (serve, with its file watcher) plus a TLS front for
// its session endpoint, as the edge proxy provides in production.
type rig struct {
	t      *testing.T
	path   string
	key    ed25519.PrivateKey
	srv    *Server
	front  *httptest.Server // TLS → hub session endpoint
	local  string           // http://127.0.0.1:port
	access string           // access log path
	logBuf *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// hubFile renders a hub file for key with sites (YAML under sites:).
func hubFile(key ed25519.PrivateKey, access, sites string) string {
	return "public: 127.0.0.1:0\nlocal: 127.0.0.1:0\nserver-key: " + base64.StdEncoding.EncodeToString(key.Seed()) +
		"\naccess-log: " + access + "\nsites:\n" + sites
}

func newRig(t *testing.T, sites string) *rig {
	t.Helper()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	dir := t.TempDir()
	r := &rig{t: t, path: filepath.Join(dir, "connect.yaml"), key: key, access: filepath.Join(dir, "access.log"), logBuf: &syncBuffer{}}
	r.write(sites)
	r.srv = &Server{Path: r.path, ReloadInterval: 25 * time.Millisecond, Log: slog.New(slog.NewTextHandler(r.logBuf, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.srv.Serve(ctx) }()
	select {
	case <-r.srv.Ready():
	case err := <-done:
		t.Fatal(err)
	}
	r.front = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { r.srv.Hub().ServeSession(w, req) }))
	r.local = "http://" + r.srv.LocalAddr.String()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("hub did not stop")
		}
		r.front.Close()
	})
	return r
}

func (r *rig) hub() *Hub { return r.srv.Hub() }

// resetLimits empties the upgrade rate limiter.
func (r *rig) resetLimits() {
	l := r.hub().limiter
	l.mu.Lock()
	l.buckets = map[string]*bucket{}
	l.mu.Unlock()
}

// write replaces the file (keeping its mode).
func (r *rig) write(sites string) {
	r.t.Helper()
	if err := os.WriteFile(r.path, []byte(hubFile(r.key, r.access, sites)), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) client(app string) *client.Client { return client.New(r.local, app) }

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitService waits until site offers service with the given health.
func (r *rig) waitService(site, service string, healthy bool) {
	r.t.Helper()
	waitFor(r.t, site+"/"+service, func() bool {
		for _, s := range r.hub().Catalog().Sites {
			if s.Name != site || !s.Online {
				continue
			}
			for _, svc := range s.Services {
				if svc.Name == service && svc.Healthy == healthy {
					return true
				}
			}
		}
		return false
	})
}

func (r *rig) sessions(site string) int { return len(r.hub().sessionsOf(site)) }

// accessEntries reads the hub's access log.
func (r *rig) accessEntries() []AccessEntry {
	r.t.Helper()
	b, _ := os.ReadFile(r.access)
	var out []AccessEntry
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var e AccessEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			r.t.Fatalf("access log line %q: %v", line, err)
		}
		out = append(out, e)
	}
	return out
}

// do sends one request to the local port.
func (r *rig) do(method, path string, body io.Reader, header http.Header) (*http.Response, []byte) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, r.local+path, body)
	if err != nil {
		r.t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	return resp, b
}

// errorCode checks the hub's error shape and returns its code.
func errorCode(t *testing.T, resp *http.Response, body []byte) string {
	t.Helper()
	var e ErrorBody
	if err := json.Unmarshal(body, &e); err != nil || e.Error.Code == "" || e.Error.Message == "" {
		t.Fatalf("not an error body (%d): %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get(protocol.HeaderError); got != e.Error.Code {
		t.Fatalf("X-Eidonix-Error %q, body code %q", got, e.Error.Code)
	}
	return e.Error.Code
}

// ---- the real connector ----------------------------------------------------

// site is a real connector (internal/connector + internal/session) dialing
// the rig through its TLS front. Every stream header it receives is kept.
type site struct {
	conn     *connector.Connector
	audit    string
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
	mu       sync.Mutex
	headers  []protocol.StreamHeader
}

func (s *site) HelloServices() []protocol.Service        { return s.conn.HelloServices() }
func (s *site) StatusServices() []protocol.ServiceStatus { return s.conn.StatusServices() }

// ServeStream records the stream header, then hands the whole stream to
// the real connector.
func (s *site) ServeStream(ctx context.Context, c net.Conn) {
	br := bufio.NewReader(c)
	line, err := br.ReadSlice('\n')
	if err != nil {
		c.Close()
		return
	}
	line = append([]byte(nil), line...)
	var h protocol.StreamHeader
	if json.Unmarshal(line, &h) == nil {
		s.mu.Lock()
		s.headers = append(s.headers, h)
		s.mu.Unlock()
	}
	s.conn.ServeStream(ctx, &prefixConn{Conn: c, r: io.MultiReader(bytes.NewReader(line), br)})
}

func (s *site) lastHeader() protocol.StreamHeader {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.headers) == 0 {
		return protocol.StreamHeader{}
	}
	return s.headers[len(s.headers)-1]
}

func (s *site) stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		select {
		case <-s.done:
		case <-time.After(5 * time.Second):
		}
	})
}

type prefixConn struct {
	net.Conn
	r io.Reader
}

func (c *prefixConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// connectorFile is a connector configuration for name with services (YAML
// under services:).
func connectorFile(name, token string, pub ed25519.PublicKey, services string) string {
	return "site: " + name + "\nupstream:\n  url: https://127.0.0.1\n  token: " + token + "\n  public-keys: [" +
		base64.StdEncoding.EncodeToString(pub) + "]\nservices:\n" + services
}

// connect starts a real connector for name; pinned is the key it trusts
// (nil: the rig's).
func (r *rig) connect(name, token, services string, pinned ed25519.PublicKey) *site {
	r.t.Helper()
	if pinned == nil {
		pinned = r.key.Public().(ed25519.PublicKey)
	}
	cfg, err := config.Parse([]byte(connectorFile(name, token, pinned, services)))
	if err != nil {
		r.t.Fatal(err)
	}
	cfg.AccessLog = filepath.Join(r.t.TempDir(), "connector-access.log")
	c, err := connector.New(cfg)
	if err != nil {
		r.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &site{conn: c, audit: cfg.AccessLog, cancel: cancel, done: make(chan error, 1)}
	tlsConfig := r.front.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	addr := r.front.Listener.Addr().String()
	go func() {
		s.done <- csession.RunOptions(ctx, csession.Options{Server: "https://127.0.0.1", Token: token, Site: name, Version: "0.3.0-test", Keys: []ed25519.PublicKey{pinned}, TLSConfig: tlsConfig,
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}}, s)
	}()
	r.t.Cleanup(func() { s.stop(); c.Close() })
	return s
}

// connectorAccess reads the connector's access log lines as maps.
func (s *site) connectorAccess(t *testing.T) []map[string]any {
	t.Helper()
	b, _ := os.ReadFile(s.audit)
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

// ---- a scripted connector ----------------------------------------------------

// fake is a minimal protocol-level connector: it can send draining,
// misbehave, and serve streams with any function.
type fake struct {
	mux     *yamux.Session
	control net.Conn
	writeMu sync.Mutex
	ack     protocol.HelloAck
	closed  chan string // the hub's close reason
}

type fakeOptions struct {
	key      ed25519.PublicKey // pinned (default: the rig's)
	site     string
	token    string
	caps     []string
	services []protocol.Service
	serve    func(net.Conn) // nil: close every stream
	xff      string         // X-Forwarded-For on the upgrade
}

var errAuthFailed = errors.New("auth_failed")

func (r *rig) fakeConnect(o fakeOptions) (*fake, error) {
	if o.key == nil {
		o.key = r.key.Public().(ed25519.PublicKey)
	}
	if o.caps == nil {
		o.caps = []string{protocol.CapabilityInference, protocol.CapabilityStorage}
	}
	nonce, _ := protocol.NewNonce()
	instance, _ := protocol.NewInstanceID()
	d := websocket.Dialer{Subprotocols: []string{protocol.Subprotocol}}
	h := http.Header{protocol.HeaderNonce: {nonce}, protocol.HeaderInstance: {instance}}
	if o.xff != "" {
		h.Set("X-Forwarded-For", o.xff)
	}
	ws, resp, err := d.Dial("ws://"+r.srv.PublicAddr.String()+protocol.SessionPath, h)
	if err != nil {
		return nil, err
	}
	if err := protocol.VerifyIdentity(resp.Header, nonce, instance, []ed25519.PublicKey{o.key}, time.Now()); err != nil {
		ws.Close()
		return nil, fmt.Errorf("server identity: %w", err)
	}
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	cfg.MaxStreamWindowSize = 256 << 10
	mux, err := yamux.Client(protocol.NewWebSocketConn(ws), cfg)
	if err != nil {
		return nil, err
	}
	control, err := mux.OpenStream()
	if err != nil {
		mux.Close()
		return nil, err
	}
	f := &fake{mux: mux, control: control, closed: make(chan string, 1)}
	br := bufio.NewReader(control)
	fail := func(err error) (*fake, error) { mux.Close(); return nil, err }
	if err := protocol.WriteMessage(control, protocol.Auth{V: protocol.Version, Type: protocol.MessageAuth, Token: o.token}); err != nil {
		return fail(err)
	}
	env, err := protocol.ReadMessage(br)
	if err != nil {
		return fail(err)
	}
	var auth protocol.AuthResult
	env.Decode(&auth)
	if env.Type != protocol.MessageAuthOK {
		return fail(fmt.Errorf("%w: %s", errAuthFailed, auth.Reason))
	}
	if err := protocol.WriteMessage(control, protocol.Hello{V: protocol.Version, Type: protocol.MessageHello, Site: o.site, InstanceID: instance, Version: "test", Capabilities: o.caps, Services: o.services}); err != nil {
		return fail(err)
	}
	if env, err = protocol.ReadMessage(br); err != nil {
		return fail(err)
	}
	if env.Type != protocol.MessageHelloAck {
		var c protocol.Control
		env.Decode(&c)
		return fail(fmt.Errorf("closed: %s", c.Reason))
	}
	env.Decode(&f.ack)
	go func() {
		for {
			st, err := mux.AcceptStream()
			if err != nil {
				return
			}
			if o.serve == nil {
				st.Close()
				continue
			}
			go o.serve(st)
		}
	}()
	go func() {
		for {
			env, err := protocol.ReadMessage(br)
			if err != nil {
				return
			}
			switch env.Type {
			case protocol.MessagePing:
				f.send(protocol.Control{V: protocol.Version, Type: protocol.MessagePong})
			case protocol.MessageClose:
				var c protocol.Control
				env.Decode(&c)
				select {
				case f.closed <- c.Reason:
				default:
				}
			}
		}
	}()
	r.t.Cleanup(func() { mux.Close() })
	return f, nil
}

func (f *fake) send(m any) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	return protocol.WriteMessage(f.control, m)
}

func (f *fake) drain() error {
	return f.send(protocol.Control{V: protocol.Version, Type: protocol.MessageDraining})
}

// httpServe answers every http stream with handler, like a connector
// relaying to a service.
func httpServe(handler http.Handler) func(net.Conn) {
	return func(c net.Conn) {
		defer c.Close()
		br := bufio.NewReader(c)
		if _, err := protocol.ReadStreamHeader(br); err != nil {
			return
		}
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		resp := rec.Result()
		resp.Close = true
		resp.Write(c)
	}
}

// ---- fake services -------------------------------------------------------------

// fakeS3 is an in-memory S3 double: bucket probes, put, head, get (range,
// If-Match), delete and ListObjectsV2.
type fakeS3 struct {
	bucket  string
	mu      sync.Mutex
	objects map[string]s3object
}

type s3object struct {
	data        []byte
	contentType string
	etag        string
	modified    time.Time
}

func newFakeS3(bucket string) *fakeS3 { return &fakeS3{bucket: bucket, objects: map[string]s3object{}} }

func s3error(w http.ResponseWriter, status int, code string, body bool) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if body {
		fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/"+f.bucket)
	key := strings.TrimPrefix(path, "/")
	q := r.URL.Query()
	switch {
	case key == "" && r.Method == "HEAD":
		w.WriteHeader(200)
	case key == "" && r.Method == "GET" && q.Has("uploads"):
		io.WriteString(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
	case key == "" && r.Method == "GET" && q.Get("list-type") == "2":
		f.list(w, q)
	case key == "":
		s3error(w, 400, "InvalidRequest", true)
	case r.Method == "PUT":
		b, _ := io.ReadAll(r.Body)
		sum := md5.Sum(b)
		ct := r.Header.Get("Content-Type")
		f.objects[key] = s3object{data: b, contentType: ct, etag: hex.EncodeToString(sum[:]), modified: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
		w.Header().Set("ETag", `"`+f.objects[key].etag+`"`)
	case r.Method == "DELETE":
		delete(f.objects, key)
		w.WriteHeader(204)
	case r.Method == "HEAD" || r.Method == "GET":
		o, ok := f.objects[key]
		if !ok {
			s3error(w, 404, "NoSuchKey", r.Method == "GET")
			return
		}
		if m := r.Header.Get("If-Match"); m != "" && strings.Trim(m, `"`) != o.etag {
			s3error(w, 412, "PreconditionFailed", r.Method == "GET")
			return
		}
		w.Header().Set("ETag", `"`+o.etag+`"`)
		w.Header().Set("Content-Type", o.contentType)
		w.Header().Set("Last-Modified", o.modified.Format(http.TimeFormat))
		data, status := o.data, 200
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int64
			if _, err := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); err != nil || a > b || b >= int64(len(o.data)) {
				s3error(w, 416, "InvalidRange", true)
				return
			}
			data, status = o.data[a:b+1], 206
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(o.data)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		if r.Method == "GET" {
			w.Write(data)
		}
	default:
		s3error(w, 405, "MethodNotAllowed", true)
	}
}

func (f *fakeS3) list(w http.ResponseWriter, q map[string][]string) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	prefix, delim := get("prefix"), get("delimiter")
	start := get("start-after")
	if t := get("continuation-token"); t > start {
		start = t
	}
	max := 1000
	if m, err := strconv.Atoi(get("max-keys")); err == nil && m > 0 {
		max = m
	}
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	type content struct {
		Key          string
		LastModified string
		ETag         string
		Size         int
		StorageClass string
	}
	type cp struct{ Prefix string }
	var res struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Name                  string
		Prefix                string
		KeyCount              int
		MaxKeys               int
		IsTruncated           bool
		NextContinuationToken string `xml:",omitempty"`
		Contents              []content
		CommonPrefixes        []cp
	}
	res.Name, res.Prefix, res.MaxKeys = f.bucket, prefix, max
	seen := map[string]bool{}
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || k <= start {
			continue
		}
		entry := k
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				entry = k[:len(prefix)+i+1]
				if seen[entry] || entry <= start || strings.HasPrefix(start, entry) {
					continue
				}
			}
		}
		if res.KeyCount == max {
			res.IsTruncated = true
			break
		}
		res.KeyCount++
		res.NextContinuationToken = entry
		if entry != k {
			seen[entry] = true
			res.CommonPrefixes = append(res.CommonPrefixes, cp{entry})
			continue
		}
		o := f.objects[k]
		res.Contents = append(res.Contents, content{k, o.modified.Format("2006-01-02T15:04:05.000Z"), `"` + o.etag + `"`, len(o.data), "STANDARD"})
	}
	if !res.IsTruncated {
		res.NextContinuationToken = ""
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}

// s3Service starts a TLS fake S3 and returns it with the connector service
// YAML for it (kind s3, sse-c).
func s3Service(t *testing.T, name string, extra string) (*fakeS3, string) {
	t.Helper()
	fs := newFakeS3("files-bucket")
	srv := httptest.NewTLSServer(fs)
	t.Cleanup(srv.Close)
	cert := strings.TrimSpace(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})))
	return fs, fmt.Sprintf(`  %s:
    kind: s3
    address: %s
    bucket: files-bucket
    access-key: placeholder-access
    secret-key: placeholder-secret
    encryption: sse-c
    encryption-key: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
    ca-certificate: |
      %s
%s`, name, srv.URL, strings.ReplaceAll(cert, "\n", "\n      "), extra)
}

// httpService is connector YAML for an http service at a test server.
func httpService(name, url, endpoints, extra string) string {
	return fmt.Sprintf("  %s:\n    address: %s\n    allowed-endpoints: %s\n%s", name, strings.TrimPrefix(url, "http://"), endpoints, extra)
}
