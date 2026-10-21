package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/status"
	"github.com/eidonix-ai/eidonix-connector/internal/testgateway"
)

// Synthetic all-zero credential used only by local tests.
const testToken = "cxn_abcdefghijklmnop_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// syncBuffer is a log sink the run loop and the test can both use.
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

// runRig is a test gateway, a fake service and a connector run loop.
type runRig struct {
	t        *testing.T
	g        *testgateway.Gateway
	config   string
	access   string
	status   string
	signals  chan os.Signal
	done     chan error
	log      *syncBuffer
	release  chan struct{} // lets a held stream finish
	holding  atomic.Int32  // streams currently held by the service
	exited   atomic.Bool   // run has returned and its result was taken
	yamlTmpl string
}

func (r *runRig) services(idle string) string {
	return fmt.Sprintf(r.yamlTmpl, idle)
}

// newRunRig starts the loop. The service answers GET /v1/models at once and
// POST /v1/chat/completions as an event stream that sends one event, waits
// for release (sending a keepalive comment every 100 ms so it never goes
// quiet), then sends a final event.
func newRunRig(t *testing.T, o runOptions, idle string) *runRig {
	t.Helper()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	g, err := testgateway.New(testToken, key)
	if err != nil {
		t.Fatal(err)
	}
	g.Site = "local-test"
	g.Application, g.For = "app-one", "acct-9"
	hub := httptest.NewTLSServer(g)
	t.Cleanup(hub.Close)
	r := &runRig{t: t, g: g, signals: make(chan os.Signal, 4), done: make(chan error, 1), log: &syncBuffer{}, release: make(chan struct{})}
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v1/models" {
			io.WriteString(w, `{"data":[]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		r.holding.Add(1)
		defer r.holding.Add(-1)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.release:
				io.WriteString(w, "data: two\n\n")
				return
			case <-req.Context().Done():
				return
			case <-tick.C:
				io.WriteString(w, ": keepalive\n\n")
				w.(http.Flusher).Flush()
			}
		}
	}))
	t.Cleanup(service.Close)
	dir := t.TempDir()
	r.config, r.access, r.status = filepath.Join(dir, "connector.yaml"), filepath.Join(dir, "access.log"), filepath.Join(dir, "run", "status.json")
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: hub.Certificate().Raw}))
	r.yamlTmpl = "site: local-test\nupstream:\n  url: " + hub.URL + "\n  token: " + testToken + "\n  public-keys: [" + base64.StdEncoding.EncodeToString(pub) + "]\n  ca-certificate: |\n" + indent(ca) +
		"access-log: " + r.access + "\nstatus-file: " + r.status + "\nservices:\n  chat:\n    address: " + strings.TrimPrefix(service.URL, "http://") + "\n    allowed-endpoints: [POST /v1/chat/completions, GET /v1/models]\n    idle-timeout: %s\n"
	r.write(idle)
	cfg, err := loadConfigForTest(r.config)
	if err != nil {
		t.Fatal(err)
	}
	o.signals = r.signals
	go func() { r.done <- run(r.config, cfg, r.log, o) }()
	t.Cleanup(func() {
		if !r.exited.Load() {
			r.signals <- syscall.SIGTERM
			r.signals <- syscall.SIGTERM
			select {
			case <-r.done:
			case <-time.After(10 * time.Second):
			}
		}
	})
	return r
}

func indent(s string) string {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("    " + l + "\n")
	}
	return b.String()
}
func (r *runRig) write(idle string) {
	if err := os.WriteFile(r.config, []byte(r.services(idle)), 0600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *runRig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s\nlog:\n%s", what, r.log.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func (r *runRig) sessions(live, draining int) func() bool {
	return func() bool { l, d := r.g.Sessions(); return l == live && d == draining }
}

// open starts a chat stream and returns the response once its first event is readable.
func (r *runRig) open() *http.Response {
	r.t.Helper()
	req, _ := http.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.g.RoundTrip(req, "chat")
	if err != nil {
		r.t.Fatal(err)
	}
	buf := make([]byte, len("data: one\n\n"))
	if _, err = io.ReadFull(resp.Body, buf); err != nil || string(buf) != "data: one\n\n" {
		r.t.Fatalf("first event: %q %v", buf, err)
	}
	return resp
}
func (r *runRig) models() (int, error) {
	req, _ := http.NewRequest("GET", "http://x/v1/models", nil)
	resp, err := r.g.RoundTrip(req, "chat")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}
func (r *runRig) sessionsInAccessLog() []string {
	data, _ := os.ReadFile(r.access)
	var ids []string
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var e map[string]any
		if json.Unmarshal(line, &e) == nil && e["path"] == "/v1/models" {
			ids = append(ids, fmt.Sprint(e["session"]))
		}
	}
	return ids
}
func (r *runRig) exit() error {
	r.t.Helper()
	select {
	case err := <-r.done:
		r.exited.Store(true)
		return err
	case <-time.After(15 * time.Second):
		r.t.Fatalf("run did not return\nlog:\n%s", r.log.String())
		return nil
	}
}

func fast() runOptions {
	return runOptions{drainTimeout: 30 * time.Second, establishTimeout: 10 * time.Second, statusInterval: 100 * time.Millisecond}
}

// TestReloadKeepsStreamAlive: a reload starts a new session, the old one
// drains (the hub is told), a stream running on it finishes whole, and new
// requests go to the new session.
func TestReloadKeepsStreamAlive(t *testing.T) {
	r := newRunRig(t, fast(), "30s")
	r.waitFor("first session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	if code, err := r.models(); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	first := r.sessionsInAccessLog()
	r.waitFor("access line", func() bool { return len(r.sessionsInAccessLog()) == 1 })
	first = r.sessionsInAccessLog()

	r.write("45s")
	r.signals <- syscall.SIGHUP
	r.waitFor("old session draining beside the new one", r.sessions(2, 1))

	// New requests go to the new session while the stream still runs.
	if code, err := r.models(); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	r.waitFor("second access line", func() bool { return len(r.sessionsInAccessLog()) == 2 })
	if ids := r.sessionsInAccessLog(); ids[0] != first[0] || ids[1] == ids[0] {
		t.Fatalf("both requests used one session: %v", ids)
	}
	if r.holding.Load() != 1 {
		t.Fatal("the stream was dropped by the reload")
	}
	// The status file lists the draining session with its running stream.
	r.waitFor("draining in status", func() bool {
		f, err := status.Read(r.status)
		return err == nil && f.Connected && len(f.Draining) == 1 && f.Draining[0].InFlight == 1
	})
	// Let the stream finish: it arrives whole, and the old session closes.
	close(r.release)
	rest, err := io.ReadAll(stream.Body)
	if err != nil || !strings.Contains(string(rest), "data: two\n\n") {
		t.Fatalf("stream cut: %q %v", rest, err)
	}
	r.waitFor("old session closed", r.sessions(1, 0))
	r.waitFor("drained log", func() bool { return strings.Contains(r.log.String(), "drained and closed") })
	r.signals <- syscall.SIGTERM
	if err := r.exit(); err != nil {
		t.Fatal(err)
	}
}

// TestReloadWithBadFileKeepsSession: a rejected file changes nothing.
func TestReloadWithBadFileKeepsSession(t *testing.T) {
	r := newRunRig(t, fast(), "30s")
	r.waitFor("session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	os.WriteFile(r.config, []byte("site: local-test\nbogus: 1\n"), 0600)
	r.signals <- syscall.SIGHUP
	r.waitFor("rejection logged", func() bool { return strings.Contains(r.log.String(), "reload rejected") })
	if l, d := r.g.Sessions(); l != 1 || d != 0 {
		t.Fatalf("sessions %d/%d", l, d)
	}
	close(r.release)
	rest, err := io.ReadAll(stream.Body)
	if err != nil || !strings.Contains(string(rest), "data: two") {
		t.Fatalf("stream cut: %q %v", rest, err)
	}
	if code, err := r.models(); err != nil || code != 200 {
		t.Fatal(code, err)
	}
}

// TestReloadWithoutNewSessionKeepsOld: when the new session cannot come up
// the old one keeps serving, and the reason is logged.
func TestReloadWithoutNewSessionKeepsOld(t *testing.T) {
	o := fast()
	o.establishTimeout = 1500 * time.Millisecond
	r := newRunRig(t, o, "30s")
	r.waitFor("session", r.sessions(1, 0))
	// A token the hub refuses: the new session never gets past auth.
	data, _ := os.ReadFile(r.config)
	os.WriteFile(r.config, bytes.Replace(data, []byte(testToken), []byte("cxn_abcdefghijklmnop_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"), 1), 0600)
	r.signals <- syscall.SIGHUP
	r.waitFor("reload failure logged", func() bool { return strings.Contains(r.log.String(), "reload failed") })
	if !strings.Contains(r.log.String(), "refused the connection token") {
		t.Fatalf("reason missing:\n%s", r.log.String())
	}
	if l, d := r.g.Sessions(); l != 1 || d != 0 {
		t.Fatalf("sessions %d/%d", l, d)
	}
	if code, err := r.models(); err != nil || code != 200 {
		t.Fatal(code, err)
	}
}

// TestReloadDrainBoundEndsStragglers: a stream that outlasts the old
// configuration's idle-timeout is ended when the drain bound passes.
func TestReloadDrainBoundEndsStragglers(t *testing.T) {
	r := newRunRig(t, fast(), "1s")
	r.waitFor("session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	start := time.Now()
	r.signals <- syscall.SIGHUP
	r.waitFor("new session", r.sessions(2, 1))
	r.waitFor("old session closed", r.sessions(1, 0))
	if took := time.Since(start); took < 900*time.Millisecond {
		t.Fatalf("drained after %s, before the 1s bound", took)
	}
	if _, err := io.ReadAll(stream.Body); err == nil {
		t.Fatal("the straggler finished cleanly")
	}
	if !strings.Contains(r.log.String(), "reached its 1s bound") {
		t.Fatalf("bound not logged:\n%s", r.log.String())
	}
}

// TestStopDrainsThenExits: SIGTERM lets a running stream finish, then run returns.
func TestStopDrainsThenExits(t *testing.T) {
	r := newRunRig(t, fast(), "30s")
	r.waitFor("session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	r.signals <- syscall.SIGTERM
	r.waitFor("draining", r.sessions(1, 1))
	select {
	case err := <-r.done:
		t.Fatalf("exited with a stream running: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := r.models(); err == nil {
		t.Fatal("a draining session took a new request")
	}
	close(r.release)
	rest, err := io.ReadAll(stream.Body)
	if err != nil || !strings.Contains(string(rest), "data: two") {
		t.Fatalf("stream cut: %q %v", rest, err)
	}
	if err := r.exit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.status); err == nil {
		t.Fatal("status file left behind")
	}
}

// TestStopTimeoutEndsStreams: the drain bound ends a stream that will not finish.
func TestStopTimeoutEndsStreams(t *testing.T) {
	o := fast()
	o.drainTimeout = 600 * time.Millisecond
	r := newRunRig(t, o, "30s")
	r.waitFor("session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	start := time.Now()
	r.signals <- syscall.SIGTERM
	if err := r.exit(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 500*time.Millisecond {
		t.Fatalf("stopped after %s, before the drain timeout", took)
	}
	if _, err := io.ReadAll(stream.Body); err == nil {
		t.Fatal("the stream finished cleanly")
	}
}

// TestSecondSignalStopsAtOnce: a second SIGTERM ends a long drain immediately.
func TestSecondSignalStopsAtOnce(t *testing.T) {
	r := newRunRig(t, fast(), "30s") // drain timeout 30s
	r.waitFor("session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	r.signals <- syscall.SIGTERM
	r.waitFor("draining", r.sessions(1, 1))
	start := time.Now()
	r.signals <- syscall.SIGTERM
	if err := r.exit(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("second signal took %s", took)
	}
	if !strings.Contains(r.log.String(), "second stop signal") {
		t.Fatalf("log:\n%s", r.log.String())
	}
}

// TestStatusFileAndCommand: the running connector keeps the status file; the
// status command prints it as a table and as JSON.
func TestStatusFileAndCommand(t *testing.T) {
	r := newRunRig(t, fast(), "30s")
	r.waitFor("session", r.sessions(1, 0))
	stream := r.open()
	defer stream.Body.Close()
	if code, err := r.models(); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	var f status.File
	r.waitFor("status file", func() bool {
		var err error
		f, err = status.Read(r.status)
		return err == nil && f.Connected && len(f.Services) == 1 && f.Services[0].Requests1h >= 1 && f.Services[0].InFlight == 1
	})
	s := f.Services[0]
	if f.Site != "local-test" || f.Version != version || f.PID != os.Getpid() || f.SessionID == "" || f.Hub != "local-test" || f.ConnectedSince == nil || !strings.HasPrefix(f.HubURL, "https://127.0.0.1:") || len(f.Draining) != 0 ||
		s.Name != "chat" || s.Kind != "http" || !s.Healthy || s.MaxConcurrent != 16 || s.Errors1h != 0 {
		t.Fatalf("status %+v", f)
	}
	info, err := os.Stat(r.status)
	if err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("status file mode %v %v", info, err)
	}
	if dir, err := os.Stat(filepath.Dir(r.status)); err != nil || dir.Mode().Perm() != 0750 {
		t.Fatalf("status dir mode %v %v", dir, err)
	}

	var out bytes.Buffer
	if err := execute([]string{"status", "--file", r.status}, &out, &out, 1000); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Site        local-test", "connected to https://127.0.0.1:", f.SessionID, "Last error  none", "Draining    none", "SERVICE", "REQUESTS (1H)"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in\n%s", want, out.String())
		}
	}
	row := ""
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "chat ") {
			row = strings.Join(strings.Fields(line), " ")
		}
	}
	if row != "chat http ok 1 16 1 0" {
		t.Fatalf("service row %q in\n%s", row, out.String())
	}
	out.Reset()
	if err := execute([]string{"status", "--file", r.status, "--json"}, &out, &out, 1000); err != nil {
		t.Fatal(err)
	}
	var decoded status.File
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil || decoded.SessionID != f.SessionID || !strings.Contains(out.String(), `"requests_1h"`) {
		t.Fatalf("%v\n%s", err, out.String())
	}
	close(r.release)
}

// TestStatusCommandNotRunning: no file, and a stale file, are explained.
func TestStatusCommandNotRunning(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	err := execute([]string{"status", "--file", filepath.Join(dir, "none.json")}, &out, &out, 1000)
	if err == nil || !strings.Contains(err.Error(), "not running") {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "status.json")
	if err := status.Write(path, status.File{Version: "0.4.0", Site: "s", UpdatedAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := execute([]string{"status", "--file", path}, &out, &out, 1000); err != nil || !strings.Contains(out.String(), "Not running: the status file was last updated 1h0m0s ago") || !strings.Contains(out.String(), "NOT connected") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if err := execute([]string{"check", "--json"}, &out, &out, 1000); err == nil {
		t.Fatal("--json accepted outside status")
	}
	if err := execute([]string{"check", "--drain-timeout", "1m"}, &out, &out, 1000); err == nil {
		t.Fatal("--drain-timeout accepted outside run")
	}
}

// TestAccessLogCarriesApplicationAndFor: the labels the hub sets reach the log
// through a real session.
func TestAccessLogCarriesApplicationAndFor(t *testing.T) {
	r := newRunRig(t, fast(), "30s")
	r.waitFor("session", r.sessions(1, 0))
	if code, err := r.models(); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	r.waitFor("access line", func() bool {
		data, _ := os.ReadFile(r.access)
		return bytes.Contains(data, []byte(`"application":"app-one"`)) && bytes.Contains(data, []byte(`"for":"acct-9"`))
	})
}

func loadConfigForTest(path string) (*config.Config, error) { return config.Load(path) }
