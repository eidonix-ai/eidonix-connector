package hub

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// Application label headers (§4.4 of docs/HUB-DESIGN.md).
const (
	HeaderApplication = "X-Eidonix-Application"
	HeaderFor         = "X-Eidonix-For"
	HeaderRequestID   = "X-Request-Id"
)

// hubErrors are the codes the hub itself answers with.
var hubErrors = map[string]struct {
	status  int
	message string
}{
	"unknown_site":       {404, "no such site"},
	"unknown_service":    {404, "the site offers no such service"},
	"site_offline":       {503, "the site is not connected"},
	"service_unhealthy":  {503, "the site reports the service unhealthy"},
	"busy":               {503, "the service is at its concurrency limit"},
	"not_allowed":        {403, "this application may not use this service"},
	"read_only":          {403, "the storage service is read-only"},
	"invalid_request":    {400, "invalid request"},
	"not_found":          {404, "not found"},
	"method_not_allowed": {405, "method not allowed"},
	"connector_failed":   {502, "the connector session failed during the request"},
	"response_too_large": {502, "response exceeds size limit"},
}

// codeStatus is the HTTP status of a hub or connector code.
func codeStatus(code string) int {
	if e, ok := hubErrors[code]; ok {
		return e.status
	}
	return protocol.HTTPStatus(code)
}

func codeMessage(code string) string {
	if e, ok := hubErrors[code]; ok {
		return e.message
	}
	return protocol.ErrorMessage(code)
}

// ErrorBody is the JSON body of every error the hub or a connector makes.
type ErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeError answers with code at status (0: the code's own status).
func writeError(w http.ResponseWriter, status int, code string) {
	if status == 0 {
		status = codeStatus(code)
	}
	var b ErrorBody
	b.Error.Code, b.Error.Message = code, codeMessage(code)
	body, _ := json.Marshal(b)
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set(protocol.HeaderError, code)
	if code == "busy" || code == "rate_limited" {
		h.Set("Retry-After", "1")
	}
	h.Del("Content-Length")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// requestID is the caller's X-Request-Id when it is a valid protocol
// request id, otherwise a new one.
func requestID(r *http.Request) string {
	if id := r.Header.Get(HeaderRequestID); requestIDPattern.MatchString(id) {
		return id
	}
	id, _ := protocol.NewInstanceID()
	return id
}

// label returns a printable label header (≤64 bytes), "" when absent or invalid.
func label(r *http.Request, name string) string {
	if v := r.Header.Get(name); validLabel(v) {
		return v
	}
	return ""
}

// call is one local request being served.
type call struct {
	w     http.ResponseWriter
	r     *http.Request
	entry *AccessEntry
	// rest is the escaped path below /<site>/<service> ("/" at least).
	rest string
}

func (c *call) header(kind string) protocol.StreamHeader {
	return protocol.StreamHeader{V: protocol.Version, Service: c.entry.Service, Kind: kind, RequestID: c.entry.RequestID,
		Application: c.entry.Application, For: c.entry.For}
}

func (c *call) fail(code string) {
	c.entry.Error = code
	c.entry.Status = codeStatus(code)
	writeError(c.w, 0, code)
}

// LocalHandler serves the local port: /healthz, /v1/catalog[/<site>] and
// /<site>/<service>/<path>.
func (h *Hub) LocalHandler() http.Handler { return http.HandlerFunc(h.serveLocal) }

func (h *Hub) serveLocal(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	switch {
	case path == "/healthz":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
		return
	case path == "/v1/catalog" || strings.HasPrefix(path, "/v1/catalog/"):
		h.serveCatalog(w, r, strings.TrimPrefix(strings.TrimPrefix(path, "/v1/catalog"), "/"))
		return
	}
	start := time.Now()
	e := &AccessEntry{Application: label(r, HeaderApplication), For: label(r, HeaderFor), Method: r.Method, RequestID: requestID(r)}
	w.Header().Set(HeaderRequestID, e.RequestID)
	c := &call{w: w, r: r, entry: e, rest: "/"}
	defer func() {
		e.Time, e.DurationMS = start.UTC(), time.Since(start).Milliseconds()
		h.access.Write(e)
	}()
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 3)
	if parts[0] == "v1" {
		c.fail("not_found")
		return
	}
	if !protocol.ValidSiteName(parts[0]) {
		c.fail("unknown_site")
		return
	}
	e.Site = parts[0]
	if len(parts) < 2 || !protocol.ValidServiceName(parts[1]) {
		c.fail("unknown_service")
		return
	}
	e.Service = parts[1]
	if len(parts) == 3 {
		c.rest = "/" + parts[2]
	}
	site := h.config().Sites[e.Site]
	if site == nil {
		c.fail("unknown_site")
		return
	}
	if !site.Allows(e.Service, e.Application) {
		c.fail("not_allowed")
		return
	}
	l, code := h.pick(e.Site, e.Service, nil)
	if code != "" {
		c.fail(code)
		return
	}
	e.Session = l.s.id
	switch {
	case l.rt.kind == protocol.KindS3:
		e.Kind = "storage"
		h.serveStorage(c, l)
	case isWebSocketUpgrade(r):
		e.Kind = "websocket"
		e.Path = c.rest
		h.serveWebSocket(c, l)
	default:
		e.Kind = "http"
		e.Path = c.rest
		h.serveHTTP(c, l)
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	return headerHasToken(r.Header, "Connection", "upgrade") && headerHasToken(r.Header, "Upgrade", "websocket")
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

var hopHeaders = []string{"Connection", "Keep-Alive", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Expect"}

// credentialHeaders are dropped too: the connector applies its own
// configured credential and would drop these anyway.
var credentialHeaders = []string{"Authorization", "Cookie", "X-Api-Key"}

// cleanHeaders copies h without hop-by-hop headers (and those Connection
// names), X-Eidonix-* headers, credentials and X-Request-Id.
func cleanHeaders(h http.Header, extra ...string) http.Header {
	out := h.Clone()
	for _, v := range h.Values("Connection") {
		for _, k := range strings.Split(v, ",") {
			out.Del(strings.TrimSpace(k))
		}
	}
	for _, list := range [][]string{hopHeaders, credentialHeaders, extra, {HeaderRequestID}} {
		for _, k := range list {
			out.Del(k)
		}
	}
	for k := range out {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), "X-Eidonix-") {
			delete(out, k)
		}
	}
	return out
}

// serveHTTP relays one HTTP request to the service and streams the reply,
// flushing every write. Closing the request closes the stream, which
// cancels the request at the site. A connector's busy refusal (a draining
// session refuses new streams with it, before contacting the service) is
// retried once on another session of the site.
func (h *Hub) serveHTTP(c *call, l lease) {
	r, w, e := c.r, c.w, c.entry
	if strings.HasPrefix(c.rest, "//") {
		l.release()
		c.fail("path_not_allowed")
		return
	}
	body := &replayBody{r: r.Body, limit: maxReplayBytes}
	defer func() { e.BytesIn = body.n.Load() }()
	var (
		resp   *http.Response
		finish func()
		err    error
	)
	for attempt := 0; ; attempt++ {
		var in io.Reader = body
		if attempt > 0 {
			in = body.replay()
		}
		var wrote chan struct{}
		resp, finish, wrote, err = h.roundTrip(c, l, in)
		if err != nil {
			if r.Context().Err() != nil {
				e.Status, e.Error = 499, "client_closed"
				return
			}
			c.fail("connector_failed")
			return
		}
		if attempt > 0 || resp.Header.Get(protocol.HeaderError) != "busy" {
			break
		}
		next, code := h.pick(e.Site, e.Service, l.s)
		if code != "" {
			break
		}
		resp.Body.Close()
		finish()
		if !body.waitReplayable(wrote, replayWait) {
			next.release()
			h.connectorError(c, 503, "busy", "")
			return
		}
		l, e.Session = next, next.s.id
	}
	defer finish()
	defer resp.Body.Close()
	if code := resp.Header.Get(protocol.HeaderError); code != "" {
		h.connectorError(c, resp.StatusCode, code, resp.Header.Get("Retry-After"))
		return
	}
	if resp.ContentLength > maxResponseBody {
		c.fail("response_too_large")
		return
	}
	for k, v := range cleanHeaders(resp.Header, "Content-Length") {
		w.Header()[k] = v
	}
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", itoa(resp.ContentLength))
	}
	w.WriteHeader(resp.StatusCode)
	e.Status = resp.StatusCode
	n, err := relay(w, &cappedBody{r: resp.Body, left: maxResponseBody})
	e.BytesOut = n
	if err != nil {
		if errors.Is(err, errTooLarge) {
			e.Error = "response_too_large"
		} else if errors.Is(err, errClientGone) || r.Context().Err() != nil {
			e.Error = "client_closed"
			return
		} else {
			e.Error = "response_interrupted"
		}
		// The client must see a truncated reply, not a complete one.
		panic(http.ErrAbortHandler)
	}
}

// roundTrip sends the request on a new stream of l with body and reads the
// response headers. wrote is closed once the request writer has stopped
// reading body. finish ends the stream and releases l.
func (h *Hub) roundTrip(c *call, l lease, body io.Reader) (*http.Response, func(), chan struct{}, error) {
	r := c.r
	wrote := make(chan struct{})
	conn, finish, err := l.open(r.Context(), c.header("http"), streamIdle)
	if err != nil {
		close(wrote)
		return nil, nil, wrote, err
	}
	out := &http.Request{Method: r.Method, URL: &url.URL{Opaque: c.rest, RawQuery: r.URL.RawQuery}, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: cleanHeaders(r.Header), ContentLength: r.ContentLength, Host: "connector", Close: true}
	out.Header.Set(HeaderRequestID, c.entry.RequestID)
	if r.ContentLength != 0 {
		out.Body = io.NopCloser(body)
	}
	// Write concurrently so an early refusal (413, 403, busy) can be read
	// while the body is still uploading.
	go func() {
		defer close(wrote)
		if err := out.Write(conn); err != nil {
			_ = conn.SetReadDeadline(time.Now())
		}
	}()
	hr := &limitedHeader{r: conn, left: maxResponseHeader}
	resp, err := http.ReadResponse(bufio.NewReader(hr), out)
	if err != nil {
		finish()
		return nil, nil, wrote, err
	}
	hr.unlimited.Store(true)
	return resp, finish, wrote, nil
}

// connectorError rewrites a connector-generated refusal (it carries
// X-Eidonix-Error; the connector never relays that header from a service)
// into the JSON error shape, keeping its status. A connector's busy becomes
// the hub's busy (503, Retry-After: 1).
func (h *Hub) connectorError(c *call, status int, code, retryAfter string) {
	if !protocol.ValidErrorCode(code) {
		code = "internal"
	}
	if code == "busy" {
		status = 503
	}
	c.entry.Status, c.entry.Error = status, code
	if retryAfter != "" && printableClip(retryAfter, 16) == retryAfter {
		c.w.Header().Set("Retry-After", retryAfter)
	}
	writeError(c.w, status, code)
}

var errClientGone = errors.New("client closed the connection")

// relay copies src to w, flushing after every write. A write error is
// errClientGone; any other error is src's.
func relay(w http.ResponseWriter, src io.Reader) (int64, error) {
	rc := http.NewResponseController(w)
	buf := make([]byte, 32<<10)
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return total, errClientGone
			}
			total += int64(n)
			_ = rc.Flush()
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
