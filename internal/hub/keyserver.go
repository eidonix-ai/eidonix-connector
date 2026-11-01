package hub

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

const (
	// keyServerPath is the path segment below /<site>/ that addresses a
	// site's key server: POST /<site>/.keyserver/<op>.
	keyServerPath = ".keyserver"
	// keyServerService is the name application lists use for it.
	keyServerService = "key-server"
	maxKeyServerBody = 64 << 10
	keyServerTimeout = 2 * time.Minute
)

var opPattern = regexp.MustCompile(`^[a-z]{1,16}$`)

// keyServerSession returns a live session of the site that offers the key
// server, or the hub's code for why none does.
func (h *Hub) keyServerSession(site string) (*session, string) {
	live := false
	for _, s := range h.sessionsOf(site) {
		if s.mux.IsClosed() || s.draining.Load() {
			continue
		}
		live = true
		if s.has(protocol.CapabilityKeyServer) {
			return s, ""
		}
	}
	if live {
		return nil, "key_server_unavailable"
	}
	return nil, "site_offline"
}

// serveKeyServer relays POST /<site>/.keyserver/<op>: the JSON body (at most
// 64 KiB) goes to the site's connector as a keyserver stream and its one-line
// JSON answer comes back unchanged, with status 200 whether the key server
// accepted the operation or refused it (ok:false); only a failure to reach
// the key server uses the hub's error shape. Bodies are never logged.
func (h *Hub) serveKeyServer(c *call, op string) {
	r, w, e := c.r, c.w, c.entry
	e.Service, e.Kind, e.Op = keyServerService, "keyserver", op
	switch {
	case !h.config().Sites[e.Site].Allows(keyServerService, e.Application):
		c.fail("not_allowed")
		return
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", "POST")
		c.fail("method_not_allowed")
		return
	case !opPattern.MatchString(op):
		c.fail("not_found")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxKeyServerBody+1))
	if err != nil || len(body) > maxKeyServerBody {
		c.fail("invalid_request")
		return
	}
	e.BytesIn = int64(len(body))
	s, code := h.keyServerSession(e.Site)
	if code != "" {
		c.fail(code)
		return
	}
	e.Session = s.id
	stream, err := s.mux.OpenStream()
	if err != nil {
		c.fail("connector_failed")
		return
	}
	defer stream.Close()
	stop := context.AfterFunc(r.Context(), func() { _ = stream.SetDeadline(time.Now()) })
	defer stop()
	_ = stream.SetDeadline(time.Now().Add(keyServerTimeout))
	hd := c.header("keyserver")
	hd.Op = op
	if protocol.WriteStreamHeader(stream, hd) != nil {
		c.fail("connector_failed")
		return
	}
	// The body ends with the stream's write side, so the connector reads to EOF.
	if _, err = stream.Write(body); err != nil || stream.Close() != nil {
		c.fail("connector_failed")
		return
	}
	var answer json.RawMessage
	var ok struct {
		OK    *bool  `json:"ok"`
		Error string `json:"error"`
	}
	if protocol.ReadJSONLine(bufio.NewReader(stream), maxKeyServerBody, &answer) != nil || json.Unmarshal(answer, &ok) != nil || ok.OK == nil {
		c.fail("connector_failed")
		return
	}
	if !*ok.OK {
		e.Error = printableClip(ok.Error, 32)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	e.Status = http.StatusOK
	n, _ := w.Write(append(answer, '\n'))
	e.BytesOut = int64(n)
}
