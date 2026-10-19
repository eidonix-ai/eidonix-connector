package hub

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
)

// wsHandshakeHeaders are the client's own handshake headers; the hub makes
// its own handshake with the connector.
var wsHandshakeHeaders = []string{"Sec-Websocket-Key", "Sec-Websocket-Version", "Sec-Websocket-Extensions", "Sec-Websocket-Protocol", "Origin"}

// serveWebSocket opens the service's WEBSOCKET endpoint through the
// connector, then upgrades the client and relays messages unchanged.
func (h *Hub) serveWebSocket(c *call, l lease) {
	r, e := c.r, c.entry
	if r.Method != http.MethodGet || strings.HasPrefix(c.rest, "//") {
		l.release()
		c.fail("invalid_request")
		return
	}
	var (
		conn   *idleConn
		finish func()
		br     *bufio.Reader
	)
	for attempt := 0; ; attempt++ {
		var resp *http.Response
		var err error
		conn, finish, br, resp, err = h.wsHandshake(c, l)
		if err != nil {
			c.fail("connector_failed")
			return
		}
		if resp.StatusCode == http.StatusSwitchingProtocols {
			break
		}
		resp.Body.Close()
		finish()
		code := resp.Header.Get(protocol.HeaderError)
		if code == "busy" && attempt == 0 {
			// A draining connector refuses new streams with busy.
			if next, nc := h.pick(e.Site, e.Service, l.s); nc == "" {
				l, e.Session = next, next.s.id
				continue
			}
		}
		if code == "" {
			code = "upstream_error"
		}
		h.connectorError(c, resp.StatusCode, code, resp.Header.Get("Retry-After"))
		return
	}
	defer finish()
	up := websocket.Upgrader{HandshakeTimeout: 10 * time.Second, CheckOrigin: func(*http.Request) bool { return true }}
	ws, err := up.Upgrade(c.w, r, http.Header{HeaderRequestID: {e.RequestID}})
	if err != nil {
		e.Status, e.Error = 400, "invalid_request"
		return
	}
	defer ws.Close()
	e.Status = http.StatusSwitchingProtocols
	ws.SetReadLimit(maxWSMessage)
	var in, out atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			typ, p, err := ws.ReadMessage()
			if err != nil {
				var ce *websocket.CloseError
				if errors.As(err, &ce) {
					msg := []byte{}
					if ce.Code != websocket.CloseNoStatusReceived {
						msg = websocket.FormatCloseMessage(ce.Code, ce.Text)
					}
					_ = protocol.WriteFrame(conn, protocol.Frame{Type: websocket.CloseMessage, Payload: msg}, maxWSMessage)
					return
				}
				finish()
				return
			}
			in.Add(int64(len(p)))
			if err := protocol.WriteFrame(conn, protocol.Frame{Type: byte(typ), Payload: p}, maxWSMessage); err != nil {
				ws.Close()
				return
			}
		}
	}()
	for {
		f, err := protocol.ReadFrame(br, maxWSMessage)
		if err != nil {
			break
		}
		_ = ws.SetWriteDeadline(time.Now().Add(streamIdle))
		if f.Type == websocket.CloseMessage {
			_ = ws.WriteControl(websocket.CloseMessage, f.Payload, time.Now().Add(time.Second))
			break
		}
		out.Add(int64(len(f.Payload)))
		if ws.WriteMessage(int(f.Type), f.Payload) != nil {
			break
		}
	}
	// Give the client a moment to answer a close, then end both sides.
	select {
	case <-done:
	case <-time.After(time.Second):
	}
	ws.Close()
	finish()
	<-done
	e.BytesIn, e.BytesOut = in.Load(), out.Load()
}

// wsHandshake opens a websocket stream on l and makes the upgrade request
// to the connector.
func (h *Hub) wsHandshake(c *call, l lease) (*idleConn, func(), *bufio.Reader, *http.Response, error) {
	r := c.r
	conn, finish, err := l.open(r.Context(), c.header("websocket"), streamIdle)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	key := make([]byte, 16)
	_, _ = rand.Read(key)
	req := &http.Request{Method: http.MethodGet, URL: &url.URL{Opaque: c.rest, RawQuery: r.URL.RawQuery}, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: cleanHeaders(r.Header, wsHandshakeHeaders...), Host: "connector"}
	req.Header.Set(HeaderRequestID, c.entry.RequestID)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", base64.StdEncoding.EncodeToString(key))
	if err := req.Write(conn); err != nil {
		finish()
		return nil, nil, nil, nil, err
	}
	hr := &limitedHeader{r: conn, left: maxResponseHeader}
	br := bufio.NewReader(hr)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		finish()
		return nil, nil, nil, nil, err
	}
	hr.unlimited.Store(true)
	return conn, finish, br, resp, nil
}
