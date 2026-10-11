package protocol

import (
	"errors"
	"github.com/gorilla/websocket"
	"io"
	"net"
	"sync"
	"time"
)

// WebSocketConn carries a byte stream in bounded binary WebSocket messages.
type WebSocketConn struct {
	conn            *websocket.Conn
	reader          io.Reader
	readMu, writeMu sync.Mutex
}

func NewWebSocketConn(c *websocket.Conn) *WebSocketConn {
	c.SetReadLimit(256 << 10)
	return &WebSocketConn{conn: c}
}
func (c *WebSocketConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		if c.reader == nil {
			t, r, e := c.conn.NextReader()
			if e != nil {
				return 0, e
			}
			if t != websocket.BinaryMessage {
				return 0, errors.New("non-binary transport message")
			}
			c.reader = r
		}
		n, e := c.reader.Read(p)
		if e == io.EOF {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, e
	}
}
func (c *WebSocketConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > 256<<10 {
			n = 256 << 10
		}
		if e := c.conn.WriteMessage(websocket.BinaryMessage, p[:n]); e != nil {
			return total, e
		}
		total += n
		p = p[n:]
	}
	return total, nil
}
func (c *WebSocketConn) Close() error         { return c.conn.Close() }
func (c *WebSocketConn) LocalAddr() net.Addr  { return c.conn.LocalAddr() }
func (c *WebSocketConn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }
func (c *WebSocketConn) SetDeadline(t time.Time) error {
	if e := c.SetReadDeadline(t); e != nil {
		return e
	}
	return c.SetWriteDeadline(t)
}
func (c *WebSocketConn) SetReadDeadline(t time.Time) error {
	return c.conn.UnderlyingConn().SetReadDeadline(t)
}
func (c *WebSocketConn) SetWriteDeadline(t time.Time) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.SetWriteDeadline(t)
}
