package hub

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// open opens a request stream on a lease and writes its header. The
// returned finish closes the stream and releases the lease (idempotent);
// cancelling ctx closes the stream.
func (l lease) open(ctx context.Context, h protocol.StreamHeader, idle time.Duration) (*idleConn, func(), error) {
	stream, err := l.s.mux.OpenStream()
	if err != nil {
		l.release()
		return nil, nil, err
	}
	conn := &idleConn{Conn: stream, idle: idle}
	stop := context.AfterFunc(ctx, func() { _ = stream.SetDeadline(time.Now()); stream.Close() })
	var once sync.Once
	finish := func() {
		once.Do(func() {
			stop()
			stream.Close()
			l.release()
		})
	}
	h.V = protocol.Version
	if err := protocol.WriteStreamHeader(conn, h); err != nil {
		finish()
		return nil, nil, err
	}
	return conn, finish, nil
}

// idleConn closes a stream after idle with no bytes either way. An explicit
// deadline (the storage FIN wait) caps the idle extension instead of being
// overwritten by it.
type idleConn struct {
	net.Conn
	idle              time.Duration
	mu                sync.Mutex
	readCap, writeCap time.Time
}

func earliest(a, b time.Time) time.Time {
	if !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

func (c *idleConn) touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := time.Now().Add(c.idle)
	_ = c.Conn.SetReadDeadline(earliest(next, c.readCap))
	_ = c.Conn.SetWriteDeadline(earliest(next, c.writeCap))
}

func (c *idleConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readCap = t
	return c.Conn.SetReadDeadline(earliest(time.Now().Add(c.idle), t))
}

func (c *idleConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCap = t
	return c.Conn.SetWriteDeadline(earliest(time.Now().Add(c.idle), t))
}

func (c *idleConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readCap, c.writeCap = t, t
	return c.Conn.SetDeadline(earliest(time.Now().Add(c.idle), t))
}

func (c *idleConn) Read(b []byte) (int, error) {
	c.touch()
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.touch()
	}
	return n, err
}

func (c *idleConn) Write(b []byte) (int, error) {
	c.touch()
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.touch()
	}
	return n, err
}

// limitedHeader bounds the bytes read until the response headers are parsed.
type limitedHeader struct {
	r         io.Reader
	left      int
	unlimited atomic.Bool
}

func (h *limitedHeader) Read(p []byte) (int, error) {
	if !h.unlimited.Load() {
		if h.left <= 0 {
			return 0, errors.New("connector response headers too large")
		}
		if len(p) > h.left {
			p = p[:h.left]
		}
	}
	n, err := h.r.Read(p)
	h.left -= n
	return n, err
}

var errTooLarge = errors.New("connector response too large")

// cappedBody fails a response body past max bytes.
type cappedBody struct {
	r    io.Reader
	left int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		var probe [1]byte
		if n, _ := b.r.Read(probe[:]); n > 0 {
			return 0, errTooLarge
		}
		return 0, io.EOF
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.r.Read(p)
	b.left -= int64(n)
	return n, err
}

// countingReader counts bytes read.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// A request body is kept, up to maxReplayBytes, while it is sent, so a
// refusal that never reached the service (a draining connector's busy) can
// be sent again to another session. The connector refuses before reading
// the body, so what was read is at most the stream window plus buffers.
const (
	maxReplayBytes = 1 << 20
	replayWait     = 10 * time.Second
)

// replayBody records what is read from r until limit is passed.
type replayBody struct {
	r     io.Reader
	limit int
	buf   []byte
	over  bool
	n     atomic.Int64 // bytes read from r
}

func (b *replayBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n.Add(int64(n))
	if !b.over {
		if len(b.buf)+n > b.limit {
			b.over, b.buf = true, nil
		} else {
			b.buf = append(b.buf, p[:n]...)
		}
	}
	return n, err
}

// waitReplayable waits (at most d) for the writer that reads b to stop,
// and reports whether everything it read was kept.
func (b *replayBody) waitReplayable(writerDone <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-writerDone:
		return !b.over
	case <-t.C:
		return false
	}
}

// replay returns a reader of everything from the start: the kept bytes,
// then the rest of r. Recording stops.
func (b *replayBody) replay() io.Reader {
	kept := b.buf
	b.over, b.buf = true, nil
	return io.MultiReader(bytes.NewReader(kept), b)
}
