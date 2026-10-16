package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"io"
	"net"
	"regexp"
	"sync"
	"time"
)

type streamConn struct {
	net.Conn
	ctx         context.Context
	idle        time.Duration
	mu          sync.Mutex
	finDeadline bool
	stopped     bool
}

func (c *streamConn) touch() {
	c.mu.Lock()
	if c.stopped || c.ctx.Err() != nil {
		c.Conn.SetDeadline(time.Now())
		c.mu.Unlock()
		return
	}
	if !c.finDeadline {
		c.Conn.SetReadDeadline(time.Now().Add(c.idle))
	}
	c.Conn.SetWriteDeadline(time.Now().Add(c.idle))
	c.mu.Unlock()
}
func (c *streamConn) Read(p []byte) (int, error) {
	if e := c.ctx.Err(); e != nil {
		return 0, e
	}
	c.touch()
	n, e := c.Conn.Read(p)
	if n > 0 {
		c.touch()
	}
	return n, e
}
func (c *streamConn) Write(p []byte) (int, error) {
	if e := c.ctx.Err(); e != nil {
		return 0, e
	}
	c.touch()
	n, e := c.Conn.Write(p)
	if n > 0 {
		c.touch()
	}
	return n, e
}
func (c *streamConn) expectFIN(d time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finDeadline = true
	if deadline, ok := c.ctx.Deadline(); ok && deadline.Before(d) {
		d = deadline
	}
	return c.Conn.SetReadDeadline(d)
}
func code(e error) string {
	var storageError *protocol.StorageError
	if errors.As(e, &storageError) {
		return storageError.Code
	}
	var timeout net.Error
	if errors.As(e, &timeout) && timeout.Timeout() || errors.Is(e, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(e, protocol.ErrStorageProtocol) || errors.Is(e, io.ErrUnexpectedEOF) || errors.Is(e, io.EOF) {
		return "invalid_request"
	}
	return backendCode(e)
}

// Serve receives an already parsed storage header. No operation can trigger checks or maintenance.
func (h *Handler) Serve(parent context.Context, raw net.Conn, br *bufio.Reader, header protocol.StreamHeader) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	c := &streamConn{Conn: raw, ctx: ctx, idle: h.idle}
	defer c.end()
	stop := context.AfterFunc(ctx, c.end)
	defer stop()
	c.touch()
	buffered := make([]byte, br.Buffered())
	io.ReadFull(br, buffered)
	br = bufio.NewReader(io.MultiReader(bytes.NewReader(buffered), c))
	start := time.Now()
	entry := AuditEntry{Origin: "cloud", Session: protocol.SessionID(ctx), RequestID: safeRequestID(header.RequestID), Op: safeOp(header.Op), Status: "invalid_request"}
	defer func() { entry.DurationMS = time.Since(start).Milliseconds(); h.record(entry) }()
	fail := func(err error) {
		entry.Status = code(err)
		protocol.WriteStorageResult(c, protocol.StorageErrorResult(entry.Status), header.Op)
	}
	if e := protocol.ValidateStorageHeader(header, h.max, len(h.cfg.Prefix)); e != nil {
		fail(e)
		return
	}
	rawKey := header.Key
	if header.Op == "copy" {
		rawKey = header.DstKey
	} else if header.Op == "list" {
		rawKey = header.Prefix
	}
	key, e := protocol.DecodeStorageKey(rawKey, len(h.cfg.Prefix), header.Op == "list")
	if e != nil {
		fail(e)
		return
	}
	entry.KeyHash = h.hash(key)
	if e := h.reserve(); e != "" {
		fail(protocol.NewStorageError(e))
		return
	}
	defer h.release()
	if header.Op != "put" {
		go func() {
			br.ReadByte()
			cancel()
		}()
	}
	var result protocol.StorageResult
	var body io.ReadCloser
	var bodySize int64
	switch header.Op {
	case "put":
		lifetimeCtx, lifetimeCancel := context.WithTimeout(ctx, h.lifetime)
		defer lifetimeCancel()
		lifetimeStop := context.AfterFunc(lifetimeCtx, func() { raw.SetDeadline(time.Now()) })
		defer lifetimeStop()
		c.ctx = lifetimeCtx
		reader := protocol.NewStorageBodyReader(br, *header.Size, h.max, c.expectFIN)
		info, err := h.upload(lifetimeCtx, h.cfg.Prefix+key, header.ContentType, reader, -1)
		e = err
		entry.Size = info.Size
		if err == nil {
			result = protocol.StorageResult{V: protocol.Version, OK: true, Size: &info.Size, ETag: info.ETag}
			entry.Verified = true
			entry.Size = info.Size
		}
	case "head":
		result, e = h.head(ctx, h.cfg.Prefix+key)
	case "delete":
		e = h.core.RemoveObject(ctx, h.cfg.Bucket, h.cfg.Prefix+key, minioRemoveOptions())
		result = protocol.StorageResult{V: protocol.Version, OK: true}
	case "list":
		result, e = h.list(ctx, key, header)
	case "get":
		result, body, bodySize, e = h.get(ctx, h.cfg.Prefix+key, header)
	case "copy":
		src, err := protocol.DecodeStorageKey(header.SrcKey, len(h.cfg.Prefix), false)
		if err != nil {
			e = err
			break
		}
		result, e = h.copy(ctx, h.cfg.Prefix+src, h.cfg.Prefix+key)
	}
	if e != nil {
		fail(e)
		return
	}
	if body != nil {
		defer body.Close()
	}
	entry.Status = "ok"
	if result.Size != nil {
		entry.Size = *result.Size
	}
	if e = protocol.WriteStorageResult(c, result, header.Op); e != nil {
		entry.Status = "backend_invalid"
		return
	}
	if body == nil {
		return
	}
	writer := protocol.NewStorageBodyWriter(c, bodySize)
	n, e := io.CopyBuffer(writer, io.LimitReader(body, bodySize+1), make([]byte, 64<<10))
	entry.Size = n
	if e != nil || n != bodySize {
		entry.Status = "backend_invalid"
		return
	}
	if e = writer.Finish(); e != nil {
		entry.Status = "backend_unavailable"
		return
	}
	entry.Verified = true
}
func safeOp(op string) string {
	switch op {
	case "put", "get", "head", "copy", "delete", "list":
		return op
	}
	return "invalid"
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

func safeRequestID(id string) string {
	if requestIDPattern.MatchString(id) {
		return id
	}
	return ""
}

func (c *streamConn) end() {
	c.mu.Lock()
	c.stopped = true
	c.Conn.SetReadDeadline(time.Now())
	c.Conn.Close()
	c.mu.Unlock()
}
