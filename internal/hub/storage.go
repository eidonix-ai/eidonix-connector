package hub

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// StoredObject is one list entry of the storage API.
type StoredObject struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	LastModified string `json:"last_modified"`
	ContentType  string `json:"content_type"`
	Dir          bool   `json:"dir"`
}

// ListPage is the storage API's list answer.
type ListPage struct {
	Objects   []StoredObject `json:"objects"`
	Truncated bool           `json:"truncated"`
	NextToken string         `json:"next_token,omitempty"`
}

// storageLimits are a service's object size cap and the bytes its prefix
// takes from the 1024-byte key budget.
func storageLimits(rt *route) (maxObject int64, prefixBytes int) {
	maxObject = protocol.MaxStorageObjectBytes
	if st := rt.storage; st != nil {
		if st.MaxObjectBytes > 0 && st.MaxObjectBytes < maxObject {
			maxObject = st.MaxObjectBytes
		}
		if st.MaxKeyBytes > 0 && st.MaxKeyBytes <= protocol.MaxStorageKeyBytes {
			prefixBytes = protocol.MaxStorageFullKeyBytes - st.MaxKeyBytes
		}
	}
	return maxObject, prefixBytes
}

// storageStatus maps a storage result code to the API's status and code.
func storageStatus(code, op string, readOnly bool) (int, string) {
	switch code {
	case "not_found":
		return 404, code
	case "precondition_failed":
		return 412, code
	case "too_large":
		return 413, code
	case "invalid_key", "key_too_long", "invalid_request":
		return 400, code
	case "forbidden":
		if readOnly && (op == "put" || op == "delete" || op == "copy") {
			return 403, "read_only"
		}
	case "busy", "rate_limited":
		return 503, code
	case "timeout":
		return 504, code
	case "size_mismatch":
		if op == "put" {
			return 400, code
		}
	}
	if !protocol.ValidErrorCode(code) {
		code = "internal"
	}
	return 502, code
}

// etagValue strips quotes (and a weak prefix) from an If-Match value.
func etagValue(s string) string {
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(s), "W/"), `"`)
}

// parseRange reads "bytes=a-b" or "bytes=a-" (one range).
func parseRange(s string) ([]int64, bool) {
	spec, ok := strings.CutPrefix(s, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return nil, false
	}
	a, b, ok := strings.Cut(spec, "-")
	start, err := strconv.ParseInt(a, 10, 64)
	if !ok || err != nil || start < 0 {
		return nil, false
	}
	end := protocol.MaxStorageObjectBytes - 1
	if b != "" {
		if end, err = strconv.ParseInt(b, 10, 64); err != nil || end < start {
			return nil, false
		}
	}
	return []int64{start, end}, true
}

// serveStorage serves the storage API (§4.3) for an s3 service.
func (h *Hub) serveStorage(c *call, l lease) {
	r, w, e := c.r, c.w, c.entry
	readOnly := l.rt.storage != nil && l.rt.storage.ReadOnly
	refuse := func(status int, code string) {
		l.release()
		e.Status, e.Error = status, code
		writeError(w, status, code)
	}
	if isWebSocketUpgrade(r) {
		refuse(400, "invalid_request")
		return
	}
	key, err := url.PathUnescape(strings.TrimPrefix(c.rest, "/"))
	if err != nil {
		refuse(400, "invalid_key")
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		refuse(400, "invalid_request")
		return
	}
	maxObject, prefixBytes := storageLimits(l.rt)
	hd := c.header("storage")
	switch {
	case r.Method == http.MethodGet && q.Has("list"):
		if key != "" {
			refuse(400, "invalid_request")
			return
		}
		hd.Op, hd.Prefix, hd.Token, hd.Max = "list", protocol.EncodeStorageKey(q.Get("prefix")), q.Get("token"), protocol.MaxStorageListEntries
		if m := q.Get("max"); m != "" {
			if hd.Max, err = strconv.Atoi(m); err != nil {
				refuse(400, "invalid_request")
				return
			}
		}
		hd.Recursive = q.Has("recursive") && q.Get("recursive") != "false" && q.Get("recursive") != "0"
	case r.Method == http.MethodGet:
		hd.Op, hd.Key = "get", protocol.EncodeStorageKey(key)
		if rg := r.Header.Get("Range"); rg != "" {
			var ok bool
			if hd.Range, ok = parseRange(rg); !ok {
				refuse(400, "invalid_request")
				return
			}
		}
		if m := r.Header.Get("If-Match"); m != "" && strings.TrimSpace(m) != "*" {
			hd.IfMatch = etagValue(m)
		}
	case r.Method == http.MethodHead:
		hd.Op, hd.Key = "head", protocol.EncodeStorageKey(key)
	case r.Method == http.MethodPut:
		size := r.ContentLength
		if size < 0 {
			size = -1
		}
		hd.Op, hd.Key, hd.Size, hd.ContentType = "put", protocol.EncodeStorageKey(key), &size, r.Header.Get("Content-Type")
	case r.Method == http.MethodDelete:
		hd.Op, hd.Key = "delete", protocol.EncodeStorageKey(key)
	case r.Method == http.MethodPost:
		if !q.Has("copy-from") {
			refuse(400, "invalid_request")
			return
		}
		hd.Op, hd.SrcKey, hd.DstKey = "copy", protocol.EncodeStorageKey(q.Get("copy-from")), protocol.EncodeStorageKey(key)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE, POST")
		refuse(405, "method_not_allowed")
		return
	}
	e.Op = hd.Op
	if readOnly && (hd.Op == "put" || hd.Op == "delete" || hd.Op == "copy") {
		refuse(403, "read_only")
		return
	}
	if err := protocol.ValidateStorageHeader(hd, maxObject, prefixBytes); err != nil {
		code := "invalid_request"
		var se *protocol.StorageError
		if errors.As(err, &se) {
			code = se.Code
		}
		status, code := storageStatus(code, hd.Op, readOnly)
		refuse(status, code)
		return
	}
	body := &replayBody{r: r.Body, limit: maxReplayBytes}
	if hd.Op == "put" {
		defer func() { e.BytesIn = body.n.Load() }()
	}
	var x *exchange
	for attempt := 0; ; attempt++ {
		var upload io.Reader = body
		if attempt > 0 {
			upload = body.replay()
		}
		x = h.storageExchange(r.Context(), l, hd, upload)
		if x.err != nil || attempt > 0 || x.result.OK || x.result.Error.Code != "busy" {
			break
		}
		// A draining connector refuses with busy before touching storage:
		// try once more on another session of the site.
		next, code := h.pick(e.Site, e.Service, l.s)
		if code != "" {
			break
		}
		x.finish()
		if !body.waitReplayable(x.uploaded, replayWait) {
			next.release()
			break
		}
		l, e.Session = next, next.s.id
		readOnly = l.rt.storage != nil && l.rt.storage.ReadOnly
		maxObject, _ = storageLimits(l.rt)
	}
	defer x.finish()
	result, br, conn := x.result, x.br, x.conn
	if x.err != nil {
		select {
		case <-x.uploaded:
			var se *protocol.StorageError
			if errors.As(x.uploadErr, &se) {
				status, code := storageStatus(se.Code, hd.Op, readOnly)
				e.Status, e.Error = status, code
				writeError(w, status, code)
				return
			}
			if x.uploadErr != nil {
				e.Status, e.Error = 400, "client_closed"
				return
			}
		default:
		}
		if r.Context().Err() != nil {
			e.Status, e.Error = 499, "client_closed"
			return
		}
		c.fail("connector_failed")
		return
	}
	if !result.OK {
		status, code := storageStatus(result.Error.Code, hd.Op, readOnly)
		e.Status, e.Error = status, code
		writeError(w, status, code)
		return
	}
	writeJSON := func(status int, v any) {
		b, _ := json.Marshal(v)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(append(b, '\n'))
		e.Status = status
	}
	switch hd.Op {
	case "put":
		w.Header().Set("ETag", quoteETag(result.ETag))
		writeJSON(200, map[string]any{"size": *result.Size, "etag": result.ETag})
	case "copy":
		w.Header().Set("ETag", quoteETag(result.ETag))
		writeJSON(200, map[string]any{"size": *result.Size, "etag": result.ETag})
	case "delete":
		w.WriteHeader(http.StatusNoContent)
		e.Status = http.StatusNoContent
	case "list":
		page := ListPage{Objects: make([]StoredObject, 0, len(result.Objects)), Truncated: result.Truncated, NextToken: result.NextToken}
		for _, o := range result.Objects {
			k, err := protocol.DecodeStorageKey(o.Key, 0, false)
			if err != nil {
				continue // validated by ReadStorageResult; defensive
			}
			page.Objects = append(page.Objects, StoredObject{Key: k, Size: o.Size, ETag: o.ETag, LastModified: o.LastModified, ContentType: o.ContentType, Dir: o.Dir})
		}
		writeJSON(200, page)
	case "head", "get":
		hdr := w.Header()
		hdr.Set("Content-Type", result.ContentType)
		hdr.Set("ETag", quoteETag(result.ETag))
		hdr.Set("Accept-Ranges", "bytes")
		if t, err := time.Parse(time.RFC3339, result.LastModified); err == nil {
			hdr.Set("Last-Modified", t.UTC().Format(http.TimeFormat))
		}
		size, status := *result.Size, http.StatusOK
		if len(result.Range) == 2 {
			hdr.Set("Content-Range", "bytes "+itoa(result.Range[0])+"-"+itoa(result.Range[1])+"/"+itoa(size))
			size, status = result.Range[1]-result.Range[0]+1, http.StatusPartialContent
		}
		hdr.Set("Content-Length", itoa(size))
		w.WriteHeader(status)
		e.Status = status
		if hd.Op == "head" {
			return
		}
		body := protocol.NewStorageBodyReader(br, size, maxObject, conn.SetReadDeadline)
		n, err := relay(w, body)
		e.BytesOut = n
		if err != nil {
			if errors.Is(err, errClientGone) || r.Context().Err() != nil {
				e.Error = "client_closed"
				return
			}
			// A truncated or corrupt body must never look complete.
			e.Error = "transfer_interrupted"
			panic(http.ErrAbortHandler)
		}
	}
}

func quoteETag(s string) string {
	if s == "" {
		return ""
	}
	return `"` + s + `"`
}

// exchange is one storage operation on one stream: its result line, and for
// a get the reader positioned at the body.
type exchange struct {
	result protocol.StorageResult
	br     *bufio.Reader
	conn   *idleConn
	finish func()
	err    error // no result line
	// uploaded is closed once the put writer has stopped reading the
	// upload; uploadErr is its error.
	uploaded  chan struct{}
	uploadErr error
}

// storageExchange opens a stream on l, sends the header (and a put's
// framed upload) and reads the result line.
func (h *Hub) storageExchange(ctx context.Context, l lease, hd protocol.StreamHeader, upload io.Reader) *exchange {
	x := &exchange{uploaded: make(chan struct{}), finish: func() {}}
	maxObject, _ := storageLimits(l.rt)
	conn, finish, err := l.open(ctx, hd, storageIdle)
	if err != nil {
		close(x.uploaded)
		x.err = err
		return x
	}
	x.conn, x.finish = conn, finish
	if hd.Op == "put" {
		// Write concurrently so an early error line (invalid_key, too_large,
		// busy) can be read while the body is still uploading.
		go func() {
			defer close(x.uploaded)
			w := protocol.NewStorageBodyWriter(conn, maxObject)
			if _, err := io.Copy(w, upload); err != nil {
				x.uploadErr = err
				finish() // no end marker: the connector aborts the upload
				return
			}
			if err := w.Finish(); err != nil {
				x.uploadErr = err
				finish()
				return
			}
			// FIN is the put's verified completion boundary; yamux
			// half-closes, so the result can still be read.
			conn.Close()
		}()
	} else {
		close(x.uploaded)
	}
	x.br = bufio.NewReader(conn)
	if x.result, err = protocol.ReadStorageResult(x.br, hd.Op); err != nil {
		finish()
		x.err = err
	}
	return x
}
