package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/minio/minio-go/v7"
	"io"
	"log"
	"strings"
	"time"
)

type CheckResult struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Warning bool   `json:"warning"`
	Message string `json:"message,omitempty"`
}

// Start begins local maintenance; constructing a handler never touches a bucket.
func (h *Handler) Start(parent context.Context) {
	h.start.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		h.mu.Lock()
		h.cancel = cancel
		h.mu.Unlock()
		h.warnings()
		h.maintenance.Add(1)
		go func() {
			defer h.maintenance.Done()
			h.startup(ctx)
			if h.Healthy() {
				h.sweep(ctx, time.Now())
			}
			lastSweep := time.Now()
			tick := time.NewTicker(60 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					h.startup(ctx)
					if time.Since(lastSweep) >= 6*time.Hour {
						h.sweep(ctx, time.Now())
						lastSweep = time.Now()
					}
				}
			}
		}()
	})
}
func (h *Handler) startup(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ok, e := h.core.BucketExists(ctx, h.cfg.Bucket)
	if e == nil && !ok {
		e = errors.New("configured bucket missing")
	}
	if e == nil {
		c, stop := context.WithCancel(ctx)
		for info := range h.core.Client.ListObjects(c, h.cfg.Bucket, minio.ListObjectsOptions{Prefix: h.cfg.Prefix, Recursive: true, MaxKeys: 1}) {
			e = info.Err
			break
		}
		stop()
	}
	if e == nil {
		e = h.policy.Refresh(ctx)
	}
	h.mu.Lock()
	h.healthy = e == nil
	h.mu.Unlock()
	status := "ok"
	if e != nil {
		status = backendCode(e)
		log.Printf("storage service %s read-only startup check failed: %s", h.name, status)
	}
	h.record(AuditEntry{Origin: "local", Op: "startup_check", Status: status})
}
func (h *Handler) sweep(ctx context.Context, now time.Time) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	keyMarker, idMarker := "", ""
	for pages := 0; pages < 10000; pages++ {
		result, e := h.core.ListMultipartUploads(ctx, h.cfg.Bucket, h.cfg.Prefix, keyMarker, idMarker, "", 1000)
		if e != nil {
			h.mu.Lock()
			warn := !h.warnedCleanup
			h.warnedCleanup = true
			h.mu.Unlock()
			if warn {
				log.Printf("storage service %s cannot list incomplete multipart uploads; use a bucket lifecycle cleanup rule", h.name)
			}
			h.record(AuditEntry{Origin: "local", Op: "multipart_cleanup", Status: backendCode(e)})
			return
		}
		if len(result.Uploads) > 1000 {
			return
		}
		for _, u := range result.Uploads {
			if !strings.HasPrefix(u.Key, h.cfg.Prefix) || u.Initiated.After(now.Add(-h.age)) {
				continue
			}
			relative := strings.TrimPrefix(u.Key, h.cfg.Prefix)
			checkRelative := relative
			prefixBytes := len(h.cfg.Prefix)
			if strings.HasPrefix(relative, protocol.ReservedKeySegment+"/") {
				checkRelative = strings.TrimPrefix(relative, protocol.ReservedKeySegment+"/")
				prefixBytes += len(protocol.ReservedKeySegment + "/")
			}
			if protocol.ValidateRelativeKey(checkRelative, prefixBytes, false) != nil {
				continue
			}
			h.mu.Lock()
			active := h.active[u.UploadID]
			h.mu.Unlock()
			if active {
				continue
			}
			e := h.core.AbortMultipartUpload(ctx, h.cfg.Bucket, u.Key, u.UploadID)
			status := "ok"
			if e != nil {
				status = backendCode(e)
			}
			h.record(AuditEntry{Origin: "local", Op: "multipart_cleanup", KeyHash: h.hash(relative), Status: status, UploadAgeSeconds: int64(now.Sub(u.Initiated).Seconds())})
		}
		if !result.IsTruncated {
			return
		}
		if result.NextKeyMarker == keyMarker && result.NextUploadIDMarker == idMarker {
			return
		}
		if !strings.HasPrefix(result.NextKeyMarker, h.cfg.Prefix) {
			return
		}
		keyMarker, idMarker = result.NextKeyMarker, result.NextUploadIDMarker
	}
}

// Check performs operator-side read-only checks unless writeProbe is explicitly true.
func (h *Handler) Check(parent context.Context, writeProbe bool) ([]CheckResult, error) {
	h.warnings()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	out := []CheckResult{}
	appendCheck := func(name string, e error, warning bool) {
		r := CheckResult{Name: name, OK: e == nil, Warning: warning}
		if e != nil {
			r.Message = backendCode(e)
		}
		out = append(out, r)
		status := "ok"
		if e != nil {
			status = r.Message
		}
		h.record(AuditEntry{Origin: "local", Op: name, Status: status})
	}
	exists, e := h.core.BucketExists(ctx, h.cfg.Bucket)
	if e == nil && !exists {
		e = errors.New("bucket missing")
	}
	appendCheck("head_bucket", e, false)
	if e != nil {
		return out, protocol.NewStorageError(backendCode(e))
	}
	c, stop := context.WithCancel(ctx)
	var existing *minio.ObjectInfo
	for info := range h.core.Client.ListObjects(c, h.cfg.Bucket, minio.ListObjectsOptions{Prefix: h.cfg.Prefix, Recursive: true, MaxKeys: 1}) {
		e = info.Err
		if e == nil && strings.HasPrefix(info.Key, h.cfg.Prefix) && protocol.ValidateRelativeKey(strings.TrimPrefix(info.Key, h.cfg.Prefix), len(h.cfg.Prefix), false) == nil && info.Size > 0 {
			existing = &info
		}
		break
	}
	stop()
	appendCheck("list_objects", e, false)
	if e != nil {
		return out, protocol.NewStorageError(backendCode(e))
	}
	if existing != nil {
		_, e = h.stat(ctx, existing.Key)
		appendCheck("existing_encryption", e, false)
		if e != nil && h.cfg.Encryption == "sse-c" {
			out[len(out)-1].Message = "unable to verify configured encryption key; check key and permissions"
		}
		if e != nil {
			return out, protocol.NewStorageError(backendCode(e))
		}
	}
	_, e = h.core.ListBuckets(ctx)
	appendCheck("list_buckets_overprivileged", e, e == nil)
	if e == nil {
		out[len(out)-1].Message = "credential can list buckets; reduce permissions"
	}
	_, e = h.core.ListMultipartUploads(ctx, h.cfg.Bucket, h.cfg.Prefix, "", "", "", 1)
	appendCheck("multipart_cleanup_permission", e, e != nil)
	out = append(out, CheckResult{Name: "lifecycle_reminder", OK: true, Warning: true, Message: "configure AbortIncompleteMultipartUpload lifecycle under the prefix"})
	if writeProbe {
		e = h.probe(ctx)
		appendCheck("write_probe", e, false)
		if e != nil {
			return out, protocol.NewStorageError(backendCode(e))
		}
	}
	return out, nil
}
func (h *Handler) probe(ctx context.Context) (err error) {
	random := make([]byte, 16)
	if _, e := rand.Read(random); e != nil {
		return e
	}
	key := h.cfg.Prefix + protocol.ReservedKeySegment + "/probe/" + hex.EncodeToString(random)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		e := h.core.RemoveObject(cleanup, h.cfg.Bucket, key, minio.RemoveObjectOptions{})
		if e != nil {
			h.record(AuditEntry{Origin: "local", Op: "probe_cleanup", KeyHash: h.hash(strings.TrimPrefix(key, h.cfg.Prefix)), Status: backendCode(e)})
			if err == nil {
				err = e
			}
		}
	}()
	data := make([]byte, 128)
	if _, e := rand.Read(data); e != nil {
		return e
	}
	var wire bytes.Buffer
	writer := protocol.NewStorageBodyWriter(&wire, int64(len(data)))
	writer.Write(data)
	writer.Finish()
	reader := protocol.NewStorageBodyReader(bufio.NewReader(&wire), int64(len(data)), h.max)
	if _, e := h.upload(ctx, key, "application/octet-stream", reader, int64(len(data))); e != nil {
		return e
	}
	body, _, _, e := h.core.GetObject(ctx, h.cfg.Bucket, key, minio.GetObjectOptions{ServerSideEncryption: h.sse})
	if e != nil {
		return e
	}
	got, e := io.ReadAll(io.LimitReader(body, int64(len(data))+1))
	body.Close()
	if e != nil || !bytes.Equal(got, data) {
		return errBackendInvalid
	}
	if h.cfg.Encryption == "sse-c" {
		plain, _, _, e := h.core.GetObject(ctx, h.cfg.Bucket, key, minio.GetObjectOptions{})
		if e == nil {
			plain.Close()
			return errors.New("backend returned SSE-C object without key")
		}
	}
	if e = h.core.RemoveObject(ctx, h.cfg.Bucket, key, minio.RemoveObjectOptions{}); e != nil {
		return e
	}
	_, e = h.core.StatObject(ctx, h.cfg.Bucket, key, minio.StatObjectOptions{ServerSideEncryption: h.sse})
	if e == nil || backendCode(e) != "not_found" {
		return errBackendInvalid
	}
	return nil
}
