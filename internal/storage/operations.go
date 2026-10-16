package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/eidonix-ai/cletus-connector/protocol"
	"github.com/minio/minio-go/v7"
	"io"
	"mime"
	"strings"
	"time"
)

func minioRemoveOptions() minio.RemoveObjectOptions { return minio.RemoveObjectOptions{} }
func (h *Handler) putOptions(contentType string, size int64) minio.PutObjectOptions {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	sse := h.sse
	if size == 0 && h.cfg.Encryption.Mode == "sse-c" {
		sse = nil
	}
	return minio.PutObjectOptions{ContentType: contentType, ServerSideEncryption: sse, DisableContentSha256: false}
}
func validInfo(info minio.ObjectInfo, max int64) bool {
	if _, _, e := mime.ParseMediaType(info.ContentType); info.ContentType != "" && e != nil {
		return false
	}
	for _, r := range info.ETag + info.ContentType {
		if r < 32 || r > 126 {
			return false
		}
	}
	return info.Size >= 0 && info.Size <= max && len(info.ETag) <= 256 && len(info.ContentType) <= 255 && !strings.ContainsAny(info.ETag+info.ContentType, "\r\n\x00")
}
func (h *Handler) stat(ctx context.Context, key string) (minio.ObjectInfo, error) {
	info, e := h.core.StatObject(ctx, h.cfg.Bucket, key, minio.StatObjectOptions{ServerSideEncryption: h.sse})
	if e != nil && h.cfg.Encryption.Mode == "sse-c" {
		empty, err := h.core.StatObject(ctx, h.cfg.Bucket, key, minio.StatObjectOptions{})
		if err == nil && empty.Size == 0 {
			info, e = empty, nil
		}
	}
	if e == nil && !validInfo(info, h.max) {
		return info, errBackendInvalid
	}
	return info, e
}
func (h *Handler) head(ctx context.Context, key string) (protocol.StorageResult, error) {
	info, e := h.stat(ctx, key)
	if e != nil {
		return protocol.StorageResult{}, e
	}
	return infoResult(info), nil
}
func infoResult(info minio.ObjectInfo) protocol.StorageResult {
	return protocol.StorageResult{V: protocol.Version, OK: true, Size: &info.Size, ETag: info.ETag, ContentType: info.ContentType, LastModified: info.LastModified.UTC().Format(time.RFC3339)}
}
func (h *Handler) get(ctx context.Context, key string, header protocol.StreamHeader) (protocol.StorageResult, io.ReadCloser, int64, error) {
	info, e := h.stat(ctx, key)
	if e != nil {
		return protocol.StorageResult{}, nil, 0, e
	}
	n := info.Size
	opts := minio.GetObjectOptions{ServerSideEncryption: h.sse}
	if h.cfg.Encryption.Mode == "sse-c" && n == 0 {
		opts.ServerSideEncryption = nil
	}
	if header.IfMatch != "" {
		e = opts.SetMatchETag(header.IfMatch)
	} else if info.ETag != "" {
		e = opts.SetMatchETag(info.ETag)
	}
	if e != nil {
		return protocol.StorageResult{}, nil, 0, protocol.NewStorageError("invalid_request")
	}
	result := infoResult(info)
	if len(header.Range) == 2 {
		if n == 0 || header.Range[0] >= n {
			return result, nil, 0, protocol.NewStorageError("invalid_request")
		}
		end := header.Range[1]
		if end >= n {
			end = n - 1
		}
		n = end - header.Range[0] + 1
		opts.SetRange(header.Range[0], end)
		result.Range = []int64{header.Range[0], end}
	}
	if !h.readBudget(n) {
		return result, nil, 0, protocol.NewStorageError("rate_limited")
	}
	reader, got, _, e := h.core.GetObject(ctx, h.cfg.Bucket, key, opts)
	if e != nil {
		return result, nil, 0, e
	}
	if got.ETag != info.ETag || header.IfMatch != "" && header.IfMatch != "*" && strings.Trim(header.IfMatch, "\"") != got.ETag {
		reader.Close()
		return result, nil, 0, protocol.NewStorageError("precondition_failed")
	}
	if !validInfo(got, h.max) || got.Size != n {
		reader.Close()
		return result, nil, 0, errBackendInvalid
	}
	return result, reader, n, nil
}
func (h *Handler) upload(ctx context.Context, key, contentType string, reader io.Reader, expected int64) (info minio.UploadInfo, err error) {
	buf := make([]byte, int(h.part))
	var uploadID string
	parts := []minio.CompletePart{}
	var total int64
	defer func() {
		info.Size = total
		if uploadID != "" {
			h.mu.Lock()
			delete(h.active, uploadID)
			h.mu.Unlock()
			if err != nil {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				abortErr := h.core.AbortMultipartUpload(cleanup, h.cfg.Bucket, key, uploadID)
				cancel()
				status := "ok"
				if abortErr != nil {
					status = backendCode(abortErr)
				}
				h.record(AuditEntry{Origin: "local", Op: "abort_multipart", KeyHash: h.hash(strings.TrimPrefix(key, h.cfg.Prefix)), Status: status})
			}
		}
	}()
	for {
		n, e := readPart(reader, buf)
		total += int64(n)
		if total > h.max {
			return info, protocol.NewStorageError("too_large")
		}
		if e != nil && e != io.EOF {
			return info, e
		}
		final := e == io.EOF
		if final {
			if v, ok := reader.(interface{ Verified() bool }); ok && !v.Verified() {
				return info, protocol.NewStorageError("invalid_request")
			}
		}
		if final && expected >= 0 && total != expected {
			return info, protocol.NewStorageError("size_mismatch")
		}
		if uploadID == "" && final {
			sum := sha256.Sum256(buf[:n])
			info, err = h.core.PutObject(ctx, h.cfg.Bucket, key, bytes.NewReader(buf[:n]), int64(n), "", hex.EncodeToString(sum[:]), h.putOptions(contentType, int64(n)))
			info.Size = total
			if err == nil && len(info.ETag) > 256 {
				err = errBackendInvalid
			}
			return info, err
		}
		if uploadID == "" {
			uploadID, err = h.core.NewMultipartUpload(ctx, h.cfg.Bucket, key, h.putOptions(contentType, -1))
			if err != nil {
				return info, err
			}
			if len(uploadID) > 1024 || uploadID == "" {
				return info, errBackendInvalid
			}
			h.mu.Lock()
			h.active[uploadID] = true
			h.mu.Unlock()
		}
		if n > 0 {
			if len(parts) >= 10000 {
				return info, protocol.NewStorageError("too_large")
			}
			sum := sha256.Sum256(buf[:n])
			part, e := h.core.PutObjectPart(ctx, h.cfg.Bucket, key, uploadID, len(parts)+1, bytes.NewReader(buf[:n]), int64(n), minio.PutObjectPartOptions{SSE: h.sse, Sha256Hex: hex.EncodeToString(sum[:])})
			if e != nil {
				return info, e
			}
			if len(part.ETag) > 256 {
				return info, errBackendInvalid
			}
			parts = append(parts, minio.CompletePart{PartNumber: len(parts) + 1, ETag: part.ETag})
		}
		if final {
			info, err = h.core.CompleteMultipartUpload(ctx, h.cfg.Bucket, key, uploadID, parts, h.putOptions(contentType, total))
			info.Size = total
			if err == nil && len(info.ETag) > 256 {
				err = errBackendInvalid
			}
			return info, err
		}
	}
}
func (h *Handler) copy(ctx context.Context, src, dst string) (protocol.StorageResult, error) {
	ctx, cancel := context.WithTimeout(ctx, h.lifetime)
	defer cancel()
	result, reader, n, e := h.get(ctx, src, protocol.StreamHeader{})
	if e != nil {
		return protocol.StorageResult{}, e
	}
	defer reader.Close()
	verified := &exactReader{r: reader, left: n}
	info, e := h.upload(ctx, dst, result.ContentType, verified, n)
	if e != nil {
		return protocol.StorageResult{}, e
	}
	return protocol.StorageResult{V: protocol.Version, OK: true, Size: &info.Size, ETag: info.ETag}, nil
}

type exactReader struct {
	r    io.Reader
	left int64
}

func (r *exactReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		var probe [1]byte
		n, e := r.r.Read(probe[:])
		if n > 0 {
			return 0, errBackendInvalid
		}
		return 0, e
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, e := r.r.Read(p)
	r.left -= int64(n)
	if e == io.EOF && r.left != 0 {
		return n, errBackendInvalid
	}
	return n, e
}
func (h *Handler) list(ctx context.Context, key string, header protocol.StreamHeader) (protocol.StorageResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := ""
	if header.Token != "" {
		decoded, e := protocol.DecodeStorageKey(header.Token, len(h.cfg.Prefix), false)
		if e != nil {
			var v *protocol.StorageError
			if errors.As(e, &v) && v.Code == "key_too_long" {
				return protocol.StorageResult{}, e
			}
			return protocol.StorageResult{}, protocol.NewStorageError("invalid_request")
		}
		start = h.cfg.Prefix + decoded
	}
	result := protocol.StorageResult{V: protocol.Version, OK: true, Objects: []protocol.StorageObject{}}
	last := ""
	encodedBytes := 128 + 1400 // Reserve the envelope and largest continuation token.
	scanned := 0
	for info := range h.core.Client.ListObjects(ctx, h.cfg.Bucket, minio.ListObjectsOptions{Prefix: h.cfg.Prefix + key, Recursive: header.Recursive, StartAfter: start, MaxKeys: header.Max}) {
		if info.Err != nil {
			return result, info.Err
		}
		scanned++
		if scanned > 10000 {
			return result, errBackendInvalid
		}
		if !strings.HasPrefix(info.Key, h.cfg.Prefix+key) {
			continue
		}
		relative := strings.TrimPrefix(info.Key, h.cfg.Prefix)
		if protocol.ValidateRelativeKey(relative, len(h.cfg.Prefix), false) != nil {
			continue
		}
		if !validInfo(info, h.max) {
			return result, errBackendInvalid
		}
		if len(result.Objects) >= header.Max {
			result.Truncated = true
			result.NextToken = protocol.EncodeStorageKey(last)
			break
		}
		entry := protocol.StorageObject{Key: protocol.EncodeStorageKey(relative), Size: info.Size, ETag: info.ETag, ContentType: info.ContentType, LastModified: info.LastModified.UTC().Format(time.RFC3339), Dir: strings.HasSuffix(info.Key, "/") && info.Size == 0}
		encoded, _ := json.Marshal(entry)
		if encodedBytes+len(encoded)+1 > protocol.MaxStorageListResultBytes {
			result.Truncated = true
			result.NextToken = protocol.EncodeStorageKey(last)
			break
		}
		result.Objects = append(result.Objects, entry)
		encodedBytes += len(encoded) + 1
		last = relative
	}
	return result, nil
}

// readPart preserves the source error; io.ReadFull conflates a valid short final
// part with a truncated wire body reporting io.ErrUnexpectedEOF.
func readPart(r io.Reader, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		count, e := r.Read(b[n:])
		n += count
		if e != nil {
			return n, e
		}
		if count == 0 {
			return n, io.ErrNoProgress
		}
	}
	return n, nil
}
