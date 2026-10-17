package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

// moveProgressEvery is how often MoveToRoot reports progress.
var moveProgressEvery = 100

// MoveToRoot moves every object under the folder from (for example
// "cletus/") to the same key without the folder, inside the service's bucket:
// a server-side copy of each object (with the service's SSE-C key when it
// uses sse-c), a check that every copy is there with its size, then the
// deletion of the originals. It refuses, changing nothing, when any
// destination key already exists. A failed copy removes the copies made so
// far. With dryRun it only reports what it would do.
func MoveToRoot(ctx context.Context, svc config.Service, from string, dryRun bool, out io.Writer) error {
	if svc.Storage == nil {
		return fmt.Errorf("service %q is not an s3 service", svc.Name)
	}
	if !strings.HasSuffix(from, "/") || protocol.ValidateRelativeKey(from, 0, false) != nil {
		return fmt.Errorf("--from %q must be a folder such as cletus/ (ending in /)", from)
	}
	if svc.Storage.ReadOnly && !dryRun {
		return fmt.Errorf("service %q is read-only", svc.Name)
	}
	h, err := New(ctx, svc, nil)
	if err != nil {
		return err
	}
	defer h.Close()
	bucket := h.cfg.Bucket
	if ok, err := h.core.BucketExists(ctx, bucket); err != nil || !ok {
		if err == nil {
			err = errors.New("bucket missing")
		}
		return fmt.Errorf("bucket %s: %s", bucket, backendCode(err))
	}
	all, err := h.listAll(ctx)
	if err != nil {
		return fmt.Errorf("listing bucket %s: %s", bucket, backendCode(err))
	}
	var sources, markers []string
	var total int64
	for key, size := range all {
		switch {
		case !strings.HasPrefix(key, from):
		case key == from:
			markers = append(markers, key) // the folder's own marker object
		default:
			sources = append(sources, key)
			total += size
		}
	}
	sort.Strings(sources)
	var collisions []string
	for _, src := range sources {
		if _, exists := all[strings.TrimPrefix(src, from)]; exists {
			collisions = append(collisions, strings.TrimPrefix(src, from))
		}
	}
	fmt.Fprintf(out, "Bucket %s: %d objects (%s) under %s.\n", bucket, len(sources), byteCount(total), from)
	if len(collisions) > 0 {
		shown := collisions
		if len(shown) > 10 {
			shown = shown[:10]
		}
		return fmt.Errorf("refusing: %d destination keys already exist (%s); nothing was changed", len(collisions), strings.Join(shown, ", "))
	}
	if len(sources) == 0 && len(markers) == 0 {
		fmt.Fprintln(out, "Nothing to move.")
		return nil
	}
	if dryRun {
		for i, src := range sources {
			if i == 3 {
				fmt.Fprintf(out, "  … and %d more\n", len(sources)-3)
				break
			}
			fmt.Fprintf(out, "  %s → %s\n", src, strings.TrimPrefix(src, from))
		}
		fmt.Fprintln(out, "Dry run: nothing was changed.")
		return nil
	}
	// Copy.
	var copied []string
	for i, src := range sources {
		dst := strings.TrimPrefix(src, from)
		if err := h.copyWithin(ctx, src, dst, all[src]); err != nil {
			code := backendCode(err)
			removed := 0
			for _, k := range copied {
				if h.core.RemoveObject(context.Background(), bucket, k, minio.RemoveObjectOptions{}) == nil {
					removed++
				}
			}
			return fmt.Errorf("copying object %d of %d failed (%s); removed %d of the %d copies made, kept every original; nothing else was changed", i+1, len(sources), code, removed, len(copied))
		}
		copied = append(copied, dst)
		if (i+1)%moveProgressEvery == 0 || i+1 == len(sources) {
			fmt.Fprintf(out, "Copied %d/%d.\n", i+1, len(sources))
		}
	}
	// Verify every copy before anything is deleted.
	after, err := h.listAll(ctx)
	if err != nil {
		return fmt.Errorf("verifying: listing bucket %s: %s; nothing was deleted", bucket, backendCode(err))
	}
	for _, src := range sources {
		dst := strings.TrimPrefix(src, from)
		if size, ok := after[dst]; !ok || size != all[src] {
			return fmt.Errorf("verifying: a copy is missing or has the wrong size; nothing was deleted (both copies remain)")
		}
	}
	fmt.Fprintf(out, "Verified %d copies (%s).\n", len(sources), byteCount(total))
	// Delete the originals.
	originals := append(sources, markers...)
	for i, src := range originals {
		if err := h.core.RemoveObject(ctx, bucket, src, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("removing original %d of %d failed (%s); the copies are complete, %d originals remain under %s", i+1, len(originals), backendCode(err), len(originals)-i, from)
		}
		if (i+1)%moveProgressEvery == 0 || i+1 == len(originals) {
			fmt.Fprintf(out, "Removed %d/%d originals.\n", i+1, len(originals))
		}
	}
	fmt.Fprintf(out, "Moved %d objects from %s to the top of bucket %s.\n", len(sources), from, bucket)
	return nil
}

// listAll returns every object in the bucket with its size.
func (h *Handler) listAll(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for info := range h.core.Client.ListObjects(ctx, h.cfg.Bucket, minio.ListObjectsOptions{Recursive: true}) {
		if info.Err != nil {
			return nil, info.Err
		}
		out[info.Key] = info.Size
	}
	return out, nil
}

// copyWithin copies src to dst inside the bucket on the server, keeping the
// object's metadata and encryption. Empty objects are stored unencrypted
// under sse-c (as uploads store them), so they are copied without the key
// first.
func (h *Handler) copyWithin(ctx context.Context, src, dst string, size int64) error {
	copyWith := func(sse encrypt.ServerSide) error {
		source := minio.CopySrcOptions{Bucket: h.cfg.Bucket, Object: src}
		if sse != nil && sse.Type() == encrypt.SSEC {
			source.Encryption = encrypt.SSECopy(sse)
		}
		_, err := h.core.Client.CopyObject(ctx, minio.CopyDestOptions{Bucket: h.cfg.Bucket, Object: dst, Encryption: sse}, source)
		return err
	}
	if h.cfg.Encryption == "sse-c" && size == 0 {
		if copyWith(nil) == nil {
			return nil
		}
	}
	return copyWith(h.sse)
}

func byteCount(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", n)
}
