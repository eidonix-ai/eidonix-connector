package protocol

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"time"
)

const StorageEOFTimeout = 30 * time.Second
const MaxUploadLifetime = 6 * time.Hour

var ErrStorageProtocol = errors.New("invalid storage body protocol")

type StorageTrailer struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func ReadStorageFrame(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return nil, e
	}
	n := binary.BigEndian.Uint32(h[:])
	if n > MaxStorageFrameBytes {
		return nil, ErrStorageProtocol
	}
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, int(n))
	_, e := io.ReadFull(r, b)
	return b, e
}
func WriteStorageFrame(w io.Writer, b []byte) error {
	if len(b) > MaxStorageFrameBytes {
		return ErrStorageProtocol
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	if e := writeAll(w, h[:]); e != nil {
		return e
	}
	return writeAll(w, b)
}
func validTrailer(t StorageTrailer) bool {
	if t.Bytes < 0 || t.Bytes > MaxStorageObjectBytes || len(t.SHA256) != 64 {
		return false
	}
	for _, c := range t.SHA256 {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func ReadStorageTrailer(r *bufio.Reader) (StorageTrailer, error) {
	var raw json.RawMessage
	if e := ReadJSONLine(r, MaxStorageTrailerBytes, &raw); e != nil {
		return StorageTrailer{}, e
	}
	var required struct {
		Bytes  *int64  `json:"bytes"`
		SHA256 *string `json:"sha256"`
	}
	if e := json.Unmarshal(raw, &required); e != nil || required.Bytes == nil || required.SHA256 == nil {
		return StorageTrailer{}, ErrStorageProtocol
	}
	t := StorageTrailer{*required.Bytes, *required.SHA256}
	if !validTrailer(t) {
		return t, ErrStorageProtocol
	}
	return t, nil
}
func WriteStorageTrailer(w io.Writer, t StorageTrailer) error {
	if !validTrailer(t) {
		return ErrStorageProtocol
	}
	return WriteJSONLine(w, MaxStorageTrailerBytes, t)
}

// StorageBodyReader returns EOF only after a verified trailer and sender FIN.
// The optional deadline hook must be supplied on live streams to bound FIN wait.
type StorageBodyReader struct {
	r                *bufio.Reader
	expected, max, n int64
	hash             hash.Hash
	remaining        []byte
	done             bool
	err              error
	deadline         func(time.Time) error
}

func NewStorageBodyReader(r *bufio.Reader, expected, max int64, deadline ...func(time.Time) error) *StorageBodyReader {
	b := &StorageBodyReader{r: r, expected: expected, max: max, hash: sha256.New()}
	if len(deadline) > 0 {
		b.deadline = deadline[0]
	}
	if expected < -1 || max < 0 || max > MaxStorageObjectBytes || expected > max {
		b.err = ErrStorageProtocol
	}
	return b
}
func (b *StorageBodyReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	if b.done {
		return 0, io.EOF
	}
	if len(b.remaining) == 0 {
		frame, e := ReadStorageFrame(b.r)
		if e != nil {
			e = fmt.Errorf("%w: %w", ErrStorageProtocol, e)
			b.err = e
			return 0, e
		}
		if len(frame) == 0 {
			b.err = b.finish()
			if b.err != nil {
				return 0, b.err
			}
			b.done = true
			return 0, io.EOF
		}
		if int64(len(frame)) > b.max-b.n {
			b.err = NewStorageError("too_large")
			return 0, b.err
		}
		if b.expected >= 0 && int64(len(frame)) > b.expected-b.n {
			b.err = NewStorageError("size_mismatch")
			return 0, b.err
		}
		b.n += int64(len(frame))
		b.hash.Write(frame)
		b.remaining = frame
	}
	n := copy(p, b.remaining)
	b.remaining = b.remaining[n:]
	return n, nil
}
func (b *StorageBodyReader) finish() error {
	trailer, e := ReadStorageTrailer(b.r)
	if e != nil {
		e = fmt.Errorf("%w: %w", ErrStorageProtocol, e)
		return e
	}
	if trailer.Bytes != b.n || b.expected >= 0 && b.n != b.expected {
		return NewStorageError("size_mismatch")
	}
	if hex.EncodeToString(b.hash.Sum(nil)) != trailer.SHA256 {
		return NewStorageError("checksum_mismatch")
	}
	if b.deadline != nil {
		if e = b.deadline(time.Now().Add(StorageEOFTimeout)); e != nil {
			return e
		}
	}
	_, e = b.r.ReadByte()
	if e == nil {
		return ErrStorageProtocol
	}
	if e != io.EOF {
		var ne net.Error
		if errors.As(e, &ne) && ne.Timeout() {
			return NewStorageError("timeout")
		}
		return e
	}
	return nil
}
func (b *StorageBodyReader) Bytes() int64   { return b.n }
func (b *StorageBodyReader) Verified() bool { return b.done && b.err == nil }

type StorageBodyWriter struct {
	w      io.Writer
	max, n int64
	hash   hash.Hash
	done   bool
	err    error
}

func NewStorageBodyWriter(w io.Writer, max int64) *StorageBodyWriter {
	b := &StorageBodyWriter{w: w, max: max, hash: sha256.New()}
	if max < 0 || max > MaxStorageObjectBytes {
		b.err = ErrStorageProtocol
	}
	return b
}
func (b *StorageBodyWriter) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.done {
		return 0, ErrStorageProtocol
	}
	if int64(len(p)) > b.max-b.n {
		return 0, NewStorageError("too_large")
	}
	n := 0
	for len(p) > 0 {
		count := len(p)
		if count > MaxStorageFrameBytes {
			count = MaxStorageFrameBytes
		}
		if e := WriteStorageFrame(b.w, p[:count]); e != nil {
			b.err = e
			return n, e
		}
		b.hash.Write(p[:count])
		b.n += int64(count)
		n += count
		p = p[count:]
	}
	return n, nil
}

// Finish writes marker and trailer. The caller must then half-close its stream.
func (b *StorageBodyWriter) Finish() error {
	if b.err != nil {
		return b.err
	}
	if b.done {
		return ErrStorageProtocol
	}
	b.done = true
	if e := WriteStorageFrame(b.w, nil); e != nil {
		b.err = e
		return e
	}
	b.err = WriteStorageTrailer(b.w, StorageTrailer{b.n, hex.EncodeToString(b.hash.Sum(nil))})
	return b.err
}
func (b *StorageBodyWriter) Bytes() int64 { return b.n }
