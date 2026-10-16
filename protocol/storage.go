package protocol

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	CapabilityInference               = "inference/1"
	CapabilityStorage                 = "storage/1"
	MaxStorageFrameBytes              = 1 << 20
	MaxStorageTrailerBytes            = 256
	MaxStorageResultBytes             = 4 << 10
	MaxStorageListResultBytes         = 1 << 20
	MaxStorageListEntries             = 1000
	MaxStorageErrorMessageBytes       = 256
	MaxStorageETagBytes               = 256
	MaxStorageContentTypeBytes        = 255
	MaxStorageObjectBytes       int64 = 5 << 30
	MaxStorageKeyBytes                = 1023
	MaxStorageFullKeyBytes            = 1024
	MaxStoragePrefixBytes             = 256
	MinStoragePartBytes               = 5 << 20
	MaxStoragePartBytes               = 64 << 20
	DefaultStoragePartBytes           = 16 << 20
	MaxStorageParts                   = 10000
)

// StorageService is the storage block of an advertised storage service.
// Encryption is the owner's choice: none, sse-s3, sse-kms or sse-c.
type StorageService struct {
	MaxKeyBytes    int    `json:"max_key_bytes"`
	MaxObjectBytes int64  `json:"max_object_bytes"`
	Encryption     string `json:"encryption"`
}

// ReservedKeySegment is the first key segment reserved for connector-local probes.
const ReservedKeySegment = ".eidonix-connector"

type StorageObject struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	Dir          bool   `json:"dir"`
}
type StorageResult struct {
	V            int             `json:"v"`
	OK           bool            `json:"ok"`
	Error        *StorageError   `json:"error,omitempty"`
	Size         *int64          `json:"size,omitempty"`
	ETag         string          `json:"etag,omitempty"`
	ContentType  string          `json:"content_type,omitempty"`
	LastModified string          `json:"last_modified,omitempty"`
	Range        []int64         `json:"range,omitempty"`
	Objects      []StorageObject `json:"objects,omitempty"`
	Truncated    bool            `json:"truncated,omitempty"`
	NextToken    string          `json:"next_token,omitempty"`
}
type StorageError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *StorageError) Error() string { return e.Code }

// NewStorageError returns the closed-set error for code ("internal" if unknown).
func NewStorageError(code string) *StorageError {
	if !ValidErrorCode(code) {
		code = "internal"
	}
	return &StorageError{code, ErrorMessage(code)}
}
func StorageErrorResult(code string) StorageResult {
	return StorageResult{V: Version, Error: NewStorageError(code)}
}
func EncodeStorageKey(key string) string { return base64.RawURLEncoding.EncodeToString([]byte(key)) }
func DecodeStorageKey(encoded string, prefixBytes int, allowEmpty bool) (string, error) {
	if len(encoded) > base64.RawURLEncoding.EncodedLen(MaxStorageKeyBytes) {
		return "", NewStorageError("key_too_long")
	}
	b, e := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if e != nil || base64.RawURLEncoding.EncodeToString(b) != encoded {
		return "", NewStorageError("invalid_key")
	}
	key := string(b)
	return key, ValidateRelativeKey(key, prefixBytes, allowEmpty)
}
func ValidateRelativeKey(key string, prefixBytes int, allowEmpty bool) error {
	if len(key) > MaxStorageKeyBytes || prefixBytes < 0 || prefixBytes+len(key) > MaxStorageFullKeyBytes {
		return NewStorageError("key_too_long")
	}
	if (!allowEmpty && key == "") || !utf8.ValidString(key) || strings.HasPrefix(key, "/") || strings.Contains(key, "//") || strings.Contains(key, "\\") {
		return NewStorageError("invalid_key")
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return NewStorageError("invalid_key")
		}
	}
	parts := strings.Split(key, "/")
	if parts[0] == ReservedKeySegment {
		return NewStorageError("invalid_key")
	}
	for _, part := range parts {
		if part == "." || part == ".." {
			return NewStorageError("invalid_key")
		}
	}
	return nil
}

var storageRequestPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
var storageTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]*$`)

func validStorageToken(s string) bool { return len(s) <= 1400 && storageTokenPattern.MatchString(s) }
func printable(s string, max int) bool {
	if len(s) > max {
		return false
	}
	for _, b := range []byte(s) {
		if b < 32 || b > 126 {
			return false
		}
	}
	return true
}
func mediaType(s string) bool {
	if !printable(s, MaxStorageContentTypeBytes) {
		return false
	}
	if s == "" {
		return true
	}
	_, _, e := mime.ParseMediaType(s)
	return e == nil
}
func validRange(r []int64) bool { return len(r) == 2 && r[0] >= 0 && r[1] >= r[0] }
func ValidateStorageHeader(h StreamHeader, maxObject int64, prefixBytes int) error {
	if h.V != Version || h.Kind != "storage" || !ValidServiceName(h.Service) || !storageRequestPattern.MatchString(h.RequestID) || maxObject < 0 || maxObject > MaxStorageObjectBytes {
		return NewStorageError("invalid_request")
	}
	keys := []string{}
	switch h.Op {
	case "put":
		keys = []string{h.Key}
		if h.Size == nil || *h.Size < -1 {
			return NewStorageError("invalid_request")
		}
		if *h.Size > maxObject {
			return NewStorageError("too_large")
		}
		if !mediaType(h.ContentType) {
			return NewStorageError("invalid_request")
		}
	case "get":
		keys = []string{h.Key}
		if h.Range != nil && !validRange(h.Range) || !printable(h.IfMatch, MaxStorageETagBytes) {
			return NewStorageError("invalid_request")
		}
	case "head", "delete":
		keys = []string{h.Key}
	case "copy":
		keys = []string{h.SrcKey, h.DstKey}
	case "list":
		if h.Max < 1 || h.Max > MaxStorageListEntries || !validStorageToken(h.Token) {
			return NewStorageError("invalid_request")
		}
		if _, e := DecodeStorageKey(h.Prefix, prefixBytes, true); e != nil {
			return e
		}
		if h.Token != "" {
			if _, e := DecodeStorageKey(h.Token, prefixBytes, false); e != nil {
				if se, ok := e.(*StorageError); ok && se.Code == "key_too_long" {
					return e
				}
				return NewStorageError("invalid_request")
			}
		}
	default:
		return NewStorageError("invalid_request")
	}
	for _, k := range keys {
		if _, e := DecodeStorageKey(k, prefixBytes, false); e != nil {
			return e
		}
	}
	return nil
}
func resultLimit(op string) int {
	if op == "list" {
		return MaxStorageListResultBytes
	}
	return MaxStorageResultBytes
}
func ReadStorageResult(r *bufio.Reader, op string) (StorageResult, error) {
	var raw json.RawMessage
	if e := ReadJSONLine(r, resultLimit(op), &raw); e != nil {
		return StorageResult{}, e
	}
	if e := boundedStorageObjects(raw); e != nil {
		return StorageResult{}, e
	}
	var fields struct {
		V  *int  `json:"v"`
		OK *bool `json:"ok"`
	}
	if e := json.Unmarshal(raw, &fields); e != nil || fields.V == nil || fields.OK == nil {
		return StorageResult{}, fmt.Errorf("missing result fields")
	}
	if *fields.OK {
		var presence map[string]json.RawMessage
		if e := json.Unmarshal(raw, &presence); e != nil {
			return StorageResult{}, e
		}
		required := []string{}
		switch op {
		case "put", "copy":
			required = []string{"size", "etag"}
		case "get", "head":
			required = []string{"size", "etag", "content_type", "last_modified"}
		case "list":
			required = []string{"objects", "truncated"}
		}
		for _, field := range required {
			value, ok := presence[field]
			if !ok || string(value) == "null" {
				return StorageResult{}, fmt.Errorf("missing result fields")
			}
		}
	}
	var result StorageResult
	if e := json.Unmarshal(raw, &result); e != nil {
		return result, e
	}
	return result, ValidateStorageResult(result, op)
}

// Check every objects occurrence before decoding the full result, including
// duplicate fields and case variants accepted by encoding/json.
func boundedStorageObjects(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	start, e := d.Token()
	if e != nil || start != json.Delim('{') {
		return fmt.Errorf("invalid storage result")
	}
	for d.More() {
		name, e := d.Token()
		if e != nil {
			return e
		}
		if key, ok := name.(string); ok && strings.EqualFold(key, "objects") {
			start, e = d.Token()
			if e != nil {
				return e
			}
			if start == nil {
				continue
			}
			if start != json.Delim('[') {
				return fmt.Errorf("invalid storage objects")
			}
			count := 0
			for d.More() {
				if count >= MaxStorageListEntries {
					return fmt.Errorf("too many storage objects")
				}
				var object json.RawMessage
				if e = d.Decode(&object); e != nil {
					return e
				}
				count++
			}
			if _, e = d.Token(); e != nil {
				return e
			}
		} else {
			var value json.RawMessage
			if e = d.Decode(&value); e != nil {
				return e
			}
		}
	}
	_, e = d.Token()
	return e
}

func WriteStorageResult(w io.Writer, r StorageResult, op string) error {
	if e := ValidateStorageResult(r, op); e != nil {
		return e
	}
	if !r.OK {
		r.Error = NewStorageError(r.Error.Code)
		return WriteJSONLine(w, resultLimit(op), r)
	}
	encoded, e := json.Marshal(r)
	if e != nil {
		return e
	}
	var fields map[string]json.RawMessage
	if e = json.Unmarshal(encoded, &fields); e != nil {
		return e
	}
	set := func(k string, v any) { fields[k], _ = json.Marshal(v) }
	switch op {
	case "put", "copy":
		set("etag", r.ETag)
	case "get", "head":
		set("etag", r.ETag)
		set("content_type", r.ContentType)
		set("last_modified", r.LastModified)
	case "list":
		objects := r.Objects
		if objects == nil {
			objects = []StorageObject{}
		}
		set("objects", objects)
		set("truncated", r.Truncated)
	}
	return WriteJSONLine(w, resultLimit(op), fields)
}

func validTime(s string) bool {
	t, e := time.Parse(time.RFC3339, s)
	_, offset := t.Zone()
	return e == nil && offset == 0
}
func ValidateStorageResult(r StorageResult, op string) error {
	invalid := func() error { return fmt.Errorf("invalid storage result") }
	if r.V != Version {
		return invalid()
	}
	if !r.OK {
		if r.Error == nil {
			return invalid()
		}
		if !ValidErrorCode(r.Error.Code) || !printable(r.Error.Message, MaxStorageErrorMessageBytes) {
			return invalid()
		}
		return nil
	}
	if r.Error != nil || !printable(r.ETag, MaxStorageETagBytes) || !mediaType(r.ContentType) {
		return invalid()
	}
	switch op {
	case "put", "copy", "get", "head":
		if r.Size == nil || *r.Size < 0 || *r.Size > MaxStorageObjectBytes {
			return invalid()
		}
		if op == "get" || op == "head" {
			if !validTime(r.LastModified) {
				return invalid()
			}
		}
		if r.Range != nil && (op != "get" || !validRange(r.Range) || r.Range[1] >= *r.Size) {
			return invalid()
		}
	case "list":
		if len(r.Objects) > MaxStorageListEntries || !validStorageToken(r.NextToken) || r.Truncated && r.NextToken == "" || !r.Truncated && r.NextToken != "" {
			return invalid()
		}
		if r.NextToken != "" {
			if _, e := DecodeStorageKey(r.NextToken, 0, false); e != nil {
				return invalid()
			}
		}
		for _, obj := range r.Objects {
			if _, e := DecodeStorageKey(obj.Key, 0, false); e != nil {
				return invalid()
			}
			if obj.Size < 0 || obj.Size > MaxStorageObjectBytes || obj.Dir && obj.Size != 0 || !printable(obj.ETag, MaxStorageETagBytes) || !mediaType(obj.ContentType) || (obj.LastModified != "" && !validTime(obj.LastModified)) {
				return invalid()
			}
		}
	case "delete":
	default:
		return invalid()
	}
	return nil
}
