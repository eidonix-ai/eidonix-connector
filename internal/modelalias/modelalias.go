// Package modelalias translates gateway model names to the ids local upstreams serve.
//
// A route's alias table maps the name the gateway sends (alias) to the upstream
// model id (target). Requests are rewritten only in the top-level "model" field of
// a JSON object or a multipart form field named "model"; nothing else in a body is
// touched. Names that are not aliases pass through unchanged, so aliases are a
// naming convenience, not an access control.
package modelalias

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"sort"
	"strings"
)

// ErrInvalid reports a request body that cannot be rewritten unambiguously.
var ErrInvalid = errors.New("invalid_request")

// maxFieldBytes bounds how much of a multipart "model" value is buffered for lookup.
const maxFieldBytes = 1024

// Table is an immutable alias table. A nil *Table has no aliases.
type Table struct {
	forward map[string]string
	reverse map[string][]string
	names   []string
}

// New builds a table from alias -> target, or returns nil when there are no aliases.
func New(models map[string]string) *Table {
	if len(models) == 0 {
		return nil
	}
	t := &Table{forward: make(map[string]string, len(models)), reverse: map[string][]string{}}
	for alias, target := range models {
		t.forward[alias] = target
		t.reverse[target] = append(t.reverse[target], alias)
		t.names = append(t.names, alias)
	}
	sort.Strings(t.names)
	for _, v := range t.reverse {
		sort.Strings(v)
	}
	return t
}

// Lookup returns the upstream id for an alias.
func (t *Table) Lookup(name string) (string, bool) {
	if t == nil {
		return "", false
	}
	v, ok := t.forward[name]
	return v, ok
}

// Names returns the sorted alias names.
func (t *Table) Names() []string {
	if t == nil {
		return nil
	}
	return append([]string(nil), t.names...)
}

// Present maps upstream model ids to the names the gateway should see: ids with
// aliases are replaced by each of their aliases; other ids are kept.
func (t *Table) Present(ids []string) []string {
	if t == nil {
		return append([]string{}, ids...)
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if aliases, ok := t.reverse[id]; ok {
			out = append(out, aliases...)
			continue
		}
		out = append(out, id)
	}
	return out
}

// Rewrite records a performed substitution.
type Rewrite struct {
	Alias, Target string
}

// RewriteJSON validates a JSON request body and replaces a top-level string
// "model" equal to an alias with its target. All other bytes are preserved.
// Malformed JSON, trailing data and duplicate top-level model keys (compared
// after unescaping, case-insensitively) are rejected with ErrInvalid.
func (t *Table) RewriteJSON(body []byte) ([]byte, *Rewrite, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, ErrInvalid
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		// A valid non-object document has no top-level model field.
		if !json.Valid(body) {
			return nil, nil, ErrInvalid
		}
		return body, nil, nil
	}
	modelKeys := 0
	start, end := -1, -1
	var value json.RawMessage
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, nil, ErrInvalid
		}
		key, ok := tok.(string)
		if !ok {
			return nil, nil, ErrInvalid
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, nil, ErrInvalid
		}
		if strings.EqualFold(key, "model") {
			modelKeys++
			if modelKeys > 1 {
				return nil, nil, ErrInvalid
			}
			if key == "model" {
				end = int(dec.InputOffset())
				start = end - len(raw)
				value = raw
			}
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, nil, ErrInvalid
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, nil, ErrInvalid
	}
	if value == nil || len(value) == 0 || value[0] != '"' {
		return body, nil, nil
	}
	var name string
	if json.Unmarshal(value, &name) != nil {
		return nil, nil, ErrInvalid
	}
	target, ok := t.Lookup(name)
	if !ok {
		return body, nil, nil
	}
	if start < 0 || end > len(body) || !bytes.Equal(body[start:end], value) {
		return nil, nil, ErrInvalid
	}
	encoded, err := encodeString(target)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	out := make([]byte, 0, len(body)-len(value)+len(encoded))
	out = append(out, body[:start]...)
	out = append(out, encoded...)
	out = append(out, body[end:]...)
	return out, &Rewrite{name, target}, nil
}

func encodeString(s string) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// RewriteMultipart streams a multipart/form-data body from src to dst, keeping
// the boundary and every part's bytes, and replacing the value of a form field
// named "model" when it equals an alias. Parse failures, an unusable boundary and
// duplicate model fields return an error wrapping ErrInvalid; dst write failures
// are returned as-is.
func (t *Table) RewriteMultipart(dst io.Writer, src io.Reader, boundary string) (*Rewrite, error) {
	mw := multipart.NewWriter(dst)
	if boundary == "" || mw.SetBoundary(boundary) != nil {
		return nil, ErrInvalid
	}
	mr := multipart.NewReader(src, boundary)
	var done *Rewrite
	modelParts := 0
	for {
		part, err := mr.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, invalid(err)
		}
		name := part.FormName()
		if strings.EqualFold(name, "model") {
			modelParts++
			if modelParts > 1 {
				return nil, ErrInvalid
			}
		}
		w, err := mw.CreatePart(part.Header)
		if err != nil {
			return nil, err
		}
		if name == "model" && part.FileName() == "" {
			value, err := io.ReadAll(io.LimitReader(part, maxFieldBytes+1))
			if err != nil {
				return nil, invalid(err)
			}
			if target, ok := t.Lookup(string(value)); ok && len(value) <= maxFieldBytes {
				done = &Rewrite{string(value), target}
				value = []byte(target)
			}
			if _, err := w.Write(value); err != nil {
				return nil, err
			}
		}
		if _, err := copyPart(w, part); err != nil {
			return nil, err
		}
	}
	return done, mw.Close()
}

type readError struct{ error }

func (e readError) Unwrap() []error { return []error{e.error, ErrInvalid} }

func invalid(err error) error {
	var re readError
	if errors.As(err, &re) {
		return err
	}
	return readError{err}
}

// copyPart separates source (parse) errors from destination (write) errors.
func copyPart(w io.Writer, part *multipart.Part) (int64, error) {
	var n int64
	buf := make([]byte, 32<<10)
	for {
		m, rerr := part.Read(buf)
		if m > 0 {
			k, werr := w.Write(buf[:m])
			n += int64(k)
			if werr != nil {
				return n, werr
			}
		}
		if rerr == io.EOF {
			return n, nil
		}
		if rerr != nil {
			return n, invalid(rerr)
		}
	}
}

// RewriteModelList renames upstream ids in an OpenAI-style {"data":[{"id":...}]}
// list to their aliases (one entry per alias). It reports false, leaving the
// caller to forward the original bytes, when the body is not such a list.
func (t *Table) RewriteModelList(body []byte) ([]byte, bool) {
	if t == nil {
		return body, false
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil || top == nil {
		return body, false
	}
	var items []map[string]json.RawMessage
	if raw, ok := top["data"]; !ok || json.Unmarshal(raw, &items) != nil {
		return body, false
	}
	out := make([]map[string]json.RawMessage, 0, len(items))
	changed := false
	for _, item := range items {
		var id string
		if raw, ok := item["id"]; ok && json.Unmarshal(raw, &id) == nil {
			if aliases, ok := t.reverse[id]; ok {
				changed = true
				for _, alias := range aliases {
					clone := make(map[string]json.RawMessage, len(item))
					for k, v := range item {
						clone[k] = v
					}
					clone["id"], _ = encodeString(alias)
					out = append(out, clone)
				}
				continue
			}
		}
		out = append(out, item)
	}
	if !changed {
		return body, false
	}
	data, err := json.Marshal(out)
	if err != nil {
		return body, false
	}
	top["data"] = data
	result, err := json.Marshal(top)
	if err != nil {
		return body, false
	}
	return result, true
}
