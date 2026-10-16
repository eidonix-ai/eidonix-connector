// Package models enforces a service's model allowlist on request bodies.
//
// An allowlist maps each name a caller may use to the name the server uses.
// Requests are checked (and rewritten) only in the top-level "model" field of
// a JSON object or a multipart form field named "model"; nothing else in a
// body is touched. A request naming any other model is refused before anything
// is sent upstream. A request without a model is allowed only when the list
// has exactly one entry, and is then given that model.
package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"strings"
)

var (
	// ErrInvalid: the body cannot be checked unambiguously (malformed, a
	// duplicate or differently-cased model field, a non-string model).
	ErrInvalid = errors.New("invalid_request")
	// ErrNotAllowed: the request names a model outside the allowlist.
	ErrNotAllowed = errors.New("model_not_allowed")
	// ErrRequired: the request names no model and the allowlist has several.
	ErrRequired = errors.New("model_required")
)

// maxFieldBytes bounds how much of a multipart "model" value is buffered.
const maxFieldBytes = 1024

// Policy is an immutable allowlist.
type Policy struct {
	names  []string
	target map[string]string
}

// New builds a policy from the allowed names (in order) and each name's server name.
func New(names []string, target map[string]string) *Policy {
	p := &Policy{names: append([]string(nil), names...), target: map[string]string{}}
	for _, n := range names {
		p.target[n] = n
		if t, ok := target[n]; ok && t != "" {
			p.target[n] = t
		}
	}
	return p
}

// Names returns the allowed names in configured order.
func (p *Policy) Names() []string { return append([]string(nil), p.names...) }

// Result records the model a request used and the name sent upstream.
type Result struct{ Model, Target string }

// Renamed reports whether the upstream name differs from the caller's.
func (r Result) Renamed() bool { return r.Target != r.Model }

// resolve applies the allowlist to a model name; present is false when the
// request named no model.
func (p *Policy) resolve(name string, present bool) (Result, error) {
	if !present {
		if len(p.names) != 1 {
			return Result{}, ErrRequired
		}
		name = p.names[0]
	}
	t, ok := p.target[name]
	if !ok {
		return Result{Model: name}, ErrNotAllowed
	}
	return Result{name, t}, nil
}

// RewriteJSON checks a JSON request body and returns it with the top-level
// model replaced by its server name (or added, for a one-entry list). All
// other bytes are preserved.
func (p *Policy) RewriteJSON(body []byte) ([]byte, Result, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if d, ok := tok.(json.Delim); err != nil || !ok || d != '{' {
		return nil, Result{}, ErrInvalid
	}
	open := int(dec.InputOffset())
	start, end := -1, -1
	var value json.RawMessage
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, Result{}, ErrInvalid
		}
		key, ok := tok.(string)
		if !ok {
			return nil, Result{}, ErrInvalid
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, Result{}, ErrInvalid
		}
		if strings.EqualFold(key, "model") {
			// Servers differ in case sensitivity: only one exact "model" is unambiguous.
			if key != "model" || value != nil {
				return nil, Result{}, ErrInvalid
			}
			end = int(dec.InputOffset())
			start = end - len(raw)
			value = raw
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, Result{}, ErrInvalid
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, Result{}, ErrInvalid
	}
	var name string
	if value != nil && (json.Unmarshal(value, &name) != nil || value[0] != '"') {
		return nil, Result{}, ErrInvalid
	}
	r, err := p.resolve(name, value != nil)
	if err != nil {
		return nil, r, err
	}
	encoded, err := encodeString(r.Target)
	if err != nil {
		return nil, Result{}, ErrInvalid
	}
	if value == nil {
		field := append([]byte(`"model":`), encoded...)
		rest := bytes.TrimSpace(body[open:])
		if len(rest) > 0 && rest[0] != '}' {
			field = append(field, ',')
		}
		out := append(append(append([]byte{}, body[:open]...), field...), body[open:]...)
		return out, r, nil
	}
	if start < 0 || end > len(body) || !bytes.Equal(body[start:end], value) {
		return nil, Result{}, ErrInvalid
	}
	if !r.Renamed() {
		return body, r, nil
	}
	out := make([]byte, 0, len(body)-len(value)+len(encoded))
	out = append(out, body[:start]...)
	out = append(out, encoded...)
	out = append(out, body[end:]...)
	return out, r, nil
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

// gate holds output in memory until it is opened, so nothing reaches the
// destination before the model has been accepted.
type gate struct {
	held bytes.Buffer
	dst  io.Writer
}

func (g *gate) Write(b []byte) (int, error) {
	if g.dst == nil {
		return g.held.Write(b)
	}
	return g.dst.Write(b)
}
func (g *gate) open(dst io.Writer) error {
	g.dst = dst
	_, err := dst.Write(g.held.Bytes())
	g.held = bytes.Buffer{}
	return err
}

// RewriteMultipart copies a multipart/form-data body from src to dst, keeping
// the boundary and every part's bytes, and checking (and rewriting) the
// "model" form field. Nothing is written to dst until the model is resolved:
// parts before it are held in memory, bounded by the caller's limit on src.
// ready is called exactly once, before the first byte reaches dst, with the
// result or the error that refuses the request. Parse failures wrap
// ErrInvalid; dst write failures are returned as-is.
func (p *Policy) RewriteMultipart(dst io.Writer, src io.Reader, boundary string, ready func(Result, error)) (err error) {
	decided := false
	decide := func(r Result, e error) error {
		decided = true
		ready(r, e)
		return e
	}
	defer func() {
		if !decided {
			ready(Result{}, err)
		}
	}()
	g := &gate{}
	mw := multipart.NewWriter(g)
	if boundary == "" || mw.SetBoundary(boundary) != nil {
		return ErrInvalid
	}
	mr := multipart.NewReader(src, boundary)
	var result *Result
	for {
		part, err := mr.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return invalid(err)
		}
		name := part.FormName()
		if strings.EqualFold(name, "model") && (name != "model" || result != nil || part.FileName() != "") {
			return ErrInvalid
		}
		w, err := mw.CreatePart(part.Header)
		if err != nil {
			return err
		}
		if name == "model" {
			value, err := io.ReadAll(io.LimitReader(part, maxFieldBytes+1))
			if err != nil {
				return invalid(err)
			}
			if len(value) > maxFieldBytes {
				return ErrInvalid
			}
			r, e := p.resolve(string(value), true)
			if e != nil {
				return decide(r, e)
			}
			result = &r
			if _, err := w.Write([]byte(r.Target)); err != nil {
				return err
			}
			decide(r, nil)
			if err := g.open(dst); err != nil {
				return err
			}
			continue
		}
		if _, err := copyPart(w, part); err != nil {
			return err
		}
	}
	if result == nil {
		r, e := p.resolve("", false)
		if e != nil {
			return decide(r, e)
		}
		if err := mw.WriteField("model", r.Target); err != nil {
			return err
		}
		decide(r, nil)
		if err := g.open(dst); err != nil {
			return err
		}
	}
	return mw.Close()
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

// ModelList renders the allowlist as an OpenAI-style model list.
func (p *Policy) ModelList() []byte {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	data := make([]model, 0, len(p.names))
	for _, n := range p.names {
		data = append(data, model{n, "model", 0, "eidonix-connector"})
	}
	b, _ := json.Marshal(struct {
		Object string  `json:"object"`
		Data   []model `json:"data"`
	}{"list", data})
	return b
}
