package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"reflect"
	"testing"
)

var policy = New([]string{"jina-reranker", "whisper", "plain"}, map[string]string{"jina-reranker": "jina-reranker-v3", "whisper": "/models/faster-whisper-large-v3-turbo"})
var single = New([]string{"only"}, map[string]string{"only": "server-only"})

func TestRewriteJSON(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{`{"model":"jina-reranker","query":"q","documents":["a"]}`, `{"model":"jina-reranker-v3","query":"q","documents":["a"]}`},
		{"{ \"query\" : \"model\" ,\n \"model\" :  \"whisper\" , \"n\":1 }", "{ \"query\" : \"model\" ,\n \"model\" :  \"/models/faster-whisper-large-v3-turbo\" , \"n\":1 }"},
		{`{"model":"jina-reranker"}`, `{"model":"jina-reranker-v3"}`},
		{`{"model":"plain","x":{"model":"other"}}`, `{"model":"plain","x":{"model":"other"}}`},
		{`{"model":"jina-reranker"}` + "\n", `{"model":"jina-reranker-v3"}` + "\n"},
	} {
		got, _, err := policy.RewriteJSON([]byte(tc.in))
		if err != nil || string(got) != tc.out {
			t.Errorf("%s: got %s, %v", tc.in, got, err)
		}
	}
	_, r, _ := policy.RewriteJSON([]byte(`{"model":"whisper"}`))
	if r.Model != "whisper" || r.Target != "/models/faster-whisper-large-v3-turbo" || !r.Renamed() {
		t.Fatalf("%+v", r)
	}
}
func TestJSONAllowlist(t *testing.T) {
	for _, in := range []string{`{"model":"jina-reranker-v3"}`, `{"model":"other"}`, `{"model":""}`} {
		if _, _, err := policy.RewriteJSON([]byte(in)); !errors.Is(err, ErrNotAllowed) {
			t.Errorf("%s: %v", in, err)
		}
	}
	for _, in := range []string{`{}`, `{"messages":[{"model":"plain"}]}`} {
		if _, _, err := policy.RewriteJSON([]byte(in)); !errors.Is(err, ErrRequired) {
			t.Errorf("%s: %v", in, err)
		}
	}
	for in, want := range map[string]string{`{}`: `{"model":"server-only"}`, `{"input":"x"}`: `{"model":"server-only","input":"x"}`, ` { } `: ` {"model":"server-only" } `} {
		got, r, err := single.RewriteJSON([]byte(in))
		if err != nil || string(got) != want || r.Model != "only" || !json.Valid(got) {
			t.Errorf("%q: %s %v", in, got, err)
		}
	}
}
func TestRewriteJSONRejectsAmbiguity(t *testing.T) {
	for _, in := range []string{
		`{"model":"plain","model":"secret-model"}`,
		`{"model":"plain","Model":"secret"}`,
		`{"Model":"secret"}`,
		`{"MODEL":"plain"}`,
		`{"model":null}`,
		`{"model":1}`,
		`{"model":"plain"`,
		`{"model":"plain"} {}`,
		`{"model":"plain"}x`,
		``,
		`not json`,
		`[{"model":"plain"}]`,
		`"plain"`,
		`{"model":"plain",}`,
		"\xef\xbb\xbf{\"model\":\"plain\"}",
	} {
		if _, _, err := policy.RewriteJSON([]byte(in)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q accepted: %v", in, err)
		}
	}
}

func multipartBody(t *testing.T, fields [][2]string, file []byte) ([]byte, string) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	for _, f := range fields {
		if f[0] == "file" {
			fw, _ := w.CreateFormFile("file", "a.wav")
			fw.Write(file)
			continue
		}
		w.WriteField(f[0], f[1])
	}
	w.Close()
	return b.Bytes(), w.Boundary()
}
func parseForm(t *testing.T, body []byte, boundary string) (map[string][]string, []byte) {
	t.Helper()
	form, err := multipart.NewReader(bytes.NewReader(body), boundary).ReadForm(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	var file []byte
	if fh := form.File["file"]; len(fh) == 1 {
		f, _ := fh[0].Open()
		file, _ = io.ReadAll(f)
	}
	return form.Value, file
}

type recorder struct {
	calls  int
	result Result
	err    error
	wrote  *bytes.Buffer
	before int // bytes written to dst when ready was called
}

func (r *recorder) ready(res Result, err error) {
	r.calls++
	r.result, r.err, r.before = res, err, r.wrote.Len()
}
func TestRewriteMultipart(t *testing.T) {
	file := bytes.Repeat([]byte{0, 1, 2}, 1000)
	for _, order := range [][][2]string{{{"model", "whisper"}, {"file", ""}, {"language", "en"}}, {{"file", ""}, {"model", "whisper"}}} {
		body, boundary := multipartBody(t, order, file)
		var out bytes.Buffer
		rec := &recorder{wrote: &out}
		if err := policy.RewriteMultipart(&out, bytes.NewReader(body), boundary, rec.ready); err != nil {
			t.Fatal(err)
		}
		values, got := parseForm(t, out.Bytes(), boundary)
		if rec.calls != 1 || rec.err != nil || rec.before != 0 || values["model"][0] != "/models/faster-whisper-large-v3-turbo" || !bytes.Equal(got, file) {
			t.Fatalf("%+v %v", rec, values)
		}
	}
	// One-entry list: a body without a model gets the model added.
	body, boundary := multipartBody(t, [][2]string{{"file", ""}}, file)
	var out bytes.Buffer
	rec := &recorder{wrote: &out}
	if err := single.RewriteMultipart(&out, bytes.NewReader(body), boundary, rec.ready); err != nil || rec.result.Model != "only" {
		t.Fatal(err, rec)
	}
	if values, _ := parseForm(t, out.Bytes(), boundary); !reflect.DeepEqual(values["model"], []string{"server-only"}) {
		t.Fatal(values)
	}
}

// A refused model is decided before any byte reaches the destination.
func TestMultipartRefusalWritesNothing(t *testing.T) {
	file := bytes.Repeat([]byte("x"), 4096)
	for _, tc := range []struct {
		p      *Policy
		fields [][2]string
		want   error
	}{
		{policy, [][2]string{{"file", ""}, {"model", "secret"}}, ErrNotAllowed},
		{policy, [][2]string{{"file", ""}}, ErrRequired},
		{policy, [][2]string{{"model", "plain"}, {"model", "secret"}}, ErrInvalid},
		{policy, [][2]string{{"Model", "secret"}, {"file", ""}}, ErrInvalid},
	} {
		body, boundary := multipartBody(t, tc.fields, file)
		var out bytes.Buffer
		rec := &recorder{wrote: &out}
		err := tc.p.RewriteMultipart(&out, bytes.NewReader(body), boundary, rec.ready)
		if !errors.Is(err, tc.want) || rec.calls != 1 {
			t.Fatalf("%v: err=%v rec=%+v", tc.fields, err, rec)
		}
		// A duplicate is found after the first (allowed) field opened the gate; the
		// error then ends the body before the second field. Everything else wrote nothing.
		if len(tc.fields) == 2 && tc.fields[0][0] == "model" && tc.fields[1][0] == "model" {
			if bytes.Contains(out.Bytes(), []byte("secret")) {
				t.Fatal("duplicate model forwarded")
			}
			continue
		}
		if !errors.Is(rec.err, tc.want) || out.Len() != 0 {
			t.Fatalf("%v: %d bytes forwarded, ready(%v)", tc.fields, out.Len(), rec.err)
		}
	}
}
func TestRewriteMultipartRejects(t *testing.T) {
	body, boundary := multipartBody(t, [][2]string{{"model", "whisper"}, {"file", ""}}, []byte("x"))
	ready := func(Result, error) {}
	if err := policy.RewriteMultipart(io.Discard, bytes.NewReader(body[:len(body)/2]), boundary, ready); !errors.Is(err, ErrInvalid) {
		t.Fatal("truncated body accepted", err)
	}
	if err := policy.RewriteMultipart(io.Discard, bytes.NewReader(body), "", ready); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing boundary accepted", err)
	}
	broken := errors.New("pipe closed")
	if err := policy.RewriteMultipart(failWriter{broken}, bytes.NewReader(body), boundary, ready); !errors.Is(err, broken) || errors.Is(err, ErrInvalid) {
		t.Fatal("write error misclassified", err)
	}
}

type failWriter struct{ err error }

func (f failWriter) Write([]byte) (int, error) { return 0, f.err }

func TestModelList(t *testing.T) {
	var got struct {
		Object string
		Data   []struct{ ID, Object string }
	}
	if err := json.Unmarshal(policy.ModelList(), &got); err != nil || got.Object != "list" || len(got.Data) != 3 || got.Data[0].ID != "jina-reranker" || got.Data[2].ID != "plain" {
		t.Fatal(got, err)
	}
}

func FuzzRewriteJSON(f *testing.F) {
	for _, s := range []string{`{"model":"jina-reranker"}`, `{"model":"x","model":"y"}`, `{"a":[1,{"model":"whisper"}],"model":"whisper"}`, `[]`, `"s"`, `{"model":1}`, `{}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		out, r, err := policy.RewriteJSON(in)
		if err != nil {
			if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrNotAllowed) && !errors.Is(err, ErrRequired) {
				t.Fatal(err)
			}
			return
		}
		var a, b map[string]any
		if json.Unmarshal(in, &a) != nil || json.Unmarshal(out, &b) != nil {
			t.Fatal("accepted body is not an object")
		}
		if b["model"] != r.Target || a["model"] != r.Model {
			t.Fatalf("wrong substitution %v -> %v", a["model"], b["model"])
		}
		b["model"] = r.Model
		if !reflect.DeepEqual(a, b) {
			t.Fatal("other fields changed")
		}
	})
}
func FuzzRewriteMultipart(f *testing.F) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	w.SetBoundary("BOUNDARY")
	w.WriteField("model", "whisper")
	fw, _ := w.CreateFormFile("file", "a.wav")
	fw.Write([]byte("data"))
	w.Close()
	f.Add(b.Bytes(), "BOUNDARY")
	f.Add([]byte("--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nplain\r\n--B--\r\n"), "B")
	f.Fuzz(func(t *testing.T, in []byte, boundary string) {
		var out bytes.Buffer
		var r Result
		err := policy.RewriteMultipart(&out, bytes.NewReader(in), boundary, func(res Result, e error) { r = res })
		if err != nil {
			return
		}
		mr := multipart.NewReader(bytes.NewReader(out.Bytes()), boundary)
		models := 0
		for {
			p, err := mr.NextRawPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("output does not parse: %v", err)
			}
			if p.FormName() == "model" {
				models++
				v, _ := io.ReadAll(p)
				if string(v) != r.Target {
					t.Fatalf("model %q, want %q", v, r.Target)
				}
			}
		}
		if models != 1 {
			t.Fatalf("%d model fields forwarded", models)
		}
	})
}
