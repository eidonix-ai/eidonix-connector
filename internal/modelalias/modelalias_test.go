package modelalias

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"reflect"
	"testing"
)

var table = New(map[string]string{"jina-reranker": "jina-reranker-v3", "whisper": "/models/faster-whisper-large-v3-turbo", "rr": "jina-reranker-v3"})

func TestRewriteJSON(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{`{"model":"jina-reranker","query":"q","documents":["a"]}`, `{"model":"jina-reranker-v3","query":"q","documents":["a"]}`},
		{"{ \"query\" : \"model\" ,\n \"model\" :  \"whisper\" , \"n\":1 }", "{ \"query\" : \"model\" ,\n \"model\" :  \"/models/faster-whisper-large-v3-turbo\" , \"n\":1 }"},
		{`{"model":"jina\u002dreranker"}`, `{"model":"jina-reranker-v3"}`},
		{`{"model":"jina-reranker-v3"}`, `{"model":"jina-reranker-v3"}`},
		{`{"model":"other"}`, `{"model":"other"}`},
		{`{"model":null,"x":{"model":"jina-reranker"}}`, `{"model":null,"x":{"model":"jina-reranker"}}`},
		{`{"messages":[{"model":"jina-reranker"}]}`, `{"messages":[{"model":"jina-reranker"}]}`},
		{`[{"model":"jina-reranker"}]`, `[{"model":"jina-reranker"}]`},
		{`{"model":"jina-reranker"}`, `{"model":"jina-reranker-v3"}`},
		{`{"model":"jina-reranker"}` + "\n", `{"model":"jina-reranker-v3"}` + "\n"},
	} {
		got, _, err := table.RewriteJSON([]byte(tc.in))
		if err != nil || string(got) != tc.out {
			t.Errorf("%s: got %s, %v", tc.in, got, err)
		}
	}
	_, rw, _ := table.RewriteJSON([]byte(`{"model":"whisper"}`))
	if rw == nil || rw.Alias != "whisper" || rw.Target != "/models/faster-whisper-large-v3-turbo" {
		t.Fatalf("rewrite not recorded: %+v", rw)
	}
}

func TestRewriteJSONRejectsAmbiguity(t *testing.T) {
	for _, in := range []string{
		`{"model":"jina-reranker","model":"secret-model"}`,
		`{"model":"x","model":"y"}`,
		`{"model":"x","Model":"y"}`,
		`{"model":"jina-reranker"`,
		`{"model":"jina-reranker"} {}`,
		`{"model":"jina-reranker"}x`,
		``,
		`not json`,
		`{"model":"jina-reranker",}`,
		"\xef\xbb\xbf{\"model\":\"jina-reranker\"}",
	} {
		if _, _, err := table.RewriteJSON([]byte(in)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q accepted: %v", in, err)
		}
	}
}

func multipartBody(t *testing.T, fields [][2]string, file []byte) ([]byte, string) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	fw, _ := w.CreateFormFile("file", "audio.wav")
	fw.Write(file)
	for _, f := range fields {
		w.WriteField(f[0], f[1])
	}
	w.Close()
	return b.Bytes(), w.Boundary()
}

func parseForm(t *testing.T, body []byte, boundary string) (map[string][]string, []byte) {
	t.Helper()
	form, err := multipart.NewReader(bytes.NewReader(body), boundary).ReadForm(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	f, err := form.File["file"][0].Open()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(f)
	return form.Value, data
}

func TestRewriteMultipart(t *testing.T) {
	file := bytes.Repeat([]byte("RIFF\x00\x01model=whisper\r\n--"), 50000)
	body, boundary := multipartBody(t, [][2]string{{"model", "whisper"}, {"language", "en"}}, file)
	var out bytes.Buffer
	rw, err := table.RewriteMultipart(&out, bytes.NewReader(body), boundary)
	if err != nil || rw == nil || rw.Target != "/models/faster-whisper-large-v3-turbo" {
		t.Fatal(rw, err)
	}
	values, data := parseForm(t, out.Bytes(), boundary)
	if values["model"][0] != "/models/faster-whisper-large-v3-turbo" || values["language"][0] != "en" || !bytes.Equal(data, file) {
		t.Fatalf("bad rewrite: %v", values)
	}
	// Non-alias names and file parts named model are untouched.
	body, boundary = multipartBody(t, [][2]string{{"model", "large-v3"}}, file)
	out.Reset()
	if rw, err = table.RewriteMultipart(&out, bytes.NewReader(body), boundary); err != nil || rw != nil {
		t.Fatal(rw, err)
	}
	if values, _ = parseForm(t, out.Bytes(), boundary); values["model"][0] != "large-v3" {
		t.Fatal(values)
	}
}

func TestRewriteMultipartRejects(t *testing.T) {
	body, boundary := multipartBody(t, [][2]string{{"model", "whisper"}, {"model", "secret"}}, []byte("x"))
	if _, err := table.RewriteMultipart(io.Discard, bytes.NewReader(body), boundary); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate model field accepted", err)
	}
	if _, err := table.RewriteMultipart(io.Discard, bytes.NewReader(body[:len(body)/2]), boundary); !errors.Is(err, ErrInvalid) {
		t.Fatal("truncated body accepted", err)
	}
	if _, err := table.RewriteMultipart(io.Discard, bytes.NewReader(body), ""); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing boundary accepted", err)
	}
	broken := errors.New("pipe closed")
	if _, err := table.RewriteMultipart(failWriter{broken}, bytes.NewReader(body), boundary); !errors.Is(err, broken) || errors.Is(err, ErrInvalid) {
		t.Fatal("write error misclassified", err)
	}
}

type failWriter struct{ err error }

func (f failWriter) Write([]byte) (int, error) { return 0, f.err }

func TestRewriteModelList(t *testing.T) {
	in := `{"object":"list","data":[{"id":"jina-reranker-v3","object":"model","owned_by":"x"},{"id":"bge","object":"model"}]}`
	out, ok := table.RewriteModelList([]byte(in))
	if !ok {
		t.Fatal("not rewritten")
	}
	var got struct {
		Object string
		Data   []struct{ ID, Object, OwnedBy string }
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, d := range got.Data {
		ids = append(ids, d.ID)
	}
	if got.Object != "list" || !reflect.DeepEqual(ids, []string{"jina-reranker", "rr", "bge"}) || got.Data[0].Object != "model" {
		t.Fatalf("%s", out)
	}
	for _, in := range []string{`not json`, `{"data":"x"}`, `{"data":[{"id":"bge"}]}`, `[]`} {
		if out, ok := table.RewriteModelList([]byte(in)); ok || string(out) != in {
			t.Fatalf("%s changed", in)
		}
	}
}

func TestPresent(t *testing.T) {
	if got := table.Present([]string{"jina-reranker-v3", "bge"}); !reflect.DeepEqual(got, []string{"jina-reranker", "rr", "bge"}) {
		t.Fatal(got)
	}
	var none *Table
	if got := none.Present([]string{"a"}); !reflect.DeepEqual(got, []string{"a"}) || none.Names() != nil {
		t.Fatal(got)
	}
	if _, ok := none.Lookup("a"); ok || New(nil) != nil {
		t.Fatal("empty table aliases")
	}
	if !reflect.DeepEqual(table.Names(), []string{"jina-reranker", "rr", "whisper"}) {
		t.Fatal(table.Names())
	}
}

func FuzzRewriteJSON(f *testing.F) {
	for _, s := range []string{`{"model":"jina-reranker"}`, `{"model":"x","model":"y"}`, `{"a":[1,{"model":"whisper"}],"model":"whisper"}`, `[]`, `"s"`, `{"model":1}`, `{"model":"rr"}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		out, rw, err := table.RewriteJSON(in)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			return
		}
		if !json.Valid(out) {
			t.Fatalf("invalid output %q from %q", out, in)
		}
		if rw == nil {
			if !bytes.Equal(out, in) {
				t.Fatal("unchanged request modified")
			}
			return
		}
		// Exactly the model field changed: decoding both and restoring the alias must agree.
		var a, b map[string]any
		if json.Unmarshal(in, &a) != nil || json.Unmarshal(out, &b) != nil {
			t.Fatal("rewritten body is not an object")
		}
		if b["model"] != rw.Target || a["model"] != rw.Alias {
			t.Fatalf("wrong substitution %v -> %v", a["model"], b["model"])
		}
		b["model"] = rw.Alias
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
	f.Add([]byte("--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nrr\r\n--B--\r\n"), "B")
	f.Fuzz(func(t *testing.T, in []byte, boundary string) {
		var out bytes.Buffer
		rw, err := table.RewriteMultipart(&out, bytes.NewReader(in), boundary)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			return
		}
		// The output must re-parse with the same boundary and carry at most one model field.
		r := multipart.NewReader(bytes.NewReader(out.Bytes()), boundary)
		models := 0
		for {
			p, err := r.NextRawPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("output does not parse: %v", err)
			}
			if p.FormName() == "model" && p.FileName() == "" {
				models++
				v, _ := io.ReadAll(p)
				if rw != nil && string(v) != rw.Target {
					t.Fatalf("model %q, want %q", v, rw.Target)
				}
				if _, isAlias := table.Lookup(string(v)); isAlias {
					t.Fatalf("alias %q forwarded", v)
				}
			}
		}
		if models > 1 {
			t.Fatal("duplicate model fields forwarded")
		}
	})
}
