package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEscapeKeyKeepsSlashes(t *testing.T) {
	for in, want := range map[string]string{
		"a/b.txt":             "a/b.txt",
		"reports/q3 100%.txt": "reports/q3%20100%25.txt",
		"dir/a?b#c":           "dir/a%3Fb%23c",
	} {
		if got := escapeKey(in); got != want {
			t.Errorf("%q: %q", in, got)
		}
	}
}

func TestLabelsAndErrors(t *testing.T) {
	var seen http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		switch r.URL.Path {
		case "/site-a/files/missing":
			w.Header().Set(HeaderError, "not_found")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			io.WriteString(w, `{"error":{"code":"not_found","message":"object not found"}}`)
		case "/site-a/chat/v1/models":
			w.WriteHeader(500)
			io.WriteString(w, "service says no")
		}
	}))
	defer srv.Close()
	c := New(srv.URL+"/", "app-one")
	if c.ServiceURL("site-a", "chat") != srv.URL+"/site-a/chat" {
		t.Fatal(c.ServiceURL("site-a", "chat"))
	}
	ctx := WithFor(context.Background(), "user-7")
	_, err := c.Storage("site-a", "files").Head(ctx, "missing")
	var e *Error
	// HEAD has no body: the code comes from the header.
	if !errors.As(err, &e) || e.Code != "not_found" || e.Status != 404 {
		t.Fatalf("%v", err)
	}
	_, _, err = c.Storage("site-a", "files").Get(ctx, "missing")
	if !errors.As(err, &e) || e.Message != "object not found" || !IsCode(err, "not_found") {
		t.Fatalf("%v", err)
	}
	if seen.Get(HeaderApplication) != "app-one" || seen.Get(HeaderFor) != "user-7" {
		t.Fatal(seen)
	}
	// A service's own error has no code.
	hc := &http.Client{Transport: c.Transport(nil)}
	req, _ := http.NewRequestWithContext(ctx, "GET", c.ServiceURL("site-a", "chat")+"/v1/models", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if seen.Get(HeaderApplication) != "app-one" || seen.Get(HeaderFor) != "user-7" || req.Header.Get(HeaderApplication) != "" {
		t.Fatal("transport did not label the request (or changed the caller's)")
	}
	if !strings.Contains((&Error{Status: 500, Message: "x"}).Error(), "HTTP 500") {
		t.Fatal()
	}
}
