// Package client is a Go client of eidonix-connect's local API (the hub's
// local port, docs/HUB.md): the catalog, connected services' base URLs and
// site storage. It uses the standard library only.
//
//	c := client.New("http://connect:8080", "app-one")
//	chatBase := c.ServiceURL("site-a", "chat") + "/v1" // an OpenAI-style base URL
//	files := c.Storage("site-a", "files")
//	etag, err := files.Put(ctx, "reports/q3.pdf", f, size, "application/pdf")
//
// Every request names the application (X-Eidonix-Application); WithFor
// adds the X-Eidonix-For label to the requests made with a context.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Header names of the hub's local API.
const (
	HeaderApplication = "X-Eidonix-Application"
	HeaderFor         = "X-Eidonix-For"
	HeaderError       = "X-Eidonix-Error"
)

// Client talks to one hub's local port.
type Client struct {
	base        string
	application string
	http        *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client used for requests (default: a client
// without timeout, so long downloads and uploads are not cut).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New returns a client for the hub at baseURL (for example
// http://connect:8080) acting as application (the X-Eidonix-Application
// label; "" sends none).
func New(baseURL, application string, opts ...Option) *Client {
	c := &Client{base: strings.TrimRight(baseURL, "/"), application: application, http: &http.Client{}}
	for _, o := range opts {
		o(c)
	}
	return c
}

type forKey struct{}

// WithFor returns a context whose requests carry X-Eidonix-For: label (for
// example the account or user an application serves). It is a label for
// logs, not a credential.
func WithFor(ctx context.Context, label string) context.Context {
	return context.WithValue(ctx, forKey{}, label)
}

func forOf(ctx context.Context) string { s, _ := ctx.Value(forKey{}).(string); return s }

// ServiceURL is the base URL of a connected service:
// <base>/<site>/<service>. Requests below it reach the service's paths.
func (c *Client) ServiceURL(site, service string) string {
	return c.base + "/" + site + "/" + service
}

// Transport wraps base (nil: http.DefaultTransport) so every request it
// sends carries this client's application label, and the For label of its
// context. Give it to SDKs that take an *http.Client and a ServiceURL.
func (c *Client) Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return labelTransport{c: c, base: base}
}

type labelTransport struct {
	c    *Client
	base http.RoundTripper
}

func (t labelTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	t.c.label(r)
	return t.base.RoundTrip(r)
}

func (c *Client) label(r *http.Request) {
	if c.application != "" {
		r.Header.Set(HeaderApplication, c.application)
	}
	if f := forOf(r.Context()); f != "" {
		r.Header.Set(HeaderFor, f)
	}
}

// Error is an error the hub or a connector made (Code is the
// X-Eidonix-Error code, e.g. site_offline, not_found, busy), or a non-2xx
// reply from a service (Code empty).
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("eidonix-connect: HTTP %d: %s", e.Status, e.Message)
	}
	return fmt.Sprintf("eidonix-connect: %s (HTTP %d): %s", e.Code, e.Status, e.Message)
}

// IsCode reports whether err is an *Error with code.
func IsCode(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

func responseError(resp *http.Response) error {
	defer resp.Body.Close()
	e := &Error{Status: resp.StatusCode, Code: resp.Header.Get(HeaderError), Message: http.StatusText(resp.StatusCode)}
	if e.Code != "" {
		var body struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body) == nil && body.Error.Message != "" {
			e.Message = body.Error.Message
		}
	}
	return e
}

func (c *Client) do(ctx context.Context, method, u string, body io.Reader, size int64, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = size
		if size == 0 {
			req.Body = http.NoBody
		}
	}
	for k, v := range header {
		req.Header[k] = v
	}
	c.label(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, responseError(resp)
	}
	return resp, nil
}

func (c *Client) getJSON(ctx context.Context, u string, out any) error {
	resp, err := c.do(ctx, http.MethodGet, u, nil, 0, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// Catalog is GET /v1/catalog.
type Catalog struct {
	Sites []Site `json:"sites"`
}

// Site is one site of the catalog.
type Site struct {
	Name     string    `json:"name"`
	Online   bool      `json:"online"`
	Disabled bool      `json:"disabled,omitempty"`
	Sessions []Session `json:"sessions"`
	Services []Service `json:"services"`
}

// Session is one live connector session of a site.
type Session struct {
	Instance    string    `json:"instance"`
	Version     string    `json:"version"`
	ConnectedAt time.Time `json:"connected_at"`
	RemoteIP    string    `json:"remote_ip"`
	Draining    bool      `json:"draining,omitempty"`
}

// Service is one service a site offers, merged across its sessions.
type Service struct {
	Name                  string         `json:"name"`
	Kind                  string         `json:"kind"`
	Healthy               bool           `json:"healthy"`
	Endpoints             []string       `json:"endpoints,omitempty"`
	Models                []string       `json:"models,omitempty"`
	MaxConcurrentRequests int            `json:"max_concurrent_requests"`
	InFlight              int            `json:"in_flight"`
	Storage               *StorageLimits `json:"storage,omitempty"`
}

// StorageLimits is an s3 service's storage block.
type StorageLimits struct {
	Encryption     string `json:"encryption"`
	ReadOnly       bool   `json:"read_only"`
	MaxObjectBytes int64  `json:"max_object_bytes"`
	MaxKeyBytes    int    `json:"max_key_bytes"`
}

// Catalog returns every site in the hub's file and what it offers now.
func (c *Client) Catalog(ctx context.Context) (*Catalog, error) {
	var out Catalog
	if err := c.getJSON(ctx, c.base+"/v1/catalog", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Site returns one site's catalog entry (Error code unknown_site if the
// file has no such site).
func (c *Client) Site(ctx context.Context, name string) (*Site, error) {
	var out Site
	if err := c.getJSON(ctx, c.base+"/v1/catalog/"+url.PathEscape(name), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Storage is a site's s3 service.
type Storage struct {
	c    *Client
	base string
}

// Storage returns a handle on a site's storage service.
func (c *Client) Storage(site, service string) *Storage {
	return &Storage{c: c, base: c.ServiceURL(site, service)}
}

// escapeKey escapes each segment of key, keeping its slashes.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func (s *Storage) url(key string) string { return s.base + "/" + escapeKey(key) }

// ObjectInfo describes an object. Size is the whole object's size; for a
// ranged get, Start and End are the inclusive bounds returned.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
	Partial      bool
	Start, End   int64
}

func objectInfo(key string, resp *http.Response) *ObjectInfo {
	info := &ObjectInfo{Key: key, Size: resp.ContentLength, ETag: strings.Trim(resp.Header.Get("ETag"), `"`), ContentType: resp.Header.Get("Content-Type")}
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}
	if cr := resp.Header.Get("Content-Range"); resp.StatusCode == http.StatusPartialContent && cr != "" {
		var a, b, n int64
		if _, err := fmt.Sscanf(cr, "bytes %d-%d/%d", &a, &b, &n); err == nil {
			info.Partial, info.Start, info.End, info.Size = true, a, b, n
		}
	}
	if info.End == 0 && !info.Partial && info.Size > 0 {
		info.End = info.Size - 1
	}
	return info
}

// Put stores r as key. size is its length, or -1 when unknown (sent
// chunked). It returns the stored object's ETag.
func (s *Storage) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) (string, error) {
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	if r == nil {
		r, size = strings.NewReader(""), 0
	}
	resp, err := s.c.do(ctx, http.MethodPut, s.url(key), r, size, h)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		ETag string `json:"etag"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.ETag, nil
}

// Get reads a whole object. Close the reader; a truncated transfer is a
// read error, never a clean end.
func (s *Storage) Get(ctx context.Context, key string) (io.ReadCloser, *ObjectInfo, error) {
	return s.get(ctx, key, nil)
}

// GetRange reads bytes start..end (inclusive; end < 0 means to the end of
// the object). ifMatch, when set, makes the read fail with
// precondition_failed unless the object's ETag is ifMatch.
func (s *Storage) GetRange(ctx context.Context, key string, start, end int64, ifMatch string) (io.ReadCloser, *ObjectInfo, error) {
	h := http.Header{}
	rg := "bytes=" + strconv.FormatInt(start, 10) + "-"
	if end >= 0 {
		rg += strconv.FormatInt(end, 10)
	}
	h.Set("Range", rg)
	if ifMatch != "" {
		h.Set("If-Match", `"`+strings.Trim(ifMatch, `"`)+`"`)
	}
	return s.get(ctx, key, h)
}

func (s *Storage) get(ctx context.Context, key string, h http.Header) (io.ReadCloser, *ObjectInfo, error) {
	resp, err := s.c.do(ctx, http.MethodGet, s.url(key), nil, 0, h)
	if err != nil {
		return nil, nil, err
	}
	info := objectInfo(key, resp)
	if !info.Partial {
		info.Size = resp.ContentLength
	}
	return resp.Body, info, nil
}

// Head returns an object's metadata.
func (s *Storage) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	resp, err := s.c.do(ctx, http.MethodHead, s.url(key), nil, 0, nil)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return objectInfo(key, resp), nil
}

// Delete removes an object (deleting a missing object succeeds).
func (s *Storage) Delete(ctx context.Context, key string) error {
	resp, err := s.c.do(ctx, http.MethodDelete, s.url(key), nil, 0, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Copy copies src to dst within the service and returns dst's ETag.
func (s *Storage) Copy(ctx context.Context, src, dst string) (string, error) {
	resp, err := s.c.do(ctx, http.MethodPost, s.url(dst)+"?copy-from="+url.QueryEscape(src), nil, 0, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		ETag string `json:"etag"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.ETag, err
}

// Object is one list entry.
type Object struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	ContentType  string    `json:"content_type"`
	// Dir marks a common prefix (non-recursive lists).
	Dir bool `json:"dir"`
}

// ListPage is one page of a list. When Truncated, pass NextToken to the
// next List call.
type ListPage struct {
	Objects   []Object `json:"objects"`
	Truncated bool     `json:"truncated"`
	NextToken string   `json:"next_token,omitempty"`
}

// List lists objects under prefix, at most max (1..1000; 0 means 1000).
// Without recursive, keys below the next "/" are folded into Dir entries.
func (s *Storage) List(ctx context.Context, prefix string, recursive bool, token string, max int) (*ListPage, error) {
	q := url.Values{}
	q.Set("prefix", prefix)
	if max > 0 {
		q.Set("max", strconv.Itoa(max))
	}
	if recursive {
		q.Set("recursive", "true")
	}
	if token != "" {
		q.Set("token", token)
	}
	var out ListPage
	if err := s.c.getJSON(ctx, s.base+"/?list&"+q.Encode(), &out); err != nil {
		return nil, err
	}
	return &out, nil
}
