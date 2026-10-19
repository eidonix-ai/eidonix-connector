package hub

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AccessEntry is one line of the hub's access log: which application used
// which site's service, how, and how it ended. Bodies, queries and storage
// keys are never logged.
type AccessEntry struct {
	Time        time.Time `json:"time"`
	Application string    `json:"application"`
	For         string    `json:"for"`
	Site        string    `json:"site"`
	Service     string    `json:"service"`
	// Kind is http, websocket or storage ("" when the request named no
	// service the hub could reach).
	Kind   string `json:"kind"`
	Method string `json:"method"`
	// Path is the request path below /<site>/<service> for http and
	// websocket requests; storage requests log their operation in Op
	// instead (keys are file names).
	Path       string `json:"path"`
	Op         string `json:"op,omitempty"`
	Status     int    `json:"status"`
	BytesIn    int64  `json:"bytes_in"`
	BytesOut   int64  `json:"bytes_out"`
	DurationMS int64  `json:"duration_ms"`
	RequestID  string `json:"request_id"`
	Session    string `json:"session,omitempty"`
	Error      string `json:"error"`
}

// AccessLog writes one JSON line per request to a file (reopened on
// Reopen, for log rotation) or to standard output ("-").
type AccessLog struct {
	mu   sync.Mutex
	path string
	w    io.Writer
	f    *os.File
}

// OpenAccessLog opens path for appending ("-" is standard output).
func OpenAccessLog(path string) (*AccessLog, error) {
	l := &AccessLog{}
	if err := l.SetPath(path); err != nil {
		return nil, err
	}
	return l, nil
}

func openLog(path string) (io.Writer, *os.File, error) {
	if path == "-" {
		return os.Stdout, nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

// SetPath switches to another file (or "-"); on error the old one stays.
func (l *AccessLog) SetPath(path string) error {
	w, f, err := openLog(path)
	if err != nil {
		return err
	}
	l.mu.Lock()
	old := l.f
	l.path, l.w, l.f = path, w, f
	l.mu.Unlock()
	if old != nil {
		old.Close()
	}
	return nil
}

// Reopen reopens the current path (SIGUSR1 after rotation).
func (l *AccessLog) Reopen() error {
	l.mu.Lock()
	path := l.path
	l.mu.Unlock()
	return l.SetPath(path)
}

// Write appends one entry.
func (l *AccessLog) Write(e *AccessEntry) {
	if l == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil {
		_, _ = l.w.Write(b)
	}
}

// Close closes the file.
func (l *AccessLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		err := l.f.Close()
		l.f, l.w = nil, nil
		return err
	}
	return nil
}
