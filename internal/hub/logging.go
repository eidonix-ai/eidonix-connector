package hub

import (
	"log"
	"log/slog"
	"strings"
)

// slogError routes net/http's server errors to the operational log.
func slogError(l *slog.Logger) *log.Logger {
	return log.New(writerFunc(func(p []byte) (int, error) {
		l.Warn("http server", "error", strings.TrimSpace(string(p)))
		return len(p), nil
	}), "", 0)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
