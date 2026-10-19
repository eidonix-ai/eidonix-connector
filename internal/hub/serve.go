package hub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// DefaultReloadInterval is how often serve checks the file for changes.
const DefaultReloadInterval = time.Second

// Server runs a hub from its file: both listeners and the file watcher.
type Server struct {
	Path string
	// ReloadInterval defaults to DefaultReloadInterval.
	ReloadInterval time.Duration
	Log            *slog.Logger

	hub    *Hub
	access *AccessLog
	// Bound addresses, once listening.
	PublicAddr, LocalAddr net.Addr
	readyOnce             sync.Once
	ready                 chan struct{}
}

// Hub returns the running hub (nil before Serve has loaded the file).
func (s *Server) Hub() *Hub { return s.hub }

// Ready is closed once both ports listen.
func (s *Server) Ready() <-chan struct{} {
	s.readyOnce.Do(func() { s.ready = make(chan struct{}) })
	return s.ready
}

// ReopenAccessLog reopens the access log (SIGUSR1).
func (s *Server) ReopenAccessLog() error {
	if s.access == nil {
		return nil
	}
	return s.access.Reopen()
}

// Serve loads the file and serves until ctx ends.
func (s *Server) Serve(ctx context.Context) error {
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.ReloadInterval <= 0 {
		s.ReloadInterval = DefaultReloadInterval
	}
	s.Ready()
	data, err := ReadFile(s.Path)
	if err != nil {
		return err
	}
	cfg, err := Parse(data)
	if err != nil {
		return errors.New(s.Path + ": " + err.Error())
	}
	if s.access, err = OpenAccessLog(cfg.AccessLog); err != nil {
		return err
	}
	defer s.access.Close()
	s.hub = New(cfg, s.access, s.Log)
	pl, err := net.Listen("tcp", cfg.Public)
	if err != nil {
		return err
	}
	ll, err := net.Listen("tcp", cfg.Local)
	if err != nil {
		pl.Close()
		return err
	}
	s.PublicAddr, s.LocalAddr = pl.Addr(), ll.Addr()
	public := &http.Server{Handler: http.HandlerFunc(s.hub.ServeSession), ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: slogError(s.Log)}
	local := &http.Server{Handler: s.hub.LocalHandler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10, ErrorLog: slogError(s.Log)}
	errs := make(chan error, 2)
	go func() { errs <- public.Serve(pl) }()
	go func() { errs <- local.Serve(ll) }()
	s.Log.Info("eidonix-connect listening", "public", s.PublicAddr.String(), "local", s.LocalAddr.String(), "key_id", cfg.KeyID, "sites", len(cfg.Order))
	close(s.ready)

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go s.watch(watchCtx, sha256.Sum256(data))

	select {
	case <-ctx.Done():
		err = nil
	case err = <-errs:
	}
	stopWatch()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = public.Shutdown(shutdown)
	s.hub.Close()
	_ = local.Shutdown(shutdown)
	public.Close()
	local.Close()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

// watch polls the file and applies every valid change. A bad file is
// reported once and the last good configuration stays in force.
func (s *Server) watch(ctx context.Context, last [32]byte) {
	t := time.NewTicker(s.ReloadInterval)
	defer t.Stop()
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		data, err := ReadFile(s.Path)
		if err != nil {
			if err.Error() != lastErr {
				lastErr = err.Error()
				s.Log.Error("configuration not reloaded; keeping the last good one", "error", err)
			}
			continue
		}
		sum := sha256.Sum256(data)
		if bytes.Equal(sum[:], last[:]) {
			lastErr = ""
			continue
		}
		cfg, err := Parse(data)
		if err != nil {
			if msg := s.Path + ": " + err.Error(); msg != lastErr {
				lastErr = msg
				s.Log.Error("configuration not reloaded; keeping the last good one", "error", msg)
			}
			continue
		}
		last, lastErr = sum, ""
		s.hub.Apply(cfg)
		s.Log.Info("configuration reloaded", "sites", len(cfg.Order))
	}
}
