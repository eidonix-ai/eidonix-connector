package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/connector"
	"github.com/eidonix-ai/eidonix-connector/internal/session"
	"github.com/eidonix-ai/eidonix-connector/internal/status"
	"syscall"
)

// Defaults for run; tests shorten them.
const (
	defaultDrainTimeout = 10 * time.Minute
	// reloadEstablishTimeout is how long a reload waits for the new session
	// to come up before giving up and keeping the old one.
	reloadEstablishTimeout = time.Minute
)

type runOptions struct {
	// signals delivers SIGINT, SIGTERM, SIGHUP and SIGUSR1 (as unix.Signal).
	signals <-chan os.Signal
	// drainTimeout bounds a stop (SIGINT/SIGTERM).
	drainTimeout time.Duration
	// establishTimeout bounds how long a reload waits for its new session.
	establishTimeout time.Duration
	// statusInterval is how often the status file is rewritten when nothing changed.
	statusInterval time.Duration
}

// liveSession is one upstream session with the configuration and handler it
// serves. Its lifecycle: running, then (optionally) draining, then exited.
type liveSession struct {
	cfg     *config.Config
	handler *connector.Connector
	cancel  context.CancelFunc
	drain   chan struct{}
	up      chan struct{} // closed when the session is first established
	upOnce  sync.Once

	mu    sync.Mutex
	state session.State

	// Supervisor-only.
	draining bool
	timer    *time.Timer // ends a drain that outlasts its bound
	err      error       // result of the session, valid once it has exited
}

func (l *liveSession) snapshot() session.State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// startDrain asks the session to drain; after bound the supervisor ends
// whatever is still running.
func (l *liveSession) startDrain(bound time.Duration, logger *log.Logger) {
	if l.draining {
		return
	}
	l.draining = true
	close(l.drain)
	l.timer = time.AfterFunc(bound, func() {
		logger.Printf("drain of session %s reached its %s bound; ending the streams still running", l.snapshot().SessionID, bound)
		l.cancel()
	})
}

// run serves until SIGINT/SIGTERM. SIGHUP reloads the file; SIGUSR1 reopens
// the access log.
//
// A reload loads and validates the file (a rejected file keeps the running
// configuration), starts a new session with the new services and, once that
// session is established, drains the old one: it tells the hub (draining),
// refuses streams that still arrive, lets running streams finish and closes,
// at most the longest idle-timeout of the old configuration (15 minutes at
// most) later. If the new session does not come up in time the old one keeps
// serving.
//
// A stop drains the same way, bounded by drainTimeout; a second stop signal
// ends everything at once.
func run(path string, cfg *config.Config, errOut io.Writer, o runOptions) error {
	logger := log.New(errOut, "", log.LstdFlags)
	if o.drainTimeout <= 0 {
		o.drainTimeout = defaultDrainTimeout
	}
	if o.establishTimeout <= 0 {
		o.establishTimeout = reloadEstablishTimeout
	}
	if o.statusInterval <= 0 {
		o.statusInterval = status.Interval
	}
	if err := session.ValidateConfig(context.Background(), cfg); err != nil {
		return err
	}
	handler, err := connector.New(cfg)
	if err != nil {
		return err
	}
	changed := make(chan struct{}, 1)
	exited := make(chan *liveSession, 8)
	running := 0
	start := func(cfg *config.Config, h *connector.Connector) *liveSession {
		ctx, cancel := context.WithCancel(context.Background())
		l := &liveSession{cfg: cfg, handler: h, cancel: cancel, drain: make(chan struct{}), up: make(chan struct{})}
		onState := func(st session.State) {
			l.mu.Lock()
			l.state = st
			l.mu.Unlock()
			if st.Connected {
				l.upOnce.Do(func() { close(l.up) })
			}
			select {
			case changed <- struct{}{}:
			default:
			}
		}
		running++
		go func() { l.err = session.RunWith(ctx, cfg, h, version, l.drain, onState); exited <- l }()
		return l
	}

	var (
		active      = start(cfg, handler)
		next        *liveSession // a reload's session, not yet serving
		nextUp      <-chan struct{}
		nextTimeout <-chan time.Time
		olds        []*liveSession // draining, abandoned or ended sessions awaiting exit
		stopping    bool
		stopTimeout <-chan time.Time
		statusPath  = cfg.StatusFile
		lastWriteOK = true
	)
	tick := time.NewTicker(o.statusInterval)
	defer tick.Stop()
	defer func() { status.Remove(statusPath) }()

	handlers := func() []*connector.Connector {
		var hs []*connector.Connector
		for _, l := range append([]*liveSession{active, next}, olds...) {
			if l != nil {
				hs = append(hs, l.handler)
			}
		}
		return hs
	}
	writeStatus := func() {
		f := status.File{Version: version, PID: os.Getpid(), UpdatedAt: time.Now().UTC(), Draining: []status.Draining{}, Services: []status.Service{}}
		shown := active
		if shown != nil {
			st := shown.snapshot()
			f.Connected, f.SessionID, f.Hub = st.Connected, st.SessionID, st.Hub
			if st.Connected {
				since := st.Since.UTC()
				f.ConnectedSince = &since
			}
			if st.LastError != "" {
				at := st.LastErrorAt.UTC()
				f.LastError, f.LastErrorAt = st.LastError, &at
			}
		}
		for _, l := range olds {
			if l.draining && l.snapshot().Connected {
				st := l.snapshot()
				f.Draining = append(f.Draining, status.Draining{SessionID: st.SessionID, Since: st.DrainingAt.UTC(), InFlight: l.handler.InFlight()})
				if shown == nil {
					shown = l
				}
			}
		}
		if shown == nil {
			return
		}
		f.Site, f.HubURL = shown.cfg.Site, shown.cfg.Upstream.URL
		for _, s := range shown.handler.Snapshot() {
			f.Services = append(f.Services, status.Service{Name: s.Name, Kind: s.Kind, Healthy: s.Healthy, Health: s.Health, InFlight: s.InFlight, MaxConcurrent: s.MaxConcurrent, Requests1h: s.Requests, Errors1h: s.Errors})
		}
		if err := status.Write(statusPath, f); err != nil {
			if lastWriteOK {
				logger.Printf("cannot write the status file %s: %v", statusPath, err)
			}
			lastWriteOK = false
		} else {
			lastWriteOK = true
		}
	}
	// retire parks a session that should just end and have its handler closed.
	retire := func(l *liveSession) {
		l.cancel()
		olds = append(olds, l)
	}
	stopAll := func() {
		for _, l := range olds {
			l.cancel()
		}
	}
	// finish ends everything and waits for the sessions to exit.
	finish := func() {
		if next != nil {
			retire(next)
			next = nil
		}
		if active != nil {
			retire(active)
			active = nil
		}
		for running > 0 {
			l := <-exited
			running--
			l.handler.Close()
		}
	}
	defer func() { finish() }()

	writeStatus()
	for {
		if stopping && active == nil && next == nil && len(olds) == 0 {
			return nil
		}
		select {
		case <-changed:
			writeStatus()
		case <-tick.C:
			writeStatus()
		case l := <-exited:
			running--
			switch {
			case l == active:
				active = nil
				l.handler.Close()
				if errors.Is(l.err, context.Canceled) || l.err == nil {
					return nil
				}
				return l.err
			case l == next:
				logger.Printf("reload: the new session ended before it was established (%v); keeping the previous session", l.err)
				l.handler.Close()
				next, nextUp, nextTimeout = nil, nil, nil
			default:
				for i, old := range olds {
					if old != l {
						continue
					}
					olds = append(olds[:i], olds[i+1:]...)
					if l.timer != nil {
						l.timer.Stop()
					}
					l.handler.Close()
					switch {
					case !l.draining:
					case l.err == nil:
						logger.Printf("session %s drained and closed", l.snapshot().SessionID)
					default:
						logger.Printf("session %s ended before its streams finished", l.snapshot().SessionID)
					}
					break
				}
			}
			writeStatus()
		case <-nextUp:
			// The new session is established: it serves from now on, and the
			// previous one drains.
			previous := active
			active, next, nextUp, nextTimeout = next, nil, nil, nil
			bound := connector.DrainBound(previous.cfg)
			previous.startDrain(bound, logger)
			olds = append(olds, previous)
			if active.cfg.StatusFile != statusPath {
				status.Remove(statusPath)
				statusPath = active.cfg.StatusFile
			}
			logger.Printf("configuration reloaded: session %s serves the new configuration; session %s drains for up to %s and its streams finish", active.snapshot().SessionID, previous.snapshot().SessionID, bound)
			writeStatus()
		case <-nextTimeout:
			st := next.snapshot()
			reason := "no connection attempt has completed"
			if st.LastError != "" {
				reason = "last error: " + st.LastError
			}
			logger.Printf("reload failed: the new session was not established within %s (%s); keeping the previous session and configuration", o.establishTimeout, reason)
			retire(next)
			next, nextUp, nextTimeout = nil, nil, nil
		case <-stopTimeout:
			logger.Printf("drain timeout reached; ending the streams still running")
			stopAll()
			stopTimeout = nil
		case sig := <-o.signals:
			switch sig {
			case syscall.SIGUSR1:
				for _, h := range handlers() {
					if err := h.ReopenLogs(); err != nil {
						logger.Printf("access log reopen failed: %v", err)
					}
				}
			case syscall.SIGHUP:
				if stopping {
					continue
				}
				replacement, err := config.Load(path)
				if err == nil {
					err = session.ValidateConfig(context.Background(), replacement)
				}
				var nh *connector.Connector
				if err == nil {
					nh, err = connector.New(replacement)
				}
				if err != nil {
					logger.Printf("reload rejected, keeping the running configuration: %v", err)
					continue
				}
				if next != nil {
					logger.Print("reload: replacing the reload still waiting for its session")
					retire(next)
				}
				nh.InheritStats(active.handler)
				next = start(replacement, nh)
				nextUp, nextTimeout = next.up, time.After(o.establishTimeout)
				logger.Printf("reload: new session starting; the current session keeps serving until it is up (waiting up to %s)", o.establishTimeout)
			default:
				if stopping {
					logger.Print("second stop signal: stopping now")
					stopAll()
					continue
				}
				stopping = true
				logger.Printf("stopping: draining the session (up to %s); signal again to stop at once", o.drainTimeout)
				if next != nil {
					if next.snapshot().Connected {
						next.startDrain(o.drainTimeout, logger)
						olds = append(olds, next)
					} else {
						retire(next)
					}
					next, nextUp, nextTimeout = nil, nil, nil
				}
				active.startDrain(o.drainTimeout, logger)
				olds = append(olds, active)
				active = nil
				stopTimeout = time.After(o.drainTimeout)
				writeStatus()
			}
		}
	}
}
