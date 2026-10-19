package hub

import (
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
)

// Hub holds the connector sessions and routes local requests to them. Its
// configuration is replaced as a whole by Apply (file reloads).
type Hub struct {
	cfg     atomic.Pointer[Config]
	log     *slog.Logger
	access  *AccessLog
	limiter *rateLimiter

	mu       sync.Mutex
	sessions map[string][]*session // site → live sessions, oldest first
	closed   bool
	wg       sync.WaitGroup

	// authAttempts counts auth messages received (tests: a hub whose
	// identity a connector refused never sees one).
	authAttempts atomic.Int64
}

// New returns a hub for a validated configuration. access may be nil (no
// access log); log may be nil (slog.Default).
func New(cfg *Config, access *AccessLog, log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	h := &Hub{log: log, access: access, limiter: newRateLimiter(upgradeRate, upgradeBurst), sessions: map[string][]*session{}}
	h.cfg.Store(cfg)
	return h
}

func (h *Hub) config() *Config { return h.cfg.Load() }

// Apply replaces the configuration. Sessions the new file no longer admits
// (site removed or disabled, token rotated away) are closed at once, ending
// their in-flight streams. The listen addresses are fixed at start; a change
// to them is reported and takes effect at the next start.
func (h *Hub) Apply(next *Config) {
	prev := h.cfg.Swap(next)
	if prev != nil && (prev.Public != next.Public || prev.Local != next.Local) {
		h.log.Warn("public and local take effect at the next start; still listening on the old addresses", "public", prev.Public, "local", prev.Local)
	}
	if prev != nil && prev.KeyID != next.KeyID {
		h.log.Info("server key changed; new sessions prove the new key", "key_id", next.KeyID)
	}
	if h.access != nil && prev != nil && prev.AccessLog != next.AccessLog {
		if err := h.access.SetPath(next.AccessLog); err != nil {
			h.log.Error("access log: cannot open the new path; keeping the old one", "path", next.AccessLog, "error", err)
		}
	}
	h.enforce()
}

// enforce closes every session the current file does not admit.
func (h *Hub) enforce() {
	cfg := h.config()
	var stale []*session
	var reasons []string
	h.mu.Lock()
	for _, list := range h.sessions {
		for _, s := range list {
			if reason := cfg.refuses(s); reason != "" {
				stale = append(stale, s)
				reasons = append(reasons, reason)
			}
		}
	}
	h.mu.Unlock()
	for i, s := range stale {
		h.log.Info("closing session", "site", s.site, "session", s.id, "reason", reasons[i])
		go s.close(reasons[i])
	}
}

// Close ends every session (shutdown) and waits for their handlers.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	all := h.sessions
	h.sessions = map[string][]*session{}
	h.mu.Unlock()
	for _, list := range all {
		for _, s := range list {
			s.close("hub shutting down")
		}
	}
	h.wg.Wait()
}

// lease is one acquired request slot: the session's stream slot and the
// service's own.
type lease struct {
	s  *session
	rt *route
}

func (l lease) release() {
	l.rt.inFlight.Add(-1)
	<-l.rt.slots
	<-l.s.slots
}

// pick acquires a slot for one request to site/service on any session but
// except (nil: any). The error is the hub's code: site_offline (no session
// that takes new requests), unknown_service, service_unhealthy, or busy
// (every offering session is at the service's limit).
func (h *Hub) pick(site, service string, except *session) (lease, string) {
	type cand struct {
		l       lease
		healthy bool
		n       int64
	}
	var cs []cand
	live := 0
	for _, s := range h.sessionsOf(site) {
		if s == except || s.mux.IsClosed() || s.draining.Load() {
			continue
		}
		live++
		s.mu.Lock()
		rt := s.routes[service]
		healthy := rt != nil && rt.healthy
		s.mu.Unlock()
		if rt == nil || rt.kind == "s3" && !s.has("storage/1") {
			continue
		}
		cs = append(cs, cand{lease{s, rt}, healthy, rt.inFlight.Load()})
	}
	switch {
	case live == 0:
		return lease{}, "site_offline"
	case len(cs) == 0:
		return lease{}, "unknown_service"
	}
	// Healthy sessions only, least loaded first; ties in random order so
	// idle sessions share the load.
	rand.Shuffle(len(cs), func(i, j int) { cs[i], cs[j] = cs[j], cs[i] })
	sort.SliceStable(cs, func(i, j int) bool { return cs[i].n < cs[j].n })
	anyHealthy := false
	for _, c := range cs {
		if !c.healthy {
			continue
		}
		anyHealthy = true
		select {
		case c.l.s.slots <- struct{}{}:
		default:
			continue
		}
		select {
		case c.l.rt.slots <- struct{}{}:
		default:
			<-c.l.s.slots
			continue
		}
		c.l.rt.inFlight.Add(1)
		return c.l, ""
	}
	if !anyHealthy {
		return lease{}, "service_unhealthy"
	}
	return lease{}, "busy"
}
