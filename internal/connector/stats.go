package connector

import (
	"bufio"
	"context"
	"net"
	"sync"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// window is how far back the request and error counters look.
const window = time.Hour

// rolling counts requests and errors per minute over the last hour.
type rolling struct {
	mu      sync.Mutex
	buckets [60]struct{ minute, requests, errors int64 }
}

func (r *rolling) add(now time.Time, failed bool) {
	minute := now.Unix() / 60
	r.mu.Lock()
	defer r.mu.Unlock()
	b := &r.buckets[minute%60]
	if b.minute != minute {
		*b = struct{ minute, requests, errors int64 }{minute: minute}
	}
	b.requests++
	if failed {
		b.errors++
	}
}

func (r *rolling) totals(now time.Time) (requests, errors int64) {
	minute := now.Unix() / 60
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.buckets {
		if b.minute > minute-60 && b.minute <= minute {
			requests += b.requests
			errors += b.errors
		}
	}
	return
}

// ServiceSnapshot is one service as the status file reports it.
type ServiceSnapshot struct {
	Name          string
	Kind          string
	Healthy       bool
	InFlight      int
	MaxConcurrent int
	// Requests and Errors are the last hour's counts. A request is an error
	// when the connector refused or ended it (it carries an error code) or the
	// service answered 5xx; storage operations count when they do not end ok.
	Requests int64
	Errors   int64
}

// Snapshot returns every configured service's current state.
func (x *Connector) Snapshot() []ServiceSnapshot {
	now := time.Now()
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make([]ServiceSnapshot, 0, len(x.order))
	for _, name := range x.order {
		s := x.services[name]
		flight := s.flight
		if s.storage != nil {
			flight = s.storage.InFlight()
		}
		requests, errs := s.stats.totals(now)
		out = append(out, ServiceSnapshot{Name: name, Kind: s.cfg.Kind, Healthy: x.healthy(s), InFlight: flight, MaxConcurrent: s.cfg.MaxConcurrentRequests, Requests: requests, Errors: errs})
	}
	return out
}

// InFlight is the number of requests running now across every service.
func (x *Connector) InFlight() int {
	n := 0
	for _, s := range x.Snapshot() {
		n += s.InFlight
	}
	return n
}

// InheritStats makes the services this connector shares by name with old
// count into old's request and error totals, so a reload does not reset the
// last hour (requests still finishing on old keep counting in them).
func (x *Connector) InheritStats(old *Connector) {
	old.mu.Lock()
	previous := map[string]*rolling{}
	for name, s := range old.services {
		previous[name] = s.stats
	}
	old.mu.Unlock()
	x.mu.Lock()
	defer x.mu.Unlock()
	for name, s := range x.services {
		if r := previous[name]; r != nil {
			s.stats = r
		}
	}
}

// count records one finished request for the named service.
func (x *Connector) count(name string, failed bool) {
	x.mu.Lock()
	s := x.services[name]
	var r *rolling
	if s != nil {
		r = s.stats
	}
	x.mu.Unlock()
	if r != nil {
		r.add(time.Now(), failed)
	}
}

// RefuseStream answers a stream the session will not serve (it is draining):
// it reads the stream header and replies with the closed error code, as the
// connector would for any request it declines, then closes. Nothing reaches a
// service and nothing counts against one; the refusal is logged. A malformed
// header closes the stream without a reply, as everywhere.
func (x *Connector) RefuseStream(ctx context.Context, raw net.Conn, code string) {
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	h, err := protocol.ReadStreamHeader(bufio.NewReaderSize(raw, 4096))
	if err != nil {
		return
	}
	entry := &accessEntry{Time: start.UTC(), Session: protocol.SessionID(ctx), Service: h.Service, Kind: h.Kind, RequestID: h.RequestID, OrgID: h.OrgID, OrgSlug: h.OrgSlug, UserKeyID: h.UserKeyID, Application: h.Application, For: h.For, Error: code}
	x.mu.Lock()
	if s := x.services[h.Service]; s != nil {
		entry.Kind = s.cfg.Kind
	}
	x.mu.Unlock()
	if h.Kind == "storage" {
		entry.Status = protocol.HTTPStatus(code)
		protocol.WriteStorageResult(raw, protocol.StorageErrorResult(code), h.Op)
	} else {
		entry.Status = errorResponse(raw, code)
	}
	entry.DurationMS = time.Since(start).Milliseconds()
	x.writeAccess(entry)
}

// DrainBound is how long a session on this configuration may keep draining:
// the longest idle-timeout among the services, at most MaxDrain.
func DrainBound(c *config.Config) time.Duration {
	var longest time.Duration
	for _, s := range c.Services {
		d := s.IdleTimeout
		if s.Storage != nil {
			d = s.Storage.IdleTimeout
		}
		longest = max(longest, d)
	}
	return min(longest, MaxDrain)
}

// MaxDrain caps DrainBound.
const MaxDrain = 15 * time.Minute
