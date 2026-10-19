package hub

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Limits on what a connector may claim or do. A connector is semi-trusted:
// it serves only traffic the hub routes to it and must never be able to
// redirect or flood the hub.
const (
	maxServices            = 128
	maxModelsPerService    = 256
	maxModelBytes          = 200
	maxEndpointsPerService = 64
	// maxStreamsPerSession caps a session's streams: the sum of its
	// services' advertised limits, at most this.
	maxStreamsPerSession = 256
	// maxSessionsPerSite bounds the live sessions of one site (several
	// connector instances, plus a draining one during a reload).
	maxSessionsPerSite = 16
	maxCapabilities    = 16
	helloTimeout       = 10 * time.Second
	pingInterval       = 15 * time.Second
	pongTimeout        = 45 * time.Second
	controlWrite       = 10 * time.Second

	// maxResponseHeader / maxResponseBody bound what a connector can make
	// the hub buffer or relay.
	maxResponseHeader = 64 << 10
	maxResponseBody   = 512 << 20
	// streamIdle closes an HTTP or WebSocket stream with no bytes either
	// way. The connector enforces each service's own idle-timeout; this is
	// the hub's backstop, generous enough for a long prefill.
	streamIdle = 15 * time.Minute
	// storageIdle backs a storage service's own idle timeout (300 s).
	storageIdle = 10 * time.Minute
	// maxWSMessage bounds one relayed WebSocket message.
	maxWSMessage = 4 << 20

	// Session upgrade rate: per client IP before authentication and per
	// token after it.
	upgradeRate  = 0.5
	upgradeBurst = 10
)

// rateLimiter is a token bucket per key.
type rateLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	buckets map[string]*bucket
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(rate, burst float64) *rateLimiter {
	return &rateLimiter{rate: rate, burst: burst, buckets: map[string]*bucket{}, now: time.Now}
}

// allow takes one token from key's bucket.
func (l *rateLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if len(l.buckets) > 10000 {
		// Forget buckets that have refilled: they are indistinguishable
		// from new ones.
		for k, b := range l.buckets {
			if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
				delete(l.buckets, k)
			}
		}
	}
	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// clientIP is the address a session came from. The public port sits behind
// the TLS front proxy, so when the direct peer is a loopback or private
// address the last X-Forwarded-For entry (the one the proxy added) is used;
// a peer on a public address is used as-is.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer == nil || !(peer.IsLoopback() || peer.IsPrivate()) {
		return host
	}
	xff := r.Header.Values("X-Forwarded-For")
	if len(xff) == 0 {
		return host
	}
	parts := strings.Split(xff[len(xff)-1], ",")
	if ip := net.ParseIP(strings.TrimSpace(parts[len(parts)-1])); ip != nil {
		return ip.String()
	}
	return host
}
