// Package network pins configured destinations; DNS changes cannot expand egress.
//
// A Policy may dial only the host:port pairs it was built from, at the
// addresses those hosts resolved to at start. There is no implicit localhost:
// a destination is reachable only if the configuration names it.
package network

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type Policy struct {
	mu      sync.RWMutex
	hosts   map[string][]string
	allowed map[string]bool
	lookup  func(context.Context, string) ([]net.IPAddr, error)
}

func New(ctx context.Context, origins []string) (*Policy, error) {
	p := &Policy{hosts: map[string][]string{}, allowed: map[string]bool{}, lookup: net.DefaultResolver.LookupIPAddr}
	for _, s := range origins {
		u, e := url.Parse(s)
		if e != nil || u.Hostname() == "" || strings.Contains(u.Hostname(), "%") {
			return nil, fmt.Errorf("invalid origin")
		}
		port := u.Port()
		if port == "" {
			port = "443"
			if u.Scheme == "http" {
				port = "80"
			}
		}
		ips, e := p.lookup(ctx, u.Hostname())
		if e != nil {
			return nil, e
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("empty DNS answer")
		}
		key := net.JoinHostPort(u.Hostname(), port)
		for _, ip := range ips {
			if ip.Zone != "" || ip.IP.To16() == nil {
				return nil, fmt.Errorf("invalid resolved IP address")
			}
			addr := net.JoinHostPort(ip.IP.String(), port)
			p.hosts[key] = append(p.hosts[key], addr)
			p.allowed[addr] = true
		}
	}
	return p, nil
}
func (p *Policy) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("network denied")
	}
	p.mu.RLock()
	ips := append([]string{}, p.hosts[addr]...)
	p.mu.RUnlock()
	if len(ips) == 0 {
		return nil, fmt.Errorf("destination denied: %s", addr)
	}
	var last error
	for _, ip := range ips {
		d := net.Dialer{Control: func(_, address string, _ syscall.RawConn) error {
			p.mu.RLock()
			ok := p.allowed[address]
			p.mu.RUnlock()
			if !ok {
				return fmt.Errorf("dial control denied %s", address)
			}
			return nil
		}}
		c, e := d.DialContext(ctx, network, ip)
		if e == nil {
			return c, nil
		}
		last = e
	}
	return nil, last
}
func (p *Policy) Addresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := []string{}
	for a := range p.allowed {
		h, _, _ := net.SplitHostPort(a)
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// Refresh verifies DNS still resolves within the startup pins; a changed address needs a local restart.
func (p *Policy) Refresh(ctx context.Context) error {
	p.mu.RLock()
	keys := []string{}
	for k := range p.hosts {
		keys = append(keys, k)
	}
	p.mu.RUnlock()
	for _, k := range keys {
		h, port, _ := net.SplitHostPort(k)
		ips, e := p.lookup(ctx, h)
		if e != nil {
			return e
		}
		if len(ips) == 0 {
			return fmt.Errorf("empty DNS answer")
		}
		for _, ip := range ips {
			if ip.Zone != "" || ip.IP.To16() == nil {
				return fmt.Errorf("invalid resolved IP address")
			}
			p.mu.RLock()
			ok := p.allowed[net.JoinHostPort(ip.IP.String(), port)]
			p.mu.RUnlock()
			if !ok {
				return fmt.Errorf("%s now resolves to %s, which was not its address at start; restart to accept it", h, ip.IP)
			}
		}
	}
	return nil
}
