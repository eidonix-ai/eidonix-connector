// Package network pins configured destinations; DNS changes cannot expand egress.
package network

import (
	"bufio"
	"context"
	"fmt"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
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

func New(ctx context.Context, origins, cidrs []string) (*Policy, error) {
	p := &Policy{hosts: map[string][]string{}, allowed: map[string]bool{}, lookup: net.DefaultResolver.LookupIPAddr}
	nets := []*net.IPNet{}
	for _, s := range cidrs {
		_, n, e := net.ParseCIDR(s)
		if e != nil {
			return nil, e
		}
		nets = append(nets, n)
	}
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
			if len(nets) > 0 {
				ok := false
				for _, n := range nets {
					ok = ok || n.Contains(ip.IP)
				}
				if !ok {
					return nil, fmt.Errorf("DNS address outside allow_cidrs")
				}
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

// Refresh verifies DNS remains within the startup pins; changed addresses require local restart and firewall regeneration.
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
				return fmt.Errorf("DNS changed; regenerate print-systemd-network and restart")
			}
		}
	}
	return nil
}
func Systemd(ctx context.Context, c *config.Config) (string, error) {
	origins := []string{c.Server}
	request, _ := http.NewRequest("GET", c.Server, nil)
	proxy, err := http.ProxyFromEnvironment(request)
	if err != nil {
		return "", err
	}
	if proxy != nil {
		origins = append(origins, proxy.String())
	}
	for _, r := range c.Routes {
		origins = append(origins, r.Upstreams...)
		if r.Type == "object_storage" {
			origins = append(origins, r.Endpoint)
			if !r.PathStyle {
				u, err := url.Parse(r.Endpoint)
				if err != nil {
					return "", err
				}
				u.Host = r.Bucket + "." + u.Host
				origins = append(origins, u.String())
			}
		}
	}
	p, e := New(ctx, origins, nil)
	if e != nil {
		return "", e
	}
	lines := []string{"[Service]", "IPAddressDeny=any", "IPAddressAllow=localhost"}
	seen := map[string]bool{}
	for _, ip := range p.Addresses() {
		if !seen[ip] {
			seen[ip] = true
			lines = append(lines, "IPAddressAllow="+ip)
		}
	}
	f, err := os.Open("/etc/resolv.conf")
	if err != nil {
		return "", fmt.Errorf("read DNS resolver policy: %w", err)
	}
	defer f.Close()
	resolvers, err := resolverAddresses(f)
	if err != nil {
		return "", err
	}
	lines = append(lines, "# DNS resolvers below are not enabled automatically. Review and uncomment remote resolver IPs before starting the service.")
	for _, ip := range resolvers {
		if !net.ParseIP(ip).IsLoopback() {
			lines = append(lines, "# IPAddressAllow="+ip)
		}
	}
	return strings.Join(lines, "\n") + "\n", nil
}

func resolverAddresses(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(io.LimitReader(r, 65537))
	seen := map[string]bool{}
	for scanner.Scan() {
		fields := strings.Fields(strings.Split(scanner.Text(), "#")[0])
		if len(fields) == 0 || fields[0] != "nameserver" {
			continue
		}
		if len(fields) < 2 || net.ParseIP(fields[1]) == nil {
			return nil, fmt.Errorf("invalid nameserver in resolv.conf; review DNS configuration")
		}
		seen[net.ParseIP(fields[1]).String()] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for ip := range seen {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out, nil
}
