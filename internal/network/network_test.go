package network

import (
	"context"
	"net"
	"testing"
)

func TestPinnedDestinations(t *testing.T) {
	p, e := New(context.Background(), []string{"http://127.0.0.1:9000"}, []string{"127.0.0.0/8"})
	if e != nil {
		t.Fatal(e)
	}
	for _, addr := range []string{"127.0.0.1:9001", "169.254.169.254:80", "localhost:9000"} {
		if _, e = p.DialContext(context.Background(), "tcp", addr); e == nil {
			t.Fatalf("dial allowed %s", addr)
		}
	}
	if _, e = New(context.Background(), []string{"http://127.0.0.1:9000"}, []string{"192.0.2.0/24"}); e == nil {
		t.Fatal("CIDR restriction bypassed")
	}
}

func TestDNSRebindingCannotExpandPins(t *testing.T) {
	ctx := context.Background()
	p, e := New(ctx, []string{"http://127.0.0.1:9000"}, nil)
	if e != nil {
		t.Fatal(e)
	}
	for _, answers := range [][]net.IPAddr{{{IP: net.ParseIP("192.0.2.77")}}, nil, {{IP: net.ParseIP("127.0.0.1"), Zone: "eth0"}}, {{IP: net.IP{1, 2}}}} {
		p.lookup = func(context.Context, string) ([]net.IPAddr, error) { return answers, nil }
		if p.Refresh(ctx) == nil {
			t.Fatalf("refresh accepted unsafe answers %v", answers)
		}
		if len(p.allowed) != 1 || !p.allowed["127.0.0.1:9000"] || p.allowed["192.0.2.77:9000"] {
			t.Fatal("refresh widened immutable pins")
		}
		if _, e := p.DialContext(ctx, "tcp", "192.0.2.77:9000"); e == nil {
			t.Fatal("rebound destination dialed")
		}
	}
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}
	if e := p.Refresh(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestIPv6ZoneRejected(t *testing.T) {
	if _, e := New(context.Background(), []string{"http://[fe80::1%25eth0]:80"}, nil); e == nil {
		t.Fatal("IPv6 zone accepted")
	}
}
