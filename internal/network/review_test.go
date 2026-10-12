package network

import (
	"reflect"
	"strings"
	"testing"
)

func TestResolverPolicyComments(t *testing.T) {
	ips, err := resolverAddresses(strings.NewReader("# comment\nnameserver 192.0.2.53\nnameserver 2001:db8::53 # remote\nnameserver 127.0.0.53\nnameserver 192.0.2.53\nsearch example.com\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"127.0.0.53", "192.0.2.53", "2001:db8::53"}
	if !reflect.DeepEqual(ips, want) {
		t.Fatalf("resolvers %v", ips)
	}
	if _, err = resolverAddresses(strings.NewReader("nameserver not-an-ip\n")); err == nil {
		t.Fatal("malformed resolver accepted")
	}
}
