package hub

import (
	"fmt"
	"net"
	"sort"
	"strings"
)

// Describe explains a configuration in sentences, for check. It never
// prints a secret.
func (c *Config) Describe() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Connectors connect to %s (put TLS in front of it).\n", c.Public)
	fmt.Fprintf(&b, "Applications on this host use %s (never publish it).\n", c.Local)
	fmt.Fprintf(&b, "Server key %s; its public key goes in each connector's public-keys:\n  %s\n", c.KeyID, c.PublicKey)
	if c.AccessLog == "-" {
		b.WriteString("Logs every request to standard output.\n")
	} else {
		fmt.Fprintf(&b, "Logs every request to %s.\n", c.AccessLog)
	}
	fmt.Fprintf(&b, "%d site%s:\n", len(c.Order), plural(len(c.Order)))
	for _, name := range c.Order {
		s := c.Sites[name]
		parts := []string{"token set"}
		if s.NextToken != "" {
			parts = append(parts, "next-token set (rotation)")
		}
		if s.Disabled {
			parts = append(parts, "DISABLED (sessions refused)")
		}
		if s.Applications != nil {
			parts = append(parts, "only for applications: "+strings.Join(s.Applications, ", "))
		} else {
			parts = append(parts, "any application")
		}
		fmt.Fprintf(&b, "  %s  %s\n", name, strings.Join(parts, "; "))
		names := make([]string, 0, len(s.Services))
		for n := range s.Services {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			if apps := s.Services[n].Applications; apps != nil {
				fmt.Fprintf(&b, "    service %s: only for applications: %s\n", n, strings.Join(apps, ", "))
			} else {
				fmt.Fprintf(&b, "    service %s: as the site\n", n)
			}
		}
	}
	return b.String()
}

// LocalURL is the URL this host's tools use to reach the local port.
func (c *Config) LocalURL() string {
	host, port, _ := net.SplitHostPort(c.Local)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// UnofferedServices lists "site/service" for each per-service setting whose
// site is online in cat but offers no such service (check warns about them).
func (c *Config) UnofferedServices(cat Catalog) []string {
	byName := map[string]CatalogSite{}
	for _, s := range cat.Sites {
		byName[s.Name] = s
	}
	var out []string
	for _, name := range c.Order {
		cs, ok := byName[name]
		if !ok || len(cs.Sessions) == 0 {
			continue
		}
		offered := map[string]bool{}
		for _, svc := range cs.Services {
			offered[svc.Name] = true
		}
		names := []string{}
		for n := range c.Sites[name].Services {
			if !offered[n] {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		for _, n := range names {
			out = append(out, name+"/"+n)
		}
	}
	return out
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
