package hub

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// Catalog is GET /v1/catalog: every site in the file and what it offers now.
type Catalog struct {
	Sites []CatalogSite `json:"sites"`
}

// CatalogSite is one site (also GET /v1/catalog/<site>).
type CatalogSite struct {
	Name string `json:"name"`
	// Online: at least one session takes new requests.
	Online   bool             `json:"online"`
	Disabled bool             `json:"disabled,omitempty"`
	Sessions []CatalogSession `json:"sessions"`
	Services []CatalogService `json:"services"`
}

// CatalogSession is one live connector session.
type CatalogSession struct {
	Instance    string `json:"instance"`
	Version     string `json:"version"`
	ConnectedAt string `json:"connected_at"`
	RemoteIP    string `json:"remote_ip"`
	Draining    bool   `json:"draining,omitempty"`
}

// CatalogService is one service, merged across the site's sessions:
// healthy when any non-draining session reports it healthy;
// max_concurrent_requests the largest of the non-draining sessions' (each
// session enforces its own); in_flight summed over all.
type CatalogService struct {
	Name                  string                   `json:"name"`
	Kind                  string                   `json:"kind"`
	Healthy               bool                     `json:"healthy"`
	Endpoints             []string                 `json:"endpoints,omitempty"`
	Models                []string                 `json:"models,omitempty"`
	MaxConcurrentRequests int                      `json:"max_concurrent_requests"`
	InFlight              int                      `json:"in_flight"`
	Storage               *protocol.StorageService `json:"storage,omitempty"`
}

// Catalog builds the catalog from the live sessions.
func (h *Hub) Catalog() Catalog {
	cfg := h.config()
	out := Catalog{Sites: []CatalogSite{}}
	for _, name := range cfg.Order {
		out.Sites = append(out.Sites, h.catalogSite(cfg.Sites[name]))
	}
	return out
}

func (h *Hub) catalogSite(site *Site) CatalogSite {
	cs := CatalogSite{Name: site.Name, Disabled: site.Disabled, Sessions: []CatalogSession{}, Services: []CatalogService{}}
	byName := map[string]*CatalogService{}
	for _, s := range h.sessionsOf(site.Name) {
		if s.mux.IsClosed() {
			continue
		}
		draining := s.draining.Load()
		cs.Online = cs.Online || !draining
		cs.Sessions = append(cs.Sessions, CatalogSession{Instance: s.instance, Version: s.version,
			ConnectedAt: s.connectedAt.Format(time.RFC3339), RemoteIP: s.remoteIP, Draining: draining})
		s.mu.Lock()
		for _, rt := range s.routes {
			svc := byName[rt.name]
			if svc == nil {
				svc = &CatalogService{Name: rt.name, Kind: rt.kind, Endpoints: append([]string(nil), rt.endpoints...), Storage: rt.storage}
				byName[rt.name] = svc
			}
			svc.InFlight += int(rt.inFlight.Load())
			if draining {
				continue
			}
			svc.Healthy = svc.Healthy || rt.healthy
			svc.MaxConcurrentRequests = max(svc.MaxConcurrentRequests, rt.maxConcurrent)
			for _, m := range rt.models {
				if !contains(svc.Models, m) {
					svc.Models = append(svc.Models, m)
				}
			}
		}
		s.mu.Unlock()
	}
	for _, svc := range byName {
		cs.Services = append(cs.Services, *svc)
	}
	sort.Slice(cs.Services, func(i, j int) bool { return cs.Services[i].Name < cs.Services[j].Name })
	return cs
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func (h *Hub) serveCatalog(w http.ResponseWriter, r *http.Request, site string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, 0, "method_not_allowed")
		return
	}
	var v any
	if site == "" {
		v = h.Catalog()
	} else {
		s := h.config().Sites[site]
		if s == nil {
			writeError(w, 0, "unknown_site")
			return
		}
		v = h.catalogSite(s)
	}
	b, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(append(b, '\n'))
}
