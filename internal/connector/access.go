package connector

import (
	"encoding/json"
	"time"
)

// accessEntry is one line of the access log: what was asked of which service,
// by whom (as the gateway reported it), and how it ended. Bodies are never logged.
type accessEntry struct {
	Time          time.Time `json:"time"`
	Session       string    `json:"session"`
	Service       string    `json:"service"`
	Kind          string    `json:"kind"`
	Method        string    `json:"method"`
	Path          string    `json:"path"`
	Status        int       `json:"status"`
	BytesIn       int64     `json:"bytes_in"`
	BytesOut      int64     `json:"bytes_out"`
	DurationMS    int64     `json:"duration_ms"`
	Model         string    `json:"model"`
	UpstreamModel string    `json:"upstream_model,omitempty"`
	OrgSlug       string    `json:"org_slug"`
	OrgID         string    `json:"org_id"`
	UserKeyID     string    `json:"user_key_id"`
	RequestID     string    `json:"request_id"`
	UpstreamHost  string    `json:"upstream_host"`
	Error         string    `json:"error"`
}

func (x *Connector) writeAccess(e *accessEntry) {
	x.auditMu.Lock()
	defer x.auditMu.Unlock()
	if x.audit != nil {
		json.NewEncoder(x.audit).Encode(e)
	}
}
