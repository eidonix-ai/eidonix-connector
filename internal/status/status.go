// Package status is the running connector's status file: a small JSON
// document the connector rewrites every 15 seconds and whenever its session
// changes, and that "eidonix-connector status" prints. It holds no secrets.
package status

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

// Interval is how often the connector rewrites the file when nothing changes.
const Interval = 15 * time.Second

// Stale is how old a file may be before it no longer describes a running
// connector (three missed updates).
const Stale = 3 * Interval

// File is the document. Times are RFC 3339 UTC; absent times are omitted.
type File struct {
	Version   string    `json:"version"`
	Site      string    `json:"site"`
	PID       int       `json:"pid"`
	UpdatedAt time.Time `json:"updated_at"`

	// Connected reports an established session with the hub. HubURL is the
	// configured upstream; Hub is the name the hub calls this connector.
	Connected      bool       `json:"connected"`
	HubURL         string     `json:"hub_url"`
	Hub            string     `json:"hub,omitempty"`
	SessionID      string     `json:"session_id,omitempty"`
	ConnectedSince *time.Time `json:"connected_since,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	LastErrorAt    *time.Time `json:"last_error_at,omitempty"`

	// Draining lists sessions of an earlier configuration (or a stopping
	// connector) that are finishing their streams.
	Draining []Draining `json:"draining"`
	Services []Service  `json:"services"`
}

// Draining is one session that accepts no new streams.
type Draining struct {
	SessionID string    `json:"session_id"`
	Since     time.Time `json:"since"`
	InFlight  int       `json:"in_flight"`
}

// Service is one shared service. Requests1h and Errors1h are the last hour's
// counts (an error is a request the connector refused or ended, a 5xx
// answer, or a storage operation that did not end ok).
type Service struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Healthy bool   `json:"healthy"`
	// Health is ok, degraded (some of a machine's ports down) or down;
	// files written before it have only Healthy.
	Health        string `json:"health,omitempty"`
	InFlight      int    `json:"in_flight"`
	MaxConcurrent int    `json:"max_concurrent"`
	Requests1h    int64  `json:"requests_1h"`
	Errors1h      int64  `json:"errors_1h"`
}

// Write replaces the file at path atomically (a temporary file in the same
// directory, then a rename). A missing directory is created readable only by
// its owner and group (0750); the file is 0640.
func Write(path string, f File) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".status-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err = tmp.Chmod(0640); err == nil {
		_, err = tmp.Write(append(data, '\n'))
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// ErrNotRunning reports that there is no status file.
var ErrNotRunning = errors.New("no status file")

// Read parses the file at path.
func Read(path string) (File, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return File{}, fmt.Errorf("%w at %s: the connector is not running, or it keeps its status elsewhere (--file, or status-file in the configuration)", ErrNotRunning, path)
	}
	if errors.Is(err, fs.ErrPermission) {
		return File{}, fmt.Errorf("%w; run it as root or as the connector's service account (sudo eidonix-connector status)", err)
	}
	if err != nil {
		return File{}, err
	}
	var f File
	if err = json.Unmarshal(data, &f); err != nil {
		return File{}, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

// Remove deletes the file (best effort) when the connector exits.
func Remove(path string) { os.Remove(path) }

// Print writes f as a readable report. now is the reference for ages.
func Print(w io.Writer, f File, now time.Time) {
	age := now.Sub(f.UpdatedAt).Round(time.Second)
	if age >= Stale {
		fmt.Fprintf(w, "Not running: the status file was last updated %s ago (%s); the connector has stopped or is stuck.\n\n", age, f.UpdatedAt.Local().Format("2006-01-02 15:04:05"))
	}
	fmt.Fprintf(w, "Site        %s (eidonix-connector %s, pid %d)\n", f.Site, f.Version, f.PID)
	switch {
	case f.Connected && f.ConnectedSince != nil:
		hub := f.HubURL
		if f.Hub != "" {
			hub += ` as "` + f.Hub + `"`
		}
		fmt.Fprintf(w, "Connection  connected to %s since %s (%s)\n", hub, f.ConnectedSince.Local().Format("2006-01-02 15:04:05"), since(now, *f.ConnectedSince))
		fmt.Fprintf(w, "Session     %s\n", f.SessionID)
	default:
		fmt.Fprintf(w, "Connection  NOT connected to %s\n", f.HubURL)
	}
	if f.LastError != "" {
		when := ""
		if f.LastErrorAt != nil {
			when = " (" + since(now, *f.LastErrorAt) + " ago)"
		}
		fmt.Fprintf(w, "Last error  %s%s\n", f.LastError, when)
	} else {
		fmt.Fprintln(w, "Last error  none")
	}
	if len(f.Draining) == 0 {
		fmt.Fprintln(w, "Draining    none")
	}
	for i, d := range f.Draining {
		label := "Draining    "
		if i > 0 {
			label = "            "
		}
		fmt.Fprintf(w, "%ssession %s since %s ago, %d in flight\n", label, d.SessionID, since(now, d.Since), d.InFlight)
	}
	fmt.Fprintf(w, "Updated     %s ago\n\n", age)
	if len(f.Services) == 0 {
		fmt.Fprintln(w, "No services configured.")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SERVICE\tKIND\tHEALTH\tIN FLIGHT\tMAX\tREQUESTS (1H)\tERRORS (1H)")
	for _, s := range f.Services {
		healthy := s.Health
		switch {
		case healthy == "" && s.Healthy:
			healthy = "ok"
		case healthy == "" || healthy == "down":
			healthy = "DOWN"
		case healthy == "degraded":
			healthy = "DEGRADED"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\n", s.Name, s.Kind, healthy, s.InFlight, s.MaxConcurrent, s.Requests1h, s.Errors1h)
	}
	tw.Flush()
}

// since formats a duration compactly: 45s, 12m, 3h10m, 2d4h.
func since(now, t time.Time) string {
	d := max(now.Sub(t), 0).Round(time.Second)
	switch {
	case d < time.Minute:
		return d.String()
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		if m := int(d.Minutes()) % 60; m > 0 {
			return fmt.Sprintf("%dh%dm", int(d.Hours()), m)
		}
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
}
