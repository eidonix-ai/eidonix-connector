package status

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sample(now time.Time) File {
	since, errAt := now.Add(-3*time.Hour-10*time.Minute), now.Add(-5*time.Minute)
	return File{
		Version: "0.4.0", Site: "plant-a", PID: 42, UpdatedAt: now.Add(-4 * time.Second),
		Connected: true, HubURL: "https://connect.example.com", Hub: "plant-a", SessionID: "s-7", ConnectedSince: &since,
		LastError: "upstream upgrade: connection refused", LastErrorAt: &errAt,
		Draining: []Draining{{SessionID: "s-6", Since: now.Add(-90 * time.Second), InFlight: 2}},
		Services: []Service{
			{Name: "chat", Kind: "http", Healthy: true, InFlight: 3, MaxConcurrent: 32, Requests1h: 120, Errors1h: 4},
			{Name: "files", Kind: "s3", Healthy: false, MaxConcurrent: 16},
		},
	}
}

func TestWriteIsAtomicPrivateAndReadable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run", "eidonix-connector")
	path := filepath.Join(dir, "status.json")
	now := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 3; i++ { // rewrites replace the file
		f := sample(now)
		f.PID = i
		if err := Write(path, f); err != nil {
			t.Fatal(err)
		}
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0640 {
		t.Fatalf("file mode %v", info.Mode())
	}
	if info, _ := os.Stat(dir); info.Mode().Perm() != 0750 {
		t.Fatalf("directory mode %v", info.Mode())
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("temporary files left: %v", entries)
	}
	got, err := Read(path)
	if err != nil || got.PID != 2 || got.Services[0].Requests1h != 120 || got.Draining[0].InFlight != 2 || !got.UpdatedAt.Equal(now.Add(-4*time.Second)) {
		t.Fatalf("%+v %v", got, err)
	}
	Remove(path)
	if _, err := Read(path); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
}

func TestPrint(t *testing.T) {
	now := time.Now()
	var out bytes.Buffer
	Print(&out, sample(now), now)
	for _, want := range []string{
		"Site        plant-a (eidonix-connector 0.4.0, pid 42)",
		`connected to https://connect.example.com as "plant-a" since `, "(3h10m)",
		"Session     s-7",
		"Last error  upstream upgrade: connection refused (5m ago)",
		"Draining    session s-6 since 1m ago, 2 in flight",
		"Updated     4s ago",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in\n%s", want, out.String())
		}
	}
	rows := map[string]string{}
	for _, line := range strings.Split(out.String(), "\n") {
		if fields := strings.Fields(line); len(fields) > 0 {
			rows[fields[0]] = strings.Join(fields, " ")
		}
	}
	if rows["chat"] != "chat http yes 3 32 120 4" || rows["files"] != "files s3 NO 0 16 0 0" {
		t.Fatalf("rows %v", rows)
	}
	if strings.Contains(out.String(), "Not running") {
		t.Fatal("fresh file reported stale")
	}

	stale := sample(now)
	stale.UpdatedAt = now.Add(-10 * time.Minute)
	stale.Connected = false
	out.Reset()
	Print(&out, stale, now)
	if !strings.HasPrefix(out.String(), "Not running: the status file was last updated 10m0s ago") || !strings.Contains(out.String(), "NOT connected to https://connect.example.com") {
		t.Fatal(out.String())
	}
}
