package keyserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
)

// How long a spent or expired claim stays in the file, so a late claim is
// answered claim_used or claim_expired rather than claim_invalid.
const claimKeep = 7 * 24 * time.Hour

// state is the key server's file: the keys issued by uid, and the claims
// that hand them over. A claim's raw key is deleted when it is claimed or
// expires; nothing else in the file can sign in as anyone.
type state struct {
	Keys   map[string]*entry `json:"keys"`
	Claims []*claim          `json:"claims"`
	dirty  bool
}

type entry struct {
	Email   string `json:"email"`
	Name    string `json:"name"`
	KConn   string `json:"k_conn"` // hex
	Status  string `json:"status"` // active | revoked
	Created string `json:"created"`
}

type claim struct {
	ClaimID string    `json:"claim_id"`
	UID     string    `json:"uid"`
	Secret  string    `json:"secret"` // base64url
	Key     string    `json:"key,omitempty"`
	Used    bool      `json:"used,omitempty"`
	Expires time.Time `json:"expires"`
}

// lock takes the state file's lock (a sibling .lock file, shared by the
// running connector and the keys command) and reads the file after the
// permission gate. The returned func releases the lock.
func lock(path string) (*state, func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, nil, err
	}
	lf, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	if err = syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		lf.Close()
		return nil, nil, err
	}
	unlock := func() { syscall.Flock(int(lf.Fd()), syscall.LOCK_UN); lf.Close() }
	st := &state{Keys: map[string]*entry{}}
	fi, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return st, unlock, nil
	case err != nil:
	default:
		if _, err = config.CheckPermissions(path, fi, os.Geteuid()); err == nil {
			var data []byte
			if data, err = os.ReadFile(path); err == nil {
				if err = json.Unmarshal(data, st); err == nil && st.Keys == nil {
					st.Keys = map[string]*entry{}
				}
			}
		}
	}
	if err != nil {
		unlock()
		return nil, nil, fmt.Errorf("state file %s: %w", path, err)
	}
	return st, unlock, nil
}

// save replaces the file atomically (a temporary file in the same
// directory, then a rename); the file is 0600.
func (st *state) save(path string) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".keys-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(append(data, '\n'))
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	st.dirty = false
	return os.Rename(tmp.Name(), path)
}

// sweep deletes the raw key of every expired claim and forgets claims that
// ended long ago.
func (st *state) sweep(now time.Time) {
	kept := st.Claims[:0]
	for _, c := range st.Claims {
		if now.After(c.Expires) && c.Key != "" {
			c.Key, st.dirty = "", true
		}
		if now.After(c.Expires.Add(claimKeep)) {
			st.dirty = true
			continue
		}
		kept = append(kept, c)
	}
	st.Claims = kept
}

// dropClaims forgets every claim of uid.
func (st *state) dropClaims(uid string) {
	kept := st.Claims[:0]
	for _, c := range st.Claims {
		if c.UID == uid {
			st.dirty = true
			continue
		}
		kept = append(kept, c)
	}
	st.Claims = kept
}
