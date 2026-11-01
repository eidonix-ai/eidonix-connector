package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/keyserver"
)

// keysCommand runs `keys list | revoke UID | restore UID` against the state
// file of the key-server block, locally (the running connector reads the file
// for every operation, so no reload is needed). Run it as the user the
// connector runs as: the state file is theirs.
func keysCommand(path string, rest []string, out io.Writer) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if cfg.KeyServer == nil {
		return fmt.Errorf("%s has no key-server block", path)
	}
	srv := keyserver.New(cfg.Site, *cfg.KeyServer, nil)
	if rest[0] == "list" {
		ans, _ := srv.Handle("list", []byte("{}"))
		var l struct {
			Keys []struct {
				UID, Email, Name, Status, Created string
				PendingClaim                      bool `json:"pending_claim"`
			}
		}
		b, _ := json.Marshal(ans)
		if err = json.Unmarshal(b, &l); err != nil {
			return err
		}
		w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "UID\tEMAIL\tNAME\tSTATUS\tCREATED\tCLAIM")
		for _, k := range l.Keys {
			claim := "-"
			if k.PendingClaim {
				claim = "pending"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", k.UID, k.Email, k.Name, k.Status, k.Created, claim)
		}
		return w.Flush()
	}
	body, _ := json.Marshal(map[string]string{"uid": rest[1]})
	ans, _ := srv.Handle(rest[0], body)
	if ans["ok"] != true {
		return errors.New(fmt.Sprint(ans["error"], ": ", ans["message"]))
	}
	fmt.Fprintf(out, "%s %s\n", rest[1], map[string]string{"revoke": "revoked", "restore": "restored"}[rest[0]])
	return nil
}
