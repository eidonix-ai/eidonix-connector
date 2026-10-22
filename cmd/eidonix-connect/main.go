// eidonix-connect is the hub of the Eidonix connector system: it accepts
// connector sessions and makes every connected service reachable on one
// local port, by site and service name.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/hub"
)

var version = "0.5.1"

const usage = `usage:
  eidonix-connect serve [--config PATH]        accept connector sessions and serve the local port
  eidonix-connect check [--config PATH]        validate the file and explain it
  eidonix-connect status [--config PATH | --url URL] [--healthz]
                                               show sites and services (--healthz: exit 0 if the hub answers)
  eidonix-connect token                        print a new site token
  eidonix-connect server-key                   print a new server-key (its public key to stderr)
  eidonix-connect version`

func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	path := fs.String("config", hub.DefaultPath, "configuration file")
	rawURL := fs.String("url", "", "status: the hub's local URL (default: from the file's local)")
	healthz := fs.Bool("healthz", false, "status: only check that the hub answers")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return errors.New("unexpected arguments\n" + usage)
	}
	if command != "status" && (*rawURL != "" || *healthz) {
		return errors.New("--url and --healthz apply to status only")
	}
	switch command {
	case "version":
		fmt.Fprintln(out, version)
		return nil
	case "token":
		t, err := hub.NewToken()
		if err != nil {
			return err
		}
		fmt.Fprintln(out, t)
		return nil
	case "server-key":
		private, public, err := hub.NewServerKey()
		if err != nil {
			return err
		}
		fmt.Fprintln(out, private)
		fmt.Fprintf(errOut, "public key (for each connector's public-keys): %s\n", public)
		return nil
	case "check":
		cfg, err := hub.Load(*path)
		if err != nil {
			return err
		}
		if _, err := io.WriteString(out, cfg.Describe()); err != nil {
			return err
		}
		warnUnoffered(cfg, out)
		return nil
	case "status":
		return status(*path, *rawURL, *healthz, out)
	case "serve":
		return serve(*path, errOut)
	}
	return fmt.Errorf("unknown command %q\n%s", command, usage)
}

// warnUnoffered asks a running hub which per-service settings name services
// their (connected) site does not offer. Best effort: no running hub, no
// warning.
func warnUnoffered(cfg *hub.Config, out io.Writer) {
	cat, err := fetchCatalog(cfg.LocalURL(), 2*time.Second)
	if err != nil {
		fmt.Fprintf(out, "Not checked against the running hub (%s not reachable).\n", cfg.LocalURL())
		return
	}
	for _, s := range cfg.UnofferedServices(*cat) {
		fmt.Fprintf(out, "Warning: %s has settings in the file, but the connected site does not offer that service.\n", s)
	}
}

func fetchCatalog(base string, timeout time.Duration) (*hub.Catalog, error) {
	c := &http.Client{Timeout: timeout}
	resp, err := c.Get(strings.TrimRight(base, "/") + "/v1/catalog")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("catalog: HTTP %d", resp.StatusCode)
	}
	var cat hub.Catalog
	return &cat, json.NewDecoder(resp.Body).Decode(&cat)
}

func status(path, base string, healthz bool, out io.Writer) error {
	if base == "" {
		cfg, err := hub.Load(path)
		if err != nil {
			return err
		}
		base = cfg.LocalURL()
	}
	if healthz {
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(strings.TrimRight(base, "/") + "/healthz")
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("healthz: HTTP %d", resp.StatusCode)
		}
		fmt.Fprintln(out, "ok")
		return nil
	}
	cat, err := fetchCatalog(base, 10*time.Second)
	if err != nil {
		return fmt.Errorf("the hub at %s did not answer: %w", base, err)
	}
	printCatalog(cat, out)
	return nil
}

func printCatalog(cat *hub.Catalog, out io.Writer) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SITE\tSTATE\tSESSIONS\tSERVICE\tKIND\tHEALTH\tIN-FLIGHT\tMAX")
	for _, s := range cat.Sites {
		state := "offline"
		switch {
		case s.Disabled:
			state = "disabled"
		case s.Online:
			state = "online"
		case len(s.Sessions) > 0:
			state = "draining"
		}
		sessions := fmt.Sprint(len(s.Sessions))
		if len(s.Services) == 0 {
			fmt.Fprintf(tw, "%s\t%s\t%s\t-\t\t\t\t\n", s.Name, state, sessions)
			continue
		}
		for i, svc := range s.Services {
			health := "unhealthy"
			if svc.Healthy {
				health = "healthy"
			}
			name, st, n := s.Name, state, sessions
			if i > 0 {
				name, st, n = "", "", ""
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\n", name, st, n, svc.Name, svc.Kind, health, svc.InFlight, svc.MaxConcurrentRequests)
		}
	}
	tw.Flush()
}

// serve runs until SIGINT/SIGTERM. The file is reloaded when it changes;
// SIGUSR1 reopens the access log.
func serve(path string, errOut io.Writer) error {
	log := slog.New(slog.NewTextHandler(errOut, nil))
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &hub.Server{Path: path, Log: log}
	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, syscall.SIGUSR1)
	defer signal.Stop(usr1)
	go func() {
		for range usr1 {
			if err := srv.ReopenAccessLog(); err != nil {
				log.Error("access log reopen failed", "error", err)
			} else {
				log.Info("access log reopened")
			}
		}
	}()
	return srv.Serve(ctx)
}
