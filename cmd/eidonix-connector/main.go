// eidonix-connector shares the services one configuration file names with
// Eidonix, over one outbound connection.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/connector"
	"github.com/eidonix-ai/eidonix-connector/internal/convert"
	"github.com/eidonix-ai/eidonix-connector/internal/session"
	"github.com/eidonix-ai/eidonix-connector/internal/storage"
)

var version = "0.3.0"

const usage = `usage:
  eidonix-connector run [--config PATH]                          connect and share the configured services
  eidonix-connector check [--config PATH] [--redacted] [--write-probe]
                                                                 explain the configuration (--redacted: print it with secrets hidden)
  eidonix-connector init [PATH]                                  write a blank configuration (stdout without PATH)
  eidonix-connector generate-key                                 print a new storage encryption-key
  eidonix-connector convert --site NAME OLD                      convert a v0.2 (or v0.1) configuration to v0.3
                                                                 (YAML to stdout, review list to stderr; NAME is the connector's name in Setup)
  eidonix-connector storage move-to-root SERVICE --from FOLDER/ [--config PATH] [--dry-run]
                                                                 move a storage service's files from a folder to the top of its bucket
  eidonix-connector version`

func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr, os.Geteuid()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func execute(args []string, out, errOut io.Writer, uid int) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	path := fs.String("config", config.DefaultPath, "configuration file")
	allowRoot := fs.Bool("allow-root", false, "allow running as root (diagnostics only)")
	redacted := fs.Bool("redacted", false, "check: print the file with every secret replaced")
	writeProbe := fs.Bool("write-probe", false, "check: also write, read and delete a temporary storage object")
	site := fs.String("site", "", "convert: the connector's name in Eidonix Setup")
	from := fs.String("from", "", "storage move-to-root: the folder to move (ending in /)")
	dryRun := fs.Bool("dry-run", false, "storage move-to-root: report what would be moved, change nothing")
	rest, err := parseInterleaved(fs, args[1:])
	if err != nil {
		return err
	}
	maxArgs := map[string]int{"init": 1, "convert": 1, "storage": 2}[command]
	if len(rest) > maxArgs || command == "convert" && len(rest) != 1 || command == "storage" && (len(rest) != 2 || rest[0] != "move-to-root") {
		return errors.New("unexpected arguments\n" + usage)
	}
	if (*redacted || *writeProbe) && command != "check" {
		return errors.New("--redacted and --write-probe apply to check only")
	}
	if *site != "" && command != "convert" {
		return errors.New("--site applies to convert only")
	}
	if (*from != "" || *dryRun) && command != "storage" {
		return errors.New("--from and --dry-run apply to storage move-to-root only")
	}
	switch command {
	case "version":
		fmt.Fprintln(out, version)
		return nil
	case "run":
		if uid == 0 && !*allowRoot {
			return errors.New("refusing to run as root; use the eidonix-connector service account (diagnostics: --allow-root)")
		}
		cfg, err := config.Load(*path)
		if err != nil {
			return err
		}
		return run(*path, cfg, errOut)
	case "check":
		return check(*path, *redacted, *writeProbe, out)
	case "init":
		if len(rest) == 0 {
			_, err := io.WriteString(out, config.Template)
			return err
		}
		f, err := os.OpenFile(rest[0], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("%s already exists; init never overwrites a configuration", rest[0])
			}
			return err
		}
		if _, err = io.WriteString(f, config.Template); err != nil {
			f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		fmt.Fprintf(errOut, "wrote %s; paste the token and public key from Eidonix Setup, add your services, then run eidonix-connector check --config %s\n", rest[0], rest[0])
		return nil
	case "generate-key":
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return err
		}
		fmt.Fprintln(out, base64.StdEncoding.EncodeToString(key))
		return nil
	case "convert":
		if *site == "" {
			return errors.New("convert needs --site NAME: the connector's name in Eidonix Setup\n" + usage)
		}
		yaml, notes, err := convert.Convert(rest[0], *site)
		if err != nil {
			return err
		}
		if _, err = out.Write(yaml); err != nil {
			return err
		}
		if len(notes) > 0 {
			fmt.Fprintln(errOut, "Review before use:")
			for _, n := range notes {
				fmt.Fprintln(errOut, "  - "+n)
			}
		}
		return nil
	case "storage":
		if *from == "" {
			return errors.New("storage move-to-root needs --from FOLDER/ (for example --from cletus/)")
		}
		return moveToRoot(*path, rest[1], *from, *dryRun, out)
	}
	return fmt.Errorf("unknown command %q\n%s", command, usage)
}

// parseInterleaved parses flags that may come before, between or after the
// positional arguments, and returns the positional arguments.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var rest []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return rest, nil
		}
		if args[0] == "--" {
			return append(rest, args[1:]...), nil
		}
		rest = append(rest, args[0])
		args = args[1:]
	}
}

// moveToRoot runs storage move-to-root for the named s3 service of the file
// at path.
func moveToRoot(path, name, from string, dryRun bool, out io.Writer) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	for _, svc := range cfg.Services {
		if svc.Name != name {
			continue
		}
		if svc.Kind != config.KindS3 {
			return fmt.Errorf("service %q is not an s3 service", name)
		}
		if !dryRun {
			fmt.Fprintln(out, "Make sure the connector is stopped (systemctl stop eidonix-connector) so nothing writes while files move.")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return storage.MoveToRoot(ctx, svc, from, dryRun, out)
	}
	return fmt.Errorf("%s has no service %q", path, name)
}

// check validates the file (permission gate included) and explains it, then
// runs the read-only storage checks. With redacted it prints the file itself,
// secrets hidden, for an auditor or a support ticket.
func check(path string, redacted, writeProbe bool, out io.Writer) error {
	data, _, err := config.ReadFile(path)
	if err != nil {
		return err
	}
	if redacted {
		safe, err := config.Redact(data)
		if err != nil {
			return err
		}
		if _, err = out.Write(safe); err != nil {
			return err
		}
		_, err = config.Load(path)
		return err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if _, err = io.WriteString(out, cfg.Describe()); err != nil {
		return err
	}
	var failed error
	for _, svc := range cfg.Services {
		if svc.Storage == nil {
			continue
		}
		if err := checkStorage(svc, writeProbe, out); err != nil && failed == nil {
			failed = err
		}
	}
	return failed
}

func checkStorage(svc config.Service, writeProbe bool, out io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	h, err := storage.New(ctx, svc, nil)
	if err != nil {
		fmt.Fprintf(out, "Storage %s: cannot start: %v\n", svc.Name, err)
		return err
	}
	defer h.Close()
	checks, err := h.Check(ctx, writeProbe)
	for _, c := range checks {
		state := "ok"
		if !c.OK {
			state = "FAILED"
		} else if c.Warning {
			state = "warning"
		}
		line := fmt.Sprintf("Storage %s: %s: %s", svc.Name, strings.ReplaceAll(c.Name, "_", " "), state)
		if c.Message != "" {
			line += " (" + c.Message + ")"
		}
		fmt.Fprintln(out, line)
	}
	return err
}

type runningContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newRunningContext() *runningContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &runningContext{ctx, cancel}
}

// run serves until SIGINT/SIGTERM. SIGHUP reloads the file (a rejected reload
// keeps the running configuration); SIGUSR1 reopens the access log.
func run(path string, cfg *config.Config, errOut io.Writer) error {
	logger := log.New(errOut, "", log.LstdFlags)
	if err := session.ValidateConfig(context.Background(), cfg); err != nil {
		return err
	}
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGUSR1)
	defer signal.Stop(signals)
	handler, err := connector.New(cfg)
	if err != nil {
		return err
	}
	active := newRunningContext()
	done := make(chan error, 1)
	start := func() { go func() { done <- session.Run(active.ctx, cfg, handler, version) }() }
	start()
	defer func() { active.cancel(); handler.Close() }()
	for {
		select {
		case err := <-done:
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case sig := <-signals:
			if sig == syscall.SIGUSR1 {
				if err := handler.ReopenLogs(); err != nil {
					logger.Printf("access log reopen failed: %v", err)
				}
				continue
			}
			if sig != syscall.SIGHUP {
				active.cancel()
				<-done
				return nil
			}
			replacement, err := config.Load(path)
			if err == nil {
				err = session.ValidateConfig(context.Background(), replacement)
			}
			var next *connector.Connector
			if err == nil {
				next, err = connector.New(replacement)
			}
			if err != nil {
				logger.Printf("reload rejected, keeping the running configuration: %v", err)
				continue
			}
			active.cancel()
			<-done
			handler.Close()
			cfg, handler = replacement, next
			active = newRunningContext()
			start()
			logger.Print("configuration reloaded; access log reopened; active streams ended")
		}
	}
}
