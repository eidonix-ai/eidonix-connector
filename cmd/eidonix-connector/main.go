// eidonix-connector shares the services one configuration file names with
// Eidonix, over one outbound connection.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/connector"
	"github.com/eidonix-ai/eidonix-connector/internal/session"
)

var version = "0.2.0"

const usage = "usage: eidonix-connector run|check|version [--config PATH]"

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
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments; " + usage)
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
		if _, err := config.Load(*path); err != nil {
			return err
		}
		fmt.Fprintln(out, "configuration ok")
		return nil
	}
	return fmt.Errorf("unknown command %q; %s", command, usage)
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
