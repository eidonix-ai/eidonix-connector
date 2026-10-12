package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/eidonix-ai/cletus-connector/internal/config"
	"github.com/eidonix-ai/cletus-connector/internal/connector"
	"github.com/eidonix-ai/cletus-connector/internal/network"
	"github.com/eidonix-ai/cletus-connector/internal/session"
	"github.com/eidonix-ai/cletus-connector/internal/storage"
	"time"
)

var version = "dev"

type upstreamFlags []string

func (u *upstreamFlags) String() string     { return fmt.Sprint([]string(*u)) }
func (u *upstreamFlags) Set(v string) error { *u = append(*u, v); return nil }

func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr, os.Geteuid()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func execute(args []string, out, errOut io.Writer, uid int) error {
	if len(args) == 0 {
		return errors.New("usage: cletus-connector run|check-config|print-systemd-network|init|version")
	}
	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	path := fs.String("config", config.DefaultPath, "local TOML config file")
	writeProbe := fs.Bool("write-probe", false, "perform a temporary storage write/read/delete probe (check-config only)")
	allowRoot := fs.Bool("allow-root", false, "allow root for diagnostics only")
	profile := fs.String("profile", "vllm", "starter server profile")
	var upstream upstreamFlags
	fs.Var(&upstream, "upstream", "starter upstream origin (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	if *writeProbe && command != "check-config" {
		return errors.New("--write-probe requires check-config")
	}
	switch command {
	case "version":
		fmt.Fprintln(out, version)
		return nil
	case "init":
		return starter(out, *profile, upstream)
	case "run", "check-config", "print-systemd-network":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	if command == "run" && uid == 0 && !*allowRoot {
		return errors.New("refusing to run as root; use the service account (diagnostics: --allow-root)")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	switch command {
	case "check-config":
		for _, r := range cfg.Routes {
			if r.Type == "object_storage" {
				if err := checkStorage(r, cfg.AuditLog, *writeProbe, out); err != nil {
					return err
				}
				continue
			}
			// Print only policy fields: locally injected header values may contain secrets.
			policy := struct {
				ID          string            `json:"id"`
				Type        string            `json:"type"`
				Flavor      string            `json:"flavor"`
				Upstreams   []string          `json:"upstreams"`
				Methods     [][2]string       `json:"methods_paths"`
				Websocket   []string          `json:"websocket_paths"`
				Concurrency int               `json:"max_concurrency"`
				Request     string            `json:"max_request_bytes"`
				Response    string            `json:"max_response_bytes"`
				Idle        string            `json:"idle_timeout"`
				Models      map[string]string `json:"models,omitempty"`
			}{r.ID, r.Type, r.Flavor, r.Upstreams, r.MethodsPaths, r.WebsocketPaths, r.MaxConcurrency, r.MaxRequestBytes, r.MaxResponseBytes, r.IdleTimeout, r.Models}
			if err := json.NewEncoder(out).Encode(policy); err != nil {
				return err
			}
		}
		return nil
	case "print-systemd-network":
		s, err := network.Systemd(context.Background(), cfg)
		if err != nil {
			return err
		}
		_, err = fmt.Fprint(out, s)
		return err
	default:
		return run(*path, cfg, errOut)
	}
}
func starter(out io.Writer, profile string, upstream []string) error {
	types := map[string]string{"vllm": "openai_chat", "sglang": "openai_chat", "ollama": "openai_chat", "llamacpp": "openai_chat", "go-llm-proxy": "openai_chat", "litellm": "openai_chat", "tei": "openai_embeddings", "infinity": "openai_embeddings", "gotenberg": "document_conversion", "tika": "document_conversion"}
	flavor := profile
	if profile == "generic" {
		flavor = ""
		types[profile] = "openai_chat"
	}
	typ, ok := types[profile]
	if !ok {
		return fmt.Errorf("unknown starter profile %q", profile)
	}
	if len(upstream) == 0 {
		return errors.New("init requires --upstream")
	}
	cfg := config.Config{Server: "https://gateway.example.com", TokenFile: "/etc/cletus-connector/token", ServerKeys: map[string]string{"operator-key-id": "REPLACE_WITH_VERIFIED_BASE64_ED25519_PUBLIC_KEY"}, TLS: config.TLS{CA: "system"}, AuditLog: "/var/log/cletus-connector/audit.jsonl", Routes: []config.Route{{ID: "inference", Type: typ, Flavor: flavor, Upstreams: upstream}}}
	if profile == "generic" {
		cfg.Routes[0].ID = "chat"
	}
	// Validate the origin and flavor using a temporary syntactically valid key.
	validation := cfg
	validation.ServerKeys = map[string]string{"validation-only": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
	if err := validation.Validate(); err != nil {
		return err
	}
	fmt.Fprintln(out, "# Cletus connector configuration (TOML). Replace the placeholder with the server")
	fmt.Fprintln(out, "# operator's independently verified Ed25519 key. Add each additional service as its")
	fmt.Fprintln(out, "# own [[routes]] entry with its locally reviewed profile and upstream origin; map")
	fmt.Fprintln(out, "# gateway model names to upstream ids under [routes.models] where needed.")
	return config.Encode(out, &cfg)
}

type runningContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newRunningContext() *runningContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &runningContext{ctx, cancel}
}

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
					logger.Printf("audit log reopen failed: %v", err)
				}
				continue
			}
			if sig != syscall.SIGHUP {
				active.cancel()
				<-done
				return nil
			}
			replacement, err := config.Load(path)
			if err != nil {
				logger.Printf("reload rejected: %v", err)
				continue
			}
			if err := session.ValidateConfig(context.Background(), replacement); err != nil {
				logger.Printf("reload rejected: %v", err)
				continue
			}
			next, err := connector.NewReplacement(replacement, handler)
			if err != nil {
				logger.Printf("reload rejected: %v", err)
				continue
			}
			active.cancel()
			<-done
			handler.Close()
			cfg, handler = replacement, next
			active = newRunningContext()
			start()
			logger.Print("local configuration reloaded; audit log reopened; active streams ended")
		}
	}
}

func checkStorage(route config.Route, auditPath string, writeProbe bool, out io.Writer) error {
	var audit *os.File
	if auditPath != "" {
		var err error
		audit, err = os.OpenFile(auditPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer audit.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	h, err := storage.New(ctx, route, func(entry storage.AuditEntry) {
		if audit != nil {
			json.NewEncoder(audit).Encode(entry)
		}
	})
	if err != nil {
		return err
	}
	defer h.Close()
	checks, checkErr := h.Check(ctx, writeProbe)
	endpoint, _ := url.Parse(route.Endpoint)
	scheme := endpoint.Scheme
	if err = json.NewEncoder(out).Encode(map[string]any{"id": route.ID, "type": route.Type, "bucket": route.Bucket, "prefix": route.Prefix, "encryption": route.Encryption.Mode, "endpoint_scheme": scheme, "checks": checks, "lifecycle_reminder": "configure a prefix-scoped AbortIncompleteMultipartUpload rule using operator tools"}); err != nil {
		return err
	}
	return checkErr
}
