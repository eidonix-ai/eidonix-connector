package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/eidonix-ai/cletus-connector/internal/config"
	"github.com/eidonix-ai/cletus-connector/internal/connector"
	"github.com/eidonix-ai/cletus-connector/internal/network"
	"github.com/eidonix-ai/cletus-connector/internal/session"
	"go.yaml.in/yaml/v3"
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
	path := fs.String("config", "/etc/cletus-connector/config.yaml", "local config file")
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
			// Print only policy fields: locally injected header values may contain secrets.
			policy := struct {
				ID          string      `json:"id"`
				Type        string      `json:"type"`
				Flavor      string      `json:"flavor"`
				Upstreams   []string    `json:"upstreams"`
				Methods     [][2]string `json:"methods_paths"`
				Websocket   []string    `json:"websocket_paths"`
				Concurrency int         `json:"max_concurrency"`
				Request     string      `json:"max_request_bytes"`
				Response    string      `json:"max_response_bytes"`
				Idle        string      `json:"idle_timeout"`
			}{r.ID, r.Type, r.Flavor, r.Upstreams, r.MethodsPaths, r.WebsocketPaths, r.MaxConcurrency, r.MaxRequestBytes, r.MaxResponseBytes, r.IdleTimeout}
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
	if profile == "cletus-gpu-node" {
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
	cfg := config.Config{Server: "https://cletus.eidonix.com", TokenFile: "/etc/cletus-connector/token", ServerKeys: map[string]string{"operator-key-id": "REPLACE_WITH_VERIFIED_BASE64_ED25519_PUBLIC_KEY"}, TLS: config.TLS{CA: "system"}, AuditLog: "/var/log/cletus-connector/audit.jsonl", Routes: []config.Route{{ID: "inference", Type: typ, Flavor: flavor, Upstreams: upstream}}}
	if profile == "cletus-gpu-node" {
		cfg.Routes = nil
		for _, service := range []struct{ id, kind, port string }{{"chat-qwen", "openai_chat", "8004"}, {"embed", "openai_embeddings", "8001"}, {"rerank-jina", "rerank", "8006"}, {"rerank-bge", "rerank", "8008"}, {"stt", "audio_transcription", "8007"}, {"tts", "audio_speech", "8009"}, {"image", "image", "8010"}, {"ocr", "document_ocr", "8002"}} {
			bases := make([]string, 0, len(upstream))
			for _, base := range upstream {
				u, err := url.Parse(base)
				if err != nil {
					return err
				}
				if u.Hostname() == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
					return errors.New("invalid GPU node origin")
				}
				u.Host = net.JoinHostPort(u.Hostname(), service.port)
				bases = append(bases, u.String())
			}
			cfg.Routes = append(cfg.Routes, config.Route{ID: service.id, Type: service.kind, Upstreams: bases})
		}
		fmt.Fprintln(out, "# GPU node service ports are illustrative; verify locally. Live voice requires its exact path and must be added manually.")
	}
	// Validate the origin and flavor using a temporary syntactically valid key.
	validation := cfg
	validation.ServerKeys = map[string]string{"validation-only": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
	if err := validation.Validate(); err != nil {
		return err
	}
	return yaml.NewEncoder(out).Encode(cfg)
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
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
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
