package session

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"
)

func prepare(ctx context.Context, cfg *config.Config, version string) (Options, error) {
	normalized, err := cfg.Normalized()
	if err != nil {
		return Options{}, err
	}
	cfg = normalized
	token, e := readToken(cfg.TokenFile)
	if e != nil {
		return Options{}, e
	}
	keys := []ed25519.PublicKey{}
	for _, s := range cfg.ServerKeys {
		k, e := protocol.ParsePublicKey(s)
		if e != nil {
			return Options{}, e
		}
		keys = append(keys, k)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLS.CA == "file" {
		pem, e := os.ReadFile(cfg.TLS.CAFile)
		if e != nil {
			return Options{}, e
		}
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return Options{}, errors.New("invalid CA bundle")
		}
		tlsConfig.RootCAs = roots
	}
	origins := []string{cfg.Server}
	u, e := url.Parse(cfg.Server)
	if e != nil {
		return Options{}, e
	}
	proxy, e := http.ProxyFromEnvironment(&http.Request{URL: u})
	if e != nil {
		return Options{}, e
	}
	if proxy != nil {
		origins = append(origins, proxy.String())
	}
	policy, e := network.New(ctx, origins, nil)
	if e != nil {
		return Options{}, e
	}
	if _, _, err := protocol.ParseToken(strings.TrimSpace(string(token))); err != nil {
		return Options{}, err
	}
	return Options{Server: cfg.Server, Token: strings.TrimSpace(string(token)), Version: version, Keys: keys, TLSConfig: tlsConfig, DialContext: policy.DialContext, Proxy: http.ProxyFromEnvironment, refresh: policy.Refresh}, nil
}

func readToken(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() & ^os.FileMode(0640) != 0 {
		return nil, errors.New("token must be a regular file with permissions 0600 or 0640")
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Uid != 0 && int(st.Uid) != os.Getuid() {
		return nil, errors.New("token must be owned by root or the current user")
	}
	b, e := io.ReadAll(io.LimitReader(f, 513))
	if e != nil {
		return nil, e
	}
	if len(b) > 512 {
		return nil, errors.New("token file too large")
	}
	return b, nil
}

func ValidateConfig(ctx context.Context, cfg *config.Config) error {
	_, err := prepare(ctx, cfg, "")
	return err
}
func Run(ctx context.Context, cfg *config.Config, h Handler, version string) error {
	o, e := prepare(ctx, cfg, version)
	if e != nil {
		return e
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := o.refresh(refreshCtx)
				cancel()
				if err != nil {
					slog.Error("cloud DNS refresh failed; run eidonix-connector print-systemd-network, regenerate the systemd network drop-in and restart locally", "error", err)
				}
			}
		}
	}()
	return RunOptions(ctx, o, h)
}
