package session

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// prepare builds session options from the file: the upstream origin, its
// public keys, an optional extra CA certificate and an optional web proxy.
// The session dialer may reach only the upstream and the proxy.
func prepare(ctx context.Context, cfg *config.Config, version string) (Options, error) {
	keys := []ed25519.PublicKey{}
	for _, s := range cfg.Upstream.PublicKeys {
		k, e := protocol.ParsePublicKey(s)
		if e != nil {
			return Options{}, e
		}
		keys = append(keys, k)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.Upstream.CACertificate != "" {
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM([]byte(cfg.Upstream.CACertificate)) {
			return Options{}, errors.New("invalid upstream ca-certificate")
		}
		tlsConfig.RootCAs = roots
	}
	origins := []string{cfg.Upstream.URL}
	var proxy func(*http.Request) (*url.URL, error)
	if cfg.Upstream.Proxy != "" {
		p, e := url.Parse(cfg.Upstream.Proxy)
		if e != nil {
			return Options{}, e
		}
		proxy = http.ProxyURL(p)
		origins = append(origins, cfg.Upstream.Proxy)
	}
	policy, e := network.New(ctx, origins)
	if e != nil {
		return Options{}, e
	}
	return Options{Server: cfg.Upstream.URL, Token: cfg.Upstream.Token, Site: cfg.Site, Version: version, Keys: keys, TLSConfig: tlsConfig, DialContext: policy.DialContext, Proxy: proxy, refresh: policy.Refresh}, nil
}

// ValidateConfig resolves and pins the upstream (and proxy) addresses.
func ValidateConfig(ctx context.Context, cfg *config.Config) error {
	_, err := prepare(ctx, cfg, "")
	return err
}

// Run keeps the upstream session for cfg until ctx ends.
func Run(ctx context.Context, cfg *config.Config, h Handler, version string) error {
	return RunWith(ctx, cfg, h, version, nil, nil)
}

// RunWith is Run with a drain signal and a state callback (see Options.Drain
// and Options.OnState). It returns nil after a completed drain.
func RunWith(ctx context.Context, cfg *config.Config, h Handler, version string, drain <-chan struct{}, onState func(State)) error {
	o, e := prepare(ctx, cfg, version)
	if e != nil {
		return e
	}
	o.Drain, o.OnState = drain, onState
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
					slog.Error("upstream address changed since start; restart the connector to accept the new address", "error", err)
				}
			}
		}
	}()
	return RunOptions(ctx, o, h)
}
