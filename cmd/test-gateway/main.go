// test-gateway is a loopback-only development gateway, never a production service.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base32"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/testgateway"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	dir := flag.String("state", "./dev-state", "local development keys and config directory")
	init := flag.Bool("init", false, "create development keys, token, and connector config, then exit")
	upstream := flag.String("upstream", "http://127.0.0.1:8000", "chat server for the generated connector config")
	listen := flag.String("listen", "127.0.0.1:8443", "loopback TLS address")
	flag.Parse()
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	if *init {
		return initialize(abs, *upstream, *listen)
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("listen must use a loopback IP")
	}
	token, err := os.ReadFile(filepath.Join(abs, "token"))
	if err != nil {
		return err
	}
	key, err := os.ReadFile(filepath.Join(abs, "identity.key"))
	if err != nil {
		return err
	}
	g, err := testgateway.New(strings.TrimSpace(string(token)), ed25519.PrivateKey(key))
	if err != nil {
		return err
	}
	defer g.Close()
	server := &http.Server{Addr: *listen, Handler: g, ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 64 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); g.Close(); server.Close() }()
	log.Printf("development gateway listening on https://%s; all endpoints require the local bearer token", *listen)
	err = server.ListenAndServeTLS(filepath.Join(abs, "tls.pem"), filepath.Join(abs, "tls.key"))
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
func initialize(dir, upstream, listen string) error {
	// Mkdir, rather than MkdirAll, prevents accidental replacement of existing keys.
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	tlsPub, tlsKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Eidonix connector local test gateway"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(1, 0, 0), IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, tlsPub, tlsKey)
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKCS8PrivateKey(tlsKey)
	if err != nil {
		return err
	}
	id := make([]byte, 10)
	secret := make([]byte, 32)
	if _, err = rand.Read(id); err != nil {
		return err
	}
	if _, err = rand.Read(secret); err != nil {
		return err
	}
	token := "cxn_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(id)) + "_" + base64.RawURLEncoding.EncodeToString(secret)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	var cfg bytes.Buffer
	fmt.Fprintf(&cfg, "# Development only: trusts this local test gateway, never a real upstream.\nupstream:\n  url: https://%s\n  token: %s\n  public_keys:\n    - %s\n  ca_certificate: |\n", listen, token, base64.StdEncoding.EncodeToString(pub))
	for _, line := range strings.Split(strings.TrimSpace(string(certPEM)), "\n") {
		fmt.Fprintf(&cfg, "    %s\n", line)
	}
	fmt.Fprintf(&cfg, "chat:\n  - name: chat\n    url: %s\n    models: any\naccess_log: %s\n", upstream, filepath.Join(dir, "access.log"))
	if _, err := config.Parse(cfg.Bytes()); err != nil {
		return fmt.Errorf("generated config: %w", err)
	}
	files := map[string][]byte{"identity.key": key, "tls.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), "tls.pem": certPEM, "token": []byte(token + "\n"), "connector.yaml": cfg.Bytes()}
	for name, data := range files {
		if err = os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			return err
		}
	}
	fmt.Printf("Development keys and config created in %s\nRun test-gateway --state %q, then eidonix-connector run --config %q\n", dir, dir, filepath.Join(dir, "connector.yaml"))
	return nil
}
