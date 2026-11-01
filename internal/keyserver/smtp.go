package keyserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/internal/network"
)

const smtpTimeout = 30 * time.Second

// SMTP sends mail through the configured server: TLS from the start on port
// 465, STARTTLS (required) on 587, always with a verified certificate. Like
// every other connection the connector makes, it may reach only the address
// the file names, as resolved when the first mail is sent.
type SMTP struct {
	cfg      config.SMTP
	implicit bool           // TLS from the start (port 465)
	roots    *x509.CertPool // nil: the system's (tests only)
	mu       sync.Mutex
	policy   *network.Policy
}

func NewSMTP(cfg config.SMTP) *SMTP {
	return &SMTP{cfg: cfg, implicit: strings.HasSuffix(cfg.Address, ":465")}
}

func (m *SMTP) dialer(ctx context.Context) (*network.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.policy == nil {
		p, err := network.New(ctx, []string{"smtp://" + m.cfg.Address})
		if err != nil {
			return nil, err
		}
		m.policy = p
	}
	return m.policy, nil
}

// Send mails body (plain text) to one recipient. The password is read from
// its file for each mail, so replacing the file needs no reload.
func (m *SMTP) Send(to, subject, body string) error {
	pass, err := config.ReadSecretFile(m.cfg.PasswordFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), smtpTimeout)
	defer cancel()
	policy, err := m.dialer(ctx)
	if err != nil {
		return err
	}
	conn, err := policy.DialContext(ctx, "tcp", m.cfg.Address)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(smtpTimeout))
	host, _, _ := net.SplitHostPort(m.cfg.Address)
	tlsConfig := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12, RootCAs: m.roots}
	if m.implicit {
		tc := tls.Client(conn, tlsConfig)
		if err = tc.HandshakeContext(ctx); err != nil {
			return err
		}
		conn = tc
	}
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer c.Close()
	if !m.implicit {
		if err = c.StartTLS(tlsConfig); err != nil {
			return err
		}
	}
	if err = c.Auth(smtp.PlainAuth("", m.cfg.Username, pass, host)); err != nil {
		return err
	}
	if err = c.Mail(m.cfg.From); err != nil {
		return err
	}
	if err = c.Rcpt(to); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n%s",
		m.cfg.From, to, subject, time.Now().UTC().Format(time.RFC1123Z), strings.ReplaceAll(body, "\n", "\r\n"))
	if _, err = w.Write([]byte(msg)); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
