package keyserver

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
)

// smtpServer is a minimal SMTP server (TLS from the start, or STARTTLS) that
// records the login and the message it received.
type smtpServer struct {
	mu                  sync.Mutex
	auth, from, to, msg string
}

func (s *smtpServer) serve(ln net.Listener, cert tls.Certificate, startTLS bool) {
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if !startTLS {
		conn = tls.Server(conn, cfg)
	}
	r := bufio.NewReader(conn)
	say := func(l string) { conn.Write([]byte(l + "\r\n")) }
	say("220 mail.example.com ready")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		switch cmd := strings.ToUpper(strings.Fields(line + " x")[0]); {
		case cmd == "EHLO":
			if startTLS {
				say("250-mail.example.com\r\n250-STARTTLS\r\n250 AUTH PLAIN")
			} else {
				say("250-mail.example.com\r\n250 AUTH PLAIN")
			}
		case cmd == "STARTTLS":
			say("220 go ahead")
			conn = tls.Server(conn, cfg)
			r = bufio.NewReader(conn)
			startTLS = false
		case cmd == "AUTH":
			s.mu.Lock()
			s.auth = line
			s.mu.Unlock()
			say("235 ok")
		case cmd == "MAIL":
			s.mu.Lock()
			s.from = line
			s.mu.Unlock()
			say("250 ok")
		case cmd == "RCPT":
			s.mu.Lock()
			s.to = line
			s.mu.Unlock()
			say("250 ok")
		case cmd == "DATA":
			say("354 go")
			var b strings.Builder
			for {
				l, _ := r.ReadString('\n')
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			s.mu.Lock()
			s.msg = b.String()
			s.mu.Unlock()
			say("250 queued")
		case cmd == "QUIT":
			say("221 bye")
			return
		default:
			say("250 ok")
		}
	}
}

func TestSMTPSend(t *testing.T) {
	ts := httptest.NewTLSServer(nil) // only for its certificate for 127.0.0.1
	defer ts.Close()
	cert := ts.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())
	pass := filepath.Join(t.TempDir(), "smtp.pass")
	if err := os.WriteFile(pass, []byte("hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, implicit := range []bool{true, false} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &smtpServer{}
		go srv.serve(ln, cert, !implicit)
		m := NewSMTP(config.SMTP{Address: ln.Addr().String(), Username: "keys@example.com", PasswordFile: pass, From: "keys@example.com"})
		m.implicit, m.roots = implicit, roots
		if err := m.Send("pat@example.com", "Your Cletus key", "line one\n.\nline three\n"); err != nil {
			t.Fatalf("implicit=%v: %v", implicit, err)
		}
		srv.mu.Lock()
		if !strings.Contains(srv.from, "keys@example.com") || !strings.Contains(srv.to, "pat@example.com") || !strings.HasPrefix(srv.auth, "AUTH PLAIN") ||
			!strings.Contains(srv.msg, "Subject: Your Cletus key\r\n") || !strings.Contains(srv.msg, "line one\r\n..\r\nline three") {
			t.Fatalf("implicit=%v: server saw from=%q to=%q auth=%q msg=%q", implicit, srv.from, srv.to, srv.auth, srv.msg)
		}
		srv.mu.Unlock()
		ln.Close()
	}
}

func TestSMTPRefusesAnUntrustedCertificateAndALooseSecretFile(t *testing.T) {
	ts := httptest.NewTLSServer(nil)
	defer ts.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go (&smtpServer{}).serve(ln, ts.TLS.Certificates[0], false)
	pass := filepath.Join(t.TempDir(), "smtp.pass")
	os.WriteFile(pass, []byte("hunter2\n"), 0o600)
	m := NewSMTP(config.SMTP{Address: ln.Addr().String(), Username: "u", PasswordFile: pass, From: "keys@example.com"})
	m.implicit = true // the system's roots do not trust the test certificate
	if err := m.Send("pat@example.com", "s", "b"); err == nil {
		t.Fatal("sent over an untrusted certificate")
	}
	os.Chmod(pass, 0o644)
	if err := m.Send("pat@example.com", "s", "b"); err == nil || !strings.Contains(err.Error(), "chmod") {
		t.Fatalf("loose password file: %v", err)
	}
}
