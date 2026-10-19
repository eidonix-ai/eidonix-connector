package session

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eidonix-ai/eidonix-connector/protocol"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// drainHandler serves each stream until release is closed, and refuses with the code it is given.
type drainHandler struct {
	fakeHandler
	release chan struct{}
	mu      sync.Mutex
	served  int
}

func (h *drainHandler) ServeStream(_ context.Context, c net.Conn) {
	h.mu.Lock()
	h.served++
	h.mu.Unlock()
	io.WriteString(c, "serving\n")
	<-h.release
	io.WriteString(c, "done\n")
}
func (h *drainHandler) RefuseStream(_ context.Context, c net.Conn, code string) {
	io.WriteString(c, "refused "+code+"\n")
}

// TestDrainSessionRefusesAndFinishes: a drain sends the draining message, lets
// the running stream finish, answers a stream that still arrives with the
// refusal code, and ends only then.
func TestDrainSessionRefusesAndFinishes(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	type seen struct{ drainingMessage, first, late string }
	result := make(chan seen, 1)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res seen
		defer func() { result <- res }()
		hdr := protocol.IdentityHeaders(key, r.Header.Get(protocol.HeaderNonce), r.Header.Get(protocol.HeaderInstance), time.Now())
		ws, e := (&websocket.Upgrader{Subprotocols: []string{protocol.Subprotocol}}).Upgrade(w, r, hdr)
		if e != nil {
			return
		}
		defer ws.Close()
		cfg := yamux.DefaultConfig()
		cfg.LogOutput = io.Discard
		mux, e := yamux.Server(protocol.NewWebSocketConn(ws), cfg)
		if e != nil {
			return
		}
		defer mux.Close()
		control, e := mux.AcceptStream()
		if e != nil {
			return
		}
		br := bufio.NewReader(control)
		protocol.ReadMessage(br)
		protocol.WriteMessage(control, protocol.AuthResult{V: protocol.Version, Type: protocol.MessageAuthOK, SessionID: "s-1", Org: "Example"})
		protocol.ReadMessage(br)
		protocol.WriteMessage(control, protocol.HelloAck{V: protocol.Version, Type: protocol.MessageHelloAck, Limits: protocol.Limits{MaxStreams: 4}})
		first, _ := mux.OpenStream()
		line, _ := bufio.NewReader(first).ReadString('\n')
		res.first = strings.TrimSpace(line)
		// Wait for the connector's draining message.
		control.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			m, e := protocol.ReadMessage(br)
			if e != nil {
				return
			}
			if m.Type == protocol.MessageDraining {
				res.drainingMessage = m.Type
				break
			}
		}
		// A stream that raced the message is refused.
		late, _ := mux.OpenStream()
		line, _ = bufio.NewReader(late).ReadString('\n')
		res.late = strings.TrimSpace(line)
		late.Close()
		// The first stream is still running; nothing closes the session.
		time.Sleep(200 * time.Millisecond)
		if !mux.IsClosed() {
			first.Close()
		}
		for mux.NumStreams() > 1 {
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer srv.Close()

	h := &drainHandler{release: make(chan struct{})}
	drain := make(chan struct{})
	states := make(chan State, 16)
	o := Options{Token: testToken, Keys: []ed25519.PublicKey{pub}, TLSConfig: &tls.Config{InsecureSkipVerify: true}, DialContext: dialTo(strings.TrimPrefix(srv.URL, "https://")), MaxStreams: 4, Drain: drain, OnState: func(s State) { states <- s }, Server: "https://upstream.example", Site: "x"}
	done := make(chan error, 1)
	go func() { done <- RunOptions(context.Background(), o, h) }()
	for s := range states {
		if s.Connected {
			if s.SessionID != "s-1" || s.Hub != "Example" {
				t.Fatalf("state %+v", s)
			}
			break
		}
	}
	// The first stream is being served; drain, then let it finish.
	deadline := time.Now().Add(3 * time.Second)
	for {
		h.mu.Lock()
		n := h.served
		h.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream never served")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(drain)
	select {
	case err := <-done:
		t.Fatalf("session ended before its stream finished: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(h.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end after its streams finished")
	}
	res := <-result
	if res.first != "serving" || res.drainingMessage != protocol.MessageDraining || res.late != "refused "+DrainRefusal {
		t.Fatalf("%+v", res)
	}
	var last State
	for len(states) > 0 {
		last = <-states
	}
	if last.Connected || !last.Draining || last.DrainingAt.IsZero() {
		t.Fatalf("final state %+v", last)
	}
}
