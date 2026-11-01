package connector

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/eidonix-ai/eidonix-connector/internal/keyserver"
	"github.com/eidonix-ai/eidonix-connector/protocol"
)

const (
	kindKeyServer    = "keyserver"
	keyServerService = "key-server"
	// keyServerTimeout bounds one operation, e-mail included.
	keyServerTimeout = 90 * time.Second
)

// KeyServer is the connector's key server; nil without a key-server block.
func (x *Connector) KeyServer() *keyserver.Server { return x.keys }

// Capabilities are the optional protocol capabilities beyond inference and
// storage that this connector offers.
func (x *Connector) Capabilities() []string {
	if x.keys != nil {
		return []string{protocol.CapabilityKeyServer}
	}
	return nil
}

// serveKeyServer answers one key-server stream: the body is a JSON request
// of at most keyserver.MaxRequestBytes (read to the end of the stream), the
// answer one JSON line. The access log gets the operation, the uid and the
// outcome, never a key, claim secret or proof.
func (x *Connector) serveKeyServer(raw net.Conn, br *bufio.Reader, op string, entry *accessEntry) {
	raw.SetDeadline(time.Now().Add(keyServerTimeout))
	body, err := io.ReadAll(io.LimitReader(br, keyserver.MaxRequestBytes+1))
	var answer map[string]any
	if err != nil || len(body) > keyserver.MaxRequestBytes {
		answer = map[string]any{"ok": false, "error": "invalid_request", "message": "the request is missing or larger than 64 KiB"}
	} else {
		answer, entry.UID = x.keys.Handle(op, body)
	}
	entry.Status = 200
	if answer["ok"] != true {
		entry.Error, _ = answer["error"].(string)
		entry.Status = 400
	}
	b, _ := json.Marshal(answer)
	raw.Write(append(b, '\n'))
}
