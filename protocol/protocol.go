// Package protocol defines the shared Eidonix connector wire protocol, version 3.
//
// The package has no dependencies on connector internals: the gateway side
// vendors it as-is. Every exported type and constant is part of the frozen v3
// contract.
package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

const (
	Version         = 3
	Subprotocol     = "eidonix-connector.v3"
	SessionPath     = "/v1/connectors/session"
	MaxControlBytes = 64 << 10
	MaxHeaderBytes  = 4 << 10

	// Upgrade request headers (connector → gateway). No Authorization header is sent.
	HeaderNonce    = "X-Eidonix-Nonce"
	HeaderInstance = "X-Eidonix-Instance"
	// Upgrade response headers (gateway → connector): the server's identity proof.
	HeaderServerKeyID     = "X-Eidonix-Server-Key-Id"
	HeaderServerTime      = "X-Eidonix-Server-Time"
	HeaderServerSignature = "X-Eidonix-Server-Signature"
	// HeaderError carries the closed error code on connector-generated HTTP responses.
	HeaderError = "X-Eidonix-Error"

	// MaxStreamMetadataBytes bounds request_id, application, for and the legacy
	// org_id, org_slug and user_key_id.
	MaxStreamMetadataBytes = 64
)

// Service kinds. An http service carries HTTP and WebSocket streams; an s3
// service carries storage streams.
const (
	KindHTTP = "http"
	KindS3   = "s3"
)

// Control message types.
const (
	MessageAuth       = "auth"
	MessageAuthOK     = "auth_ok"
	MessageAuthFailed = "auth_failed"
	MessageHello      = "hello"
	MessageHelloAck   = "hello_ack"
	MessageStatus     = "status"
	MessagePing       = "ping"
	MessagePong       = "pong"
	MessageClose      = "close"
	// MessageDraining (connector → hub) asks the hub to open no new streams
	// on this session; the connector closes it once its streams finish.
	// Gateways that predate it ignore it.
	MessageDraining = "draining"
)

var servicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`)

// ValidServiceName reports whether s is a legal service name.
func ValidServiceName(s string) bool { return servicePattern.MatchString(s) }

// ValidSiteName reports whether s is a legal site name (the connector's name
// in Setup); the rule is the same as for service names.
func ValidSiteName(s string) bool { return servicePattern.MatchString(s) }

// Service is one advertised service in hello. Kind is KindHTTP or KindS3.
// Endpoints lists an http service's allowed requests ("POST /v1/*",
// "WEBSOCKET /v1/stream", …). Models lists the names callers use; empty means
// the connector does not check models. Storage is set for s3 services only.
type Service struct {
	Name                  string          `json:"name"`
	Kind                  string          `json:"kind"`
	Endpoints             []string        `json:"endpoints,omitempty"`
	Models                []string        `json:"models"`
	MaxConcurrentRequests int             `json:"max_concurrent_requests"`
	Healthy               bool            `json:"healthy"`
	Storage               *StorageService `json:"storage,omitempty"`
	// Parts is set for a service that runs on several servers (a machine's
	// ports); Healthy is then true while any part is. Peers that predate it
	// ignore it.
	Parts []ServicePart `json:"parts,omitempty"`
}

// ServicePart is one server of a service that runs on several: what it
// serves and whether it is healthy, without its address.
type ServicePart struct {
	Models    []string `json:"models"`
	Endpoints []string `json:"endpoints,omitempty"`
	Healthy   bool     `json:"healthy"`
}

// Service health as a gateway shows it.
const (
	HealthOK       = "ok"       // healthy, every part
	HealthDegraded = "degraded" // some parts down
	HealthDown     = "down"     // not healthy, or every part down
)

// Health is a service's health from its Healthy flag and its parts.
func Health(healthy bool, parts []ServicePart) string {
	if !healthy {
		return HealthDown
	}
	for _, p := range parts {
		if !p.Healthy {
			return HealthDegraded
		}
	}
	return HealthOK
}

// ServiceStatus is one service in a status message. Models is always encoded;
// an empty array means the service currently advertises no models.
type ServiceStatus struct {
	Name     string   `json:"name"`
	Healthy  bool     `json:"healthy"`
	InFlight int      `json:"in_flight"`
	Models   []string `json:"models"`
	// Parts: as in Service.
	Parts []ServicePart `json:"parts,omitempty"`
}

// Rejected names a service the gateway declined and why.
type Rejected struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type Limits struct {
	MaxStreams int `json:"max_streams"`
}

// Auth is the first control message. The connector sends it only after it has
// verified the server's identity proof on the upgrade response.
type Auth struct {
	V     int    `json:"v"`
	Type  string `json:"type"`
	Token string `json:"token"`
}

// AuthResult answers Auth: auth_ok carries session_id and org (display name);
// auth_failed carries reason, after which the gateway closes the session.
type AuthResult struct {
	V         int    `json:"v"`
	Type      string `json:"type"`
	SessionID string `json:"session_id,omitempty"`
	Org       string `json:"org,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

// Hello advertises the connector's services. Site is the connector's name as
// configured on this machine; the gateway closes the session with reason
// site_mismatch when it differs from the connector's name in Setup.
type Hello struct {
	V            int       `json:"v"`
	Type         string    `json:"type"`
	Site         string    `json:"site"`
	InstanceID   string    `json:"instance_id"`
	Version      string    `json:"version"`
	OS           string    `json:"os"`
	Arch         string    `json:"arch"`
	Capabilities []string  `json:"capabilities"`
	Services     []Service `json:"services"`
}

// HelloAck lists accepted service names and rejected services with reasons.
type HelloAck struct {
	V        int        `json:"v"`
	Type     string     `json:"type"`
	Accepted []string   `json:"accepted"`
	Rejected []Rejected `json:"rejected"`
	Limits   Limits     `json:"limits"`
}

type Status struct {
	V        int             `json:"v"`
	Type     string          `json:"type"`
	Services []ServiceStatus `json:"services"`
	UptimeS  int64           `json:"uptime_s"`
}

// Control is ping, pong and close (with an optional reason).
type Control struct {
	V      int    `json:"v"`
	Type   string `json:"type"`
	Reason string `json:"reason,omitempty"`
}

// Envelope is a received control line: its version, type and raw bytes.
type Envelope struct {
	V    int
	Type string
	Raw  json.RawMessage
}

// Decode unmarshals the whole message into out.
func (e Envelope) Decode(out any) error { return json.Unmarshal(e.Raw, out) }

// ReadMessage reads one bounded control line. A version other than Version is an error.
func ReadMessage(r *bufio.Reader) (Envelope, error) {
	var raw json.RawMessage
	if e := ReadJSONLine(r, MaxControlBytes, &raw); e != nil {
		return Envelope{}, e
	}
	var head struct {
		V    int    `json:"v"`
		Type string `json:"type"`
	}
	if e := json.Unmarshal(raw, &head); e != nil {
		return Envelope{}, e
	}
	if head.V != Version {
		return Envelope{}, errors.New("unsupported control version")
	}
	if head.Type == "" {
		return Envelope{}, errors.New("control message without type")
	}
	return Envelope{head.V, head.Type, raw}, nil
}

// WriteMessage writes one bounded control line.
func WriteMessage(w io.Writer, m any) error { return WriteJSONLine(w, MaxControlBytes, m) }

// StreamHeader opens every gateway-initiated stream. Service selects a locally
// configured service; it never selects a host, URL, bucket, path or command.
// RequestID is the gateway's request id; OrgID, OrgSlug and UserKeyID identify
// the caller as the gateway reports it. The connector records them in its
// access log and does not authorise on them.
type StreamHeader struct {
	V         int    `json:"v"`
	Service   string `json:"service"`
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
	OrgID     string `json:"org_id,omitempty"`
	OrgSlug   string `json:"org_slug,omitempty"`
	UserKeyID string `json:"user_key_id,omitempty"`
	// Application and For are labels the calling application sets (hub
	// headers X-Eidonix-Application / X-Eidonix-For): which application sent
	// the request and whom it serves. Logged, never authorised on. Connectors
	// that predate them ignore them.
	Application string  `json:"application,omitempty"`
	For         string  `json:"for,omitempty"`
	Op          string  `json:"op,omitempty"`
	Key         string  `json:"key,omitempty"`
	SrcKey      string  `json:"src_key,omitempty"`
	DstKey      string  `json:"dst_key,omitempty"`
	Prefix      string  `json:"prefix,omitempty"`
	Token       string  `json:"token,omitempty"`
	Size        *int64  `json:"size,omitempty"`
	ContentType string  `json:"content_type,omitempty"`
	Range       []int64 `json:"range,omitempty"`
	IfMatch     string  `json:"if_match,omitempty"`
	Recursive   bool    `json:"recursive,omitempty"`
	Max         int     `json:"max,omitempty"`
}

func ReadJSONLine(r *bufio.Reader, max int, out any) error {
	if max <= 0 {
		return errors.New("invalid JSON limit")
	}
	var line []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(line)+len(part) > max {
			return errors.New("JSON line too large")
		}
		line = append(line, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return err
		}
		return json.Unmarshal(line, out)
	}
}
func WriteJSONLine(w io.Writer, max int, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if len(b) > max {
		return errors.New("JSON line too large")
	}
	return writeAll(w, b)
}

// ReadStreamHeader parses a stream header without dispatching its kind. The
// request and caller metadata fields must be printable ASCII of at most
// MaxStreamMetadataBytes.
func ReadStreamHeader(r *bufio.Reader) (StreamHeader, error) {
	var h StreamHeader
	err := ReadJSONLine(r, MaxHeaderBytes, &h)
	if err == nil && (h.V != Version || h.Service == "" || h.Kind == "" || !printable(h.RequestID, MaxStreamMetadataBytes) || !printable(h.OrgID, MaxStreamMetadataBytes) || !printable(h.OrgSlug, MaxStreamMetadataBytes) || !printable(h.UserKeyID, MaxStreamMetadataBytes) || !printable(h.Application, MaxStreamMetadataBytes) || !printable(h.For, MaxStreamMetadataBytes)) {
		err = errors.New("invalid stream header")
	}
	return h, err
}
func WriteStreamHeader(w io.Writer, h StreamHeader) error { return WriteJSONLine(w, MaxHeaderBytes, h) }

type Frame struct {
	Type    byte
	Payload []byte
}

func validType(t byte) bool { return t == 1 || t == 2 || t == 8 }
func ReadFrame(r io.Reader, max int) (Frame, error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(h[1:])
	if !validType(h[0]) || max < 0 || uint64(n) > uint64(max) {
		return Frame{}, errors.New("invalid websocket frame")
	}
	f := Frame{Type: h[0], Payload: make([]byte, int(n))}
	_, err := io.ReadFull(r, f.Payload)
	return f, err
}
func WriteFrame(w io.Writer, f Frame, max int) error {
	if !validType(f.Type) || len(f.Payload) > max || uint64(len(f.Payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("invalid websocket frame")
	}
	var h [5]byte
	h[0] = f.Type
	binary.BigEndian.PutUint32(h[1:], uint32(len(f.Payload)))
	if err := writeAll(w, h[:]); err != nil {
		return err
	}
	return writeAll(w, f.Payload)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// MarshalJSON encodes a nil model list as [] so the field is always an array.
func (s Service) MarshalJSON() ([]byte, error) {
	type plain Service
	if s.Models == nil {
		s.Models = []string{}
	}
	return json.Marshal(plain(s))
}

// MarshalJSON encodes a nil model list as [] so an emptied list clears the gateway's copy.
func (s ServiceStatus) MarshalJSON() ([]byte, error) {
	type plain ServiceStatus
	if s.Models == nil {
		s.Models = []string{}
	}
	return json.Marshal(plain(s))
}
