// Package protocol defines the shared connector wire protocol.
package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	Version         = 1
	Subprotocol     = "cletus-connector.v1"
	MaxControlBytes = 64 << 10
	MaxHeaderBytes  = 4 << 10
)

type Route struct {
	ID             string      `json:"id"`
	Type           string      `json:"type"`
	MethodsPaths   [][2]string `json:"methods_paths"`
	WebsocketPaths []string    `json:"websocket_paths,omitempty"`
	MaxConcurrency int         `json:"max_concurrency"`
	Healthy        bool        `json:"healthy"`
	Models         []string    `json:"models,omitempty"`
}
type RouteStatus struct {
	ID       string   `json:"id"`
	Healthy  bool     `json:"healthy"`
	InFlight int      `json:"in_flight"`
	Models   []string `json:"models,omitempty"`
}
type RejectedRoute struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}
type Limits struct {
	MaxStreams int `json:"max_streams"`
}
type Message struct {
	V              int             `json:"v"`
	Type           string          `json:"type"`
	InstanceID     string          `json:"instance_id,omitempty"`
	Version        string          `json:"version,omitempty"`
	OS             string          `json:"os,omitempty"`
	Arch           string          `json:"arch,omitempty"`
	Routes         []Route         `json:"routes,omitempty"`
	UptimeS        int64           `json:"uptime_s,omitempty"`
	SessionID      string          `json:"session_id,omitempty"`
	Org            string          `json:"org,omitempty"`
	AcceptedRoutes []string        `json:"accepted_routes,omitempty"`
	RejectedRoutes []RejectedRoute `json:"rejected_routes,omitempty"`
	Limits         Limits          `json:"limits,omitempty"`
	Reason         string          `json:"reason,omitempty"`
}

// StatusMessage uses the status-specific route shape.
type StatusMessage struct {
	V       int           `json:"v"`
	Type    string        `json:"type"`
	Routes  []RouteStatus `json:"routes"`
	UptimeS int64         `json:"uptime_s"`
}
type StreamHeader struct {
	V         int    `json:"v"`
	Route     string `json:"route"`
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
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
func ReadStreamHeader(r *bufio.Reader) (StreamHeader, error) {
	var h StreamHeader
	err := ReadJSONLine(r, MaxHeaderBytes, &h)
	if err == nil && (h.V != Version || h.Route == "" || (h.Kind != "http" && h.Kind != "websocket")) {
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
