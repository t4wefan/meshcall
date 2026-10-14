package router

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const Protocol = "meshcall/1"

type balance struct {
	Kind string  `json:"kind"`
	Key  *string `json:"key,omitempty"`
}

type method struct {
	Name    string  `json:"name"`
	Stream  string  `json:"stream"`
	Balance balance `json:"balance"`
}

type service struct {
	Name    string   `json:"name"`
	Methods []method `json:"methods"`
	// Targeted services must be called with an explicit instance_id; the
	// instances hold distinct state (a session, a loaded model, an actor).
	Targeted bool `json:"targeted,omitempty"`
	// Contract is the service's portable contract document entry (schemas,
	// descriptions, annotations). Opaque to routing; served by the Registry.
	Contract json.RawMessage `json:"contract,omitempty"`
}

type errorPayload struct {
	Code      string          `json:"code"`
	Message   string          `json:"message"`
	Retryable bool            `json:"retryable"`
	Details   json.RawMessage `json:"details,omitempty"`
}

// Payload and metadata stay raw: the router never converts business numbers or
// field names, and forwards fields it does not understand unchanged.
type frame struct {
	Kind         string          `json:"kind"`
	CallID       string          `json:"call_id,omitempty"`
	Protocol     string          `json:"protocol,omitempty"`
	Role         string          `json:"role,omitempty"`
	PeerID       string          `json:"peer_id,omitempty"`
	Capabilities []string        `json:"capabilities,omitempty"`
	Connection   string          `json:"connection_id,omitempty"`
	InstanceID   string          `json:"instance_id,omitempty"`
	Services     []service       `json:"services,omitempty"`
	MaxInflight  int             `json:"max_inflight,omitempty"`
	Service      string          `json:"service,omitempty"`
	Method       string          `json:"method,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	TimeoutMS    *int64          `json:"timeout_ms,omitempty"`
	Idempotency  string          `json:"idempotency_key,omitempty"`
	IdleTimeout  *int64          `json:"idle_timeout_ms,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	Error        *errorPayload   `json:"error,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	Direction    string          `json:"direction,omitempty"`
	Sequence     *int64          `json:"sequence,omitempty"`
	Credit       *int64          `json:"credit,omitempty"`
	Nonce        string          `json:"nonce,omitempty"`
	raw          []byte
}

// decode accepts unknown fields (the meshcall/1 compatibility rule) but rejects
// unknown frame kinds and malformed known fields.
func decode(data []byte) (frame, error) {
	var f frame
	d := json.NewDecoder(bytes.NewReader(data))
	if err := d.Decode(&f); err != nil {
		return f, errors.New("invalid frame")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return f, errors.New("expected one JSON frame")
	}
	switch f.Kind {
	case "hello":
		if f.Protocol != Protocol || (f.Role != "client" && f.Role != "server" && f.Role != "router") || f.PeerID == "" {
			return f, errors.New("invalid hello or unsupported protocol")
		}
	case "hello.ack":
		if f.Protocol != Protocol || f.Connection == "" {
			return f, errors.New("invalid hello.ack")
		}
	case "server.register.ack":
		// Received over router links, where this Router is also a server.
	case "server.register":
		if f.InstanceID == "" || f.Services == nil || f.MaxInflight < 0 {
			return f, errors.New("invalid registration")
		}
	case "call.open":
		if f.Service == "" || f.Method == "" || f.Payload == nil {
			return f, errors.New("invalid call.open")
		}
		if (f.TimeoutMS != nil && *f.TimeoutMS <= 0) || (f.IdleTimeout != nil && *f.IdleTimeout <= 0) {
			return f, errors.New("timeouts must be positive")
		}
		if len(f.Idempotency) > 128 {
			return f, errors.New("idempotency_key exceeds 128 bytes")
		}
		if !validMetadata(f.Metadata) {
			return f, errors.New("invalid metadata")
		}
	case "call.result":
		if !validMetadata(f.Metadata) {
			return f, errors.New("invalid metadata")
		}
	case "call.error":
		if f.Error == nil || f.Error.Code == "" || !validMetadata(f.Metadata) {
			return f, errors.New("invalid call.error")
		}
	case "stream.item", "stream.end", "stream.window":
		if f.Direction != "client" && f.Direction != "server" {
			return f, errors.New("invalid stream direction")
		}
		if f.Kind == "stream.item" && (f.Sequence == nil || *f.Sequence < 0 || f.Payload == nil) {
			return f, errors.New("invalid stream item")
		}
		if f.Kind == "stream.window" && (f.Credit == nil || *f.Credit <= 0) {
			return f, errors.New("invalid stream credit")
		}
	case "ping", "pong":
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil || len(fields["nonce"]) == 0 || fields["nonce"][0] != '"' {
			return f, errors.New("missing or invalid nonce")
		}
	case "call.cancel", "server.drain", "server.drain.ack":
	default:
		return f, fmt.Errorf("unexpected frame kind %q", f.Kind)
	}
	switch f.Kind {
	case "call.open", "call.result", "call.error", "call.cancel", "stream.item", "stream.end", "stream.window":
		if f.CallID == "" {
			return f, errors.New("missing call_id")
		}
	}
	f.raw = data
	return f, nil
}

// validMetadata accepts an absent metadata field or a JSON object whose values
// are all strings with non-empty keys.
func validMetadata(raw json.RawMessage) bool {
	if raw == nil {
		return true
	}
	var values map[string]string
	if json.Unmarshal(raw, &values) != nil {
		return false
	}
	for key := range values {
		if key == "" {
			return false
		}
	}
	return true
}

func (f frame) bytes() []byte {
	if f.raw != nil {
		return f.raw
	}
	if f.Kind == "server.register" {
		// services is required even when empty (a peer Router with nothing
		// local yet); other kinds omit it.
		services := f.Services
		if services == nil {
			services = []service{}
		}
		data, err := json.Marshal(struct {
			Kind        string    `json:"kind"`
			InstanceID  string    `json:"instance_id"`
			Services    []service `json:"services"`
			MaxInflight int       `json:"max_inflight,omitempty"`
		}{f.Kind, f.InstanceID, services, f.MaxInflight})
		if err != nil {
			panic(err)
		}
		return data
	}
	if f.Kind == "ping" || f.Kind == "pong" {
		// nonce is required even when empty; other frame kinds must omit it.
		data, err := json.Marshal(struct {
			Kind  string `json:"kind"`
			Nonce string `json:"nonce"`
		}{f.Kind, f.Nonce})
		if err != nil {
			panic(err)
		}
		return data
	}
	data, err := json.Marshal(f)
	if err != nil {
		panic(err) // Only internally constructed, JSON-safe control frames reach here.
	}
	return data
}

func callError(id, code, message string, retryable bool) frame {
	return frame{Kind: "call.error", CallID: id, Error: &errorPayload{Code: code, Message: message, Retryable: retryable}}
}

// routerError is a Router-synthesised error carrying a machine-readable reason
// so callers can tell "no instance registered" from "the instance died".
func routerError(id, code, message string, retryable bool, reason string, instance string) frame {
	f := callError(id, code, message, retryable)
	details := map[string]string{"source": "router", "reason": reason}
	if instance != "" {
		details["instance_id"] = instance
	}
	f.Error.Details, _ = json.Marshal(details)
	return f
}

// deadlineError is the router's own deadline_exceeded, marked so callers can
// tell it apart from the server's authoritative one.
func deadlineError(id string) frame {
	f := callError(id, "deadline_exceeded", "Call deadline exceeded", false)
	f.Error.Details = json.RawMessage(`{"source":"router"}`)
	return f
}

func (f frame) priority() bool {
	return f.Kind == "call.cancel" || f.Kind == "stream.window" || f.Kind == "ping" || f.Kind == "pong"
}
