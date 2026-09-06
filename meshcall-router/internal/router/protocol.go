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
}

type errorPayload struct {
	Code      string          `json:"code"`
	Message   string          `json:"message"`
	Retryable bool            `json:"retryable"`
	Details   json.RawMessage `json:"details,omitempty"`
}

// Payload stays raw: the router never converts business numbers or field names.
type frame struct {
	Kind       string          `json:"kind"`
	CallID     string          `json:"call_id,omitempty"`
	Protocol   string          `json:"protocol,omitempty"`
	Role       string          `json:"role,omitempty"`
	PeerID     string          `json:"peer_id,omitempty"`
	Connection string          `json:"connection_id,omitempty"`
	InstanceID string          `json:"instance_id,omitempty"`
	Services   []service       `json:"services,omitempty"`
	Service    string          `json:"service,omitempty"`
	Method     string          `json:"method,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Deadline   *int64          `json:"deadline_unix_ms,omitempty"`
	Error      *errorPayload   `json:"error,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Direction  string          `json:"direction,omitempty"`
	Sequence   *int64          `json:"sequence,omitempty"`
	Credit     *int64          `json:"credit,omitempty"`
	Nonce      string          `json:"nonce,omitempty"`
	raw        []byte
}

func decode(data []byte) (frame, error) {
	var f frame
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&f); err != nil {
		return f, errors.New("invalid frame")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return f, errors.New("expected one JSON frame")
	}
	switch f.Kind {
	case "hello":
		if f.Protocol != Protocol || (f.Role != "client" && f.Role != "server") || f.PeerID == "" {
			return f, errors.New("invalid hello or unsupported protocol")
		}
	case "server.register":
		if f.InstanceID == "" || f.Services == nil {
			return f, errors.New("invalid registration")
		}
	case "call.open":
		if f.Service == "" || f.Method == "" || f.Payload == nil {
			return f, errors.New("invalid call.open")
		}
	case "call.error":
		if f.Error == nil || f.Error.Code == "" {
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
	case "call.result", "call.cancel", "ping", "pong":
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

func (f frame) bytes() []byte {
	if f.raw != nil {
		return f.raw
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

func (f frame) priority() bool {
	return f.Kind == "call.cancel" || f.Kind == "stream.window" || f.Kind == "ping" || f.Kind == "pong"
}
