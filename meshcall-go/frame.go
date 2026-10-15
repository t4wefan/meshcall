package meshcall

import (
	"encoding/json"
	"errors"
)

// Protocol is the wire protocol version this runtime speaks.
const Protocol = "meshcall/1"

// frame is one meshcall/1 frame (docs/protocol.md → Frames). Unknown fields
// are ignored when decoding; payloads and metadata stay raw.
type frame struct {
	Kind        string            `json:"kind"`
	CallID      string            `json:"call_id,omitempty"`
	Protocol    string            `json:"protocol,omitempty"`
	Role        string            `json:"role,omitempty"`
	PeerID      string            `json:"peer_id,omitempty"`
	Connection  string            `json:"connection_id,omitempty"`
	InstanceID  string            `json:"instance_id,omitempty"`
	Services    []registration    `json:"services,omitempty"`
	MaxInflight int               `json:"max_inflight,omitempty"`
	Service     string            `json:"service,omitempty"`
	Method      string            `json:"method,omitempty"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	TimeoutMS   *int64            `json:"timeout_ms,omitempty"`
	Idempotency string            `json:"idempotency_key,omitempty"`
	IdleTimeout *int64            `json:"idle_timeout_ms,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Error       *errorPayload     `json:"error,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	Direction   string            `json:"direction,omitempty"`
	Sequence    *int64            `json:"sequence,omitempty"`
	Credit      *int64            `json:"credit,omitempty"`
	Nonce       string            `json:"nonce,omitempty"`
}

type errorPayload struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

type registration struct {
	Name     string          `json:"name"`
	Methods  []methodShape   `json:"methods"`
	Targeted bool            `json:"targeted,omitempty"`
	Contract json.RawMessage `json:"contract,omitempty"`
}

type methodShape struct {
	Name    string        `json:"name"`
	Stream  string        `json:"stream"`
	Balance BalancePolicy `json:"balance"`
}

var knownKinds = map[string]bool{
	"hello": true, "hello.ack": true, "server.register": true, "server.register.ack": true,
	"server.drain": true, "server.drain.ack": true, "call.open": true, "call.result": true,
	"call.error": true, "call.cancel": true, "stream.item": true, "stream.end": true,
	"stream.window": true, "ping": true, "pong": true,
}

func decodeFrame(data []byte) (frame, error) {
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		return f, errors.New("invalid frame")
	}
	if !knownKinds[f.Kind] {
		return f, errors.New("unknown frame kind " + f.Kind)
	}
	return f, nil
}

func int64p(v int64) *int64 { return &v }
