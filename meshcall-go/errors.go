package meshcall

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes (docs/protocol.md → Errors). Applications may use namespaced
// codes of their own, such as "billing.v1.quota_exceeded".
const (
	CodeCancelled         = "cancelled"
	CodeDeadlineExceeded  = "deadline_exceeded"
	CodeInvalidArgument   = "invalid_argument"
	CodeMethodNotFound    = "method_not_found"
	CodePermissionDenied  = "permission_denied"
	CodeUnauthenticated   = "unauthenticated"
	CodeResourceExhausted = "resource_exhausted"
	CodeUnavailable       = "unavailable"
	CodeProtocolError     = "protocol_error"
	CodeInternal          = "internal"
)

// Error is a MeshCall call failure: a stable code, a human message, whether
// a retry may succeed, and structured details (source, reason, violations…).
// Handlers return one to fail a call with that code; any other error becomes
// a sanitised "internal".
type Error struct {
	Code      string
	Message   string
	Retryable bool
	Details   map[string]any
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// Errorf builds an *Error with a formatted message.
func Errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Reason returns details.reason, or "".
func (e *Error) Reason() string {
	reason, _ := e.Details["reason"].(string)
	return reason
}

// AsError reports whether err is (or wraps) a MeshCall *Error.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}

// IsCode reports whether err is a MeshCall error with the given code.
func IsCode(err error, code string) bool {
	e, ok := AsError(err)
	return ok && e.Code == code
}

func (e *Error) payload() *errorPayload {
	return &errorPayload{Code: e.Code, Message: e.Message, Retryable: e.Retryable, Details: e.Details}
}

func errorFromPayload(p *errorPayload) *Error {
	return &Error{Code: p.Code, Message: p.Message, Retryable: p.Retryable, Details: p.Details}
}

// frameTooLarge is the error for a call whose frame was not sent because it
// is over the size limit (docs/protocol.md → Frame size); nil for any other
// error. Not retryable: the same call would build the same frame.
func frameTooLarge(source string, err error) *Error {
	if !errors.Is(err, errFrameTooLarge) {
		return nil
	}
	return detailed(CodeResourceExhausted, "Not sent: "+err.Error(), source, "frame_too_large", false)
}

func detailed(code, message, source, reason string, retryable bool) *Error {
	details := map[string]any{"source": source}
	if reason != "" {
		details["reason"] = reason
	}
	return &Error{Code: code, Message: message, Retryable: retryable, Details: details}
}

// checkMetadata refuses request metadata keys that are empty or use the
// reserved "meshcall-" prefix, in any letter case (docs/protocol.md →
// Metadata): reserved keys are set by the runtime and the Router only.
func checkMetadata(md map[string]string) *Error {
	for key := range md {
		if key == "" {
			return detailed(CodeInvalidArgument, "metadata keys must not be empty", "client", "invalid_metadata", false)
		}
		if isReservedKey(key) {
			return detailed(CodeInvalidArgument, fmt.Sprintf("metadata key %q uses the reserved prefix \"meshcall-\"", key), "client", "reserved_metadata", false)
		}
	}
	return nil
}

func isReservedKey(key string) bool {
	return len(key) >= 9 && strings.EqualFold(key[:9], "meshcall-")
}

// closeUnauthenticated is the WebSocket close status for credentials that
// are not, or no longer, valid.
const closeUnauthenticated = 4401

// closeError is the error a Router close status stands for, or nil for a
// plain loss: 4401 the credentials are not (or no longer) valid, a revoked
// tunnel saying "tunnel_revoked"; 1008 the Router refused the connection's
// request (a registration, a role) and says why.
func closeError(code int, reason string) *Error {
	switch code {
	case closeUnauthenticated:
		e := detailed(CodeUnauthenticated, "The Router closed the connection: "+orDefault(reason, "unauthenticated"), "router", "", false)
		if reason == "tunnel_revoked" || reason == "tunnel_closed" {
			e.Details["reason"] = reason
		}
		return e
	case 1008:
		return detailed(CodePermissionDenied, "The Router refused the connection: "+orDefault(reason, "policy violation"), "router", "policy_violation", false)
	case 4403, 4429, 4503:
		// Credentials in hello refused like their header counterparts: 4000 +
		// the HTTP status. The last two are temporary.
		return handshakeError(code-4000, reason)
	}
	return nil
}

// handshakeError is the error an HTTP status refusing a WebSocket upgrade
// stands for, or nil.
func handshakeError(status int, body string) *Error {
	text := fmt.Sprintf(" (HTTP %d)", status)
	if body = strings.TrimSpace(body); body != "" {
		text += ": " + body
	}
	switch status {
	case 401:
		return detailed(CodeUnauthenticated, "The Router refused the credentials"+text, "router", "", false)
	case 403:
		return detailed(CodePermissionDenied, "The Router refused the connection"+text, "router", "", false)
	case 429:
		return detailed(CodeResourceExhausted, "The Router refused the connection"+text, "router", "", true)
	case 503:
		return detailed(CodeUnavailable, "The Router is busy"+text, "router", "", true)
	}
	return nil
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
