package meshcall

import (
	"errors"
	"fmt"
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
