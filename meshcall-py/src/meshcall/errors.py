from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar

from pydantic import ValidationError


class ErrorCode(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    METHOD_NOT_FOUND = "method_not_found"
    UNAUTHENTICATED = "unauthenticated"
    PERMISSION_DENIED = "permission_denied"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    CANCELLED = "cancelled"
    UNAVAILABLE = "unavailable"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    PROTOCOL_ERROR = "protocol_error"
    INTERNAL = "internal"


class MeshCallError(Exception):
    """An RPC error: a stable ``code``, a message and optional ``details``.

    Errors received from a peer are instantiated through :meth:`from_code`,
    which picks the subclass for the protocol code so callers can write
    ``except DeadlineExceededError`` instead of comparing strings.
    Application-defined codes stay plain ``MeshCallError``.
    """

    #: Set by subclasses; ``from_code`` dispatches on it.
    protocol_code: ClassVar[ErrorCode | None] = None

    def __init__(
        self,
        code: str | ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = message
        self.retryable = retryable
        self.details = details

    @classmethod
    def from_code(
        cls,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> MeshCallError:
        subclass = _BY_CODE.get(code)
        if subclass is None:
            return MeshCallError(code, message, retryable=retryable, details=details)
        return subclass(message, retryable=retryable, details=details)

    @property
    def source(self) -> str | None:
        """``details.source``: ``"client"``, ``"router"`` or ``"server"``."""
        value = (self.details or {}).get("source")
        return value if isinstance(value, str) else None

    @property
    def reason(self) -> str | None:
        """``details.reason`` when the sender supplied one."""
        value = (self.details or {}).get("reason")
        return value if isinstance(value, str) else None


class _CodedError(MeshCallError):
    """Base for one-class-per-code subclasses; ``protocol_code`` fixes the code."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        assert self.protocol_code is not None
        super().__init__(
            self.protocol_code, message, retryable=retryable, details=details
        )


class InvalidArgumentError(_CodedError):
    protocol_code = ErrorCode.INVALID_ARGUMENT

    @property
    def violations(self) -> list[Violation]:
        """Field-level findings when the server validated the request."""
        raw = (self.details or {}).get("violations")
        if not isinstance(raw, list):
            return []
        return [
            Violation(
                path=str(item.get("path", "")),
                code=str(item.get("code", "")),
                message=str(item.get("message", "")),
            )
            for item in raw
            if isinstance(item, dict)
        ]


class MethodNotFoundError(_CodedError):
    protocol_code = ErrorCode.METHOD_NOT_FOUND


class UnauthenticatedError(_CodedError):
    protocol_code = ErrorCode.UNAUTHENTICATED


class PermissionDeniedError(_CodedError):
    protocol_code = ErrorCode.PERMISSION_DENIED


class DeadlineExceededError(_CodedError):
    protocol_code = ErrorCode.DEADLINE_EXCEEDED


class CallCancelledError(_CodedError):
    """The call was cancelled (distinct from ``asyncio.CancelledError``)."""

    protocol_code = ErrorCode.CANCELLED


class UnavailableError(_CodedError):
    protocol_code = ErrorCode.UNAVAILABLE


class ResourceExhaustedError(_CodedError):
    protocol_code = ErrorCode.RESOURCE_EXHAUSTED


class InternalError(_CodedError):
    protocol_code = ErrorCode.INTERNAL


class ProtocolError(MeshCallError):
    """A frame violated the protocol.

    ``call_id`` is set when the offending frame could still be attributed to a
    call, so the receiver can fail that call alone instead of the connection.
    """

    protocol_code = ErrorCode.PROTOCOL_ERROR

    def __init__(
        self,
        message: str,
        *,
        call_id: str | None = None,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            ErrorCode.PROTOCOL_ERROR, message, retryable=retryable, details=details
        )
        self.call_id = call_id


_BY_CODE: dict[str, type[_CodedError | ProtocolError]] = {
    str(klass.protocol_code): klass
    for klass in (
        InvalidArgumentError,
        MethodNotFoundError,
        UnauthenticatedError,
        PermissionDeniedError,
        DeadlineExceededError,
        CallCancelledError,
        UnavailableError,
        ResourceExhaustedError,
        InternalError,
        ProtocolError,
    )
}


class ContractError(MeshCallError):
    """A service definition breaks a contract rule (raised at import time)."""

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.INVALID_ARGUMENT, message)


class DriverStateError(MeshCallError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.INTERNAL, message)


class Violation:
    """One field-level validation finding; never carries the input value."""

    __slots__ = ("code", "message", "path")

    def __init__(self, *, path: str, code: str, message: str) -> None:
        self.path = path
        self.code = code
        self.message = message

    def as_dict(self) -> dict[str, str]:
        return {"path": self.path, "code": self.code, "message": self.message}

    def __repr__(self) -> str:
        return f"Violation(path={self.path!r}, code={self.code!r}, message={self.message!r})"


def violations_of(error: ValidationError) -> list[Violation]:
    """Redact a Pydantic error to ``(path, code, message)`` triples.

    Pydantic's ``msg`` describes the rule, not the value; ``input`` and ``ctx``
    (which can embed the value) are dropped deliberately.
    """
    return [
        Violation(
            path=".".join(str(part) for part in item["loc"]),
            code=str(item["type"]),
            message=str(item["msg"]),
        )
        for item in error.errors(
            include_input=False, include_url=False, include_context=False
        )
    ]


def invalid_argument(error: ValidationError, reason: str) -> InvalidArgumentError:
    """The caller's fault: request or input item did not match the contract."""
    label = "request" if reason == "request" else "input stream item"
    return InvalidArgumentError(
        f"Invalid {label}",
        details={
            "source": "server",
            "reason": reason,
            "violations": [v.as_dict() for v in violations_of(error)],
        },
    )


def invalid_response(reason: str) -> InternalError:
    """The service's fault: its response or output item broke its own contract."""
    return InternalError(
        "Internal service error",
        details={"source": "server", "reason": reason},
    )
