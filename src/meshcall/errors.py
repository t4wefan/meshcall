from __future__ import annotations

from enum import StrEnum
from typing import Any


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


class ProtocolError(MeshCallError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.PROTOCOL_ERROR, message)


class ContractError(MeshCallError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.INVALID_ARGUMENT, message)


class DriverStateError(MeshCallError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.INTERNAL, message)

