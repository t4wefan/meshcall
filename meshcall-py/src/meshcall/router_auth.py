from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field

from meshcall.client import ClientBase
from meshcall.contract import normalize_service_name
from meshcall.tunnel import TunnelInfo

AUTH_SERVICE = "meshcall.router.v1.AuthService"
REGISTRY_SERVICE = "meshcall.router.v1.Registry"


@dataclass(frozen=True)
class RouterCredentials:
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    token: str | None = field(default=None, repr=False)

    @classmethod
    def bearer(cls, token: str) -> Self:
        """Create Bearer token credentials for Router authentication."""
        return cls(token=token)

    @classmethod
    def basic(cls, username: str, password: str) -> Self:
        """Create Basic username/password credentials for Router authentication."""
        return cls(username=username, password=password)

    def __post_init__(self) -> None:
        if self.token is not None:
            if not self.token or self.username is not None or self.password is not None:
                raise ValueError("Set either a token or username/password")
            if any(c in self.token for c in "\r\n"):
                raise ValueError("Invalid token")
        elif (
            not self.username
            or not self.password
            or any(c in self.username for c in ":\r\n")
        ):
            raise ValueError("A username and password are required")

    def authorization_header(self) -> str:
        if self.token is not None:
            return f"Bearer {self.token}"
        encoded = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return f"Basic {encoded}"


class RouterScope(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    service: str
    methods: list[str] = Field(default_factory=list)
    register_service: bool = Field(
        default=False, validation_alias="register", serialization_alias="register"
    )
    #: Restrict a registration scope to these instance ids.
    instance_ids: list[str] | None = None


class IssuedToken(BaseModel):
    token: str = Field(repr=False)
    token_id: str
    expires_at_unix_ms: int
    scopes: list[RouterScope]


class RenewedToken(BaseModel):
    """``renew_token``'s answer: the same token, with its new expiry."""

    token_id: str
    expires_at_unix_ms: int


class TokenInfo(BaseModel):
    """An outstanding temporary token as its issuer sees it (never the secret)."""

    token_id: str
    issuer: str
    instance_ids: list[str] = Field(default_factory=list)
    expires_at_unix_ms: int
    #: Past its expiry: it admits no new connection, but these stay up.
    expired: bool = False
    connections: int = 0


class RouterIdentity(BaseModel):
    username: str
    roles: list[Literal["client", "server"]]
    register_services: list[str] = Field(alias="register")
    call: list[str]
    expires_at_unix_ms: int | None
    #: The instance ids this identity may register under, when bound.
    instance_ids: list[str] | None = None
    #: The tunnel this connection belongs to, on a tunnel connection.
    tunnel: TunnelInfo | None = None


class RegistryBalance(BaseModel):
    kind: str
    key: str | None = None


class RegistryInstance(BaseModel):
    id: str
    inflight: int
    max_inflight: int | None = None
    breaker: str = "closed"
    draining: bool = False


class RegistryMethod(BaseModel):
    name: str
    stream: str
    balance: RegistryBalance
    description: str | None = None
    annotations: dict[str, Any] | None = None
    deprecated: str | None = None
    parameters: list[str] | None = None
    duplex_reason: str | None = None
    instances: list[str] = Field(default_factory=list)


class RegistryService(BaseModel):
    name: str
    version: str = ""
    description: str | None = None
    targeted: bool = False
    instances: list[RegistryInstance] = Field(default_factory=list)
    methods: list[RegistryMethod] = Field(default_factory=list)
    contract: dict[str, Any] | None = None


class _Empty(BaseModel):
    pass


class _IssueRequest(BaseModel):
    # The Router bounds it (--token-max-ttl; invalid_argument with the limit).
    ttl_seconds: int
    scopes: list[RouterScope]


class _RevokeRequest(BaseModel):
    token_id: str


class _RenewRequest(BaseModel):
    token_id: str
    ttl_seconds: int


class _ListTokensResult(BaseModel):
    tokens: list[TokenInfo]


class _RevokeResult(BaseModel):
    revoked: bool


class _DescribeServiceRequest(BaseModel):
    service: str


class _ListServicesResult(BaseModel):
    services: list[RegistryService]


def _normalize_scope_service(service: str) -> str:
    if service == "*" or "/" in service:
        return service
    try:
        return normalize_service_name(service)[0]
    except Exception:  # noqa: BLE001
        return service


class RouterAuthClient(ClientBase):
    """Typed client for the Go Router's built-in, unary management service."""

    async def whoami(self) -> RouterIdentity:
        return await self.call_unary(AUTH_SERVICE, "whoami", _Empty(), RouterIdentity)

    async def issue_token(
        self, scopes: list[RouterScope], ttl_seconds: int = 900
    ) -> IssuedToken:
        normalized = [
            scope.model_copy(
                update={"service": _normalize_scope_service(scope.service)}
            )
            for scope in scopes
        ]
        return await self.call_unary(
            AUTH_SERVICE,
            "issue_token",
            _IssueRequest(ttl_seconds=ttl_seconds, scopes=normalized),
            IssuedToken,
        )

    async def revoke_token(self, token_id: str) -> bool:
        result = await self.call_unary(
            AUTH_SERVICE,
            "revoke_token",
            _RevokeRequest(token_id=token_id),
            _RevokeResult,
        )
        return result.revoked

    async def renew_token(self, token_id: str, *, ttl_seconds: int) -> RenewedToken:
        return await self.call_unary(
            AUTH_SERVICE,
            "renew_token",
            _RenewRequest(token_id=token_id, ttl_seconds=ttl_seconds),
            RenewedToken,
        )

    async def list_tokens(self) -> list[TokenInfo]:
        result = await self.call_unary(
            AUTH_SERVICE, "list_tokens", _Empty(), _ListTokensResult
        )
        return result.tokens
