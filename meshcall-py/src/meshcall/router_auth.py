from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from meshcall.client import ClientBase

AUTH_SERVICE = "meshcall.router.v1.AuthService"


@dataclass(frozen=True)
class RouterCredentials:
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    token: str | None = field(default=None, repr=False)

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


class IssuedToken(BaseModel):
    token: str = Field(repr=False)
    token_id: str
    expires_at_unix_ms: int
    scopes: list[RouterScope]


class RouterIdentity(BaseModel):
    username: str
    roles: list[Literal["client", "server"]]
    register_services: list[str] = Field(alias="register")
    call: list[str]
    expires_at_unix_ms: int | None


class _Empty(BaseModel):
    pass


class _IssueRequest(BaseModel):
    ttl_seconds: int = Field(ge=1, le=3600)
    scopes: list[RouterScope]


class _RevokeRequest(BaseModel):
    token_id: str


class _RevokeResult(BaseModel):
    revoked: bool


class RouterAuthClient(ClientBase):
    """Typed client for the Go Router's built-in, unary management service."""

    async def whoami(self) -> RouterIdentity:
        return await self._unary(AUTH_SERVICE, "whoami", _Empty(), RouterIdentity)

    async def issue_token(
        self, *, scopes: list[RouterScope], ttl_seconds: int = 900
    ) -> IssuedToken:
        return await self._unary(
            AUTH_SERVICE,
            "issue_token",
            _IssueRequest(ttl_seconds=ttl_seconds, scopes=scopes),
            IssuedToken,
        )

    async def revoke_token(self, token_id: str) -> bool:
        result = await self._unary(
            AUTH_SERVICE,
            "revoke_token",
            _RevokeRequest(token_id=token_id),
            _RevokeResult,
        )
        return result.revoked
