"""Tunnels: instance-scoped connections through the Router (docs/protocol.md → Tunnels).

An issuer opens a tunnel with :class:`TunnelsClient` (``mesh.tunnels``); the
Router offers it to the target instance, whose runtime answers through
:class:`TunnelHost` according to the policy given at ``mesh.mount(...,
accept_tunnels=...)``. Calls arriving through an accepted tunnel expose it as
``ctx.tunnel``.
"""

from __future__ import annotations

import hmac
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from meshcall.contract import normalize_service_name
from meshcall.e2e import (
    SessionStore,
    b64decode,
    b64encode,
    check_public_key,
    e2e_public_key,
    handshake_failed,
    load_private_key,
    nk_respond,
)
from meshcall.errors import ContractError

if TYPE_CHECKING:
    from meshcall.router import Mesh
    from meshcall.worker import WorkerLoop

TUNNEL_SERVICE = "meshcall.router.v1.TunnelService"
TUNNEL_HOST_SERVICE = "meshcall.router.v1.TunnelHost"
TUNNEL_CAPABILITY = "tunnels"
TUNNEL_CREDENTIAL_PREFIX = "mtun_"
TUNNEL_ID_KEY = "meshcall-tunnel-id"
TUNNEL_PRINCIPAL_KEY = "meshcall-tunnel-principal"


class TunnelScope(BaseModel):
    """One service of the target instance and the methods a tunnel allows (``"*"``: all)."""

    model_config = ConfigDict(frozen=True)

    service: str
    methods: list[str] = Field(default_factory=lambda: ["*"])


class TunnelInfo(BaseModel):
    """A tunnel as the Router lists it (never with its credential)."""

    tunnel_id: str
    instance_id: str
    principal: str
    scopes: list[TunnelScope]
    expires_at_unix_ms: int
    max_connections: int | None = None
    e2e_key: str | None = None
    connections: int = 0
    online: bool = True


class OpenedTunnel(TunnelInfo):
    """The result of ``mesh.tunnels.open``: hand ``credential`` to the client once."""

    credential: str = Field(repr=False)


class TunnelOffer(BaseModel):
    """What an instance is asked to accept before a tunnel becomes usable."""

    model_config = ConfigDict(frozen=True)

    tunnel_id: str
    instance_id: str
    principal: str
    scopes: tuple[TunnelScope, ...]
    expires_at_unix_ms: int
    #: Offered again after this instance reconnected.
    resumed: bool = False
    #: The public key the issuer pinned for end-to-end sealing; ``None`` for a
    #: plain tunnel. An offer pinning a key this instance does not hold is
    #: refused before any hook sees it.
    e2e_key: str | None = None


@dataclass(frozen=True)
class Tunnel:
    """The tunnel a call arrived through, as the instance accepted it (``ctx.tunnel``)."""

    id: str
    principal: str
    scopes: tuple[TunnelScope, ...]
    #: The tunnel carries end-to-end sealed calls only.
    sealed: bool = False


@dataclass(frozen=True)
class TunnelClosed:
    """A tunnel this instance accepted has ended."""

    tunnel_id: str
    principal: str
    reason: str


TunnelOfferHook = Callable[[TunnelOffer], bool | Awaitable[bool]]
"""Decides an offer: return ``True`` to accept. May be async."""

TunnelClosedHook = Callable[[TunnelClosed], None | Awaitable[None]]
"""Told when an accepted tunnel ends (revoked, expired, ...). May be async."""


@dataclass(frozen=True)
class TunnelPolicy:
    """How the services of one ``mount`` call answer tunnel offers."""

    accept: bool | TunnelOfferHook
    on_closed: TunnelClosedHook | None = None


def check_tunnel_credential(credential: str) -> str:
    if not credential.startswith(TUNNEL_CREDENTIAL_PREFIX) or any(
        c in credential for c in " \r\n"
    ):
        raise ValueError(
            "A tunnel credential starts with 'mtun_'; it is the `credential` "
            "returned by mesh.tunnels.open"
        )
    return credential


class _Empty(BaseModel):
    pass


class _OpenRequest(BaseModel):
    # The Router checks the bounds (invalid_argument), so every runtime fails alike.
    instance_id: str
    scopes: list[TunnelScope]
    principal: str
    ttl_seconds: int
    max_connections: int | None = None
    e2e_key: str | None = None


class _RevokeRequest(BaseModel):
    tunnel_id: str


class _RevokeResult(BaseModel):
    revoked: bool


class _ListResult(BaseModel):
    tunnels: list[TunnelInfo]


def _scope(scope: TunnelScope | str) -> TunnelScope:
    if isinstance(scope, str):
        scope = TunnelScope(service=scope)
    try:
        name = normalize_service_name(scope.service)[0]
    except Exception:  # noqa: BLE001
        name = scope.service
    # Explicit fields: requests are sent with exclude_unset.
    return TunnelScope(service=name, methods=list(scope.methods))


class TunnelsClient:
    """Issuer API for the Router's ``meshcall.router.v1.TunnelService`` (``mesh.tunnels``)."""

    def __init__(self, mesh: Mesh) -> None:
        self._mesh = mesh

    async def open(
        self,
        *,
        instance: str,
        scopes: Sequence[TunnelScope | str],
        principal: str,
        ttl_seconds: int = 3600,
        max_connections: int | None = None,
        e2e_key: str | None = None,
    ) -> OpenedTunnel:
        """Open a tunnel to one instance; returns once the instance accepted it.

        ``scopes`` names services of the instance (a bare name allows every
        method). ``e2e_key`` pins the instance's public key (the one you
        provisioned it with, not one learnt from the Router): every call on
        the tunnel is then sealed end to end. Raises ``PermissionDeniedError`` (``reason: tunnel_rejected``)
        when the instance refuses and ``UnavailableError`` (``instance_unknown``)
        when it is not registered.
        """
        request = _OpenRequest(
            instance_id=instance,
            scopes=[_scope(s) for s in scopes],
            principal=principal,
            ttl_seconds=ttl_seconds,
        )
        if max_connections is not None:
            request.max_connections = max_connections
        if e2e_key is not None:
            request.e2e_key = check_public_key(e2e_key)
        return await self._mesh.call_unary(
            TUNNEL_SERVICE, "open", request, OpenedTunnel
        )

    async def revoke(self, tunnel_id: str) -> bool:
        """Close a tunnel and every connection on it; ``False`` if it is not yours or gone."""
        result = await self._mesh.call_unary(
            TUNNEL_SERVICE, "revoke", _RevokeRequest(tunnel_id=tunnel_id), _RevokeResult
        )
        return result.revoked

    async def list(self) -> list[TunnelInfo]:
        """The tunnels this identity opened that are still open."""
        result = await self._mesh.call_unary(
            TUNNEL_SERVICE, "list", _Empty(), _ListResult
        )
        return result.tunnels


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _same_key(a: str, b: str) -> bool:
    try:
        return hmac.compare_digest(b64decode(a), b64decode(b))
    except (ValueError, TypeError):
        return False


class TunnelHost:
    """Server side: answers the Router's TunnelHost calls and keeps accepted tunnels."""

    def __init__(
        self,
        policies: Mapping[str, TunnelPolicy],
        worker: WorkerLoop,
        *,
        e2e_private_key: str | None = None,
    ) -> None:
        self._policies = dict(policies)
        self._worker = worker
        self._tunnels: dict[str, Tunnel] = {}
        self._e2e_private_key = e2e_private_key
        self._e2e_public_key = (
            e2e_public_key(e2e_private_key) if e2e_private_key is not None else None
        )
        self.sessions = SessionStore()

    @property
    def enabled(self) -> bool:
        return bool(self._policies)

    def lookup(self, metadata: Mapping[str, str] | None) -> Tunnel | None:
        """The accepted tunnel a call names, or ``None``.

        The instance's own table is authoritative: an id it never accepted
        (a direct caller forging the key) is no tunnel.
        """
        if not metadata or TUNNEL_ID_KEY not in metadata:
            return None
        tunnel = self._tunnels.get(metadata[TUNNEL_ID_KEY])
        if tunnel is None or metadata.get(TUNNEL_PRINCIPAL_KEY) != tunnel.principal:
            return None
        return tunnel

    async def handle(
        self, method: str, payload: Any, metadata: Mapping[str, str] | None = None
    ) -> dict[str, Any]:
        if method == "offer":
            offer = TunnelOffer.model_validate(payload)
            # An E2E tunnel pins a key: only the instance holding it may accept.
            pinned_ok = offer.e2e_key is None or (
                self._e2e_public_key is not None
                and _same_key(offer.e2e_key, self._e2e_public_key)
            )
            accepted = pinned_ok and await self._worker.run(self._decide(offer))
            if accepted:
                self._tunnels[offer.tunnel_id] = Tunnel(
                    offer.tunnel_id,
                    offer.principal,
                    offer.scopes,
                    sealed=offer.e2e_key is not None,
                )
            return {"accept": accepted}
        if method == "handshake":
            return self._handshake(metadata, payload)
        if method == "closed":
            tunnel_id = str(payload.get("tunnel_id", ""))
            self.sessions.drop(tunnel_id)
            tunnel = self._tunnels.pop(tunnel_id, None)
            if tunnel is not None:
                closed = TunnelClosed(
                    tunnel_id, tunnel.principal, str(payload.get("reason", ""))
                )
                await self._worker.run(self._notify(tunnel, closed))
            return {}
        raise ContractError(f"Unknown {TUNNEL_HOST_SERVICE} method {method}")

    def _handshake(
        self, metadata: Mapping[str, str] | None, payload: Any
    ) -> dict[str, Any]:
        """Answer a client's Noise NK message 1 on an E2E tunnel this instance
        accepted; the session is kept under that tunnel."""
        tunnel = self.lookup(metadata)
        if tunnel is None or not tunnel.sealed or self._e2e_private_key is None:
            raise handshake_failed(
                "not an end-to-end tunnel of this instance", "server"
            )
        try:
            message = b64decode(str((payload or {}).get("message", "")))
            reply, _, session = nk_respond(
                load_private_key(self._e2e_private_key), message
            )
        except Exception as exc:
            raise handshake_failed(
                "message 1 does not open with this instance's key", "server"
            ) from exc
        self.sessions.add(tunnel.id, session)
        return {"message": b64encode(reply)}

    async def _decide(self, offer: TunnelOffer) -> bool:
        # Every scoped service must have opted in, and every hook must agree.
        for scope in offer.scopes:
            policy = self._policies.get(scope.service)
            if policy is None:
                return False
            if callable(policy.accept):
                if not await _resolve(policy.accept(offer)):
                    return False
            elif not policy.accept:
                return False
        return True

    async def _notify(self, tunnel: Tunnel, closed: TunnelClosed) -> None:
        called: list[TunnelClosedHook] = []
        for scope in tunnel.scopes:
            policy = self._policies.get(scope.service)
            hook = policy.on_closed if policy is not None else None
            if hook is not None and hook not in called:
                called.append(hook)
                await _resolve(hook(closed))
