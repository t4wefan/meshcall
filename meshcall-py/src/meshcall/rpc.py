"""The transport-neutral client surface generated clients are built on.

A generated client calls ``MeshCallRpc.call_*``; ``Mesh`` and the low-level
``ClientBase`` implement it. Swapping the transport therefore never touches
generated code: pass a ``Mesh`` configured with any ``Router`` or
``LocalRouter`` to the generated client's constructor.

The methods mirror ``ClientBase``'s, so the protocol is structural: anything
with those methods works, including a hand-written test double.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Mapping
from types import TracebackType
from typing import Any, Protocol, Self, cast, runtime_checkable

from pydantic import BaseModel

from meshcall.streams import RpcDuplexClient, RpcServerStream


@runtime_checkable
class MeshCallRpc(Protocol):
    """Everything a generated client needs from its transport."""

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        """Return a shallow view with per-call options applied."""
        ...

    async def call_unary(
        self,
        service: str,
        method: str,
        request: BaseModel,
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> Any: ...

    def call_server_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel | str | bytes],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> RpcServerStream[Any]: ...

    def call_subscription(
        self,
        service: str,
        method: str,
        request: BaseModel,
        item_type: type[BaseModel | str | bytes],
        *,
        cursor: str | None = None,
        resume: str | None = None,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        instance: str | None = None,
    ) -> RpcServerStream[Any]: ...

    async def call_client_stream(
        self,
        service: str,
        method: str,
        request: BaseModel,
        items: AsyncIterable[Any],
        input_type: type[BaseModel | str | bytes],
        response_type: type[BaseModel],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        idempotent: bool = False,
        instance: str | None = None,
    ) -> Any: ...

    def call_duplex(
        self,
        service: str,
        method: str,
        request: BaseModel,
        input_type: type[BaseModel | str | bytes],
        output_type: type[BaseModel | str | bytes],
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        instance: str | None = None,
    ) -> RpcDuplexClient[Any, Any]: ...


class RpcClient:
    """Base class for generated clients.

    Wraps a :class:`Mesh` (or any :class:`MeshCallRpc`); for low-level tests it
    also accepts a ``ClientDriver`` (wrapped in ``ClientBase``).
    """

    def __init__(self, rpc: MeshCallRpc | Any) -> None:
        if isinstance(rpc, RpcClient):
            self._rpc: MeshCallRpc = rpc._rpc
            self._owns_rpc = rpc._owns_rpc
            return
        if hasattr(rpc, "call_unary"):
            self._rpc = rpc
            self._owns_rpc = False
            return
        from meshcall.router import LocalRouter, Router

        if isinstance(rpc, (Router, LocalRouter)):
            raise TypeError(
                "Generated clients wrap a Mesh, not a Router directly; "
                "pass Mesh(router) instead"
            )
        from meshcall.client import ClientBase

        self._rpc = cast(MeshCallRpc, ClientBase(rpc))  # type: ignore[arg-type]
        self._owns_rpc = True

    @property
    def rpc(self) -> MeshCallRpc:
        """The transport behind this client."""
        return self._rpc

    def with_options(
        self,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
        metadata: Mapping[str, str] | None = None,
        idempotency_key: str | None = None,
        instance: str | None = None,
    ) -> Self:
        """Return a shallow copy whose calls use these options."""
        clone: Self = object.__new__(type(self))
        RpcClient.__init__(
            clone,
            self._rpc.with_options(
                timeout=timeout,
                idle_timeout=idle_timeout,
                metadata=metadata,
                idempotency_key=idempotency_key,
                instance=instance,
            ),
        )
        return clone

    async def close(self) -> None:
        """Close the transport this client owns; a shared Mesh is left open."""
        if not self._owns_rpc:
            return
        stop = getattr(self._rpc, "close", None) or getattr(self._rpc, "stop", None)
        if stop is not None:
            await stop()

    async def stop(self) -> None:
        """Alias for :meth:`close`, kept for low-level driver tests."""
        await self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
