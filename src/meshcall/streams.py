from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Generic, TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
ResultT = TypeVar("ResultT")


class RpcInputStream(AsyncIterator[InputT], ABC, Generic[InputT]):
    """Typed inbound stream supplied to a service method."""

    @abstractmethod
    def __aiter__(self) -> RpcInputStream[InputT]: ...

    @abstractmethod
    async def __anext__(self) -> InputT: ...


class RpcDuplex(RpcInputStream[InputT], ABC, Generic[InputT, OutputT]):
    """Typed duplex channel supplied to a service method."""

    @abstractmethod
    async def send(self, item: OutputT) -> None: ...

    @abstractmethod
    async def close_send(self) -> None: ...


class RpcServerStream(AsyncIterator[OutputT], ABC, Generic[OutputT]):
    @abstractmethod
    def __aiter__(self) -> RpcServerStream[OutputT]: ...

    @abstractmethod
    async def __anext__(self) -> OutputT: ...

    @abstractmethod
    async def cancel(self, reason: str = "client_closed") -> None: ...


class RpcDuplexClient(
    RpcInputStream[OutputT],
    ABC,
    Generic[InputT, OutputT, ResultT],
):
    @abstractmethod
    async def send(self, item: InputT) -> None: ...

    @abstractmethod
    async def close_send(self) -> None: ...

    @abstractmethod
    async def result(self) -> ResultT: ...

    @abstractmethod
    async def cancel(self, reason: str = "client_closed") -> None: ...
