from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping
from typing import Generic, Self, TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class RpcInputStream(AsyncIterator[InputT], ABC, Generic[InputT]):
    """Typed inbound stream supplied to a service method."""

    @abstractmethod
    def __aiter__(self) -> RpcInputStream[InputT]: ...

    @abstractmethod
    async def __anext__(self) -> InputT: ...


class RpcDuplex(RpcInputStream[InputT], ABC, Generic[InputT, OutputT]):
    """The channel a duplex handler receives.

    Iterate it for input items; ``send`` output items while doing so. Returning
    from the handler closes the output and ends the call; ``close_send`` closes
    the output earlier while input is still being read.
    """

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

    @property
    @abstractmethod
    def trailers(self) -> Mapping[str, str]:
        """Server trailers, available once the stream has terminated."""

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        """Leaving the block cancels the call if it is still running."""
        await self.cancel()


class RpcDuplexClient(RpcServerStream[OutputT], ABC, Generic[InputT, OutputT]):
    """A duplex call: ``send`` input items while iterating output items.

    The output ends when the call ends successfully; a failure is raised from
    the iteration. Read output concurrently with sending: a client that only
    sends stalls once the server's output credit runs out.
    """

    @abstractmethod
    async def send(self, item: InputT) -> None: ...

    @abstractmethod
    async def close_send(self) -> None:
        """Half-close the input; the server's iteration of it then ends."""
