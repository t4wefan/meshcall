"""A small service that demonstrates MeshCall's recommended Python API."""

from __future__ import annotations

from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import RpcDuplex, RpcInputStream, method, service


class GreetingResult(BaseModel):
    message: str
    repeat: int


class WelcomeRequest(BaseModel):
    name: str
    tags: list[str]


class WelcomeResult(BaseModel):
    message: str
    tags: list[str]


class CountItem(BaseModel):
    value: int


class NumberItem(BaseModel):
    value: int


class TotalResult(BaseModel):
    total: int


class EchoItem(BaseModel):
    value: int
    label: str


class EchoSummary(BaseModel):
    count: int
    total: int


@service(name="demo.v1.ShowcaseService")
class ShowcaseService:
    """Stateful service using expanded parameters and typed models."""

    def __init__(self, greeting_prefix: str = "Hello") -> None:
        self.greeting_prefix = greeting_prefix

    @method()
    async def greet(self, name: str, repeat: int = 1) -> GreetingResult:
        """Default unary: ordinary parameters become one request object."""
        return GreetingResult(
            message=" ".join(f"{self.greeting_prefix}, {name}!" for _ in range(repeat)),
            repeat=repeat,
        )

    @method()
    async def welcome(self, request: WelcomeRequest) -> WelcomeResult:
        """Request-model style remains available when it reads better."""
        return WelcomeResult(
            message=f"{self.greeting_prefix}, {request.name}!",
            tags=request.tags,
        )

    @method.server_stream()
    async def count(self, start: int, stop: int) -> AsyncIterator[CountItem]:
        """Server stream: the client receives typed items asynchronously."""
        for value in range(start, stop):
            yield CountItem(value=value)

    @method.client_stream()
    async def sum_values(
        self,
        offset: int,
        items: RpcInputStream[NumberItem],
    ) -> TotalResult:
        """Client stream: the service consumes a typed inbound stream."""
        total = offset
        async for item in items:
            total += item.value
        return TotalResult(total=total)

    @method.duplex()
    async def echo(
        self,
        label: str,
        channel: RpcDuplex[NumberItem, EchoItem],
    ) -> EchoSummary:
        """Duplex: consume and produce typed items while the call is open."""
        count = 0
        total = 0
        async for item in channel:
            count += 1
            total += item.value
            await channel.send(EchoItem(value=item.value, label=label))
        return EchoSummary(count=count, total=total)
