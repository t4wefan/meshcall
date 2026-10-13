"""A small service that demonstrates MeshCall's recommended Python API."""

from __future__ import annotations

from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import RpcInputStream, method, service


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


@service("demo.v1.ShowcaseService")
class ShowcaseService:
    """Stateful service using the recommended MeshCall Python API."""

    def __init__(self, greeting_prefix: str = "Hello") -> None:
        self.greeting_prefix = greeting_prefix

    @method(read_only=True)
    async def greet(self, name: str, repeat: int = 1) -> GreetingResult:
        """Parameters are the request: ``name`` and ``repeat`` are its fields."""
        return GreetingResult(
            message=" ".join(f"{self.greeting_prefix}, {name}!" for _ in range(repeat)),
            repeat=repeat,
        )

    @method(read_only=True)
    async def welcome(self, profile: WelcomeRequest) -> WelcomeResult:
        """A model-typed parameter is just one field of the request."""
        return WelcomeResult(
            message=f"{self.greeting_prefix}, {profile.name}!",
            tags=profile.tags,
        )

    @method.server_stream(read_only=True)
    async def count(self, start: int, stop: int) -> AsyncIterator[CountItem]:
        """Server stream: the client receives typed items asynchronously."""
        for value in range(start, stop):
            yield CountItem(value=value)

    @method.client_stream(idempotent=True)
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
