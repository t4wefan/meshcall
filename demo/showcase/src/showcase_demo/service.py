"""A service that demonstrates the recommended MeshCall Python API."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from pydantic import BaseModel

from meshcall import RpcInputStream, meta, mutates, query, service, stream


class GreetingResult(BaseModel):
    message: str
    repeat: int


class WelcomeRequest(BaseModel):
    name: str
    tags: list[str]


class WelcomeResult(BaseModel):
    message: str
    tags: list[str]


class VisitResult(BaseModel):
    visits: int
    caller: str


class CountItem(BaseModel):
    value: int


class NumberItem(BaseModel):
    value: int


class TotalResult(BaseModel):
    total: int


# The escape hatch for values that travel as call metadata (a gateway header,
# a CLI identity) rather than as business parameters. Injected parameters never
# appear in the contract or in generated clients.
Caller = Annotated[str | None, meta("x-caller-id")]


@service("demo.v1.ShowcaseService")
class ShowcaseService:
    """Stateful service using the recommended MeshCall Python API."""

    def __init__(self, greeting_prefix: str = "Hello") -> None:
        self.greeting_prefix = greeting_prefix
        self.visits = 0

    @query
    async def greet(self, name: str, repeat: int = 1) -> GreetingResult:
        """Parameters are the request: ``name`` and ``repeat`` are its fields.

        ``@query`` marks a pure read (read-only and idempotent).
        """
        return GreetingResult(
            message=" ".join(f"{self.greeting_prefix}, {name}!" for _ in range(repeat)),
            repeat=repeat,
        )

    @query
    async def welcome(self, profile: WelcomeRequest) -> WelcomeResult:
        """A model-typed parameter is one field of the request, kept whole."""
        return WelcomeResult(
            message=f"{self.greeting_prefix}, {profile.name}!",
            tags=profile.tags,
        )

    @mutates(idempotent=False, destructive=False)
    async def visit(self, name: str, *, caller: Caller) -> VisitResult:
        """Record a visit; ``caller`` is injected from call metadata, not sent by the client."""
        self.visits += 1
        return VisitResult(visits=self.visits, caller=caller or "anonymous")

    @stream
    async def count(self, start: int, stop: int) -> AsyncIterator[CountItem]:
        """Server stream: the client receives typed items asynchronously."""
        for value in range(start, stop):
            yield CountItem(value=value)

    @mutates(idempotent=True)
    async def sum_values(
        self,
        offset: int,
        items: RpcInputStream[NumberItem],
    ) -> TotalResult:
        """Client stream: ordinary parameters first, the inbound stream last."""
        total = offset
        async for item in items:
            total += item.value
        return TotalResult(total=total)
