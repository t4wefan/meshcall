from __future__ import annotations

from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import service, stream


class CountItem(BaseModel):
    value: int


@service("example.v1.CounterService")
class CounterService:
    """A tiny service instance registered with the router."""

    @stream
    async def count(self, stop: int) -> AsyncIterator[CountItem]:
        """Stream the integers from 0 up to, but not including, ``stop``."""
        for value in range(stop):
            yield CountItem(value=value)
