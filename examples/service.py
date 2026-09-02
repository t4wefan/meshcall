from __future__ import annotations

from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import method, service


class CountItem(BaseModel):
    value: int


@service(name="example.v1.CounterService")
class CounterService:
    @method.server_stream()
    async def count(self, stop: int) -> AsyncIterator[CountItem]:
        for value in range(stop):
            yield CountItem(value=value)
