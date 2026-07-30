from __future__ import annotations

from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import method, service


class CountRequest(BaseModel):
    stop: int


class CountItem(BaseModel):
    value: int


@service(name="example.v1.CounterService")
class CounterService:
    @method.static.server_stream
    async def count(request: CountRequest) -> AsyncIterator[CountItem]:
        for value in range(request.stop):
            yield CountItem(value=value)
