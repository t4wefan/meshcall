from __future__ import annotations

import asyncio


class CreditWindow:
    def __init__(self) -> None:
        self._credit = 0
        self._closed: BaseException | None = None
        self._condition = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._condition:
            await self._condition.wait_for(
                lambda: self._credit > 0 or self._closed is not None
            )
            if self._closed is not None:
                raise self._closed
            self._credit -= 1

    async def grant(self, credit: int) -> None:
        if credit <= 0:
            raise ValueError("Credit must be positive")
        async with self._condition:
            if self._closed is None:
                self._credit += credit
                self._condition.notify_all()

    async def close(self, error: BaseException) -> None:
        async with self._condition:
            if self._closed is None:
                self._closed = error
                self._condition.notify_all()

