from __future__ import annotations

import asyncio

from meshcall import LocalRouter


async def main() -> None:
    async with LocalRouter(host="127.0.0.1", port=8765):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
