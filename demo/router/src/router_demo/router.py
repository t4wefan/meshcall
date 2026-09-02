from __future__ import annotations

import asyncio

from meshcall import WebSocketRouter


async def main() -> None:
    router = WebSocketRouter(host="127.0.0.1", port=8765)
    await router.start()
    try:
        await asyncio.Event().wait()
    finally:
        await router.stop()


if __name__ == "__main__":
    asyncio.run(main())

