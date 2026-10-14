# meshcall (Python SDK)

Asynchronous, typed streaming RPC for Python and TypeScript over WebSocket.
Services define contracts with Pydantic v2 models, register with the standalone
Go Router (or listen directly), and expose unary, server-streaming, and
client-streaming methods.

## Install

```bash
uv add meshcall
# or: pip install meshcall
```

Prebuilt `meshcall-router` and `meshcall` CLI binaries are bundled inside the
platform wheels for Linux (`manylinux` and `musllinux`), macOS, and Windows—no
Go toolchain or separate download is required. On platforms using the pure-Python
wheel, `uv run meshcall router install` downloads the matching release binary into
the per-user cache.

If coding agents work on your project, install the first-party agent skill once:

```bash
uv run meshcall init
```

## Define and run a service

```python
import asyncio
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import (
    RpcContext,
    RpcInputStream,
    RpcLogger,
    RpcServer,
    WebSocketRouter,
    method,
    service,
)
from meshcall.drivers import WebSocketRouterServerDriver


class Message(BaseModel):
    text: str


@service("chat.v1.Chat")
class Chat:
    """Chat sessions."""

    @method(idempotent=True)
    async def send(self, session_id: str, text: str, ctx: RpcContext, logger: RpcLogger) -> Message:
        """Append a message and return the reply."""
        logger.info("session={} text={}", session_id, text)
        return Message(text=f"echo: {text}")

    @method.server_stream(read_only=True)
    async def history(self, session_id: str) -> AsyncIterator[Message]:
        """Replay messages for a session."""
        yield Message(text=f"history for {session_id}")

    @method.client_stream(idempotent=True)
    async def ingest(self, items: RpcInputStream[Message], session_id: str) -> Message:
        """Store a stream of messages."""
        count = 0
        async for _ in items:
            count += 1
        return Message(text=f"ingested {count}")


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        driver = WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="chat-1")
        server = RpcServer(services=[Chat()], driver=driver)
        await server.start()
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Set `MESHCALL_STRICT=1` during development and CI so missing docstrings or
unannotated mutating methods fail fast at import with a hint.

For direct point-to-point connections without a Router, pass
`WebSocketDirectServerDriver(host="127.0.0.1", port=8765)` (or `unix_path=...`)
to `RpcServer`.

## Explore and call from the CLI

```bash
# Inspect registered services, instances, health, and method signatures
uv run meshcall router inspect --router ws://127.0.0.1:8765

# Call a method directly from the command line
uv run meshcall call chat.v1.Chat.send '{"session_id": "s1", "text": "hello"}'
```

## Generate a typed client

```bash
# Generate an installable Python uv package (or add --single-file for a single .py file)
uv run meshcall generate my_app.service:Chat --output generated/chat-client

# Generate a TypeScript package from the same Python service
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
```

Call the service through the generated client:

```python
from meshcall.drivers import WebSocketClientDriver
from meshcall_chat_v1_chat_client import ChatClient

async with ChatClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    reply = await client.send("s1", "hello", timeout=5.0)
    print(reply.text)
```

## Router authentication

`WebSocketRouter` supports `binary_path`, `auth_file`, readiness timeouts,
`pid`, `is_running`, `bound_port`, `wait_closed()`, and async context management.
Override the bundled binary with `MESHCALL_ROUTER_BINARY` when needed.

Pass `RouterCredentials(username=..., password=...)` or
`RouterCredentials(token=...)` as `auth` to `WebSocketClientDriver` or
`WebSocketRouterServerDriver`. `RouterAuthClient` provides `whoami()`,
`issue_token(scopes=..., ...)`, and `revoke_token(...)`. Use
`RouterScope(service=..., methods=[...])` to limit a token, or
`register_service=True` for temporary service instances.

## Documentation

- [Monorepo overview & tutorials](https://github.com/t4wefan/meshcall#readme)
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md)
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md)
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md)
