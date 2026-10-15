# meshcall (Python SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-py/README.zh-CN.md)

Asynchronous, typed streaming RPC for Python, TypeScript and Go over WebSocket.
Services define contracts with Pydantic v2 models, register with the standalone
Go Router, and expose unary, server-streaming, client-streaming and subscription
methods (and, as a last resort, restricted duplex methods).

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
uv run meshcall init                  # default: .agents/skills/meshcall/SKILL.md + AGENTS.md
uv run meshcall init --agent claude   # or target: claude | cursor | copilot | windsurf | all
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

We recommend always connecting through the Router (`WebSocketRouterServerDriver`),
which allows any node to simultaneously register its own services and act as a
client calling other services in the mesh (`WebSocketDirectServerDriver` is only
retained for legacy compatibility and is not recommended).

## Subscriptions

`@method.subscription(cursor="seq", resume="since")` declares a server stream
that clients reopen after a retryable failure, passing the last item's
`cursor` field as the `resume` parameter. `Topic[T]` fans published items out
to every subscriber in the process:

```python
topic: Topic[Event] = Topic(history=100, cursor=lambda e: e.seq)

@method.subscription(cursor="seq", resume="since", read_only=True)
async def events(self, since: int | None = None) -> AsyncIterator[Event]:
    """Stream events after since."""
    async for event in topic.subscribe(after=since):
        yield event
```

`topic.publish(event)` may be called from any thread or loop. See
[Subscriptions](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md#subscriptions).

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

# Generate a TypeScript package or a Go package from the same Python service
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
uv run meshcall generate my_app.service:Chat --language go --output internal/chatclient
```

Call the service through the generated client:

```python
from meshcall.drivers import WebSocketClientDriver
from meshcall_chat_v1_chat_client import ChatClient

async with ChatClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    reply = await client.send("s1", "hello", timeout=5.0)
    print(reply.text)
```

## Router and local transport

`WebSocketRouter` takes `host` / `port`, or `unix_path` (Linux, macOS, FreeBSD)
or `pipe` (Windows named pipe) for the local transport; `max_frame_size`
(default and maximum 64 MiB), `outbox_size` (bytes queued for a peer that is
not reading; default the Router's 256 MiB, at least `max_frame_size`),
`max_connections`, `binary_path`, `auth_file` and readiness timeouts. It
exposes `pid`, `is_running`, `bound_port`, `wait_closed()` and async context
management. Override the bundled binary with `MESHCALL_ROUTER_BINARY` when needed.

`WebSocketClientDriver` and `WebSocketRouterServerDriver` connect to a local
Router with `unix_path=` or `pipe=` instead of a URI; the CLI addresses the
same listeners as `unix:///path` and `pipe://NAME`.

## Router authentication

Pass `RouterCredentials(username=..., password=...)` or
`RouterCredentials(token=...)` as `auth` to `WebSocketClientDriver` or
`WebSocketRouterServerDriver`. `RouterAuthClient` provides `whoami()`,
`issue_token(scopes=..., ...)`, and `revoke_token(...)`. Use
`RouterScope(service=..., methods=[...])` to limit a token, or
`register_service=True` for temporary service instances.

## OpenTelemetry

```bash
pip install "meshcall[otel]"
```

```python
from meshcall.otel import client_interceptor, server_interceptor

client = ChatClient(driver, interceptors=[client_interceptor()])
server = RpcServer(services=[Chat()], driver=driver, interceptors=[server_interceptor()])
```

Each call gets a span and `meshcall.calls` / `meshcall.call.duration` metrics;
the W3C trace context travels as call metadata, so a trace continues through
the Router and across languages (TypeScript: `meshcall/otel`). With no
OpenTelemetry SDK configured the interceptors are no-ops.

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))
