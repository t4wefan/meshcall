# meshcall (Python SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-py/README.zh-CN.md)

Asynchronous, typed streaming RPC for Python, TypeScript and Go over WebSocket and HTTP.
Services define contracts with Pydantic v2 models, register with the standalone
Go Router through `Mesh`, and expose unary, server-streaming, client-streaming,
and restricted duplex methods.

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

## Define a service

```python
from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import (
    LocalRouter,
    Mesh,
    RpcLogger,
    mutates,
    query,
    service,
    stream,
)


class Message(BaseModel):
    text: str


@service("chat.v1.Chat")
class Chat:
    """Chat sessions."""

    @query
    async def last_message(self, session_id: str) -> Message:
        """Read a session's latest message (read-only and idempotent)."""
        return Message(text=f"last message in {session_id}")

    @mutates(idempotent=True)
    async def send(self, session_id: str, text: str, logger: RpcLogger) -> Message:
        """Append a message and return the reply (a retry-safe write)."""
        logger.info("session={} text={}", session_id, text)  # noqa: PLE1205
        return Message(text=f"echo: {text}")

    @stream
    async def watch(self, session_id: str) -> AsyncIterator[Message]:
        """Stream replies as they arrive (server stream)."""
        yield Message(text=f"watching {session_id}")

    @mutates(idempotent=True)
    async def ingest(self, session_id: str, items: AsyncIterator[Message]) -> Message:
        """Store a stream of messages (client stream)."""
        count = 0
        async for _ in items:
            count += 1
        return Message(text=f"ingested {count}")


if __name__ == "__main__":
    mesh = Mesh(LocalRouter(port=8765))
    mesh.mount(Chat())
    mesh.run()
```

Methods are declared with three decorators:

- `@query` is a pure read (`read_only=True`, `idempotent=True`).
- `@mutates(...)` is a state-changing unary, client-stream or duplex method.
  Bare `@mutates` is destructive by default; use `@mutates(idempotent=True)`
  for a retry-safe write and `@mutates(destructive=True)` to be explicit.
- `@stream` is a server stream. Bare `@stream` is treated as read-only unless
  the method name looks mutating; pass `read_only=False, idempotent=False` for
  a side-effecting stream.

Duplex (interleaved input and output in one call) is a restricted shape of last
resort: declare it with `@mutates(reason="…")`, take a
`DuplexChannel[In, Out]` parameter, and return `None`. Duplex calls are never
retried or deduplicated, take no idempotency key, and are not reachable over
HTTP.

Handlers may also ask for `ctx: RpcContext` and `logger: RpcLogger`; both are
injected by the runtime and never become contract parameters.

Set `MESHCALL_STRICT=1` during development and CI so missing docstrings or
unannotated mutating methods fail fast at import with a hint.

## Mount, run and serve

Mount services on a `Mesh` and start the blocking `mesh.run()` entry point, which owns the process until `SIGINT` / `SIGTERM` and then drains in-flight calls. Use `LocalRouter(port=8765)` to launch a local `meshcall-router` subprocess automatically, or `Router("ws://127.0.0.1:8765")` to connect to an already running Router:

```python
from meshcall import LocalRouter, Mesh, Router

# Start a local Router subprocess on port 8765 (or pass Router() for an external Router):
mesh = Mesh(LocalRouter(port=8765))
mesh.mount(Chat())
mesh.run()
```

Embed the service in an async application or test with `LocalRouter` and `mesh.serve`:

```python
import asyncio

from meshcall import LocalRouter, Mesh


async def main() -> None:
    async with LocalRouter(port=0) as router:
        mesh = Mesh(router)
        mesh.mount(Chat())
        async with mesh.serve() as server:
            print(f"serving on {server.url}")  # e.g. ws://127.0.0.1:53281
            await asyncio.Event().wait()  # leaving the block stops and drains


asyncio.run(main())
```

`mesh.serve(...)` returns a `RunningServer` (`server.url`, `await server.stop()`,
`await server.wait_closed()`), and may also be awaited directly (`await mesh.serve(...)`).
Both `mesh.run` and `mesh.serve` accept `instance_id`, `max_inflight`, `strict`,
`interceptors` and logging options.

`Router()` with no arguments resolves `MESHCALL_ROUTER_URL` (comma-separated for
several Routers), falling back to `ws://127.0.0.1:8765`. Use `LocalRouter(...)`
when you want Python to start and stop a local `meshcall-router` subprocess.

## Generate and call a typed client

```bash
# Generate a single-file Python client (default) or an installable uv package (--package)
uv run meshcall generate my_app/service.py:Chat --output generated/chat_client.py
uv run meshcall generate my_app.service:Chat --package --output generated/chat-client

# Generate a TypeScript or Go client from one or more services in any language
uv run meshcall generate my_app/service.py ts-service/src/service.ts ./go-svc \
  --language typescript --output generated/clients.ts
uv run meshcall generate my_app.service:Chat --language go --output internal/chatclient

# Export the unified portable contract IR (*.meshcall.json)
uv run meshcall export my_app/service.py --output chat.meshcall.json
```

Call the service through the generated client in a second terminal while `server.py` is running:

```python
from meshcall import Mesh, Router
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
        chat = ChatClient(mesh)  # shares the Mesh's single connection
        reply = await chat.with_options(
            timeout=5.0,
            idempotency_key="send-s1-1",
            metadata={"x-caller-id": "cli"},
        ).send("s1", "hello")
        print(reply.text)
```

Generated methods take only the business parameters, in the same order as the
handler (client-stream methods take those, then the inbound `AsyncIterable`);
for a targeted service they also require the `instance=` keyword. Per-call
options never change the business signature:
`with_options(timeout=..., idle_timeout=..., metadata=...,
idempotency_key=..., instance=...)` returns a client view that applies them.

## Explore and call from the CLI

```bash
# Inspect registered services, instances, health, and method signatures
uv run meshcall router inspect --router ws://127.0.0.1:8765

# Call a method directly from the command line
uv run meshcall call chat.v1.Chat.send '{"session_id": "s1", "text": "hello"}'
```

## Dependency injection and metadata

Handlers can ask for server-only values without adding parameters to the
contract:

```python
from typing import Annotated

from meshcall import dependency, meta, mutates, service


@dependency
async def current_user(authorization: Annotated[str, meta("authorization")]) -> str:
    return authorization.removeprefix("Bearer ")


CallerId = Annotated[str | None, meta("x-caller-id")]


@service("chat.v1.Chat")
class Chat:
    @mutates(idempotent=True)
    async def send(
        self,
        text: str,
        user: str = current_user,
        caller: CallerId = None,
    ) -> Message:
        """Append a message and return the reply."""
        return Message(text=f"{user}: {text} (caller={caller})")
```

`@dependency` providers may be sync, async or generators; a generator's
cleanup runs when the call ends, so it can commit on success and roll back on
error or cancellation (see `best-practice/`). Dependencies may also be written
as `Annotated[T, dependency(provider)]` or resolved from a class's
`_provider()`. `meta("key")` injects a call metadata entry. Injected parameters
are stripped from the contract and from generated clients; send metadata with
`with_options(metadata=...)` or `--metadata k=v` on the CLI.

## `Router`, `LocalRouter` and `Mesh`

`Router` describes a remote Router endpoint; `LocalRouter` manages a local
`meshcall-router` subprocess; `Mesh` is the peer runtime that mounts services,
multiplexes client connections, and exposes built-in Router services via
`mesh.registry` and `mesh.auth`:

```python
from meshcall import Mesh, Router, RouterCredentials
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Mesh(
        Router("ws://127.0.0.1:8765"),
        credentials=RouterCredentials(token="…"),
    ) as mesh:
        services = await mesh.registry.list()
        print([service.name for service in services])
        chat = ChatClient(mesh)  # one connection shared by every client
        reply = await chat.with_options(timeout=5.0).last_message("s1")
```

The built-in Router services are exposed on `mesh.registry` (`await mesh.registry.list()`,
`await mesh.registry.describe(name)`) and `mesh.auth` (`await mesh.auth.whoami()`,
`await mesh.auth.issue_token(scopes, ttl_seconds=900)`, `await mesh.auth.revoke_token(token_id)`).

## Local transport

`Router` takes `unix_path` (Linux, macOS, FreeBSD) or `pipe` (Windows named
pipe) instead of a TCP URL; `Router("unix:///path")` and
`Router("pipe://NAME")` work too. The CLI addresses the same listeners as
`unix:///path` and `pipe://NAME`.

`LocalRouter` owns an explicit Router process when you want to manage one
yourself: `host` / `port`, `unix_path`, `pipe`, `max_frame_size`
(default and maximum 64 MiB), `outbox_size` (bytes queued for a peer that is
not reading; default the Router's 256 MiB, at least `max_frame_size`),
`max_connections`, `binary_path`, `auth_file` and readiness timeouts. It
exposes `pid`, `is_running`, `bound_port`, `wait_closed()` and async context
management. Override the bundled binary with `MESHCALL_ROUTER_BINARY` when
needed. Low-level primitives (`RpcServer`, `ClientBase`,
`WebSocketClientDriver` and `WebSocketRouterServerDriver`) support custom
transport wiring and accept `unix_path=` / `pipe=` as well.

## HTTP transport

When `Router` is given an `http://` or `https://` URL, `Mesh` automatically selects the HTTP client transport (`httpx`) underneath:

```python
from meshcall import Mesh, Router

mesh = Mesh(Router("https://router.example"), token="mk_…")
chat = ChatClient(mesh)
reply = await chat.with_options(timeout=5).greet("Alice")
services = await mesh.registry.list()
```

Duplex methods are WebSocket-only (they raise `method_not_found`, `reason: "duplex_over_http"`), and `mesh.mount(...)` raises `ContractError` on an HTTP-backed `Mesh`. Everything else works as over WebSocket: unary calls with the usual idempotent retry, server and client streams, and the built-in `mesh.registry` / `mesh.auth` calls.

## Router authentication

Pass `token="…"` or `credentials=RouterCredentials(username=..., password=...)` /
`RouterCredentials(token=...)` to `Mesh(router, ...)` (or `auth=` to
`WebSocketClientDriver` / `WebSocketRouterServerDriver` when wiring them
directly). `mesh.auth` exposes `whoami()`, `issue_token(scopes=..., ...)` and
`revoke_token(...)`; `RouterAuthClient` is the lower-level typed client for the
same built-in service. Use `RouterScope(service=..., methods=[...])` to limit a
token, or `register_service=True` for temporary service instances.

## OpenTelemetry

```bash
pip install "meshcall[otel]"
```

```python
from meshcall import Mesh, Router
from meshcall.otel import client_interceptor, server_interceptor
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Mesh(
        Router("ws://127.0.0.1:8765"),
        interceptors=[client_interceptor()],
    ) as mesh:
        chat = ChatClient(mesh)
        ...

    server_mesh = Mesh(Router("ws://127.0.0.1:8765"))
    server_mesh.mount(Chat())
    async with server_mesh.serve(interceptors=[server_interceptor()]):
        ...
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
