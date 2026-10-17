# meshcall (Python SDK)

**English** | [简体中文](https://github.com/t4wefan/meshcall/blob/main/meshcall-py/README.zh-CN.md)

Asynchronous, typed streaming RPC for Python, TypeScript and Go over WebSocket.
Services define contracts with Pydantic v2 models, register with the standalone
Go Router, and expose unary, server-streaming and client-streaming methods.
Subscriptions are experimental in v0.3, and duplex remains a restricted shape
of last resort.

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
    Router,
    RpcLogger,
    mutates,
    query,
    run,
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
    run(Chat(), router=Router("ws://127.0.0.1:8765"))
```

The v0.3 shorthands are the recommended way to declare methods:

- `@query` is a pure read (`read_only=True`, `idempotent=True`).
- `@mutates(...)` is a state-changing unary, client-stream or duplex method.
  Bare `@mutates` is destructive by default; use `@mutates(idempotent=True)`
  for a retry-safe write and `@mutates(destructive=True)` to be explicit.
- `@stream` is a server stream. Bare `@stream` is treated as read-only unless
  the method name looks mutating; pass `read_only=False, idempotent=False` for
  a side-effecting stream.

The long-form `@method`, `@method.server_stream`, `@method.client_stream`,
`@method.duplex` and `@method.subscription` remain available, but the
shorthands above are what new services should use. Duplex (interleaved input
and output in one call) stays the last resort: declare it with
`@mutates(reason="…")` (or `@method.duplex(reason=…)`), take a
`DuplexChannel[In, Out]` parameter, and return `None`. Duplex calls are never
retried or deduplicated, take no idempotency key, and are not reachable over
HTTP.

Handlers may also ask for `ctx: RpcContext` and `logger: RpcLogger`; both are
injected by the runtime and never become contract parameters.

Set `MESHCALL_STRICT=1` during development and CI so missing docstrings or
unannotated mutating methods fail fast at import with a hint.

## Run and serve

The blocking `run` entry point owns the process until `SIGINT` / `SIGTERM`,
then drains and stops any Router it started:

```python
from meshcall import Router, run

run(Chat(), router=Router("ws://127.0.0.1:8765"))
```

Embed the service in an async application with `serve`; an ephemeral local
Router is spawned automatically when the address is local and nothing is
listening there:

```python
import asyncio

from meshcall import Router, serve


async def main() -> None:
    async with serve(Chat(), router=Router("ws://127.0.0.1:0")) as server:
        print(f"serving on {server.url}")  # e.g. ws://127.0.0.1:53281
        await asyncio.Event().wait()       # leaving the block stops and drains


asyncio.run(main())
```

`serve(...)` returns a `RunningServer` (`server.url`, `await server.stop()`,
`await server.wait_closed()`), and may also be awaited directly:
`server = await serve(...)`. Both `run` and `serve` accept `instance_id`,
`max_inflight`, `strict`, `interceptors` and logging options, and can be given
a URL string instead of a `Router`.

Router resolution is the same for both: an explicit address wins, otherwise
`MESHCALL_ROUTER_URL` is used (comma-separated for several Routers), falling
back to `ws://127.0.0.1:8765`. Auto-spawn applies only to local addresses and
only to `run` / `serve`; disable it with `Router(..., spawn_if_local=False)`.
We recommend connecting through the Router in every scenario: any node can
then serve its own services and call others in the mesh over the same
connection (`WebSocketDirectServerDriver` is only retained for legacy
compatibility and is not recommended).

## Generate and call a typed client

```bash
# Generate an installable Python uv package (or add --single-file for a single .py file)
uv run meshcall generate my_app.service:Chat --output generated/chat-client

# Generate a TypeScript package or a Go package from the same Python service
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
uv run meshcall generate my_app.service:Chat --language go --output internal/chatclient
```

Call the service through the generated client:

```python
from meshcall import Router
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Router.connect("ws://127.0.0.1:8765") as router:
        chat = ChatClient(router)  # shares the Router's single connection
        reply = await chat.with_options(
            timeout=5.0,
            idempotency_key="send-s1-1",
            metadata={"x-caller-id": "cli"},
        ).send("s1", "hello")
        print(reply.text)
```

`ChatClient.connect(...)` is the standalone form and resolves the Router
exactly like `run`: `async with ChatClient.connect() as client:` uses
`MESHCALL_ROUTER_URL` / the default, and `ChatClient.connect("ws://…")` takes
an explicit address. Generated methods take only the business parameters, in
the same order as the handler (client-stream methods take those, then the
inbound `AsyncIterable`); for a targeted service they also require the
`instance=` keyword. Per-call options never change the business signature:
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

## Router

`Router` is the unified handle for starting services, sharing a client
connection, and calling the Router's built-in services:

```python
from meshcall import Router, RouterCredentials
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Router.connect(
        "ws://127.0.0.1:8765",
        auth=RouterCredentials(token="…"),
    ) as router:
        services = await router.list_services()
        print([service.name for service in services])
        chat = ChatClient(router)  # one connection shared by every client
        reply = await chat.with_options(timeout=5.0).last_message("s1")
```

`Router.connect(...)` may be used as `async with Router.connect(...) as router`
or `await Router.connect(...)`; with no address it resolves
`MESHCALL_ROUTER_URL` (a comma-separated list is accepted for several Routers)
and then `ws://127.0.0.1:8765`. Constructing `Router(url)` is enough when
passing it to `run` / `serve`. Clients built with `ChatClient(router)` (or
`ChatClient.connect(router)` on a connected Router) multiplex over that one
connection; closing the Router closes them.

The built-in Router services are exposed directly: `await
router.list_services()`, `await router.describe_service(name)`, `await
router.whoami()`, `await router.issue_token(scopes, ttl_seconds=900)` and
`await router.revoke_token(token_id)`.

## Router and local transport

`Router` takes `unix_path` (Linux, macOS, FreeBSD) or `pipe` (Windows named
pipe) instead of a TCP URL; `Router("unix:///path")` and
`Router("pipe://NAME")` work too. The CLI addresses the same listeners as
`unix:///path` and `pipe://NAME`.

`WebSocketRouter` still owns an explicit Router process when you want to
manage one yourself: `host` / `port`, `unix_path`, `pipe`, `max_frame_size`
(default and maximum 64 MiB), `outbox_size` (bytes queued for a peer that is
not reading; default the Router's 256 MiB, at least `max_frame_size`),
`max_connections`, `binary_path`, `auth_file` and readiness timeouts. It
exposes `pid`, `is_running`, `bound_port`, `wait_closed()` and async context
management. Override the bundled binary with `MESHCALL_ROUTER_BINARY` when
needed. The low-level `WebSocketClientDriver` and
`WebSocketRouterServerDriver` remain available for custom wiring and accept
`unix_path=` / `pipe=` as well.

## Router authentication

Pass `RouterCredentials(username=..., password=...)` or
`RouterCredentials(token=...)` as `auth` to `Router(...)` /
`Router.connect(...)` (or to `WebSocketClientDriver` /
`WebSocketRouterServerDriver` when wiring them directly). The Router exposes
`whoami()`, `issue_token(scopes=..., ...)` and `revoke_token(...)`;
`RouterAuthClient` is the typed client for the same built-in service. Use
`RouterScope(service=..., methods=[...])` to limit a token, or
`register_service=True` for temporary service instances.

## OpenTelemetry

```bash
pip install "meshcall[otel]"
```

```python
from meshcall import Router, serve
from meshcall.otel import client_interceptor, server_interceptor
from meshcall_chat_v1_chat_client import ChatClient


async def main() -> None:
    async with Router.connect(
        "ws://127.0.0.1:8765",
        interceptors=[client_interceptor()],
    ) as router:
        chat = ChatClient(router)
        ...

    async with serve(
        Chat(),
        router=Router("ws://127.0.0.1:8765"),
        interceptors=[server_interceptor()],
    ):
        ...
```

Each call gets a span and `meshcall.calls` / `meshcall.call.duration` metrics;
the W3C trace context travels as call metadata, so a trace continues through
the Router and across languages (TypeScript: `meshcall/otel`). With no
OpenTelemetry SDK configured the interceptors are no-ops.

## Experimental: subscriptions and Topic

Subscriptions and `Topic` remain available but are **experimental in v0.3**:
the API may change. Prefer plain server streams until a use case really needs
automatic resume after a dropped connection.

`@method.subscription(cursor="seq", resume="since")` declares a server stream
that clients reopen after a retryable failure, passing the last item's
`cursor` field as the `resume` parameter. `Topic[T]` fans published items out
to every subscriber in the process:

```python
from meshcall import Topic, method, service

topic: Topic[Event] = Topic(history=100, cursor=lambda e: e.seq)


@service("events.v1.Events")
class Events:
    """Event feed."""

    @method.subscription(cursor="seq", resume="since", read_only=True)
    async def events(self, since: int | None = None) -> AsyncIterator[Event]:
        """Stream events after since."""
        async for event in topic.subscribe(after=since):
            yield event
```

`topic.publish(event)` may be called from any thread or loop. A subscriber
that falls too far behind is disconnected with retryable
`resource_exhausted` (`reason: "slow_subscriber"`) and resubscribes; a cursor
older than the history buffer is refused with `invalid_argument`
(`reason: "cursor_expired"`). See
[Subscriptions](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md#subscriptions).

## Documentation

- [Monorepo overview & quickstart](https://github.com/t4wefan/meshcall#readme) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/README.zh-CN.md))
- [User Guide & Cookbook](https://github.com/t4wefan/meshcall/blob/main/docs/guide.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/guide.md))
- [Protocol specification](https://github.com/t4wefan/meshcall/blob/main/docs/protocol.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/protocol.md))
- [Runtime architecture](https://github.com/t4wefan/meshcall/blob/main/docs/architecture.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/architecture.md))
- [Router authentication & management](https://github.com/t4wefan/meshcall/blob/main/docs/router.md) ([简体中文](https://github.com/t4wefan/meshcall/blob/main/docs/zh-CN/router.md))
