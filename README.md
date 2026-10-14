<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python, TypeScript & Go" width="300">
</p>

<p align="center">
  <strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a>
</p>

# MeshCall

**Write cross-language streaming RPC the way you write ordinary Python and TypeScript functions.**

MeshCall is an asynchronous, typed RPC framework for **Python** and **TypeScript**, powered by a standalone **Go Router**.

- **Declare services with normal functions** — parameters become the request schema, return types become the response schema
- **Stream naturally** — `yield` items from a generator for server streams, iterate with `async for` for client streams
- **Cross language boundaries without `.proto` files** — generate a fully typed TypeScript client from a Python service (or vice versa) in one command
- **Keep infrastructure out of application code** — routing, load balancing, backpressure, deadlines, and reconnects belong to the Router and Driver, not your handlers
- **Built for both humans and AI agents** — every running service is self-describing and callable from the terminal

---

## Install

```bash
# Python (3.11+)
uv add meshcall
# or: pip install meshcall

# TypeScript / Node.js (20+)
yarn add meshcall
# or: npm install meshcall
```

Prebuilt `meshcall-router` and `meshcall` CLI binaries ship inside the Python wheels and npm packages—no Go toolchain is required.

---

## Quick Start

Start with the smallest possible service:

```python
from pydantic import BaseModel
from meshcall import method, service


class Reply(BaseModel):
    message: str


@service("demo.v1.Greeter")
class Greeter:
    """Greeting service."""

    @method(read_only=True)
    async def greet(self, name: str) -> Reply:
        """Return a greeting for name."""
        return Reply(message=f"hello, {name}")
```

Those few lines already show the core model of MeshCall:

- declare a service with `@service(...)`
- expose a method with `@method(...)`
- write a normal `async def` function—its parameters (`name: str`) and return model (`Reply`) **are** the contract
- no transport or networking code leaks into the service definition

As you go further, MeshCall builds on top of that same minimal model:

- **Router-first symmetric mesh** — every node connects to the Router (`ws://...`), so any process can simultaneously expose services and call other services without port juggling
- **Streaming is just an async generator or iterator** (`@method.server_stream`, `@method.client_stream`)
- **Context and logging** (`ctx: RpcContext`, `logger: RpcLogger`) are injected only when a handler asks for them

Here is a complete runnable example that starts a Router, registers the service, and calls both a unary method and a server stream:

```python
import asyncio
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcServer, WebSocketRouter, method, service
from meshcall.client import ClientBase
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver


class Reply(BaseModel):
    message: str


class Tick(BaseModel):
    step: int


@service("demo.v1.Greeter")
class Greeter:
    """Greeting and countdown service."""

    @method(read_only=True)
    async def greet(self, name: str) -> Reply:
        """Return a greeting for name."""
        return Reply(message=f"hello, {name}")

    @method.server_stream(read_only=True)
    async def countdown(self, start: int) -> AsyncIterator[Tick]:
        """Stream ticks down to 1."""
        for n in range(start, 0, -1):
            yield Tick(step=n)


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        server = RpcServer(
            services=[Greeter()],
            driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="greeter-1"),
        )
        await server.start()

        async with ClientBase(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
            reply = await client.call_unary("demo.v1.Greeter", "greet", {"name": "Alice"})
            print(reply)  # {'message': 'hello, Alice'}

            async for tick in client.call_server_stream("demo.v1.Greeter", "countdown", {"start": 3}):
                print(tick)  # {'step': 3}, {'step': 2}, {'step': 1}

        await server.stop(drain=True)


if __name__ == "__main__":
    asyncio.run(main())
```

What happens here:

- `WebSocketRouter` starts the bundled Go Router in the background
- `RpcServer` attaches `Greeter` to the Router using `WebSocketRouterServerDriver`
- `greet` handles a request/reply call and validates both inputs and outputs against the Pydantic schema
- `countdown` streams `Tick` items back to the caller with automatic credit-based backpressure

---

## Why MeshCall?

Many RPC tools force you to choose between **heavy toolchain ceremony** (`.proto` files, `protoc` plugins, rigid stubs) and **fragile hand-rolled glue** (FastAPI + raw WebSockets with no schema validation, backpressure, or cancellation).

MeshCall is built around a simpler question:

**How do you make cross-language services feel like local async functions while keeping production RPC guarantees?**

1. **Code is the contract** — You write normal Python (Pydantic v2) or TypeScript (JSON Schema) definitions. MeshCall extracts a portable contract automatically.
2. **Three streaming shapes in one model** — Unary (`request -> response`), Server Stream (`request -> stream`), and Client Stream (`stream -> response`) share the same connection, error model, and flow control.
3. **Router-first symmetric peers** — We recommend **always connecting through the Go Router**, even on a single machine. Because every participant connects outbound to the Router, any process can simultaneously serve its own RPCs and act as a client calling other services—without allocating per-service ports or configuring peer addresses.
4. **Self-describing at runtime** — Any running mesh can be inspected and called from the CLI without reading source files or writing throwaway scripts.

---

## Core Concepts

### 1. Context and Logger Injection

If a handler needs call metadata, remaining deadline, response trailers, or structured logging, declare `ctx: RpcContext` or `logger: RpcLogger` in its signature:

```python
from meshcall import RpcContext, RpcLogger, method

@method(idempotent=True)
async def greet(self, name: str, ctx: RpcContext, logger: RpcLogger) -> Reply:
    """Return a greeting and attach the serving instance ID."""
    logger.info("greeting {}", name)
    ctx.set_trailer("served_by", ctx.instance_id or "router")
    return Reply(message=f"hello, {name}")
```

`ctx` and `logger` are injected by the runtime per call—they never appear in the public request schema or generated client parameters.

---

### 2. Cross-Language Client Generation

Instead of passing raw dictionaries to `ClientBase`, generate a typed client package for Python or TypeScript in one command:

```bash
# Generate a TypeScript client package directly from the Python service
uv run meshcall generate my_app.service:Greeter --language typescript --output ./greeter-ts

# Or generate a single-file Python client
uv run meshcall generate my_app.service:Greeter --single-file --output ./greeter_client.py
```

Your TypeScript code gets full IDE autocomplete and runtime schema validation:

```typescript
import { GreeterClient } from "meshcall-demo-v1-greeter-client";

const client = new GreeterClient("ws://127.0.0.1:8765");
try {
  const reply = await client.greet("Alice");
  console.log(reply.message);

  for await (const tick of client.countdown(3)) {
    console.log(tick.step);
  }
} finally {
  await client.close();
}
```

---

### 3. Explore and Call from the Terminal

When a Router is running, you can inspect every registered service and invoke methods directly from your shell:

```bash
# List services, online instances, health, and method signatures
uv run meshcall router inspect

# Call a unary or streaming method with JSON
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

If you pass a wrong field or type, `meshcall call` reads the service's live contract and prints an actionable fix hint.

---

### 4. First-Class AI Agent Integration

If you use AI coding agents, run `meshcall init` once in your project root:

```bash
# Default: installs .agents/skills/meshcall/SKILL.md + updates AGENTS.md
# Works out of the box with OpenAI Codex, Google Antigravity, OpenCode, Amp, etc.
uv run meshcall init

# Or target a specific coding agent (claude | cursor | copilot | windsurf | all):
uv run meshcall init --agent claude
uv run meshcall init --agent cursor
uv run meshcall init --agent all
```

With the skill installed, your AI agent knows how to discover live services via `meshcall router inspect`, test endpoints with `meshcall call`, and write strict-mode-compliant contracts without guessing.

---

### 5. Why Always Use the Router: Every Node Is Both Server and Client

Because the prebuilt `meshcall-router` binary ships inside the SDK, we recommend **always routing through the Router**—even in local development.

When every node connects to `ws://127.0.0.1:8765`:

- **No port management** — nodes do not need to allocate or expose their own inbound ports
- **Bidirectional peers** — a single Python or TypeScript process can register its own `@service` with the Router **and** use a generated client to call any other service in the mesh (including inside its own RPC handlers)
- **Live observability** — `meshcall router inspect` and `meshcall call` always have a complete view of every service in the system

```python
# One node simultaneously serving OrderService and calling InventoryClient via the Router
inventory = InventoryClient(WebSocketClientDriver("ws://127.0.0.1:8765"))
server = RpcServer(
    services=[OrderService(inventory_client=inventory)],
    driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="order-1"),
)
await server.start()
```

---

## Runnable Examples

Every directory under `demo/` and `best-practice/` is a self-contained runnable project:

```bash
uv run --project demo/quickstart demo   # Minimal unary & streaming in Python
uv run --project demo/showcase demo     # Full Python feature walkthrough
uv run --project demo/router demo       # Load balancing & Router capabilities
uv run --project demo/py2ts demo        # Python service -> generated TypeScript client
uv run --project demo/ts2py demo        # TypeScript service -> generated Python client
```

---

## Documentation

When you are ready for production features—load balancing, targeted stateful instances (`targeted=True`), retries and idempotency keys, graceful drain, Router authentication, or wire protocol details—follow the guides below:

| Document | What It Covers | 中文版 |
| --- | --- | --- |
| **[User Guide & Cookbook](docs/guide.md)** | Practical recipes for streaming, load balancing, targeted workers, timeouts, retries, auth tokens, and interceptors | **[快速上手与实战指南](docs/zh-CN/guide.md)** |
| **[Python SDK](meshcall-py/README.md)** | Python package API and usage | [Python SDK 指南](meshcall-py/README.zh-CN.md) |
| **[TypeScript SDK](meshcall-ts/README.md)** | TypeScript package API and usage | [TypeScript SDK 指南](meshcall-ts/README.zh-CN.md) |
| **[Go Router & CLI](meshcall-router/README.md)** | Router binary flags, auth setup, and Docker deployment | [Go Router 部署指南](meshcall-router/README.zh-CN.md) |
| **[Router & Auth Guide](docs/router.md)** | Routing model, multi-Router failover, ACL accounts, and scoped tokens | [Router 架构与鉴权](docs/zh-CN/router.md) |
| **[Runtime Architecture](docs/architecture.md)** | Worker-loop isolation, flow control, failure boundaries, and contract lint rules | [运行时架构设计](docs/zh-CN/architecture.md) |
| **[Protocol Specification](docs/protocol.md)** | `meshcall/1` wire frames, handshake, timeout math, and error codes | [协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md) |
| **[Contributing](CONTRIBUTING.md)** | Local checks (`scripts/check.sh`), conformance suite, and release workflow | [贡献指南](CONTRIBUTING.zh-CN.md) |

---

## Philosophy in One Sentence

MeshCall takes the ergonomics of ordinary typed async functions, extends them across Python and TypeScript with native streaming, and keeps all distributed routing complexity inside a stateless Go Router.
