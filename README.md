<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python, TypeScript & Go" width="300">
</p>

<p align="center">
  <strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a>
</p>

# MeshCall

**Write cross-language streaming RPC the way you write ordinary Python and TypeScript functions.**

MeshCall is an asynchronous, strongly typed streaming RPC framework for **Python** and **TypeScript**.

The **Router** is the relay process between services. Servers register services with it, and callers find and call services through it; because every node communicates through the Router, any process can simultaneously provide services and call other services as a client. The Router binary is bundled in the package—no Go installation is required.

- **Declare services with normal functions** — function parameters define the request structure, return types define the response structure, with automatic type validation
- **Stream results naturally** — `yield` items from an async generator for server streams, iterate with `async for` for client streams
- **Generate cross-language typed clients** — generate a strongly typed TypeScript client from a Python service (or vice versa) in one command, without `.proto` files
- **Focus on business logic** — the framework handles connection management, request forwarding, timeouts, cancellation, and flow control; service functions only handle business logic
- **Inspect and call from the terminal** — every running service carries its own interface description, so you can inspect signatures and make calls directly from the CLI

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

---

## Quick Start

### 1. Define a Service

Save the following code as `service.py`:

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

`demo.v1.Greeter` is the service name callers use, including a version tag. `read_only=True` declares that this method does not modify business state. The function parameters and return model together describe the request and response structure, which this document calls the **service contract**.

### 2. Generate a Typed Client

In the directory containing `service.py`, generate `greeter_client.py` from the `Greeter` class in `service.py`:

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

### 3. Start the Server

Create `server.py` and run it to start the bundled Router and the `Greeter` service:

```python
import asyncio
from meshcall import RpcServer, WebSocketRouter
from meshcall.drivers import WebSocketRouterServerDriver
from service import Greeter


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        server = RpcServer(
            services=[Greeter()],
            driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="greeter-1"),
        )
        await server.start()
        try:
            await asyncio.Event().wait()
        finally:
            await server.stop(drain=True)


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
uv run python server.py
```

Here `instance_id="greeter-1"` is the name of this running service instance, and `drain=True` waits for in-flight requests to finish before stopping.

### 4. Call the Service

Keep `server.py` running, create `client.py`, and run it in another terminal:

```python
import asyncio
from meshcall.drivers import WebSocketClientDriver
from greeter_client import GreeterClient


async def main() -> None:
    async with GreeterClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
        reply = await client.greet("Alice")
        print(reply.message)  # hello, Alice


if __name__ == "__main__":
    asyncio.run(main())
```

```bash
uv run python client.py
```

---

## Stream Multiple Results

When a method needs to return multiple results over time (such as progress updates, log streams, or incremental output), decorate an async generator with `@method.server_stream`.

Update `service.py` to add `countdown`:

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import method, service


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
        """Count down from start to 1."""
        for n in range(start, 0, -1):
            yield Tick(step=n)
```

Regenerate the client and restart `server.py`, then consume the stream in `client.py` with `async for`:

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

```python
async with GreeterClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    async for tick in client.countdown(3):
        print(tick.step)  # 3, 2, 1
```

`countdown` yields results one by one so the caller can process items as they arrive; the framework controls sending speed according to consumption speed.

---

## Call from TypeScript

You can generate a TypeScript client package directly from the same `service.py`.

1. Generate the TypeScript client package directory `./greeter-ts` (named `meshcall-demo-v1-greeter-client` by default) from the `Greeter` class in `service.py`:

```bash
uv run meshcall generate service:Greeter --language typescript --output ./greeter-ts
```

2. Build the generated local package and add it to your TypeScript project:

```bash
yarn --cwd ./greeter-ts install && yarn --cwd ./greeter-ts build
yarn add file:./greeter-ts
```

3. Keep `server.py` running and call the service from TypeScript:

```typescript
import { MeshCallClient } from "meshcall";
import { GreeterClient } from "meshcall-demo-v1-greeter-client";

const rpc = new MeshCallClient("ws://127.0.0.1:8765");
const client = new GreeterClient(rpc);
try {
  const reply = await client.greet("Alice");
  console.log(reply.message); // hello, Alice

  for await (const tick of client.countdown(3)) {
    console.log(tick.step); // 3, 2, 1
  }
} finally {
  await rpc.close();
}
```

---

## Inspect and Call from the Terminal

Keep `server.py` running and run the following commands in another terminal (they connect to `ws://127.0.0.1:8765` by default):

```bash
# List registered services, online instances, and method signatures
uv run meshcall router inspect

# Call a unary or streaming method with JSON
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

If you pass a wrong field name or type, `meshcall call` checks the service contract and prints a field-level fix hint.

---

## AI Coding Assistant Integration

If you use an AI coding assistant, run `meshcall init` in your project root to install development instructions for AI assistants:

```bash
uv run meshcall init
```

This writes `.agents/skills/meshcall/SKILL.md` and updates `AGENTS.md` with reference instructions on defining services, generating clients, and testing methods from the CLI. See the [User Guide](docs/guide.md) for agent-specific configuration options (such as Claude Code or Cursor).

---

## Run Repository Examples

Clone this repository to run the self-contained example projects under `demo/`:

```bash
git clone https://github.com/t4wefan/meshcall.git
cd meshcall

uv run --project demo/quickstart demo   # Minimal unary & streaming in Python
uv run --project demo/showcase demo     # Full Python feature walkthrough
uv run --project demo/router demo       # Multi-instance load balancing & routing
uv run --project demo/py2ts demo        # Python service -> TypeScript client
uv run --project demo/ts2py demo        # TypeScript service -> Python client
```

---

## Documentation

| Document | Purpose | 中文版 |
| --- | --- | --- |
| **[User Guide & Cookbook](docs/guide.md)** | Handle streaming, timeouts and retries, context/logger injection, load balancing, and auth | **[快速上手与实战指南](docs/zh-CN/guide.md)** |
| **[Python SDK](meshcall-py/README.md)** | Install and use the Python package API | [Python SDK 指南](meshcall-py/README.zh-CN.md) |
| **[TypeScript SDK](meshcall-ts/README.md)** | Install and use the TypeScript package API | [TypeScript SDK 指南](meshcall-ts/README.zh-CN.md) |
| **[Go Router & CLI](meshcall-router/README.md)** | Deploy the standalone Router binary, configure CLI flags, and run with Docker | [Go Router 部署指南](meshcall-router/README.zh-CN.md) |
| **[Router & Auth Guide](docs/router.md)** | Configure multi-instance routing, accounts, and access tokens | [Router 架构与鉴权](docs/zh-CN/router.md) |
| **[Runtime Architecture](docs/architecture.md)** | Understand the internal design of the client and server runtimes | [运行时架构设计](docs/zh-CN/architecture.md) |
| **[Protocol Specification](docs/protocol.md)** | Read the `meshcall/1` wire protocol, timeout calculation, and error codes | [协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md) |
| **[Contributing](CONTRIBUTING.md)** | Run local checks, conformance tests, and release workflows | [贡献指南](CONTRIBUTING.zh-CN.md) |
