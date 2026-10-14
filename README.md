<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python & TypeScript" width="300">
</p>

<p align="center">
  <strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a>
</p>

# MeshCall

**Write cross-language streaming RPC the way you write ordinary Python and TypeScript functions.**

Building services across Python and TypeScript (such as pairing a Python AI/compute backend with a TypeScript application) shouldn't force you to choose between heavy `.proto` toolchains and fragile hand-rolled WebSocket glue.

MeshCall is an asynchronous, strongly typed streaming RPC framework for **Python** and **TypeScript**—**defining a service feels like writing a normal async function, and calling a remote endpoint feels like calling a local method.**

- **Code is the contract, zero boilerplate** — function parameters define the request and return models define the response, with no `.proto` files or manual serialization code
- **Stream as naturally as writing a loop** — `yield` items on the server and iterate with `async for` on the client, ideal for progress updates, live data feeds, and streaming AI outputs
- **End-to-end types across languages** — generate a fully typed TypeScript client from a Python service (or vice versa) in one command, catching schema changes at compile time
- **Router-first symmetric nodes** — the **Router** is the relay process between services: servers register with it, and callers find and call services through it. Because every node connects through the Router, you never have to juggle per-service ports or peer URLs, and any process can provide services and call other services at the same time
- **Transparent to both humans and AI** — running services carry their own interface descriptions, so you can inspect signatures and make test calls right from the terminal without reading source code or writing throwaway scripts

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

The package already includes the `Router` and `meshcall` CLI—ready out of the box with no extra setup required.

---

## Quick Start

In 4 small steps, you can go from **defining a service** to making a **strongly typed remote call**.

### 1. Define a Service Like a Normal Class

Save the following code as `service.py`. Notice there is no networking, connection handling, or serialization boilerplate:

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

`demo.v1.Greeter` is the service name callers use (including a version tag), and `read_only=True` declares that this method does not modify business state. The function parameters (`name: str`) and return model (`Reply`) together form the **service contract**, which the framework uses to validate inputs and outputs automatically.

### 2. Generate a Typed Client in One Command

Instead of passing string method names and raw dictionaries, run one command in the directory containing `service.py` to generate a typed client file `greeter_client.py` from `Greeter`:

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

### 3. Start the Router and Server

Create `server.py` and run it to start the built-in Router and register `Greeter`:

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

Here `instance_id="greeter-1"` names this running service instance, and `drain=True` waits for any in-flight requests to finish cleanly before stopping.

### 4. Call the Service Like a Local Async Function

Keep `server.py` running, create `client.py`, and run it in another terminal—with full IDE autocomplete and typed return values:

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

## Stream Multiple Results: Just Replace `return` with `yield`

When an endpoint needs to push results over time (such as streaming AI tokens, live task progress, or log feeds), you don't need callbacks or custom state machines—just decorate an async generator with `@method.server_stream`.

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

Regenerate the client and restart `server.py`, then consume the stream in `client.py` with a standard `async for` loop:

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

```python
async with GreeterClient(WebSocketClientDriver("ws://127.0.0.1:8765")) as client:
    async for tick in client.countdown(3):
        print(tick.step)  # 3, 2, 1
```

`countdown` yields items one by one so the caller can process them as they arrive; the framework paces sending speed to match consumption speed so slow consumers never cause memory buildup.

---

## Cross-Language Calls: Call Python Services from TypeScript

When pairing a Python backend with a TypeScript application, you never have to keep two sets of type definitions in sync by hand. The same `service.py` generates a native TypeScript client package directly.

1. Generate the TypeScript client package directory `./greeter-ts` (named `meshcall-demo-v1-greeter-client` by default) from the `Greeter` class in `service.py`:

```bash
uv run meshcall generate service:Greeter --language typescript --output ./greeter-ts
```

2. Build the generated local package and add it to your TypeScript project:

```bash
yarn --cwd ./greeter-ts install && yarn --cwd ./greeter-ts build
yarn add file:./greeter-ts
```

3. Keep `server.py` running and enjoy the same strongly typed calls and streaming iteration in TypeScript:

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

## No More Black Boxes: Inspect and Call from the Terminal

When debugging or integrating services, you don't need to dig through source files or write one-off test scripts. While `server.py` is running, open another terminal to inspect every registered service and make live calls (connecting to `ws://127.0.0.1:8765` by default):

```bash
# List online services, instance health, and method signatures
uv run meshcall router inspect

# Call a unary or streaming method directly with JSON
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

If you pass a wrong field name or type, `meshcall call` checks the live service contract and prints a clear field-level fix hint.

---

## Teach Your AI Coding Assistant

If you build with an AI coding assistant, run `meshcall init` once in your project root:

```bash
uv run meshcall init
```

This installs development instructions for AI assistants (`.agents/skills/meshcall/SKILL.md` and `AGENTS.md`), showing your assistant how to define services, generate typed clients, and use `meshcall router inspect` and `meshcall call` to explore and verify running endpoints on its own. See the [User Guide](docs/guide.md) for tool-specific flags (such as Claude Code or Cursor).

---

## Run Repository Examples

Clone this repository to run complete, self-contained example projects under `demo/`:

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

When you are ready to explore production capabilities, follow the guides below:

| Document | Purpose | 中文版 |
| --- | --- | --- |
| **[User Guide & Cookbook](docs/guide.md)** | Handle streaming, timeouts and retries, context/logger injection, load balancing, and auth | **[快速上手与实战指南](docs/zh-CN/guide.md)** |
| **[Python SDK](meshcall-py/README.md)** | Install and use the Python package API | [Python SDK 指南](meshcall-py/README.zh-CN.md) |
| **[TypeScript SDK](meshcall-ts/README.md)** | Install and use the TypeScript package API | [TypeScript SDK 指南](meshcall-ts/README.zh-CN.md) |
| **[Router & CLI](meshcall-router/README.md)** | Deploy the standalone Router binary, configure CLI flags, and run with Docker | [Router 与 CLI 部署指南](meshcall-router/README.zh-CN.md) |
| **[Router & Auth Guide](docs/router.md)** | Configure multi-instance routing, accounts, and access tokens | [Router 架构与鉴权](docs/zh-CN/router.md) |
| **[Runtime Architecture](docs/architecture.md)** | Understand the internal design of the client and server runtimes | [运行时架构设计](docs/zh-CN/architecture.md) |
| **[Protocol Specification](docs/protocol.md)** | Read the `meshcall/1` wire protocol, timeout calculation, and error codes | [协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md) |
| **[Contributing](CONTRIBUTING.md)** | Run local checks, conformance tests, and release workflows | [贡献指南](CONTRIBUTING.zh-CN.md) |

---

## Philosophy in One Sentence

MeshCall brings the intuitive feel of ordinary typed async functions to cross-language streaming RPC, while keeping connection management, flow control, and routing quietly out of your application code.
