<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall — Typed Streaming RPC for Python, TypeScript & Go" width="300">
</p>

<p align="center">
  <strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a>
</p>

# MeshCall

**Write cross-language streaming RPC the way you write ordinary Python, TypeScript and Go functions.**

When building applications across processes or languages (such as pairing a Python AI/compute backend with a TypeScript application), you shouldn't have to choose between heavy `.proto` toolchains and fragile hand-rolled WebSocket / JSON glue.

MeshCall is an asynchronous streaming RPC framework for **Python**, **TypeScript** and **Go**—**defining a remote service feels like writing a normal async function, and calling a remote endpoint feels like calling a local method:**

- **Code is the contract—no `.proto` files or raw dictionaries** — declare inputs with function parameters and outputs with return models; generated clients provide full type signatures and IDE autocomplete so callers never have to hand-craft dictionaries or read server source code to check field names
- **Catch contract changes at type-check time, validate at runtime** — generate a TypeScript client from a Python service (or vice versa) in one command. When a server changes a parameter or field, type checkers and compilers flag affected call sites immediately, while the runtime automatically validates requests and responses before handler code runs
- **Stream as naturally as writing a loop** — `yield` items on the server and iterate with `async for` on the client, ideal for progress updates, live data feeds, and streaming AI outputs
- **Focus on business logic** — the framework handles connection management, request routing, timeouts, cancellation, and flow control so your handlers stay free of networking code
- **Observable when you need it** — opt-in OpenTelemetry interceptors (`meshcall[otel]` in Python, `meshcall/otel` in TypeScript) carry W3C trace context across languages and through the Router, and the Router serves Prometheus metrics on `--metrics-listen`
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

The package already includes the routing process (`Router`) and the `meshcall` CLI—ready out of the box with no extra setup required.

---

## Quick Start

In 4 steps, let's walk through the complete flow from **defining a service** to **calling it**, introducing MeshCall's core concepts along the way.

### 1. Define a "Service" and a "Method"

Save the following code as `service.py`. Notice there is no networking or connection management boilerplate:

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

This snippet introduces the three foundational building blocks of MeshCall:

- **Service (`@service("demo.v1.Greeter")`)**: Groups related remote capabilities under a versioned name (`v1`), serving as the unit of registration and client generation.
- **Method (`@method(read_only=True)`)**: A callable function exposed by the service. `read_only=True` declares that this method only reads data and does not modify business state.
- **Service Contract**: The method's parameters (`name: str`) and return model (`Reply`) together define the request and response structure. MeshCall extracts this contract automatically and validates inputs and outputs on every call—invalid data is rejected before it ever reaches your business logic.

### 2. Generate Client Code from the Contract

Run one command in the directory containing `service.py` to generate `greeter_client.py` from `Greeter`, turning the service contract into typed Python methods and model classes:

```bash
uv run meshcall generate service:Greeter --single-file --output greeter_client.py
```

Instead of passing string method names or raw dictionaries, you get full IDE autocomplete for methods and fields, along with static type checking for parameters and return values.

### 3. Start and Register the Service Through the "Router"

How do servers and clients find each other? In MeshCall, we recommend routing all communication through a **Router (the relay process between services)** rather than connecting clients directly to individual server ports:

- **Servers** connect to the Router and register their **service instances** (`instance_id`). You can start multiple instances of the same service, and the Router automatically forwards and load-balances requests across them.
- **Clients** connect to the same Router and call services by name (such as `demo.v1.Greeter`), without needing to know which port or machine hosts the target service.
- Because every node connects to the Router, **any process can simultaneously provide services and act as a client calling other services**.

Create `server.py` and run it to start the built-in local Router and register an instance of `Greeter`:

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

Here `instance_id="greeter-1"` is the name of this running service instance, and `drain=True` waits for in-flight requests to finish cleanly before stopping.

### 4. Call the Service Like a Local Async Function

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

Both the parameters and return value of `client.greet` carry full type signatures—giving you IDE autocomplete and static type checking so missing or mismatched arguments are caught right in your editor.

---

## Streaming Methods: Replace `return` with `yield`

Beyond request-response methods (`@method`), MeshCall natively supports **streaming methods**. When a method needs to return multiple results over time (such as streaming AI tokens, live task progress, or log feeds), decorate an async generator with `@method.server_stream` (or use `@method.client_stream` when the client streams input items to the server). For events and notifications that should survive dropped connections, `@method.subscription(cursor=…, resume=…)` declares a server stream that clients resume automatically after the last item they saw; `Topic[T]` fans events out to every subscriber in the process (see [Subscriptions](docs/protocol.md#subscriptions)).

Update `service.py` to add the streaming method `countdown`:

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

Regenerate the client and restart `server.py`, then consume the stream of `Tick` items in `client.py` with a standard `async for` loop:

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

When pairing a Python backend with a TypeScript application, a common source of bugs is **maintaining duplicate type definitions by hand—when someone renames a field in Python but forgets to update TypeScript, it only breaks at runtime.**

With `meshcall generate`, the same `service.py` generates a native TypeScript client package directly. Whenever the Python service changes its contract, regenerating the client immediately flags any mismatched calls at TypeScript compile time.

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

## No More Black Boxes: Inspect and Call from the Terminal

Because every service reports its contract to the Router when registering, you don't need to dig through source files or write one-off test scripts when debugging or integrating endpoints.

Keep `server.py` running and run the following commands in another terminal (connecting to `ws://127.0.0.1:8765` by default):

```bash
# List services registered on the Router, online instances, and method signatures
uv run meshcall router inspect

# Call a unary or streaming method directly with JSON
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

If you pass a wrong field name or type, `meshcall call` checks the live service contract and prints a clear field-level fix hint.

### Plain HTTP

The Router also accepts calls over HTTP on the same port, so anything that speaks HTTP can call your services without a MeshCall client:

```bash
# Unary: JSON in, JSON out
curl http://127.0.0.1:8765/v1/call/demo.v1.Greeter/greet -d '{"name": "Alice"}'

# Server stream: one {"item": …} line per item, then {"end": …}
curl -N http://127.0.0.1:8765/v1/call/demo.v1.Greeter/countdown -d '{"start": 3}'

# Discover services: the Registry, or an OpenAPI 3.1 document for Swagger UI and client generators
curl http://127.0.0.1:8765/v1/services
curl http://127.0.0.1:8765/v1/openapi.json
```

From a browser (or Deno, Bun, an edge runtime), use the TypeScript package's `meshcall/web` entry: `new ChatClient(new MeshCallHttpClient("https://router.example", { token }))` gives a generated client the same typed calls, errors, local validation and subscription resume over `fetch` (start the Router with `--http-cors-origin https://app.example` for another origin).

Unary calls and server streams work fully, client streams take an NDJSON body, and duplex calls (a restricted shape of last resort; see [Duplex calls](docs/protocol.md#duplex-calls)) stay WebSocket-only. Accounts, tokens, timeouts (`MeshCall-Timeout-Ms`), idempotency keys (`Idempotency-Key`) and metadata (`MeshCall-Meta-*`) work as headers; see [HTTP calls](docs/protocol.md#http-calls).

---

## Go

Go services and clients use the Go runtime, [`meshcall-go`](meshcall-go/README.md): the same calls, errors, deadlines, streams, subscriptions and Router features, checked by the same conformance suite. `meshcall generate service.meshcall.json --language go --output internal/chatclient` writes a typed Go client for a service written in any of the three languages, and `meshcall.WriteContract` exports a Go service's contract for Python and TypeScript clients.

---

## Teach Your AI Coding Assistant

If you build with an AI coding assistant, run `meshcall init` once in your project root:

```bash
uv run meshcall init
```

This installs development instructions for AI assistants (`.agents/skills/meshcall/SKILL.md` and `AGENTS.md`), showing your assistant how to define services and methods, generate clients, and use `meshcall router inspect` and `meshcall call` to explore and verify running endpoints. See the [User Guide](docs/guide.md) for tool-specific flags (such as Claude Code or Cursor).

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
| **[Go SDK](meshcall-go/README.md)** | Use the Go runtime and generated Go clients | — |
| **[Router & CLI](meshcall-router/README.md)** | Deploy the standalone Router binary, configure CLI flags, and run with Docker | [Router 与 CLI 部署指南](meshcall-router/README.zh-CN.md) |
| **[Router & Auth Guide](docs/router.md)** | Configure multi-instance routing, accounts, and access tokens | [Router 架构与鉴权](docs/zh-CN/router.md) |
| **[Runtime Architecture](docs/architecture.md)** | Understand the internal design of the client and server runtimes | [运行时架构设计](docs/zh-CN/architecture.md) |
| **[Protocol Specification](docs/protocol.md)** | Read the `meshcall/1` wire protocol, timeout calculation, and error codes | [协议规范 (`meshcall/1`)](docs/zh-CN/protocol.md) |
| **[Contributing](CONTRIBUTING.md)** | Run local checks, conformance tests, and release workflows | [贡献指南](CONTRIBUTING.zh-CN.md) |

---

## Design Philosophy

**Focus on your procedure.**
