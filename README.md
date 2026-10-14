<p align="center">
  <img src="docs/assets/wordmark.svg" alt="MeshCall" width="300">
</p>

<p align="center">
  <strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a>
</p>

# MeshCall

**Typed RPC and streaming communication between Python and Node.js / TypeScript services.**

MeshCall lets you expose async functions as remote services and call them across processes or languages with full type validation.

- **Functions as services** — function parameters and return models define the request and response schema automatically
- **Streaming results** — `yield` items from a generator to stream data; iterate with `async for` or `for await` on the caller side
- **Generated typed clients** — generate Python or TypeScript clients in one command without writing `.proto` files
- **Managed communication** — the framework handles connection management, request forwarding, timeouts, cancellation, and flow control so your handlers only deal with business logic
- **Terminal debugging** — inspect registered services and call methods directly from the command line

---

## How It Connects

MeshCall uses a **Router** as the relay process between services:

```text
Caller (Client)  --->  Router (ws://127.0.0.1:8765)  <---  Service (Server)
```

- **Services** connect to the Router and register the methods they provide.
- **Callers** connect to the same Router to invoke `service.method` by name.
- Because every participant connects outbound to the Router, **we recommend always using the Router**: a single Python or Node.js process can register its own services and act as a client calling other services at the same time, without managing per-service ports or peer addresses.
- The `meshcall-router` and `meshcall` CLI binaries are bundled inside the Python and npm packages—no Go installation is required.

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

### 1. Define and start a service

Save the following code as `server.py`:

```python
import asyncio
from pydantic import BaseModel
from meshcall import RpcServer, WebSocketRouter, method, service
from meshcall.drivers import WebSocketRouterServerDriver


class Reply(BaseModel):
    message: str


@service("demo.v1.Greeter")
class Greeter:
    """Greeting service."""

    @method(read_only=True)
    async def greet(self, name: str) -> Reply:
        """Return a greeting for name."""
        return Reply(message=f"hello, {name}")


async def main() -> None:
    async with WebSocketRouter(host="127.0.0.1", port=8765):
        server = RpcServer(
            services=[Greeter()],
            driver=WebSocketRouterServerDriver(
                "ws://127.0.0.1:8765",
                instance_id="greeter-1",
            ),
        )
        await server.start()
        print("Router and Greeter running at ws://127.0.0.1:8765")
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

Key parts of this file:

- `demo.v1.Greeter` is the service name callers use, including a version tag (`v1`).
- `read_only=True` declares that this method does not modify business state.
- The parameter (`name: str`) and return model (`Reply`) together describe the request and response structure, referred to as the **service contract**.
- `WebSocketRouter` starts a local Router process on `ws://127.0.0.1:8765`, and `RpcServer` registers this running service instance (`instance_id="greeter-1"`).

Start the service in a terminal:

```bash
uv run python server.py
```

### 2. Generate a typed client

Keep `server.py` running. In a second terminal, generate a Python client from the `Greeter` class in `server.py` (`<module>:<Class>`):

```bash
uv run meshcall generate server:Greeter --single-file --output greeter_client.py
```

### 3. Call the service

Save the following code as `client.py` in the same directory:

```python
import asyncio
from meshcall.drivers import WebSocketClientDriver
from greeter_client import GreeterClient


async def main() -> None:
    driver = WebSocketClientDriver("ws://127.0.0.1:8765")
    async with GreeterClient(driver) as client:
        reply = await client.greet("Alice")
        print(reply.message)


if __name__ == "__main__":
    asyncio.run(main())
```

Run the client:

```bash
uv run python client.py
# Output: hello, Alice
```

---

## Streaming Results

To return multiple items over time, add a `@method.server_stream` generator to `Greeter` in `server.py`:

```python
from collections.abc import AsyncIterator


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
        """Stream numbers from start down to 1."""
        for n in range(start, 0, -1):
            yield Tick(step=n)
```

`countdown` yields results one by one so the caller can process items as they arrive; the framework automatically paces the sender to match the consumer's speed.

Restart `server.py`, regenerate `greeter_client.py`, and iterate over the stream in `client.py`:

```python
async for tick in client.countdown(3):
    print(tick.step)  # 3, 2, 1
```

### Calling from TypeScript

You can generate a TypeScript client package from the same `server.py`:

```bash
# 1. Generate a TypeScript package into ./greeter-ts
uv run meshcall generate server:Greeter --language typescript --output ./greeter-ts

# 2. Build the generated package and add it to your TypeScript project
yarn --cwd ./greeter-ts install
yarn --cwd ./greeter-ts build
yarn add ./greeter-ts
```

Then call the running Python service from TypeScript:

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

## Terminal Debugging & AI Support

### Inspect and call from the terminal

While `server.py` is running, open another terminal to inspect registered services or invoke methods directly (defaults to `ws://127.0.0.1:8765`, or pass `--router ws://host:port`):

```bash
# List registered services, online instances, and method signatures
uv run meshcall router inspect

# Call a unary or streaming method with a JSON object
uv run meshcall call demo.v1.Greeter.greet '{"name": "Alice"}'
uv run meshcall call demo.v1.Greeter.countdown '{"start": 3}'
```

If a field name or type is wrong, `meshcall call` checks the service contract and prints a fix hint.

### AI coding assistant instructions

To install MeshCall usage instructions for AI coding assistants in your project, run:

```bash
uv run meshcall init
```

This writes `.agents/skills/meshcall/SKILL.md` and updates `AGENTS.md` with reference documentation for CLI inspection, service definitions, and client generation. For tool-specific configuration files (`--agent claude|cursor|copilot|windsurf|all`), see the [User Guide](docs/guide.md).

---

## Runnable Examples

To run the complete example projects, clone the repository first:

```bash
git clone https://github.com/t4wefan/meshcall.git
cd meshcall

uv run --project demo/quickstart demo   # Minimal unary & streaming in Python
uv run --project demo/showcase demo     # Full Python feature walkthrough
uv run --project demo/router demo       # Multi-instance load balancing
uv run --project demo/py2ts demo        # Python service -> TypeScript client
uv run --project demo/ts2py demo        # TypeScript service -> Python client
```

---

## Documentation

| Goal | Document | 中文版 |
| --- | --- | --- |
| **Learn common patterns** (context, client streams, timeouts, retries, load balancing, auth) | **[User Guide & Cookbook](docs/guide.md)** | **[快速上手与实战指南](docs/zh-CN/guide.md)** |
| **Use the Python SDK** | **[Python SDK](meshcall-py/README.md)** | [Python SDK 指南](meshcall-py/README.zh-CN.md) |
| **Use the TypeScript SDK** | **[TypeScript SDK](meshcall-ts/README.md)** | [TypeScript SDK 指南](meshcall-ts/README.zh-CN.md) |
| **Deploy the Router & configure auth** | **[Go Router & CLI](meshcall-router/README.md)** / **[Router Guide](docs/router.md)** | [Go Router 部署指南](meshcall-router/README.zh-CN.md) / [Router 架构与鉴权](docs/zh-CN/router.md) |
| **Understand runtime & protocol internals** | **[Runtime Architecture](docs/architecture.md)** / **[Protocol Spec](docs/protocol.md)** | [运行时架构设计](docs/zh-CN/architecture.md) / [协议规范](docs/zh-CN/protocol.md) |
| **Contribute to MeshCall** | **[Contributing](CONTRIBUTING.md)** | [贡献指南](CONTRIBUTING.zh-CN.md) |
