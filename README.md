# MeshCall

MeshCall is an asynchronous, typed RPC framework for Python services. Pydantic
models define payload contracts, generated clients provide a static API, and a
shared logical protocol supports direct WebSocket connections and routed service
instances.

The current implementation includes:

- Python 3.11+ and `asyncio`;
- Pydantic v2 service contracts and JSON Schema extraction;
- static Python client generation;
- unary, server-streaming, client-streaming, and duplex calls;
- WebSocket Direct and WebSocket Router drivers over TCP or Unix sockets;
- cancellation, deadlines, half-close, per-direction flow control, and fair
  per-call sending;
- round-robin, least-inflight, random, sticky, and disabled balancing;
- immutable `call_id` to service-instance routing and disconnect cleanup.

Typed notifications and the Tags DSL are the next milestone.

## Development

```bash
uv sync
uv run ruff check src tests
uv run pyright src tests
uv run pytest -q
```

The test suite is headless and doesn't require an external service.

## Define a service

```python
from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import method, service


class CountRequest(BaseModel):
    stop: int


class CountItem(BaseModel):
    value: int


@service(name="example.v1.CounterService")
class CounterService:
    @method.server_stream()
    async def count(self, request: CountRequest) -> AsyncIterator[CountItem]:
        for value in range(request.stop):
            yield CountItem(value=value)
```

Service methods are asynchronous instance methods by default. Use
`method.unary()`, `server_stream()`, `client_stream()`, or `duplex()` to state
the RPC shape. Append `.static` when a method doesn't need a service instance.
Pass `balance=...` to the shape when a method needs to override the service's
load-balancing policy. The signature is independently inferred and checked
against the declaration.

## Generate a client

```bash
uv run meshcall generate \
  examples.service:CounterService \
  --output examples/generated_client.py
```

Generated methods retain concrete request, response, and stream item types. Every
generated method accepts an optional `timeout=` keyword.

## WebSocket Direct

TCP server:

```python
driver = WebSocketDirectServerDriver(host="127.0.0.1", port=8765)
server = RpcServer(services=[CounterService], driver=driver)
await server.start()
```

Passing a service class constructs one instance with no arguments when the
server starts. For constructor arguments or dependency injection, pass an
already constructed instance instead: `services=[CounterService(...)]`. The
server reuses that service instance across calls.

TCP client:

```python
client = CounterServiceClient(WebSocketClientDriver("ws://127.0.0.1:8765"))
async with client:
    async for item in client.count(CountRequest(stop=10), timeout=5):
        print(item.value)
```

For local IPC, configure both sides with the same short Unix socket path:

```python
WebSocketDirectServerDriver(unix_path="/tmp/meshcall.sock")
WebSocketClientDriver(unix_path="/tmp/meshcall.sock")
```

## WebSocket Router

Run a Router listener:

```python
router = WebSocketRouter(host="127.0.0.1", port=8765)
await router.start()
```

Connect one or more service instances:

```python
driver = WebSocketRouterServerDriver(
    "ws://127.0.0.1:8765",
    instance_id="counter-1",
)
server = RpcServer(services=[CounterService], driver=driver)
await server.start()
```

Clients use the normal `WebSocketClientDriver` and connect to the Router URI. A
stream is balanced only at `call.open`; all later items, windows, half-closes,
errors, and cancellation frames remain pinned to the selected instance.

Router listeners and service/client connections also accept `unix_path=`.

## Stream API

- A return type of `AsyncIterator[T]` creates a server stream.
- An `RpcInputStream[T]` parameter creates a client stream.
- An `RpcDuplex[InputT, OutputT]` parameter creates a duplex stream.
- `close_send()` half-closes the local sending direction.
- `cancel()` cancels the complete call.
- Breaking out of a custom stream iterator does not implicitly cancel it; call
  `await stream.cancel()` or use the stream as an async context manager.

## Current limits

- JSON is the only physical encoding.
- Stream credit is item-based; encoded frames have a byte-size limit.
- Active calls aren't resumed after disconnect and don't migrate between service
  instances.
- Router authentication and authorization aren't implemented yet; don't expose
  an untrusted Router endpoint to the public internet.
- Typed notifications, dynamic subscriptions, middleware, gRPC, and HTTP aren't
  part of the current implementation.

See [the protocol specification](docs/protocol.md) and
[the implementation architecture](docs/architecture.md) for details.
