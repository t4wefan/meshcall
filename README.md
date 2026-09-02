# MeshCall

MeshCall is an asynchronous, typed RPC framework for Python and TypeScript.
Pydantic models or explicit TypeScript JSON Schemas define payload contracts,
generated client packages provide a static API, and a shared logical protocol
supports direct WebSocket connections and routed service instances.

This repository is a monorepo. The Python runtime package lives in
`meshcall-py`, the TypeScript runtime package lives in `meshcall-ts`, the
standalone real-world example lives in `best-practice`, runnable examples live
in `demo`, and design/protocol documentation lives in `docs`.

## Repository layout

```text
meshcall/
├── meshcall-py/   # Python package, uv project, and Python tests
├── meshcall-ts/   # TypeScript package, Yarn project, and TypeScript tests
├── best-practice/ # Standalone Python server + TypeScript interactive CLI
├── demo/          # Independent demo projects
│   ├── quickstart/ # uv project: uv run demo
│   ├── py2ts/      # uv + Yarn: Python service -> TypeScript client
│   ├── router/    # uv project: uv run demo
│   ├── showcase/  # uv project: uv run demo
│   └── ts2py/      # uv + Yarn: TypeScript service -> Python client
├── docs/          # Architecture and protocol documentation
└── README.md      # Monorepo overview
```

The current implementation includes:

- Python 3.11+ and `asyncio`;
- Pydantic v2 service contracts and JSON Schema extraction;
- portable JSON contracts and Python/TypeScript client generation;
- complete Python uv packages and TypeScript Yarn packages by default;
- optional self-contained single-file clients with dependency instructions;
- unary, server-streaming, and client-streaming calls;
- WebSocket Direct and WebSocket Router drivers over TCP or Unix sockets;
- cancellation, deadlines, half-close, per-direction flow control, and fair
  per-call sending;
- round-robin, least-inflight, random, sticky, and disabled balancing;
- immutable `call_id` to service-instance routing and disconnect cleanup;
- a dedicated Python service worker loop, isolated from RPC transport and
  deadline handling.

The `best-practice/` application exercises Python-to-TypeScript unary,
client-streaming, and server-streaming calls. Typed notifications and the Tags
DSL remain future milestones.

## Development

```bash
uv sync --directory meshcall-py --project .
uv run --directory meshcall-py --project . ruff check src tests ../demo
uv run --directory meshcall-py --project . pyright src tests ../demo
uv run --directory meshcall-py --project . pytest -q

uv run --project demo/quickstart demo
uv run --project demo/py2ts demo
uv run --project demo/showcase demo
uv run --project demo/router demo
uv run --project demo/ts2py demo

yarn --cwd meshcall-ts install --frozen-lockfile
yarn --cwd meshcall-ts run check
yarn --cwd meshcall-ts test

MESHCALL_RUN_INTEROP=1 uv run --directory meshcall-py --project . \
  pytest -q tests/test_multilang_interop.py
```

Run these commands from the monorepo root. The normal test suite is headless
and doesn't require an external service. Cross-language tests start short-lived
local Python and Node servers and are opt-in through
`MESHCALL_RUN_INTEROP=1`.

## Define a service

```python
from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import method, service


class CountItem(BaseModel):
    value: int


@service(name="example.v1.CounterService")
class CounterService:
    @method.server_stream()
    async def count(self, stop: int) -> AsyncIterator[CountItem]:
        for value in range(stop):
            yield CountItem(value=value)
```

Unary is the default, so a unary method can use `@method()` directly. Use
`method.server_stream()` or `method.client_stream()` for the recommended
streaming shapes. Service methods are instance methods by default; append
`.static` when a method does not need a service instance. The legacy
`@staticmethod` plus `@method()` form remains supported. Duplex is currently an
experimental capability and is intentionally omitted from the recommended API
examples.

For a method such as `count(self, stop: int)`, MeshCall creates an internal
Pydantic request model with a `stop` field. The wire payload is still one JSON
object, while generated clients keep the expanded call shape:
`client.count(10)`. A method that takes one Pydantic model, such as
`greet(self, request: GreetingRequest)`, remains available and generates the
object-style call `client.greet(request)`.

Request, response, and stream item types are Pydantic models. MeshCall infers
the RPC shape from the signature unless an explicit stream decorator is used.

## Complete runnable showcase

The complete showcase is an independent uv project in `demo/showcase/`. It
starts a short-lived local WebSocket server and calls it through a generated
client. It demonstrates the recommended instance-method API, expanded
parameters, a request-model method, Pydantic payloads, unary RPC, server
streaming, and client streaming.

From the showcase directory:

```bash
cd demo/showcase
uv run demo
```

The checked-in client is intentionally a generated single-file client so the
example can run immediately. Regenerate it from `demo/showcase/` with:

```bash
uv run meshcall generate \
  showcase_demo.service:ShowcaseService \
  --single-file \
  --output src/showcase_demo/generated_client.py
```

For the normal production workflow, generate the default complete Python uv
client package instead, still from `demo/showcase/`:

```bash
uv run meshcall generate \
  showcase_demo.service:ShowcaseService \
  --output generated/showcase-client
```

The same service contract can also generate a complete TypeScript package by
adding `--language typescript` and using a separate output directory.

## Best-practice application

`best-practice/` is the recommended small real-world shape: the Python RPC
server and the TypeScript interactive CLI are separate processes. The server
keeps sessions in memory, returns deterministic fake LLM chunks, and waits
briefly between chunks so the stream is visible in a terminal.

Start them in two terminals:

```bash
cd best-practice
uv run server
```

```bash
cd best-practice/ts-client
yarn install --frozen-lockfile
yarn build
MESHCALL_SERVER_URL=ws://127.0.0.1:8765 yarn cli
```

The CLI supports ordinary prompts plus `/new`, `/sessions`, `/tokens TEXT`,
`/help`, and `/quit`. `assemble_prompt` demonstrates client streaming and
`stream_chat` demonstrates server streaming.

## Cross-language demos

The compact cross-language demos are complete projects that contain both sides
of their round trip. They remain intentionally unary; `best-practice/` is the
streaming Python-to-TypeScript example.

Python service to TypeScript client:

```bash
cd demo/py2ts
uv run demo
```

The Python service is in `src/py2ts_demo/service.py`, and the generated
TypeScript Yarn package is in `ts-client/`.

TypeScript service to Python client:

```bash
cd demo/ts2py
uv run demo
```

The TypeScript Yarn service is in `ts-server/`, its portable contract is
`ts-server/service.meshcall.json`, and the generated Python uv package is in
`py-client/`.

## Generate a complete client package

Package generation is the default. `--output` names a directory.

```bash
uv run --project demo/quickstart meshcall generate \
  quickstart_demo.service:CounterService \
  --output generated/counter-client
```

The Python output is an installable uv package:

```text
generated/counter-client/
├── pyproject.toml
├── README.md
└── src/meshcall_example_v1_counterservice_client/
    ├── __init__.py
    ├── client.py
    ├── models.py
    └── py.typed
```

It supports the recommended unary and streaming RPC shapes and can be checked
with `uv build`. Every generated method accepts an optional `timeout=` keyword.

For a unary Python service, generate a TypeScript Yarn package with:

```bash
uv run --project meshcall-py meshcall generate \
  your_app.service:GreetingService \
  --language typescript \
  --output generated/greeting-client
```

The output contains `package.json`, `tsconfig.json`, and separate
`src/models.ts`, `src/client.ts`, and `src/index.ts` modules.

## Generate one self-contained file

Single-file output is opt-in:

```bash
uv run --project demo/quickstart meshcall generate \
  quickstart_demo.service:CounterService \
  --output generated/counter_client.py \
  --single-file
```

The Python file contains its Pydantic model classes at module scope as well as
the generated client. TypeScript single files similarly contain all generated
interfaces and the client. Their header comments list the external packages to
install (`meshcall` plus `pydantic` for Python, or `@meshcall/runtime` for
TypeScript).

## Portable contracts and cross-language generation

Export a Python contract and generate a package in either language:

```bash
uv run --project meshcall-py meshcall export \
  your_app.service:GreetingService \
  --output generated/greeting.meshcall.json

uv run --project meshcall-py meshcall generate-contract \
  generated/greeting.meshcall.json \
  --language typescript \
  --output generated/greeting-ts-client
```

TypeScript cannot reflect interfaces after compilation, so a TypeScript service
attaches JSON Schema explicitly and exports the same portable contract:

```typescript
import {
  defineService,
  defineType,
  unaryMethod,
  writeContract,
} from "@meshcall/runtime";

interface AddRequest { left: number; right: number }
interface AddResult { total: number }

const request = defineType<AddRequest>("AddRequest", {
  type: "object",
  properties: {
    left: { type: "number" },
    right: { type: "number" },
  },
  required: ["left", "right"],
});

const result = defineType<AddResult>("AddResult", {
  type: "object",
  properties: { total: { type: "number" } },
  required: ["total"],
});

const service = defineService({
  name: "example.v1.MathService",
  sourceModule: "math-service",
  sourceQualname: "MathService",
  methods: {
    add: unaryMethod({
      request,
      response: result,
      handler: ({ left, right }) => ({ total: left + right }),
    }),
  },
});

await writeContract([service], "generated/math.meshcall.json");
```

Then generate the Python uv package:

```bash
uv run --project meshcall-py meshcall generate-contract \
  generated/math.meshcall.json \
  --language python \
  --output generated/math-py-client
```

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
    async for item in client.count(10, timeout=5):
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
- `close_send()` half-closes the local sending direction.
- `cancel()` cancels the complete call.
- Breaking out of a custom stream iterator does not implicitly cancel it; call
  `await stream.cancel()` or use the stream as an async context manager.

## Current limits

- JSON is the only physical encoding.
- Stream credit is item-based; encoded frames have a byte-size limit.
- Active calls aren't resumed after disconnect and don't migrate between service
  instances.
- Duplex is an experimental protocol/runtime capability. It is intentionally
  excluded from the recommended decorators, showcase, and cross-language
  target; its service and client API may change.
- TypeScript cross-language generation/runtime supports unary,
  server-streaming, and client-streaming methods. Python-to-Python package
  generation contains experimental duplex support in addition to the
  recommended unary and one-way streaming shapes.
- The TypeScript client can use a compatible Direct or Router endpoint, but the
  TypeScript server currently exposes a Direct WebSocket listener only; Router
  service-instance registration is still Python-only.
- TypeScript JSON Schemas currently drive contract export and client generation;
  runtime request/response schema validation is not implemented yet.
- Python handlers run on a dedicated service worker loop so blocking business
  code cannot block the RPC loop. TypeScript handlers are not worker-thread
  isolated yet and must avoid synchronously blocking Node's event loop.
- Router authentication and authorization aren't implemented yet; don't expose
  an untrusted Router endpoint to the public internet.
- Typed notifications, dynamic subscriptions, middleware, gRPC, and HTTP aren't
  part of the current implementation.

See [the protocol specification](docs/protocol.md) and
[the implementation architecture](docs/architecture.md) for details.
