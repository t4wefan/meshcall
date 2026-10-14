# MeshCall

MeshCall is an asynchronous, typed RPC framework for Python and TypeScript.
Pydantic models or explicit TypeScript JSON Schemas define payload contracts,
generated client packages provide a static API, and a shared logical protocol
supports direct WebSocket connections and routed service instances.

This repository is a monorepo. The Python runtime package lives in
`meshcall-py`, the TypeScript runtime package lives in `meshcall-ts`, the
standalone real-world example lives in `best-practice`, runnable examples live
in `demo`, and design/protocol documentation lives in `docs`. The shared Go
Router and `meshcall` CLI live in `meshcall-router`.

## Repository layout

```text
meshcall/
├── meshcall-py/     # Python runtime package, uv project, and Python tests
├── meshcall-ts/     # TypeScript runtime package, Yarn project, and TS tests
├── meshcall-router/ # Standalone Go Router, meshcall CLI, and conformance suite
├── best-practice/   # Standalone Python server + TypeScript interactive CLI
├── demo/            # Independent demo projects
│   ├── quickstart/  # uv project: uv run demo
│   ├── py2ts/       # uv + Yarn: Python service -> TypeScript client
│   ├── router/      # uv project: uv run demo
│   ├── showcase/    # uv project: uv run demo
│   └── ts2py/       # uv + Yarn: TypeScript service -> Python client
├── skills/meshcall/ # First-party agent skill installed by `meshcall init`
├── docs/            # Architecture, Router, and protocol documentation
└── README.md        # Monorepo overview
```

The current implementation includes:

- Python 3.11+ and `asyncio`;
- Pydantic v2 service contracts and JSON Schema extraction;
- portable JSON contracts and Python/TypeScript client generation;
- complete Python uv packages and TypeScript Yarn packages by default;
- optional self-contained single-file clients with dependency instructions;
- unary, server-streaming, and client-streaming clients and services in both languages;
- WebSocket Direct and WebSocket Router drivers over TCP or Unix sockets;
- cancellation, deadlines, half-close, per-direction flow control, and fair
  per-call sending;
- round-robin, least-inflight, random, sticky, and disabled balancing, plus
  instance-targeted calls (`targeted=True` / `instance=`);
- immutable `call_id` to service-instance routing, graceful draining, circuit
  breaking, idempotency deduplication, and disconnect cleanup;
- a dedicated Python service worker loop, isolated from RPC transport and
  deadline handling;
- built-in Python server access logs with method, status, duration, and
  Loguru-style service logger injection;
- TypeScript JSON Schema payload validation, call-bound loggers, and terminal access logs;
- Python and TypeScript launchers for one standalone Go Router;
- Router account authentication, built-in Registry inspection RPC, and built-in
  AuthService RPC for issuing/revoking temporary tokens restricted by service,
  method and registration scope;
- relative call timeouts enforced by server, Router and client, stream idle
  timeouts, call metadata and response trailers, protocol keepalive, and
  automatic reconnection with backoff (clients and Router-mode servers);
- client and server interceptor chains for cross-cutting concerns such as
  tracing, auth and retries.

The `best-practice/` application exercises Python-to-TypeScript unary,
client-streaming, and server-streaming calls. Typed notifications and the Tags
DSL remain future milestones.

## Install

```bash
uv add meshcall            # Python runtime + bundled Go Router & CLI
yarn add meshcall          # TypeScript runtime + bundled Go Router & CLI (or: npm install meshcall)
```

There is one `meshcall` command, written in Go and released together with the
Router. Both binaries ship inside the SDK packages for every platform Go
builds for (Linux glibc and musl on x86_64, aarch64, i686, armv7l, riscv64,
ppc64le and s390x; macOS and Windows on amd64 and arm64, Windows i686;
FreeBSD amd64 and arm64 via npm): PyPI publishes a platform wheel per target and npm installs a matching
`meshcall-router-<os>-<cpu>` package as an optional dependency, so
`uv run meshcall …` and `npx meshcall …` run the bundled binary with no
download. On a platform without a prebuilt binary they fall back to fetching
the release matching the SDK (verified against the release checksums) into
the per-user cache; `uv run meshcall router install` does that explicitly.
`meshcall generate` produces typed clients for every
language, `meshcall router inspect` shows what is registered and
`meshcall call svc.method '{...}'` calls it — no generated code needed.

If coding agents work on your project, run `meshcall init` once: it installs the first-party
skill at `.claude/skills/meshcall/SKILL.md` and adds a short MeshCall section
to `AGENTS.md`, so agents explore with `inspect`, call with `call`, read
`violations` and write services under strict mode instead of guessing. Set
`MESHCALL_ROUTER_BINARY` to use a binary you built or deployed yourself. All
three packages are released together from one `vX.Y.Z` tag; see
[CHANGELOG.md](CHANGELOG.md).

## Define a service

```python
from collections.abc import AsyncIterator

from pydantic import BaseModel

from meshcall import RpcLogger, method, service


class CountItem(BaseModel):
    value: int


@service("example.v1.CounterService")
class CounterService:
    """Stream sequential integer counts."""

    @method.server_stream(read_only=True)
    async def count(
        self,
        stop: int,
        logger: RpcLogger,
    ) -> AsyncIterator[CountItem]:
        """Yield integers from 0 up to stop - 1."""
        logger.debug("counting to {}", stop)
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

**Parameters are the request.** For `count(self, stop: int)`, MeshCall builds
one request model with a `stop` field; the wire payload is one JSON object and
generated clients expose the same parameters: `client.count(10)`. A parameter
typed as a Pydantic model is just one field of that request
(`welcome(self, profile: WelcomeRequest)` → `client.welcome(profile)`). There is
no separate "request object" style.

Response and stream item types are Pydantic models. MeshCall infers the RPC
shape from the signature unless an explicit stream decorator is used.

Service names carry a version segment and are normalised: `@service("chat")`
registers `chat.v0`; `chat.v1`, `demo.v1.Greeting` and `chat.v1.2.0` are all
valid and routed by exact name. Class and method docstrings become the
contract's `description`; `@method(read_only=True)`, `idempotent=True`,
`destructive=True` and `deprecated="replacement"` record method semantics that
tooling, retry policies and LLM tool exposure rely on. Set `MESHCALL_STRICT=1`
during development or CI to turn advisory contract warnings into errors with
actionable hints.

Every service instance has an id (`instance_id=` on the server driver, or
generated). Any call can address one instance with `instance="worker-3"`;
`@service("gpu.v1.Worker", targeted=True)` declares that callers *must*
(instances hold distinct state: a session, a loaded model, an actor), and
generated clients then make `instance` a required keyword, so forgetting it
is a type error rather than a runtime `instance_required`.

## Explore and call from the CLI

When services are registered with a Router, you can inspect the live mesh and
invoke methods directly from the terminal (`uv run meshcall …` in a Python
project, `npx meshcall …` in a Node project, or `meshcall …` if on `PATH`):

```bash
# List registered services, live instances, health, and method signatures
uv run meshcall router inspect --router ws://127.0.0.1:8765

# Print the full portable contract for one service as JSON
uv run meshcall router inspect example.v1.CounterService --json

# Call a unary or server-streaming method (streams print one JSON line per item)
uv run meshcall call example.v1.CounterService.count '{"stop": 5}'

# Pipe JSON lines into a client-streaming method
printf '{"value":1}\n{"value":2}\n' | uv run meshcall call demo.v1.Sum.sum '{"offset":10}' --stdin
```

`meshcall call` supports `--json`, `--timeout 10s`, `--idle-timeout 5s`,
`--metadata k=v`, `--idempotency-key K`, `--instance ID`, and `--auth user:pass`
/ `--token T`. If a call fails validation or targets an unknown method/instance,
the CLI queries the Router's Registry and prints a contract-aware hint.

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
with `uv build`. Every generated method accepts optional `timeout=`,
`idle_timeout=` (seconds), `metadata=`, `idempotency_key=`, and `instance=`
keywords. Service methods can declare a `ctx: RpcContext` parameter to read the
caller's metadata, check `ctx.remaining()`, and attach trailers with
`ctx.set_trailer()`.

For a Python service using the recommended call shapes, generate a TypeScript Yarn package with:

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
interfaces, schemas, and the client. Their header comments list the external
packages to install (`meshcall` plus `pydantic` for Python, or `meshcall` for
TypeScript).

## Portable contracts and cross-language generation

Export a Python contract and generate a package in either language from it
(`meshcall generate` accepts a contract document wherever it accepts a
`module:Class`; `--service` picks one service of a multi-service document):

```bash
uv run --project meshcall-py meshcall export \
  your_app.service:GreetingService \
  --output generated/greeting.meshcall.json

meshcall generate \
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
} from "meshcall";

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
  description: "Arithmetic operations.",
  sourceModule: "math-service",
  sourceQualname: "MathService",
  methods: {
    add: unaryMethod({
      description: "Return the sum of left and right.",
      readOnly: true,
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
npx meshcall generate \
  generated/math.meshcall.json \
  --language python \
  --output generated/math-py-client
```

## WebSocket Router

The Go Router binary (`meshcall-router`) is bundled inside the Python and
TypeScript SDK packages, so starting a Router from either language requires no Go
compiler or separate download. Set `MESHCALL_ROUTER_BINARY` only when you want
to override the bundled binary with one you built or deployed yourself. Account
auth, per-service scopes and the built-in AuthService are explained in
[Router architecture](docs/router.md).

Run a Router listener from Python (or `new WebSocketRouter({ port: 8765 })` in TypeScript):

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

## WebSocket Direct

TCP server:

```python
driver = WebSocketDirectServerDriver(host="127.0.0.1", port=8765)
server = RpcServer(
    services=[CounterService],
    driver=driver,
    access_log=True,
    log_level="INFO",
    colorize=False,
)
await server.start()
```

`access_log` is enabled by default. After each call reaches a terminal result
or error, the server emits a Loguru-style access entry containing
`service.method`, `status`, `duration_ms`, and `call_id`. Set
`access_log=False` to turn off these per-call entries while keeping other
application and exception logs. `log_level` chooses the level used for access
entries, and `colorize` enables the Loguru color tags for that entry. The
default logger is Loguru's global logger; a compatible logger can be supplied
with `RpcServer(..., logger=my_logger)`.

RPC methods may reserve a parameter named `logger` (typed as `RpcLogger`) for
server-side logging. It is injected per call, is not part of the wire request,
and therefore does not appear in the generated client method:

```python
@method(read_only=True)
async def greet(self, name: str, logger: RpcLogger) -> Greeting:
    """Return a greeting for name."""
    logger.info("greeting {}", name)
    return Greeting(...)
```

The injected logger is bound with the service, method, and `call_id`, and its
usage follows Loguru's normal `debug`, `info`, `warning`, `error`, and
`exception` methods.

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

## Stream API

- A return type of `AsyncIterator[T]` creates a server stream.
- An `RpcInputStream[T]` parameter creates a client stream.
- `close_send()` half-closes the local sending direction.
- `cancel()` cancels the complete call.
- Breaking out of a custom stream iterator does not implicitly cancel it; call
  `await stream.cancel()` or use the stream as an async context manager.

The last rule applies to Python streams. TypeScript server streams implement
iterator cleanup, so leaving a `for await` loop cancels the call automatically.

## Complete runnable showcase

The complete showcase is an independent uv project in `demo/showcase/`. It
starts a short-lived local WebSocket server and calls it through a generated
client. It demonstrates the recommended instance-method API, parameters as the
request (including a model-typed parameter), Pydantic payloads, unary RPC,
server streaming, and client streaming.

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

`uv run meshcall` is the Go CLI; a `module:Class` source is exported through
the project's Python interpreter, so run it from the project. For the normal
production workflow, generate the default complete Python uv client package
instead, still from `demo/showcase/`:

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
of their round trip. `py2ts/` is a minimal unary example; `ts2py/` demonstrates
unary and both streaming shapes from a TypeScript service to a Python client.
`best-practice/` is the streaming Python-to-TypeScript application.

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

## Development

`scripts/check.sh` runs everything CI runs (see `AGENTS.md` and
`CONTRIBUTING.md`). The individual steps:

```bash
go -C meshcall-router build -trimpath -o bin/meshcall-router ./cmd/meshcall-router
go -C meshcall-router test -race ./...

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
local Go, Python and Node processes and are opt-in through
`MESHCALL_RUN_INTEROP=1`.

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
- Both runtimes support Direct listeners and service-instance registration with
  the same Go Router, over TCP or Unix sockets. See
  [Router architecture](docs/router.md) and the
  [TypeScript service API](meshcall-ts/README.md).
- TypeScript type contracts validate requests, responses, and both stream
  directions at runtime. Python and TypeScript retain their native validation
  behavior; for example TypeScript does not coerce strings into numbers.
- Python handlers run on a dedicated service worker loop so blocking business
  code cannot block the RPC loop. TypeScript handlers are not worker-thread
  isolated yet and must avoid synchronously blocking Node's event loop.
- Go Router accounts support username/password auth, service/method ACLs and
  scoped temporary tokens through built-in RPC. Auth files load at startup;
  tokens are in memory and expire on restart. Network deployments use a TLS
  reverse proxy; HA, account hot reload, and browser credential transport are
  not implemented.
- Typed notifications, dynamic subscriptions, middleware, gRPC, and HTTP aren't
  part of the current implementation.

See [the protocol specification](docs/protocol.md) and
[the implementation architecture](docs/architecture.md) for details.
