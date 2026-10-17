# MeshCall User Guide & Cookbook

**English** | [简体中文](zh-CN/guide.md)

This guide is designed for both **human engineers** and **AI coding agents**. Each section explains **when and why** to use a MeshCall capability, followed by complete, copy-pasteable recipes in **Python** and **TypeScript**, and in **Go** (`meshcall-go`) where the Go runtime differs.

MeshCall is structured around a clean **4-layer API** across Python, TypeScript, and Go:

| Layer | Responsibility | Python | TypeScript | Go |
| --- | --- | --- | --- | --- |
| **Layer 1: Service & Contract** | Pure business logic and schemas (`@service`, `@query`, `@mutates`, `@stream`, `@dependency`, `meta`) | `@service`, `@query`, `@mutates`, `@stream` | `@service`, `@query`, `@mutates`, `@stream` | `meshcall.ServiceMeta` + `meshcall.MethodMeta` (or `NewService`) |
| **Layer 2: Router (`Router` / `LocalRouter`)** | Where the routing plane lives (`Router`), or an explicitly managed local `meshcall-router` child process (`LocalRouter`) | `Router(url=…)`, `LocalRouter(port=…)` | `new Router(…)`, `LocalRouter.start(…)` | `NewRouter(…)`, `StartLocalRouter(ctx, …)` |
| **Layer 3: Mesh (`Mesh`)** | Multiplexed connection, service mounting (`mount`), lifecycle (`run`, `serve`, `drain`), and built-in `.registry` / `.auth` | `Mesh(router)`, `mesh.mount(svc)`, `mesh.run()`, `mesh.serve()` | `new Mesh(router)`, `mesh.mount(svc)`, `mesh.serve()`, `mesh.startServing()` | `NewMesh(router)`, `mesh.Mount(svc)`, `mesh.Run()`, `mesh.Serve(ctx)`, `mesh.StartServing(ctx)` |
| **Layer 4: Generated Client (`<Service>Client`)** | Typed RPC client bound to a `Mesh`, with per-call control options via `with_options` | `MyClient(mesh)`, `client.with_options(…)` | `new MyClient(mesh)`, `client.withOptions(…)` | `NewMyClient(mesh)`, `client.WithOptions(…)` |

---

## Table of Contents

1. [Router-First Architecture & the 4-Layer Runtime](#1-router-first-architecture--the-4-layer-runtime)
   - [Layer 2 & Layer 3: `Router`, `LocalRouter`, and `Mesh`](#layer-2--layer-3-router-localrouter-and-mesh)
   - [Mounting and Serving Services: `mesh.mount`, `mesh.run`, and `mesh.serve`](#mounting-and-serving-services-meshmount-meshrun-and-meshserve)
   - [Local Transports: Unix Sockets and Named Pipes](#local-transports-unix-sockets-and-named-pipes)
2. [Service Contracts & Strict Mode (`MESHCALL_STRICT=1`)](#2-service-contracts--strict-mode-meshcall_strict1)
   - [Declaring Methods: `@query`, `@mutates`, `@stream`](#declaring-methods-query-mutates-stream)
   - [Dependency Injection and `meta()`](#dependency-injection-and-meta)
3. [The Three Call Shapes: Unary, Server Stream, Client Stream](#3-the-three-call-shapes-unary-server-stream-client-stream)
   - [Duplex: The Shape of Last Resort](#duplex-the-shape-of-last-resort)
4. [Generating & Using Typed Clients](#4-generating--using-typed-clients)
   - [Per-Call Options with `with_options`](#per-call-options-with-with_options)
   - [Calling from a Browser, an Edge Runtime or curl](#calling-from-a-browser-an-edge-runtime-or-curl)
5. [Exploring & Debugging Live Services from the CLI](#5-exploring--debugging-live-services-from-the-cli)
6. [Load Balancing vs. Targeted Stateful Instances](#6-load-balancing-vs-targeted-stateful-instances)
7. [Production Reliability: Timeouts, Retries, Idempotency & Drain](#7-production-reliability-timeouts-retries-idempotency--drain)
   - [Router Shutdown, Limits and Metrics](#router-shutdown-limits-and-metrics)
8. [Authentication & Temporary Scoped Tokens](#8-authentication--temporary-scoped-tokens)
   - [Anonymous Callers and API Keys](#anonymous-callers-and-api-keys)
9. [Interceptors, Metadata & Structured Errors](#9-interceptors-metadata--structured-errors)
   - [OpenTelemetry Tracing](#opentelemetry-tracing)
10. [AI Coding Assistant Integration (`meshcall init`)](#10-ai-coding-assistant-integration-meshcall-init)

---

## 1. Router-First Architecture & the 4-Layer Runtime

**Recommendation:** Always run MeshCall through the Go Router (`meshcall-router` or `LocalRouter`), whether you are developing on a single laptop or deploying across a cluster. Because the prebuilt Router binary is bundled directly inside the SDKs, starting a local Router requires zero extra installation.

Why go through the Router in all cases?

1. **Every node can be both a service provider and a client** — In point-to-point RPC, every service must allocate and listen on its own inbound port, and callers must track every peer's address. With the Router, there is a single routing endpoint (`ws://127.0.0.1:8765`). Any Python, TypeScript, or Go process connects outbound to the Router via a `Mesh`, mounts the `@service`s it provides, and uses that same `Mesh` to call other services.
2. **Full CLI and AI visibility** — `meshcall router inspect` and `meshcall call` can discover and test every service in your system at any time.
3. **Seamless scaling and fault tolerance** — Starting a second worker instance automatically load-balances traffic, enforces deadlines, and supports zero-downtime rolling drains.

### Layer 2 & Layer 3: `Router`, `LocalRouter`, and `Mesh`

MeshCall separates **where the Router is** (Layer 2) from **your connection and runtime plane** (Layer 3):

- **Layer 2 (`Router` / `LocalRouter`)**:
  - `Router("ws://127.0.0.1:8765")` points at an external `meshcall-router` (or reads `MESHCALL_ROUTER_URL`, defaulting to `ws://127.0.0.1:8765`). It performs no network I/O and never implicitly spawns subprocesses.
  - `LocalRouter(port=0)` explicitly spawns and owns a local `meshcall-router` child process for tests, demos, or single-machine orchestration.
- **Layer 3 (`Mesh`)**:
  - `Mesh(router, ...)` holds your multiplexed connection, credentials, retry/reconnect policies, and client interceptors.
  - `mesh.registry` (`list`, `describe`) and `mesh.auth` (`whoami`, `issue_token`, `revoke_token`) provide typed access to the Router's built-in discovery and token services.

```python
from meshcall import Mesh, Router, RouterScope

router = Router("ws://127.0.0.1:8765")
async with Mesh(router) as mesh:
    services = await mesh.registry.list()
    info = await mesh.registry.describe("chat.v1.Chat")
    me = await mesh.auth.whoami()
    issued = await mesh.auth.issue_token(
        scopes=[RouterScope(service="chat.v1.Chat", methods=["send"])],
        ttl_seconds=900,
    )
    await mesh.auth.revoke_token(issued.token_id)
```

```typescript
import { Mesh, Router } from "meshcall";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"));
const services = await mesh.registry.list();
const info = await mesh.registry.describe("chat.v1.Chat");
const me = await mesh.auth.whoami();
const issued = await mesh.auth.issueToken(
  [{ service: "chat.v1.Chat", methods: ["send"] }],
  900,
);
await mesh.auth.revokeToken(issued.token_id);
```

```go
router := meshcall.NewRouter("ws://127.0.0.1:8765")
mesh, err := meshcall.ConnectMesh(ctx, router)
if err != nil {
    return err
}
defer func() { _ = mesh.Close() }()

services, err := mesh.Registry().List(ctx)
if err != nil {
    return err
}
info, err := mesh.Registry().Describe(ctx, "chat.v1.Chat")
if err != nil {
    return err
}
me, err := mesh.Auth().Whoami(ctx)
if err != nil {
    return err
}
issued, err := mesh.Auth().IssueToken(ctx,
    []meshcall.RouterScope{{Service: "chat.v1.Chat", Methods: []string{"send"}}}, 900)
if err != nil {
    return err
}
revoked, err := mesh.Auth().RevokeToken(ctx, issued.TokenID)
if err != nil {
    return err
}
_, _, _ = services, info, me
_, _ = issued, revoked
```

### Mounting and Serving Services: `mesh.mount`, `mesh.run`, and `mesh.serve`

Mount one or more `@service` instances onto a `Mesh` with `mesh.mount(...)`, then start serving:

- `mesh.run()` (Go `mesh.Run()`) is the blocking production entry point: it connects to the `Router`, registers all mounted services, and drains in-flight calls on `SIGINT` / `SIGTERM` before exiting.
- `mesh.serve()` (TypeScript `mesh.startServing()`, Go `mesh.StartServing(ctx)`) is the async building block for tests and multi-service scripts: combine it with `LocalRouter(port=0)` to run a self-contained mesh on an ephemeral port.

```python
from meshcall import Mesh, Router
from my_app.service import MyService

if __name__ == "__main__":
    mesh = Mesh(Router())
    mesh.mount(MyService(), instance_id="worker-py-1")
    mesh.run()
```

```python
from meshcall import LocalRouter, Mesh

async with LocalRouter(port=0) as router:
    mesh = Mesh(router)
    mesh.mount(MyService())
    async with mesh.serve() as server:
        print(router.url)               # ws://127.0.0.1:<bound port>
        ...
# leaving the block drains in-flight calls and stops the LocalRouter
```

```typescript
import { Mesh, Router } from "meshcall";

const mesh = new Mesh(new Router());
mesh.mount(new MyService(), { instanceId: "worker-ts-1" });
await mesh.serve();
```

```typescript
import { LocalRouter, Mesh } from "meshcall";

await using router = await LocalRouter.start({ port: 0 });
const mesh = new Mesh(router);
mesh.mount(new MyService());
await using server = await mesh.startServing();
console.log(router.url);
```

```go
// Blocking production entry point: returns when SIGINT / SIGTERM arrives, after draining.
mesh := meshcall.NewMesh(meshcall.NewRouter())
mesh.Mount(NewMyService(), meshcall.WithInstanceID("worker-go-1"))
if err := mesh.Run(); err != nil {
    log.Fatal(err)
}

// Non-blocking alternative with an embedded LocalRouter:
router, err := meshcall.StartLocalRouter(ctx, meshcall.WithPort(0))
if err != nil {
    return err
}
defer func() { _ = router.Stop() }()

mesh := meshcall.NewMesh(router)
mesh.Mount(NewMyService())
srv, err := mesh.StartServing(ctx)
if err != nil {
    return err
}
defer func() { _ = srv.Close() }()      // Close drains first
```

`mesh.mount` accepts server-level options (`instance_id`, `max_inflight`, `strict`, `interceptors`).

### Local Transports: Unix Sockets and Named Pipes

When every node runs on one machine, skip TCP: the Router listens on a Unix socket (Linux, macOS, FreeBSD) or a named pipe (Windows), reachable only by the Router's own user (and, for a pipe, SYSTEM and Administrators). Frames and behaviour are identical.

```bash
meshcall-router --unix-socket /tmp/mesh.sock               # Windows: --pipe mesh
meshcall --router unix:///tmp/mesh.sock router inspect     # Windows: pipe://mesh
```

- **Python**: `Router(unix_path="/tmp/mesh.sock")` or `Router(pipe="mesh")` (or `LocalRouter(unix_path=...)`) — pass to `Mesh(router)`.
- **TypeScript**: `new Router({ unixPath: "/tmp/mesh.sock" })` or `new Router({ pipe: "mesh" })` (or `LocalRouter.start({ unixPath: ... })`).
- **Go** and the CLI: the endpoint strings `unix:///tmp/mesh.sock` and `pipe://mesh` (`meshcall.NewRouter(meshcall.WithUnixSocket(path))` / `meshcall.WithPipe(name)`).

---

## 2. Service Contracts & Strict Mode (`MESHCALL_STRICT=1`)

**Benefit:** MeshCall contracts carry **semantic annotations** (`read_only`, `idempotent`, `destructive`, `deprecated`, `description`) in addition to data schemas. Client retry policies use `idempotent`/`read_only` to know what is safe to retry automatically, and AI agents use `description` and annotations to invoke tools safely.

### Rules & Best Practices

1. **Service names include a version**: Use `<namespace>.v<major>.<Service>` (e.g., `billing.v1.InvoiceService`). Bare names like `chat` are normalised to `chat.v0`.
2. **Parameters are the request**: Do not wrap all parameters in a single `Request` class. Write normal parameters (`session_id: str, limit: int = 20`); MeshCall builds the request schema automatically.
3. **Return a model**: Always return a Pydantic `BaseModel` (Python), Zod object schema (TypeScript), or struct (Go) rather than bare primitives so contracts can evolve safely.
4. **Always run with `MESHCALL_STRICT=1` in dev/CI**: Strict mode turns contract warnings (`W001`–`W006`) into immediate startup errors with a `hint` telling you exactly what to add.

### Declaring Methods: `@query`, `@mutates`, `@stream`

The method decorator states the method's semantics once; the same contract drives validation, retries, `meshcall router inspect` and generated clients.

```python
from collections.abc import AsyncIterator
from meshcall import mutates, query, service, stream


@service("billing.v1.Invoices")
class Invoices:
    """Invoice operations."""

    @query
    async def get(self, invoice_id: str) -> Invoice:
        """Read one invoice (read-only and idempotent)."""
        ...

    @mutates(idempotent=True)
    async def rename(self, invoice_id: str, title: str) -> Invoice:
        """Change the title; repeating the same call is safe."""
        ...

    @mutates(destructive=True)
    async def delete(self, invoice_id: str) -> Deleted:
        """Delete an invoice (never retried without an idempotency key)."""
        ...

    @stream
    async def export(self, account_id: str) -> AsyncIterator[Row]:
        """Stream export rows; ordinary `@stream` methods are read-only."""
        ...
```

| Decorator | Meaning | Auto-retried by `RetryPolicy`? |
| --- | --- | --- |
| `@query` | Pure read; implies `read_only=True, idempotent=True` | Yes |
| `@mutates(idempotent=True)` | Mutates state, but repeating the exact call has the same effect | Yes |
| `@mutates(destructive=True)` or bare `@mutates` | Non-idempotent mutation (e.g., charge card, append log) | Only if caller supplies `idempotency_key` |
| `@stream` | Server stream; read-only/idempotent by default, unless you pass `read_only=False` / `idempotent=False` or the method name looks mutating (`delete_*`, …) | Streams are never retried |
| `deprecated="use_v2"` | Marks the method as deprecated in generated clients (`@deprecated`) | Depends on other flags |

TypeScript uses the same shapes with explicit Zod schemas:

```typescript
import { mutates, query, service, stream, z } from "meshcall";

const Invoice = z.object({ id: z.string(), title: z.string() }).meta({ id: "Invoice" });

@service("billing.v1.Invoices", { description: "Invoice operations." })
class Invoices {
  @query({ description: "Read one invoice.", input: { invoice_id: z.string() }, output: Invoice })
  async get(invoice_id: string) { ... }

  @mutates({ description: "Delete an invoice.", input: { invoice_id: z.string() }, output: Invoice, destructive: true })
  async delete(invoice_id: string) { ... }

  @stream({ description: "Stream export rows.", input: { account_id: z.string() }, output: z.object({ line: z.string() }) })
  async *export(account_id: string) { ... }
}
```

Go services can be defined via struct tags (`meshcall.ServiceMeta` and `meshcall.MethodMeta`) or registered with `HandleUnary`, `HandleServerStream`, `HandleClientStream` and `HandleDuplex`:

```go
chat := meshcall.NewService("chat.v1.Chat", meshcall.Describe("Chat sessions."))

meshcall.HandleUnary(chat, "send", func(ctx *meshcall.Context, req SendRequest) (Message, error) {
    return Message{Text: "echo " + req.Text}, nil
}, meshcall.Doc("Append a message and return the reply."), meshcall.Idempotent())
```

### Dependency Injection and `meta()`

Handlers can ask for server-only values without adding parameters to the contract. A dependency provider runs **once per call** and is cached for that call; a provider that `yield`s (a generator or async generator) runs its cleanup when the call ends, which makes commit-on-success / rollback-on-error easy to express. Injected parameters — dependencies, `meta("key")` values, `ctx: RpcContext` and `logger: RpcLogger` — are stripped from the contract and therefore from every generated client.

```python
from collections.abc import AsyncIterator
from typing import Annotated
from meshcall import dependency, meta, mutates

@dependency
async def transaction() -> AsyncIterator[SessionUnit]:
    """One unit of work per call; commit on success, drop on error or cancel."""
    unit = SessionUnit(store)
    try:
        yield unit
    except BaseException:
        await unit.rollback()
        raise
    else:
        await unit.commit()

Tx = Annotated[SessionUnit, transaction._provider()]   # or Annotated[T, dependency(provider)]
CallerId = Annotated[str | None, meta("x-caller-id")]

@mutates(idempotent=True)
async def send(self, session_id: str, text: str, *, tx: Tx, caller: CallerId) -> Message:
    """`tx` and `caller` are injected on the server only."""
    ...
```

Dependencies may themselves declare dependencies or metadata:

```python
@dependency
async def current_user(authorization: Annotated[str, meta("authorization")]) -> str:
    return authorization.removeprefix("Bearer ")
```

TypeScript declares them with `dependency(provider, { inject })` and passes them to a method with `inject`:

```typescript
import { dependency, meta, mutates } from "meshcall";

const CurrentUser = dependency(
  async function* (authorization: string | undefined) {
    yield authorization?.replace(/^Bearer\s+/, "") ?? "";
  },
  { inject: [meta("authorization")] },
);

@mutates({
  description: "Append a message.",
  input: { session_id: z.string(), text: z.string() },
  output: Message,
  inject: [CurrentUser],
})
async send(session_id: string, text: string, user: string) {
  return { text: `${user}: ${text}` };
}
```

Go registers providers once with `WithDependencies` on `mesh.Mount`; parameters whose types are registered are injected and removed from the contract:

```go
type CallerID string

func NewWorkspace() *Workspace { return &Workspace{} }

type Workspace struct {
    meshcall.ServiceMeta `name:"workspace.v1.Workspace" description:"Remote workspace."`
}

func (w *Workspace) WriteFile(ctx *meshcall.Context, path, content string, caller CallerID) (FileStat, error) {
    return FileStat{Path: path, Bytes: len(content), Caller: string(caller)}, nil
}

mesh := meshcall.NewMesh(meshcall.NewRouter())
mesh.Mount(NewWorkspace(), meshcall.WithDependencies(
    meshcall.Meta[CallerID]("x-caller-id", meshcall.WithMetaDefault(CallerID("anonymous"))),
))
if err := mesh.Run(); err != nil {
    log.Fatal(err)
}
```

`meta()` reads a call metadata entry (case-insensitively, as headers usually arrive); use it for gateways and CLI identities, not for business parameters.

---

## 3. The Three Call Shapes: Unary, Server Stream, Client Stream

**Benefit:** Stream items are flow-controlled per call (initial window of 16 items). A slow consumer naturally throttles the producer without buffering unbounded data in memory, and other calls on the same WebSocket connection continue fairly.

The rule is the same in all three runtimes: **ordinary business parameters come first; a client stream's inbound iterator comes last.** Generated clients mirror that list 1:1.

### Python Service & Client

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcContext, RpcInputStream, RpcLogger, mutates, query, service, stream


class Chunk(BaseModel):
    text: str


class Summary(BaseModel):
    count: int
    combined: str


@service("text.v1.TextProcessor")
class TextProcessor:
    """Process text via unary, server-stream, and client-stream RPCs."""

    # 1. Unary: parameters -> Model
    @query
    async def echo(self, text: str, ctx: RpcContext) -> Chunk:
        """Echo the input text and attach a response trailer."""
        ctx.set_trailer("length", str(len(text)))
        return Chunk(text=text)

    # 2. Server Stream: parameters -> AsyncIterator[Model]
    @stream
    async def tokenize(self, sentence: str, logger: RpcLogger) -> AsyncIterator[Chunk]:
        """Yield each word in sentence as a separate stream item."""
        logger.info("tokenizing sentence")
        for word in sentence.split():
            yield Chunk(text=word)

    # 3. Client Stream: inbound stream last + parameters -> Model; declared with @mutates
    @mutates(idempotent=True)
    async def join_chunks(
        self,
        separator: str,
        items: RpcInputStream[Chunk],
    ) -> Summary:
        """Consume a stream of Chunk items and join them with separator."""
        parts: list[str] = []
        async for chunk in items:
            parts.append(chunk.text)
        return Summary(count=len(parts), combined=separator.join(parts))
```

**Calling all 3 shapes from a generated Python client:**

```python
from meshcall import Mesh, Router
from text_client import TextProcessorClient

async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
    client = TextProcessorClient(mesh)

    # Unary
    chunk = await client.echo("hello")

    # Server stream (use `async with` if you might break early so it cancels cleanly)
    async with client.with_options(timeout=10.0, idle_timeout=3.0).tokenize("one two three") as stream:
        async for item in stream:
            print(item.text)

    # Client stream: the generated method takes an AsyncIterable of items, last
    async def chunks() -> AsyncIterator[Chunk]:
        yield Chunk(text="a")
        yield Chunk(text="b")

    summary = await client.with_options(timeout=10.0).join_chunks("-", chunks())
    print(summary.combined)  # "a-b"
```

### TypeScript Service & Client

```typescript
import { Mesh, mutates, query, Router, service, stream, z } from "meshcall";

const Chunk = z.object({ text: z.string() }).meta({ id: "Chunk" });
const Summary = z.object({ count: z.int(), combined: z.string() }).meta({ id: "Summary" });

@service("text.v1.TextProcessor", { description: "Process text via unary, server-stream, and client-stream RPCs." })
class TextProcessor {
  @query({ description: "Echo the input text.", input: { text: z.string() }, output: Chunk })
  async echo(text: string) {
    return { text };
  }

  @stream({ description: "Yield each word in sentence.", input: { sentence: z.string() }, output: Chunk })
  async *tokenize(sentence: string) {
    for (const word of sentence.split(" ")) yield { text: word };
  }

  @mutates({
    description: "Join a stream of chunks.",
    input: { separator: z.string().default(" ") },
    streamInput: Chunk,
    output: Summary,
    idempotent: true,
  })
  async join_chunks(separator: string, items: AsyncIterable<{ text: string }>) {
    const parts: string[] = [];
    for await (const chunk of items) parts.push(chunk.text);
    return { count: parts.length, combined: parts.join(separator) };
  }
}

const mesh = new Mesh(new Router("ws://127.0.0.1:8765"));
mesh.mount(new TextProcessor());
await mesh.serve();
```

```typescript
import { Mesh, Router } from "meshcall";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"));
const client = new TextProcessorClient(mesh);

const chunk = await client.echo("hello");
for await (const item of client.withOptions({ timeoutMs: 10_000, idleTimeoutMs: 3_000 }).tokenize("one two three")) {
  console.log(item.text);
}
async function* chunks() { yield { text: "a" }; yield { text: "b" }; }
const summary = await client.join_chunks("-", chunks());
console.log(summary.combined); // "a-b"
```

### Duplex: The Shape of Last Resort

A duplex method streams input and output at the same time. Use it only when output must interleave with input that keeps arriving within one call, such as a voice turn answered while the user is still speaking. It is never retried or deduplicated, takes no idempotency key, cannot be `idempotent` / `read_only`, is unreachable over HTTP and pins one instance for its whole life; the `reason` goes into the contract. A side that only sends stalls once the other side's credit runs out, so read output concurrently with sending. Try a server stream plus unary calls, or a client stream, first.

- **Python**: `@mutates(reason="…")` with a `channel: DuplexChannel[In, Out]` parameter; the handler returns `None`.
- **TypeScript**: `@mutates({ description, reason, input, streamInput: InSchema, streamOutput: OutSchema })` (or `channel: { input, output }`), handler takes `channel: DuplexChannel<In, Out>` and returns nothing.
- **Go**: `meshcall.HandleDuplex(svc, name, reason, handler)` with a trailing `*meshcall.DuplexChannel[In, Out]` parameter.

---

## 4. Generating & Using Typed Clients

**Benefit:** One Go CLI (`meshcall generate`) generates Python, TypeScript and Go clients from either a Python `module:Class` or a portable `.meshcall.json` file. Generated clients validate both requests **and** responses against the contract schema, and their method signatures mirror the server's business parameters 1:1 (generated TypeScript and Go methods additionally take an optional trailing options/opts argument).

```bash
# From a Python service -> Python uv package
uv run meshcall generate my_app.service:TextProcessor --output generated/text-py

# From a Python service -> Single-file Python client (great for scripts/demos)
uv run meshcall generate my_app.service:TextProcessor \
  --single-file \
  --output src/my_app/text_client.py

# From a Python service -> TypeScript Yarn/npm package
uv run meshcall generate my_app.service:TextProcessor \
  --language typescript \
  --output generated/text-ts

# From a portable .meshcall.json contract (exported by TS `writeContract`,
# Go `meshcall.WriteContract(path, svc)` or `meshcall export`)
npx meshcall generate contract.meshcall.json \
  --language python \
  --output generated/py-client

# Go client: one file, <output>/client.go (or the .go path you name)
meshcall generate contract.meshcall.json \
  --language go \
  --package-name textclient \
  --output internal/textclient
```

Generated clients wrap a `Mesh` (`TextProcessorClient(mesh)` in Python, `new TextProcessorClient(mesh)` in TypeScript, `textclient.NewTextProcessorClient(mesh)` in Go). Multiple generated clients passed the same `Mesh` share its single multiplexed connection.

```python
from meshcall import Mesh, Router
from text_client import TextProcessorClient

async with Mesh(Router("ws://127.0.0.1:8765")) as mesh:
    text = TextProcessorClient(mesh)
    chunk = await text.echo("hello")
```

```typescript
import { Mesh, Router } from "meshcall";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

await using mesh = await Mesh.connect(new Router("ws://127.0.0.1:8765"));
const client = new TextProcessorClient(mesh);
const chunk = await client.echo("hello");
```

```go
mesh := meshcall.NewMesh(meshcall.NewRouter())               // MESHCALL_ROUTER_URL
defer func() { _ = mesh.Close() }()
client := textclient.NewTextProcessorClient(mesh)
chunk, err := client.Echo(ctx, "hello")
_, _ = chunk, err
```

### Per-Call Options with `with_options`

Control parameters never enter the business method signature. Chain them once and call the method normally (generated TypeScript and Go methods also accept an optional trailing options/opts argument; `with_options` keeps call sites uniform across runtimes):

```python
client = TextProcessorClient(mesh).with_options(
    timeout=5.0,                 # whole-call budget, seconds
    idle_timeout=2.0,            # gap between stream items
    metadata={"trace-id": "abc"},
    idempotency_key="op-1",      # makes a mutating unary call retry-safe
    instance="worker-1",         # only for targeted services
)
chunk = await client.echo("hello")
```

```typescript
const client = new TextProcessorClient(mesh).withOptions({
  timeoutMs: 5_000,
  idleTimeoutMs: 2_000,
  metadata: { "trace-id": "abc" },
  idempotencyKey: "op-1",
  instance: "worker-1",
});
const chunk = await client.echo("hello");
```

```go
client := textclient.NewTextProcessorClient(mesh).WithOptions(meshcall.CallOptions{
    Timeout:        5 * time.Second,
    IdleTimeout:    2 * time.Second,
    Metadata:       map[string]string{"trace-id": "abc"},
    IdempotencyKey: "op-1",
    Instance:       "worker-1",
})
chunk, err := client.Echo(ctx, "hello")
_, _ = chunk, err
```

A targeted service's generated client keeps `instance` a compile-time requirement (Python keyword-only `instance`, TypeScript required option, Go positional argument) even though other options come from `with_options`.

### Calling from a Browser, an Edge Runtime or curl

Browsers cannot set the `Authorization` header on a WebSocket, so web and edge code calls the Router's HTTP API (same port, under `/v1/`). Generated TypeScript clients accept a `Mesh` from `meshcall/web` (no Node dependency) constructed with `new Router("https://...")`; unary and server-streaming calls work, duplex does not.

```typescript
import { Mesh, Router } from "meshcall/web";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

const mesh = new Mesh(new Router("https://router.example"), { token });
const client = new TextProcessorClient(mesh);
const chunk = await client.withOptions({ timeoutMs: 5_000 }).echo("hello");
```

Pass a short-lived token (`mt_…`) or an API key (`mk_…`) as `token`, and start the Router with `--http-cors-origin https://app.example` for other origins. Any HTTP client works the same way:

```bash
curl http://127.0.0.1:8765/v1/call/text.v1.TextProcessor/echo -d '{"text": "hi"}'
curl -N http://127.0.0.1:8765/v1/call/text.v1.TextProcessor/tokenize -d '{"sentence": "a b"}'  # {"item":…} lines, then {"end":…}
curl http://127.0.0.1:8765/v1/openapi.json    # OpenAPI 3.1 for every method you may call
```

---

## 5. Exploring & Debugging Live Services from the CLI

**Benefit:** Neither humans nor AI agents need to write throwaway scripts to test a running service. Every service sends its portable contract to the Router on registration.

```bash
# 1. Inspect all services, live instances, in-flight calls, and method signatures
uv run meshcall router inspect --router ws://127.0.0.1:8765

# 2. Dump the full JSON contract of one service
uv run meshcall router inspect text.v1.TextProcessor --json

# 3. Call a unary or server-stream method with metadata and timeout
uv run meshcall call text.v1.TextProcessor.tokenize '{"sentence": "hello mesh"}' \
  --timeout 5s \
  --metadata trace_id=req-123

# 4. Call a client-stream method by piping JSON lines into --stdin
printf '{"text":"alpha"}\n{"text":"beta"}\n' | \
  uv run meshcall call text.v1.TextProcessor.join_chunks '{"separator": ":"}' --stdin
```

If you pass an invalid field or wrong type, `meshcall call` automatically queries the Router's Registry and prints a human- and AI-friendly hint showing the exact expected schema and field `violations`.

---

## 6. Load Balancing vs. Targeted Stateful Instances

**Benefit:** Stateless services scale horizontally with automatic per-method load balancing, while stateful services (GPU workers with loaded models, in-memory game rooms, actor nodes) can be addressed directly by `instance_id`.

### Stateless Load Balancing Policies

Configure balancing per method (`round_robin` is the default):

```python
from meshcall import Balance, mutates, query, service

@service("compute.v1.Worker")
class Worker:
    """Stateless and session-affine compute methods."""

    # Route to the instance with the fewest active in-flight calls
    @query(balance=Balance.least_inflight())
    async def heavy_compute(self, payload: str) -> Result:
        """Run CPU/GPU-intensive task on the least busy worker."""
        ...

    # Route calls with the same session_id to the same instance when pool is stable
    @mutates(idempotent=True, balance=Balance.sticky("session_id"))
    async def step_session(self, session_id: str, prompt: str) -> Result:
        """Run a session step with soft cache affinity."""
        ...
```

### Stateful Instance-Targeted Services (`targeted=True`)

When every instance holds distinct state (e.g., `gpu-worker-0` holds Model A and `gpu-worker-1` holds Model B), mark the service `targeted=True`.
- The Router rejects any call that omits `instance_id` with `invalid_argument` (`reason: "instance_required"`).
- Generated Python and TypeScript clients make `instance` a **required compile-time parameter**, so forgetting to specify the target worker is caught by `pyright` / `tsc` before code ever runs!

```python
from meshcall import RpcContext, query, service

@service("gpu.v1.ModelRunner", targeted=True)
class ModelRunner:
    """Stateful GPU worker holding a specific loaded model."""

    @query
    async def infer(self, prompt: str, ctx: RpcContext) -> Output:
        """Run inference on this specific GPU instance."""
        return Output(text=f"Processed by {ctx.instance_id}: {prompt}")
```

Calling a targeted service:

```python
# `instance=` is required by the generated client signature:
res = await client.with_options(timeout=10.0).infer("Hello!", instance="gpu-worker-1")
```

```typescript
// The generated TypeScript method requires `instance` in its options:
const res = await client.infer("Hello!", { instance: "gpu-worker-1", timeoutMs: 10_000 });
```

```go
// The generated Go method takes instance right after ctx:
res, err := client.Infer(ctx, "gpu-worker-1", "Hello!")
_, _ = res, err
```

Or from the CLI:

```bash
uv run meshcall call gpu.v1.ModelRunner.infer '{"prompt": "Hello!"}' --instance gpu-worker-1
```

---

## 7. Production Reliability: Timeouts, Retries, Idempotency & Drain

**Benefit:** Achieve high availability, automatic retries, duplicate suppression, and zero-downtime rolling deployments **without** a stateful message broker or database in the Router.

### 1. Three-Layer Relative Deadlines

Always set a timeout with `with_options(timeout=…)` / `withOptions({ timeoutMs })` / `WithOptions(CallOptions{Timeout: …})`.
- **Server** enforces at `T` (cancels handler task).
- **Router** enforces at `T + 500ms` (protects caller if server hangs).
- **Client** enforces at `T + max(1s, 10%)` (protects caller if network/Router hangs).
- Inside a handler, check `ctx.remaining()` (Python, seconds) or `ctx.remainingMs()` (TS, ms) before starting expensive sub-operations.

### 2. Automatic Retries & Idempotency Keys

Configure `RetryPolicy` on the `Mesh`. It automatically retries failed **unary** calls when:
1. The error is marked `retryable=True` (e.g. `unavailable` when an instance disconnects, or `resource_exhausted` when overloaded), **AND**
2. Either the method is annotated `read_only=True` / `idempotent=True` in the contract, **OR** the caller supplies an `idempotency_key`.

When a unary call carries an `idempotency_key`, the Router deduplicates concurrent calls with the same key and replays the cached terminal result for 60 seconds.

```python
from meshcall import Mesh, RetryPolicy, Router

mesh = Mesh(
    Router("ws://127.0.0.1:8765"),
    retry=RetryPolicy(max_attempts=3, initial_delay=0.05, max_delay=1.0),
)
client = CounterServiceClient(mesh)
```

```typescript
import { Mesh, Router } from "meshcall";

const mesh = new Mesh(new Router("ws://127.0.0.1:8765"), {
  retry: { maxAttempts: 3, initialDelayMs: 50, maxDelayMs: 1000 },
});
const client = new CounterServiceClient(mesh);
```

```go
router := meshcall.NewRouter("ws://127.0.0.1:8765")
mesh := meshcall.NewMesh(router, meshcall.WithRetry(meshcall.RetryPolicy{
    MaxAttempts:  3,
    InitialDelay: 50 * time.Millisecond,
    MaxDelay:     time.Second,
    Multiplier:   2,
    Jitter:       0.2,
}))
client := counterclient.NewCounterServiceClient(mesh)
_ = client
```

### 3. Backpressure (`max_inflight`) & Circuit Breaking

Set `max_inflight` when mounting a service so the Router routes around saturated workers instead of piling up queues:

```python
mesh.mount(Worker(), max_inflight=32)
async with mesh.serve() as server:
    ...
```

```typescript
mesh.mount(new Worker(), { maxInflight: 32 });
await using server = await mesh.startServing();
```

```go
if err := mesh.Mount(NewWorker(), meshcall.WithMaxInflight(32)); err != nil {
    return err
}
srv, err := mesh.StartServing(ctx)
if err != nil {
    return err
}
defer func() { _ = srv.Close() }()
```

If an instance repeatedly times out or returns `internal`/`unavailable` (default: 5 consecutive failures), the Router's per-instance circuit breaker trips for 10 seconds and probes with a single call before restoring traffic.

### 4. Multi-Router High Availability

Run two or more independent `meshcall-router` processes. Give the same endpoint list to the `Router` object passed to `Mesh`—mounted services register with **all** Routers simultaneously, and clients fail over automatically when one Router goes down:

```python
router = Router(["ws://router-1.internal:8765", "ws://router-2.internal:8765"])
mesh = Mesh(router)
client = CounterServiceClient(mesh)
# server: mesh.mount(Worker()); mesh.run()
```

```typescript
const router = new Router(["ws://router-1.internal:8765", "ws://router-2.internal:8765"]);
const mesh = new Mesh(router);
```

```go
router := meshcall.NewRouter(
    "ws://router-1.internal:8765",
    "ws://router-2.internal:8765",
)
mesh := meshcall.NewMesh(router)
```

### 5. Zero-Downtime Rolling Restarts (`drain`)

`mesh.run()` drains on `SIGINT` / `SIGTERM` automatically. With `mesh.serve()` / `mesh.startServing()`, drain via `mesh.drain()` or stop through the server handle (or leave its context manager):

```python
# Python: draining is the default; pass drain_timeout to bound it
await mesh.drain()
await server.stop(drain=True, drain_timeout=30.0)
```

```typescript
// TypeScript
await mesh.drain();
await server.close({ drain: true, drainTimeoutMs: 30_000 });
```

Go: `srv.Close()` drains first; you can also call `mesh.Drain(ctx)` or `srv.Drain(ctx)` explicitly before `srv.Close()`.

### Router Shutdown, Limits and Metrics

Restart a Router with SIGTERM (or SIGINT): it drains first. `/readyz` answers 503, new calls get retryable `unavailable` (`reason: "router_draining"`), idle connections close with 1001 so clients and services move to another Router, and calls still in flight after `--shutdown-timeout` (default 10 s) end with retryable `unavailable` (`reason: "router_shutdown"`). A second signal skips the wait. A `LocalRouter` spawned by an SDK is stopped by closing its stdin, which ends calls in flight at once.

| Flag | Default | Effect |
| --- | --- | --- |
| `--max-calls-per-connection` | 1000 (`0`: no limit) | a further call on that connection fails with retryable `resource_exhausted` / `overloaded`; the connection stays open |
| `--max-frame-size` | 64 MiB (also the maximum) | one payload or stream item must fit in a frame; send larger data as a stream |
| `--outbox-size` | 256 MiB | how much the Router may owe a peer that stops reading before disconnecting it |
| `--metrics-listen` | off | Prometheus text at `http://ADDR/metrics` on a separate listener, e.g. `127.0.0.1:9464` |

Every flag also has a `MESHCALL_ROUTER_<FLAG>` environment variable; `LocalRouter(...)` accepts the embedded-Router options (`metrics_listen`, `max_frame_size`, `outbox_size`, `auth_file`, …) passed to the locally spawned subprocess.

---

## 8. Authentication & Temporary Scoped Tokens

**Benefit:** Lock down production Routers with PBKDF2-hashed account ACLs (`router-auth.json`), and mint short-lived, least-privilege bearer tokens through `mesh.auth` for untrusted callers or ephemeral workers.

### 1. Create `router-auth.json`

Hash passwords using the Router binary:

```bash
python3 -c 'import getpass,sys; sys.stdout.write(getpass.getpass("Password: "))' \
  | uv run meshcall-router hash-password
```

Configure accounts and ACLs in `router-auth.json`:

```json
{
  "users": [
    {
      "username": "backend-app",
      "password_hash": "<PBKDF2_HASH>",
      "roles": ["client"],
      "call": [
        "chat.v1.Chat/*",
        "meshcall.router.v1.AuthService/issue_token",
        "meshcall.router.v1.AuthService/revoke_token",
        "meshcall.router.v1.Registry/*"
      ]
    },
    {
      "username": "chat-worker",
      "password_hash": "<PBKDF2_HASH>",
      "roles": ["server"],
      "register": ["chat.v1.Chat"]
    }
  ]
}
```

Start the Router with the auth file: pass `LocalRouter(port=8765, auth_file="router-auth.json")` when spawning a local Router, or start the binary with `--auth-file router-auth.json`.

### 2. Issue & Use Temporary Scoped Tokens

```python
from meshcall import Mesh, Router, RouterCredentials, RouterScope

admin = RouterCredentials(username="backend-app", password="...")
async with Mesh(Router("ws://127.0.0.1:8765"), credentials=admin) as mesh:
    # Mint a 15-minute token restricted to calling only `chat.v1.Chat/send`
    issued = await mesh.auth.issue_token(
        scopes=[RouterScope(service="chat.v1.Chat", methods=["send"])],
        ttl_seconds=900,
    )

# Hand `issued.token` to a downstream caller:
downstream = Mesh(Router("ws://127.0.0.1:8765"), token=issued.token)
chat = ChatClient(downstream)
```

`mesh.auth.revoke_token(issued.token_id)` (TypeScript `mesh.auth.revokeToken(tokenId)`, Go `mesh.Auth().RevokeToken(ctx, issued.TokenID)`) invalidates a token before it expires.

### Anonymous Callers and API Keys

A caller that sends no `Authorization` header is the reserved user `anonymous`. It may connect, but every call is `permission_denied` unless the auth file grants it `call` rules; only the auth file can make a method public.

```json
{ "username": "anonymous", "roles": ["client"], "call": ["status.v1.Health/*"] }
```

Give machine callers (webhooks, CI, external services) long-lived **API keys** instead of passwords:

```bash
meshcall-router new-api-key ci-bot   # prints the key once, and the entry to add under "api_keys"
```

Fill in the entry's `call` list and restart the Router. Callers send the key as `Authorization: Bearer mk_…`: `Mesh(router, token="mk_…")` in Python, `new Mesh(router, { token: "mk_…" })` in TypeScript, `meshcall.NewMesh(router, meshcall.WithToken("mk_…"))` in Go, `-H 'Authorization: Bearer mk_…'` with curl. A key is checked with one hash lookup; rotate it by adding a new key and removing the old one.

---

## 9. Interceptors, Metadata & Structured Errors

**Benefit:** Attach tracing/auth headers across all calls in one place with interceptors, and catch specific error classes with structured field `violations` instead of parsing error strings.

### Client & Server Interceptors (Python)

```python
from meshcall import ClientCallInfo, ClientCallNext, Mesh, Router, RpcContext, ServerCallNext

# Client interceptor: inject a trace_id header into every outgoing call
async def trace_client_interceptor(info: ClientCallInfo, call_next: ClientCallNext):
    metadata = dict(info.options.metadata or {})
    metadata.setdefault("trace-id", "trace-abc-123")
    info.options = info.options.with_overrides(metadata=metadata)
    return await call_next(info)

# Server interceptor: read caller metadata and attach a response trailer
async def audit_server_interceptor(ctx: RpcContext, request: object, call_next: ServerCallNext):
    trace_id = ctx.metadata.get("trace-id", "none")
    ctx.set_trailer("echo-trace-id", trace_id)
    return await call_next(ctx, request)

router = Router("ws://127.0.0.1:8765")
mesh = Mesh(router, interceptors=[trace_client_interceptor])
client = TextProcessorClient(mesh)

mesh.mount(TextProcessor(), interceptors=[audit_server_interceptor])
async with mesh.serve() as server:
    ...
```

### OpenTelemetry Tracing

Add the OpenTelemetry interceptors on both sides. Trace context travels as W3C `traceparent` / `tracestate` call metadata, so one trace continues through the Router and across languages; each call gets a `service/method` span and `meshcall.calls` / `meshcall.call.duration` metrics. With no OpenTelemetry SDK configured they do nothing.

```python
# pip install "meshcall[otel]"
from meshcall import Mesh, Router
from meshcall.otel import client_interceptor, server_interceptor

router = Router("ws://127.0.0.1:8765")
mesh = Mesh(router, interceptors=[client_interceptor()])
client = TextProcessorClient(mesh)

mesh.mount(TextProcessor(), interceptors=[server_interceptor()])
async with mesh.serve() as server:
    ...
```

```typescript
// needs the optional peer dependency @opentelemetry/api
import { Mesh, Router } from "meshcall";
import { otelClientInterceptor, otelServerInterceptor } from "meshcall/otel";

const mesh = new Mesh(new Router(url), { interceptors: [otelClientInterceptor()] });
mesh.mount(new TextProcessor(), {
  interceptors: [otelServerInterceptor()],
});
await using server = await mesh.startServing();
```

Go has no OpenTelemetry package; a `ClientInterceptor` / `ServerInterceptor` can set or read the `traceparent` metadata key itself.

### Handling Typed Errors

Every protocol error maps to a dedicated exception class (`InvalidArgumentError`, `DeadlineExceededError`, `UnavailableError`, `ResourceExhaustedError`, `PermissionDeniedError`, `MethodNotFoundError`, `ProtocolError`, `InternalError`, `CallCancelledError`):

```python
from meshcall import DeadlineExceededError, InvalidArgumentError, UnavailableError

try:
    await client.with_options(timeout=2.0).add(10, 20)
except InvalidArgumentError as err:
    # Field-level validation failures (never echoes sensitive input values)
    for v in err.violations:
        print(f"Field '{v.path}' failed '{v.code}': {v.message}")
except DeadlineExceededError as err:
    print(f"Timed out (detected by {err.source}, reason={err.reason})")
except UnavailableError as err:
    print(f"Service unavailable (reason={err.reason}, retryable={err.retryable})")
```

---

## 10. AI Coding Assistant Integration (`meshcall init`)

Install MeshCall development instructions into your repository so AI coding assistants know how to define services, generate clients, and inspect or call running services from the CLI:

```bash
# Default: writes .agents/skills/meshcall/SKILL.md and updates AGENTS.md
# Works with OpenAI Codex, Google Antigravity, OpenCode, Amp, and other AGENTS.md-aware tools
uv run meshcall init

# Target specific assistants instead of the default (repeatable, or comma-separated):
uv run meshcall init --agent claude     # .claude/skills/meshcall/SKILL.md + CLAUDE.md
uv run meshcall init --agent cursor     # .cursor/rules/meshcall.mdc + AGENTS.md
uv run meshcall init --agent copilot    # .github/skills/meshcall/SKILL.md + .github/copilot-instructions.md
uv run meshcall init --agent windsurf   # .windsurf/rules/meshcall.md + AGENTS.md
uv run meshcall init --agent all        # every target above, plus the default (universal)
```

With `--agent`, only the named targets are written; add `universal` to keep the `.agents/` default as well. Re-running is idempotent, and an edited skill file is kept unless you pass `--force`.
