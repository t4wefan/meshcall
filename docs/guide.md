# MeshCall User Guide & Cookbook

**English** | [简体中文](zh-CN/guide.md)

This guide is designed for both **human engineers** and **AI coding agents**. Each section explains **when and why** to use a MeshCall capability, followed by complete, copy-pasteable recipes in **Python** and **TypeScript**, and in **Go** (`meshcall-go`) where the Go runtime differs.

---

## Table of Contents

1. [Router-First Architecture: Why Every Node Connects Through the Router](#1-router-first-architecture-why-every-node-connects-through-the-router)
   - [Local Transports: Unix Sockets and Named Pipes](#local-transports-unix-sockets-and-named-pipes)
2. [Service Contracts & Strict Mode (`MESHCALL_STRICT=1`)](#2-service-contracts--strict-mode-meshcall_strict1)
3. [The Three Call Shapes: Unary, Server Stream, Client Stream](#3-the-three-call-shapes-unary-server-stream-client-stream)
   - [Subscriptions: Streams That Resume](#subscriptions-streams-that-resume)
   - [Duplex: The Shape of Last Resort](#duplex-the-shape-of-last-resort)
4. [Generating & Using Typed Clients](#4-generating--using-typed-clients)
   - [Calling from a Browser, an Edge Runtime or curl](#calling-from-a-browser-an-edge-runtime-or-curl)
5. [Exploring & Debugging Live Services from the CLI](#5-exploring--debugging-live-services-from-the-cli)
6. [Load Balancing vs. Targeted Stateful Instances](#6-load-balancing-vs-targeted-stateful-instances)
7. [Production Reliability: Timeouts, Retries, Idempotency & Drain](#7-production-reliability-timeouts-retries-idempotency--drain)
   - [Router Shutdown, Limits & Metrics](#6-router-shutdown-limits--metrics)
8. [Authentication & Temporary Scoped Tokens](#8-authentication--temporary-scoped-tokens)
   - [Anonymous Callers & API Keys](#3-anonymous-callers--api-keys)
9. [Interceptors, Metadata & Structured Errors](#9-interceptors-metadata--structured-errors)
   - [OpenTelemetry Tracing](#opentelemetry-tracing)
10. [AI Coding Assistant Integration (`meshcall init`)](#10-ai-coding-assistant-integration-meshcall-init)

---

## 1. Router-First Architecture: Why Every Node Connects Through the Router

**Recommendation:** Always run MeshCall through the Go Router (`meshcall-router` / `WebSocketRouter`), whether you are developing on a single laptop or deploying across a cluster. Because the prebuilt Router binary is bundled directly inside the Python and TypeScript SDKs, starting a Router requires zero extra installation.

Why go through the Router in all cases?

1. **Every node can be both a service provider and a client** — In traditional point-to-point RPC, every service must allocate and listen on its own inbound port, and callers must track every peer's address. With the Router, there is only one endpoint (`ws://127.0.0.1:8765`). Any Python or TypeScript process connects outbound to the Router, registers the `@service`s it provides, and simultaneously uses clients to call other services in the mesh.
2. **Full CLI and AI visibility** — `meshcall router inspect` and `meshcall call` can discover and test every service in your system at any time.
3. **Seamless scaling and fault tolerance** — Starting a second worker instance automatically load-balances traffic, enforces deadlines, and supports zero-downtime rolling drains (`drain=True`).

### Python: Serving and Calling Through the Router

```python
from meshcall import RpcServer, WebSocketRouter
from meshcall.drivers import WebSocketClientDriver, WebSocketRouterServerDriver

# 1. Start the bundled Go Router (or run `meshcall-router --port 8765` externally)
router = WebSocketRouter(host="127.0.0.1", port=8765)
await router.start()

# 2. Any node can register its own services AND create clients to call other services
other_service_client = OtherServiceClient(WebSocketClientDriver("ws://127.0.0.1:8765"))

server = RpcServer(
    services=[MyService(downstream=other_service_client)],
    driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="worker-1"),
)
await server.start()
```

### TypeScript: Serving and Calling Through the Router

```typescript
import { MeshCallClient, MeshCallServer, WebSocketRouter } from "meshcall";

// 1. Start the bundled Go Router (or connect to an existing one)
const router = new WebSocketRouter({ host: "127.0.0.1", port: 8765 });
await router.start();

// 2. Register services on this node while also calling other services through the Router
const client = new MeshCallClient("ws://127.0.0.1:8765");

const server = new MeshCallServer({
  services: [myService],
  router: { endpoint: "ws://127.0.0.1:8765", instanceId: "worker-ts-1" },
});
await server.start();
```

### Go: Serving and Calling Through the Router

The Go runtime has no Router launcher: run `meshcall-router --port 8765` (or use a deployed Router) and connect to it.

```go
chat := meshcall.NewService("chat.v1.Chat", meshcall.Describe("Chat sessions."))
// meshcall.HandleUnary(chat, "send", handler, meshcall.Doc("…"), meshcall.Idempotent())

server, err := meshcall.NewServer(meshcall.ServerOptions{Services: []*meshcall.Service{chat}, InstanceID: "worker-go-1"})
err = server.ConnectRouter(ctx, "ws://127.0.0.1:8765")

rpc, err := meshcall.NewClient(meshcall.ClientOptions{}, "ws://127.0.0.1:8765")
```

### Local Transports: Unix Sockets and Named Pipes

When every node runs on one machine, skip TCP: the Router listens on a Unix socket (Linux, macOS, FreeBSD) or a named pipe (Windows), reachable only by the Router's own user (and, for a pipe, SYSTEM and Administrators). Frames and behaviour are identical.

```bash
meshcall-router --unix-socket /tmp/mesh.sock               # Windows: --pipe mesh
meshcall --router unix:///tmp/mesh.sock router inspect     # Windows: pipe://mesh
```

- **Python**: `WebSocketRouter(unix_path="/tmp/mesh.sock")`, `WebSocketClientDriver(unix_path=...)`, `WebSocketRouterServerDriver(unix_path=...)`; on Windows `pipe="mesh"`.
- **TypeScript**: `new WebSocketRouter({ unixPath })`, `new MeshCallClient({ unixPath: "/tmp/mesh.sock" })`, `router: { endpoint: { unixPath } }`; on Windows `{ pipe: "mesh" }`.
- **Go** and the CLI: the endpoint strings `unix:///tmp/mesh.sock` and `pipe://mesh`.

> **Note on Direct Mode (`WebSocketDirectServerDriver` / `MeshCallServer({ port })`):** Point-to-point direct mode is retained only as an early evolutionary artifact and low-level compatibility fallback. It is **not recommended** for application use, as it requires manual port management, prevents symmetric peer-to-peer service/client sharing through a single mesh endpoint, and disables `meshcall router inspect`.

---

## 2. Service Contracts & Strict Mode (`MESHCALL_STRICT=1`)

**Benefit:** MeshCall contracts carry **semantic annotations** (`read_only`, `idempotent`, `destructive`, `deprecated`, `description`) in addition to data schemas. Client retry policies use `idempotent`/`read_only` to know what is safe to retry automatically, and AI agents use `description` and annotations to invoke tools safely.

### Rules & Best Practices

1. **Service names include a version**: Use `<namespace>.v<major>.<Service>` (e.g., `billing.v1.InvoiceService`). Bare names like `chat` are normalised to `chat.v0`.
2. **Parameters are the request**: Do not wrap all parameters in a single `Request` class. Write normal parameters (`session_id: str, limit: int = 20`); MeshCall builds the request schema automatically.
3. **Return a model**: Always return a Pydantic `BaseModel` (Python) or object `TypeContract` (TypeScript) rather than bare primitives so contracts can evolve safely.
4. **Always run with `MESHCALL_STRICT=1` in dev/CI**: Strict mode turns contract warnings (`W001`–`W006`) into immediate startup errors with a `hint` telling you exactly what to add.

| Annotation | Meaning | Auto-retried by `RetryPolicy`? |
| --- | --- | --- |
| `read_only=True` (`readOnly: true`) | Pure query with no state mutation (implies `idempotent`) | Yes |
| `idempotent=True` (`idempotent: true`) | Mutates state, but repeating the exact call has the same effect | Yes |
| `destructive=True` (`destructive: true`) | Non-idempotent mutation (e.g., charge card, append log, delete) | Only if caller supplies `idempotency_key` |
| `deprecated="use_v2"` | Marks the method as deprecated in generated clients (`@deprecated`) | Depends on other flags |

---

## 3. The Three Call Shapes: Unary, Server Stream, Client Stream

**Benefit:** Stream items are flow-controlled per call (initial window of 16 items). A slow consumer naturally throttles the producer without buffering unbounded data in memory, and other calls on the same WebSocket connection continue fairly.

### Python Service & Client

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcContext, RpcInputStream, RpcLogger, method, service


class Chunk(BaseModel):
    text: str


class Summary(BaseModel):
    count: int
    combined: str


@service("text.v1.TextProcessor")
class TextProcessor:
    """Process text via unary, server-stream, and client-stream RPCs."""

    # 1. Unary: parameters -> Model
    @method(read_only=True)
    async def echo(self, text: str, ctx: RpcContext) -> Chunk:
        """Echo the input text and attach a response trailer."""
        ctx.set_trailer("length", str(len(text)))
        return Chunk(text=text)

    # 2. Server Stream: parameters -> AsyncIterator[Model]
    @method.server_stream(read_only=True)
    async def tokenize(self, sentence: str, logger: RpcLogger) -> AsyncIterator[Chunk]:
        """Yield each word in sentence as a separate stream item."""
        logger.info("tokenizing sentence")
        for word in sentence.split():
            yield Chunk(text=word)

    # 3. Client Stream: RpcInputStream[Model] + parameters -> Model
    @method.client_stream(idempotent=True)
    async def join_chunks(
        self,
        chunks: RpcInputStream[Chunk],
        separator: str = " ",
    ) -> Summary:
        """Consume a stream of Chunk items and join them with separator."""
        parts: list[str] = []
        async for chunk in chunks:
            parts.append(chunk.text)
        return Summary(count=len(parts), combined=separator.join(parts))
```

**Calling all 3 shapes from a generated Python client:**

```python
async with TextProcessorClient(driver) as client:
    # Unary
    chunk = await client.echo("hello", timeout=5.0)

    # Server stream (use `async with` if you might break early so it cancels cleanly)
    async with client.tokenize("one two three", timeout=10.0, idle_timeout=3.0) as stream:
        async for item in stream:
            print(item.text)

    # Client stream
    call = await client.join_chunks(separator="-", timeout=10.0)
    await call.send(Chunk(text="a"))
    await call.send(Chunk(text="b"))
    await call.close_send()
    summary = await call
    print(summary.combined)  # "a-b"
```

### Subscriptions: Streams That Resume

For events, notifications and live feeds, declare a **subscription**: a server stream whose item carries an ordered cursor (`seq`) and whose request takes an optional resume parameter (`since`). Generated clients reopen it on their own after a dropped connection or a retryable error, passing the last `seq` as `since`, and keep yielding items as one iterator; subscriptions have no default timeouts. The handler must yield only items after `since`.

`Topic` (Python and TypeScript) fans items out to every subscriber in one process and keeps a bounded history to replay from. A subscriber that falls too far behind is cut off with retryable `resource_exhausted` (`reason: "slow_subscriber"`) and resubscribes; a cursor older than the history fails with `invalid_argument` (`reason: "cursor_expired"`) instead of silently skipping events. Across instances, make the producing service `targeted` or bridge a broker into each instance's Topic: the Router is not a broker.

```python
from meshcall import Topic


class Event(BaseModel):
    seq: int
    text: str


@service("feed.v1.Feed")
class Feed:
    """Live events."""

    def __init__(self) -> None:
        self.topic: Topic[Event] = Topic(history=1000, cursor=lambda e: e.seq)

    @method.subscription(cursor="seq", resume="since", read_only=True)
    async def events(self, since: int | None = None) -> AsyncIterator[Event]:
        """Every event after ``since``."""
        async for event in self.topic.subscribe(after=since):
            yield event

# Publish from anywhere (any thread): feed.topic.publish(Event(seq=n, text="..."))
# Consume: async for event in client.events(): ...
```

```typescript
import { defineType, subscriptionMethod, Topic } from "meshcall";

interface Event { seq: number; text: string }
const EventType = defineType<Event>("Event", {
  type: "object", properties: { seq: { type: "integer" }, text: { type: "string" } }, required: ["seq", "text"],
});
const Since = defineType<{ since?: number | null }>("Since", {
  type: "object", properties: { since: { type: ["integer", "null"], default: null } },
});
const topic = new Topic<Event>({ history: 1000, cursor: (e) => e.seq });

// inside defineService({ methods: { ... } }):
events: subscriptionMethod({
  description: "Every event after since.", readOnly: true, cursor: "seq", resume: "since",
  request: Since, outputItem: EventType,
  handler: async function* ({ since }) {
    for await (const event of topic.subscribe(since)) yield event;
  },
}),
// Consume: for await (const event of client.events()) { ... }
```

In Go, `meshcall.HandleSubscription(svc, "events", "seq", "since", handler)` declares the same contract (Go has no `Topic`); generated Go clients return a `*meshcall.Subscription` that resubscribes the same way.

### Duplex: The Shape of Last Resort

A duplex method (`@method.duplex(reason="…")` with a `channel: RpcDuplex[In, Out]` parameter, TypeScript `duplexMethod({ reason, … })`, Go `meshcall.HandleDuplex(svc, name, reason, handler)`) streams input and output at the same time. Use it only when output must interleave with input that keeps arriving within one call, such as a voice turn answered while the user is still speaking. It is never retried or deduplicated, takes no idempotency key, cannot be `idempotent` / `read_only`, is unreachable over HTTP and pins one instance for its whole life; the `reason` goes into the contract. A side that only sends stalls once the other side's credit runs out, so read output concurrently with sending. Try a server stream plus unary calls, or a client stream, first.

---

## 4. Generating & Using Typed Clients

**Benefit:** One Go CLI (`meshcall generate`) generates Python, TypeScript and Go clients from either a Python `module:Class` or a portable `.meshcall.json` file. Generated clients validate both requests **and** responses against the contract schema.

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

### Calling from a Browser, an Edge Runtime or curl

Browsers cannot set the `Authorization` header on a WebSocket, so web and edge code calls the Router's HTTP API (same port, under `/v1/`). Generated TypeScript clients accept `MeshCallHttpClient` from `meshcall/web` (no Node dependency) in place of `MeshCallClient`; unary calls, streams and subscriptions work, duplex does not.

```typescript
import { MeshCallHttpClient } from "meshcall/web";
import { TextProcessorClient } from "./generated/text-ts/src/index.js";

const client = new TextProcessorClient(new MeshCallHttpClient("https://router.example", { token }));
const chunk = await client.echo("hello", { timeoutMs: 5_000 });
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
from meshcall import Balance, method, service

@service("compute.v1.Worker")
class Worker:
    """Stateless and session-affine compute methods."""

    # Route to the instance with the fewest active in-flight calls
    @method(read_only=True, balance=Balance.least_inflight())
    async def heavy_compute(self, payload: str) -> Result:
        """Run CPU/GPU-intensive task on the least busy worker."""
        ...

    # Route calls with the same session_id to the same instance when pool is stable
    @method(idempotent=True, balance=Balance.sticky("session_id"))
    async def step_session(self, session_id: str, prompt: str) -> Result:
        """Run a session step with soft cache affinity."""
        ...
```

### Stateful Instance-Targeted Services (`targeted=True`)

When every instance holds distinct state (e.g., `gpu-worker-0` holds Model A and `gpu-worker-1` holds Model B), mark the service `targeted=True`.
- The Router rejects any call that omits `instance_id` with `invalid_argument` (`reason: "instance_required"`).
- Generated Python and TypeScript clients make `instance` a **required compile-time parameter**, so forgetting to specify the target worker is caught by `pyright` / `tsc` before code ever runs!

```python
@service("gpu.v1.ModelRunner", targeted=True)
class ModelRunner:
    """Stateful GPU worker holding a specific loaded model."""

    @method(read_only=True)
    async def infer(self, prompt: str, ctx: RpcContext) -> Output:
        """Run inference on this specific GPU instance."""
        return Output(text=f"Processed by {ctx.instance_id}: {prompt}")
```

Calling a targeted service:

```python
# `instance=` is required by the generated client signature:
res = await client.infer("Hello!", instance="gpu-worker-1", timeout=10.0)
```

Or from the CLI:

```bash
uv run meshcall call gpu.v1.ModelRunner.infer '{"prompt": "Hello!"}' --instance gpu-worker-1
```

---

## 7. Production Reliability: Timeouts, Retries, Idempotency & Drain

**Benefit:** Achieve high availability, automatic retries, duplicate suppression, and zero-downtime rolling deployments **without** a stateful message broker or database in the Router.

### 1. Three-Layer Relative Deadlines

Always pass `timeout=` (Python, seconds) or `timeoutMs` (TypeScript, milliseconds).
- **Server** enforces at `T` (cancels handler task).
- **Router** enforces at `T + 500ms` (protects caller if server hangs).
- **Client** enforces at `T + max(1s, 10%)` (protects caller if network/Router hangs).
- Inside a handler, check `ctx.remaining()` (Python, seconds) or `ctx.remainingMs()` (TS, ms) before starting expensive sub-operations.

### 2. Automatic Retries & Idempotency Keys

Configure `RetryPolicy` on the client. It automatically retries failed **unary** calls when:
1. The error is marked `retryable=True` (e.g. `unavailable` when an instance disconnects, or `resource_exhausted` when overloaded), **AND**
2. Either the method is annotated `read_only=True` / `idempotent=True` in the contract, **OR** the caller supplies an `idempotency_key`.

When a unary call carries an `idempotency_key`, the Router deduplicates concurrent calls with the same key and replays the cached terminal result for 60 seconds.

```python
from meshcall import RetryPolicy

# Enable automatic retries on the generated client (max 3 attempts, exponential backoff)
client = CounterServiceClient(
    driver,
    retry=RetryPolicy(max_attempts=3, initial_backoff=0.05, max_backoff=1.0),
)
```

### 3. Backpressure (`max_inflight`) & Circuit Breaking

Set `max_inflight` on your server so the Router routes around saturated workers instead of piling up queues:

```python
server = RpcServer(services=[Worker()], driver=driver, max_inflight=32)
```

If an instance repeatedly times out or returns `internal`/`unavailable` (default: 5 consecutive failures), the Router's per-instance circuit breaker trips for 10 seconds and probes with a single call before restoring traffic.

### 4. Multi-Router High Availability

Run two or more independent `meshcall-router` processes. Pass the list of URLs to both servers and clients—servers register with **all** Routers simultaneously, and clients fail over automatically when one Router goes down:

```python
routers = ["ws://router-1.internal:8765", "ws://router-2.internal:8765"]
server_driver = WebSocketRouterServerDriver(routers, instance_id="worker-1")
client_driver = WebSocketClientDriver(routers)
```

### 5. Zero-Downtime Rolling Restarts (`drain=True`)

When shutting down a server (e.g., on `SIGTERM`), drain it so the Router immediately stops sending new calls while in-flight calls finish cleanly:

```python
# Python
await server.stop(drain=True, drain_timeout=30.0)
```

```typescript
// TypeScript
await server.close({ drain: true, drainTimeoutMs: 30_000 });
```

Go: `server.Drain(ctx)`, then `server.Close()`.

### 6. Router Shutdown, Limits & Metrics

Restart a Router with SIGTERM (or SIGINT): it drains first. `/readyz` answers 503, new calls get retryable `unavailable` (`reason: "router_draining"`), idle connections close with 1001 so clients and services move to another Router, and calls still in flight after `--shutdown-timeout` (default 10 s) end with retryable `unavailable` (`reason: "router_shutdown"`). A second signal skips the wait. A Router owned by the Python or TypeScript launcher is stopped by closing its stdin, which ends calls in flight at once.

| Flag | Default | Effect |
| --- | --- | --- |
| `--max-calls-per-connection` | 1000 (`0`: no limit) | a further call on that connection fails with retryable `resource_exhausted` / `overloaded`; the connection stays open |
| `--max-frame-size` | 64 MiB (also the maximum) | one payload or stream item must fit in a frame; send larger data as a stream |
| `--outbox-size` | 256 MiB | how much the Router may owe a peer that stops reading before disconnecting it |
| `--metrics-listen` | off | Prometheus text at `http://ADDR/metrics` on a separate listener, e.g. `127.0.0.1:9464` |

Every flag also has a `MESHCALL_ROUTER_<FLAG>` environment variable; the launchers take `max_frame_size` / `maxFrameSize` and `outbox_size` / `outboxSize`.

---

## 8. Authentication & Temporary Scoped Tokens

**Benefit:** Lock down production Routers with PBKDF2-hashed account ACLs (`router-auth.json`), and mint short-lived, least-privilege bearer tokens (`RouterAuthClient`) for untrusted callers or ephemeral workers.

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

Start the Router with `WebSocketRouter(port=8765, auth_file="router-auth.json")` (or `--auth-file router-auth.json`).

### 2. Issue & Use Temporary Scoped Tokens

```python
from meshcall import RouterAuthClient, RouterCredentials, RouterScope
from meshcall.drivers import WebSocketClientDriver

admin_creds = RouterCredentials(username="backend-app", password="...")
async with RouterAuthClient(WebSocketClientDriver("ws://127.0.0.1:8765", auth=admin_creds)) as auth:
    # Mint a 15-minute token restricted to calling only `chat.v1.Chat/send`
    issued = await auth.issue_token(
        ttl_seconds=900,
        scopes=[RouterScope(service="chat.v1.Chat", methods=["send"])],
    )

# Hand `issued.token` to a downstream caller:
scoped_driver = WebSocketClientDriver(
    "ws://127.0.0.1:8765",
    auth=RouterCredentials(token=issued.token),
)
```

### 3. Anonymous Callers & API Keys

A caller that sends no `Authorization` header is the reserved user `anonymous`. It may connect, but every call is `permission_denied` unless the auth file grants it `call` rules; only the auth file can make a method public.

```json
{ "username": "anonymous", "roles": ["client"], "call": ["status.v1.Health/*"] }
```

Give machine callers (webhooks, CI, external services) long-lived **API keys** instead of passwords:

```bash
meshcall-router new-api-key ci-bot   # prints the key once, and the entry to add under "api_keys"
```

Fill in the entry's `call` list and restart the Router. Callers send the key as `Authorization: Bearer mk_…`: `RouterCredentials(token="mk_…")` in Python, `{ auth: { token: "mk_…" } }` in TypeScript, `ClientOptions{Authorization: "Bearer mk_…"}` in Go, `-H 'Authorization: Bearer mk_…'` with curl. A key is checked with one hash lookup; rotate it by adding a new key and removing the old one.

---

## 9. Interceptors, Metadata & Structured Errors

**Benefit:** Attach tracing/auth headers across all calls in one place with interceptors, and catch specific error classes with structured field `violations` instead of parsing error strings.

### Client & Server Interceptors (Python)

```python
from meshcall import ClientCallInfo, ClientCallNext, RpcContext, ServerCallNext

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
```

### OpenTelemetry Tracing

Add the OpenTelemetry interceptors on both sides. Trace context travels as W3C `traceparent` / `tracestate` call metadata, so one trace continues through the Router and across languages; each call gets a `service/method` span and `meshcall.calls` / `meshcall.call.duration` metrics. With no OpenTelemetry SDK configured they do nothing.

```python
# pip install "meshcall[otel]"
from meshcall.otel import client_interceptor, server_interceptor

client = TextProcessorClient(driver, interceptors=[client_interceptor()])
server = RpcServer(services=[TextProcessor()], driver=server_driver, interceptors=[server_interceptor()])
```

```typescript
// needs the optional peer dependency @opentelemetry/api
import { otelClientInterceptor, otelServerInterceptor } from "meshcall/otel";

const client = new MeshCallClient(url, undefined, { interceptors: [otelClientInterceptor()] });
const server = new MeshCallServer({ services, router: { endpoint: url }, interceptors: [otelServerInterceptor()] });
```

Go has no OpenTelemetry package; a `ClientInterceptor` / `ServerInterceptor` can set or read the `traceparent` metadata key itself.

### Handling Typed Errors

Every protocol error maps to a dedicated exception class (`InvalidArgumentError`, `DeadlineExceededError`, `UnavailableError`, `ResourceExhaustedError`, `PermissionDeniedError`, `MethodNotFoundError`, `ProtocolError`, `InternalError`, `CallCancelledError`):

```python
from meshcall import DeadlineExceededError, InvalidArgumentError, UnavailableError

try:
    await client.add(10, 20, timeout=2.0)
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

