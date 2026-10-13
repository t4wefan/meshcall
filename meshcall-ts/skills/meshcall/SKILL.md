---
name: meshcall
description: Explore, call and build MeshCall services (typed streaming RPC between Python, TypeScript and the Go Router). Use when a project depends on the meshcall package, when asked to call or inspect a mesh service, or when writing or changing an @service / defineService.
---

# MeshCall

Typed RPC over WebSocket. Services register with a **Router**; clients call
`service.method` through it. The Router balances, enforces deadlines and keeps
every service's **contract** (schemas, descriptions, annotations), so you can
learn and use any running service without reading its source.

## Explore before you code

```bash
meshcall router inspect                      # services, instances, health, methods
meshcall router inspect chat.v1.Chat --json  # one service with its full contract
```

Each method line shows shape (`unary` / `server_stream` / `client_stream`),
parameters, annotations (`read_only`, `idempotent`, `destructive`,
`deprecated`) and the description. `--router ws://host:port` or
`MESHCALL_ROUTER_URL` selects the Router (default `ws://127.0.0.1:8765`);
`--auth user:pass` / `--token T` when accounts are configured.

## Call

```bash
meshcall call chat.v1.Chat.send '{"session_id":"s1","text":"hi"}'
meshcall call demo.v1.Counter.count '{"n":5}'                 # server stream: one JSON line per item
printf '{"value":1}\n{"value":2}\n' | meshcall call demo.v1.Sum.sum '{"offset":10}' --stdin
```

Flags: `--json` (machine-readable output and errors), `--timeout 10s`,
`--metadata k=v`, `--idempotency-key K`, `--idle-timeout D`, `--instance ID`
(address one instance; required for services `inspect` marks `[targeted]`,
and the error hint lists the live instance ids). The request is one JSON
object whose keys are the method's parameters. Do not write throwaway
scripts to probe a service; use these two commands.

## Read errors

Every error has `code`, `message`, `details.source` (`client` | `router` |
`server`) and often `details.reason`.

| code | meaning | what to do |
| --- | --- | --- |
| `invalid_argument` | your request failed validation; `details.violations` lists `{path, code, message}` per field. `reason: instance_required` means the service is targeted | fix the named fields; pass `--instance` / `instance=` for targeted services |
| `method_not_found` | wrong service or method | the hint lists available methods |
| `unavailable` | no instance (`no_instances`), instance lost, every breaker open (`circuit_open`), or the named instance is unknown / draining | `router inspect`; retry if `retryable` |
| `resource_exhausted` | Router or every instance at capacity (`overloaded`) | back off and retry |
| `deadline_exceeded` | `timeout_ms` elapsed (source says who noticed) | raise the timeout or make the handler faster |
| `permission_denied` | account lacks `service/method` | fix the auth file or token scopes |
| `internal` | the service failed; `reason: invalid_response` means its own handler broke its contract | read the service's log, fix the service |

Never guess at parameters when `invalid_argument` tells you the path.

## Write a service

**Parameters are the request.** The handler's parameters become the request
schema; the return type is the response. Service names carry a version and
are normalised (`chat` → `chat.v0`; `chat.v1`, `demo.v1.Greeting` are fine).
Docstrings/descriptions and annotations are part of the contract: tooling,
retries and LLM tool exposure depend on them.

Python:

```python
from collections.abc import AsyncIterator
from pydantic import BaseModel
from meshcall import RpcContext, RpcInputStream, method, service

class Message(BaseModel):
    text: str

@service("chat.v1.Chat")
class Chat:
    """Chat sessions."""

    @method(idempotent=True)
    async def send(self, session_id: str, text: str, ctx: RpcContext) -> Message:
        """Append a message and return the reply."""
        return Message(text=f"echo {text}")

    @method.server_stream(read_only=True)
    async def history(self, session_id: str) -> AsyncIterator[Message]:
        """Replay the session."""
        yield Message(text="…")

    @method.client_stream()
    async def ingest(self, items: RpcInputStream[Message], session_id: str) -> Message:
        """Store streamed messages."""
        async for item in items: ...
        return Message(text="ok")
```

TypeScript (schemas are explicit because types are erased):

```typescript
import { defineService, defineType, unaryMethod, serverStreamMethod } from "meshcall";

const Message = defineType<{ text: string }>("Message", {
  type: "object", properties: { text: { type: "string" } }, required: ["text"],
});
const Send = defineType<{ session_id: string; text: string }>("Send", {
  type: "object", properties: { session_id: { type: "string" }, text: { type: "string" } },
  required: ["session_id", "text"],
});

export const chat = defineService({
  name: "chat.v1.Chat", description: "Chat sessions.",
  sourceModule: "chat", sourceQualname: "Chat",
  methods: {
    send: unaryMethod({ description: "Append a message and return the reply.", idempotent: true,
      request: Send, response: Message, handler: ({ text }) => ({ text: "echo " + text }) }),
  },
});
```

Rules the runtime enforces or warns about (set `MESHCALL_STRICT=1` while
you edit so warnings fail fast with a `hint` that says exactly what to add):

- Every service and method has a description (docstring in Python).
- A service whose instances hold distinct state (a session, a loaded model,
  an actor) is `targeted=True` / `targeted: true`: callers must name the
  instance (`instance="worker-3"`, generated clients require it, handlers
  read `ctx.instance_id` / `ctx.instanceId`). Ordinary services may still be
  called with `instance=` to pin one node, e.g. for diagnostics.
- A method that mutates state is annotated `idempotent=True` or
  `destructive=True`; pure reads are `read_only=True`. Retry policies only
  retry idempotent calls.
- Raise `MeshCallError(code, message, retryable=…, details=…)` for
  application errors; anything else becomes a sanitised `internal`.
- Use `ctx: RpcContext` for metadata, `ctx.remaining()` and
  `ctx.set_trailer()`; `logger: RpcLogger` for logging. Both are injected,
  never part of the request.

Run it through the Router (Python):

```python
from meshcall import RpcServer
from meshcall.drivers import WebSocketRouterServerDriver
server = RpcServer(services=[Chat], driver=WebSocketRouterServerDriver("ws://127.0.0.1:8765", instance_id="chat-1"))
await server.start()
```

TypeScript: `new MeshCallServer({ services: [chat], router: { endpoint: "ws://127.0.0.1:8765", instanceId: "chat-1" } })`.

## Call from code

Generate a typed client instead of hand-writing frames:

```bash
uv run meshcall generate my_app.service:Chat --output generated/chat-client            # Python package
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
```

Generated methods take the same parameters as the handler plus `timeout=`,
`idle_timeout=`, `metadata=`, `idempotency_key=`. Clients raise one class per
code (`InvalidArgumentError`, `DeadlineExceededError`, `UnavailableError`,
…); catch those rather than comparing `code` strings. For a quick untyped call
in TypeScript: `new MeshCallClient(uri).unary("chat.v1.Chat", "send", {...})`.

## Best practices

The shape of a good MeshCall project (`best-practice/` in the MeshCall repo
is a complete example):

```text
my_app/
├── src/my_app/service.py      # @service classes and their Pydantic models; no transport code
├── src/my_app/server.py       # RpcServer + driver + signal handling, config from env
├── generated/<svc>-client/    # generated client package, checked in, regenerated on contract change
└── tests/test_service.py      # in-process server on port 0, generated client against it
```

**Designing a service**

- One service per bounded capability (`chat.v1.Chat`, `billing.v1.Invoices`);
  methods are verbs on it. Bump the version segment (`chat.v2.Chat`) for an
  incompatible change and run both until clients move; never edit a shipped
  method's parameters in place. Mark replaced methods
  `deprecated="new_method"` so clients see it at compile time.
- Keep the request flat: parameters are the request. Reach for a model-typed
  parameter only for a reusable structure (`profile: Profile`); never wrap
  everything in one `request` object.
- Return a model, never a bare scalar or dict: `-> Message`, not `-> str`.
  Response models are the contract other languages generate from.
- Service instances hold process-local state and dependencies: construct
  them yourself (`RpcServer(services=[Chat(store, llm)])`) and guard shared
  state with a lock; the server reuses one instance across concurrent calls.
- Annotate honestly. `read_only` implies `idempotent`; generated clients
  mark idempotent calls so `RetryPolicy` retries them on retryable
  `unavailable` / `resource_exhausted`. `destructive` methods are never
  retried without an idempotency key, and tooling asks before invoking them.
  A method that creates a record is *not* idempotent unless it dedupes on
  `ctx.idempotency_key`.
- Errors: raise `MeshCallError("my_app.v1.quota_exceeded", "…", retryable=False,
  details={"limit": 100})` for domain failures with a namespaced code;
  use the standard codes (`invalid_argument` for semantic validation the
  schema cannot express, `permission_denied`, `resource_exhausted`) when they
  fit. Let unexpected exceptions propagate: they become a sanitised
  `internal` and are logged with the traceback. Do not catch-and-wrap.
- Streams: a server stream per *long or incremental* result (LLM tokens,
  export rows); a client stream when the caller produces data over time
  (uploads, telemetry). Yield as you go, do not buffer the whole result.
  Credit-based flow control is automatic, so a slow consumer slows the
  producer rather than exhausting memory. Streams are never retried: make
  the caller able to restart from a known point.
- Respect cancellation and deadlines: check `ctx.remaining()` before an
  expensive step; in Python an `asyncio.CancelledError` means the caller
  left, clean up and re-raise. TypeScript handlers get `ctx.signal`.
- Log with the injected `logger` (it already carries service, method,
  `call_id`); never `print`. Put request-scoped facts in trailers
  (`ctx.set_trailer("cache", "hit")`) when callers should see them.

**Calling a service**

- Use the generated client, check it in, regenerate when the contract
  changes (`uv run meshcall generate … --output generated/<svc>-client`); a
  CI step that regenerates and diffs catches a drifted client at compile
  time of the consumer.
- Always pass a `timeout=`; streams also get `idle_timeout=`. The service,
  the Router and the client enforce it independently, so a stuck instance
  cannot hold a call forever.
- Send an `idempotency_key=` (a UUID per logical operation) on every
  mutating unary call you might retry; the Router dedupes concurrent and
  recent duplicates, and `RetryPolicy` is then allowed to retry it.
- Propagate tracing/auth through `metadata=` using a client interceptor
  (set once, applies to every call) rather than adding parameters.
- Catch typed errors: `except UnavailableError` / `instanceof
  DeadlineExceededError`; check `error.retryable` before retrying anything
  the built-in `RetryPolicy` did not cover.

**Running it**

- Handle SIGTERM with `server.stop(drain=True, drain_timeout=…)` /
  `server.close({ drain: true })`: the instance leaves the pool first, so a
  rolling deploy fails nothing.
- Give every instance a stable, meaningful `instance_id` (`chat-<host>-<n>`);
  `router inspect`, error `details.instance_id` and `server.instance_id`
  show it. Targeted calls skip balancing and the breaker, so a stuck node
  stays reachable for diagnosis but also stays blamed by its callers.
- Set `max_inflight` on instances with a real capacity limit (GPU, DB pool)
  so the Router routes around full instances instead of queueing on them.
- Point services and clients at **all** Routers (`[uri1, uri2]`); no
  Router-side coordination is needed.
- Keep `MESHCALL_STRICT=1` in CI so missing descriptions and unannotated
  mutating methods fail the build, not a review.

**Testing**

- Start a real `RpcServer` with `WebSocketDirectServerDriver(host="127.0.0.1",
  port=0)` (TypeScript: `new MeshCallServer({ services, port: 0 })`) in a
  fixture and call it through the generated client: this exercises
  validation, streaming and error mapping, not just the handler.
- Assert on error *classes* and `details`, not message text.
- A Router is only needed in tests that cover balancing or failover;
  `WebSocketRouter(...)` launches the real binary for that.

## Operate

- Router binary: `uv run meshcall router install` or `npx meshcall-router-install`
  fetches the release matching the SDK; `meshcall-router --port 8765` runs it.
- Rolling restart: `server.stop(drain=True)` / `server.close({ drain: true })`
  leaves the pool first, so no request fails.
- Several Routers: give services and clients the list of endpoints; no
  coordination is needed because the Router keeps no state peers cannot rebuild.
