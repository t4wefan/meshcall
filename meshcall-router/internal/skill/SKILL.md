---
name: meshcall
description: Explore, call and build MeshCall services (typed streaming RPC between Python, TypeScript and Go through the MeshCall Router). Use when a project depends on the meshcall package or meshcall-go module, when asked to call or inspect a mesh service, or when writing or changing an @service / defineService / meshcall.NewService.
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

Each method line shows shape (`unary` / `server_stream` / `client_stream` /
`duplex`), parameters, annotations (`read_only`, `idempotent`,
`destructive`, `deprecated`, `subscription` where declared), the description
and, for duplex, its reason. `--router ws://host:port` or
`MESHCALL_ROUTER_URL` selects the Router (default `ws://127.0.0.1:8765`;
`unix:///path` for a Unix socket, `pipe://NAME` for a Windows named pipe);
`--auth user:pass` / `--token T` when accounts are configured.

## Call

```bash
meshcall call chat.v1.Chat.send '{"session_id":"s1","text":"hi"}'
meshcall call demo.v1.Counter.count '{"n":5}'                 # server stream: one JSON line per item
printf '{"value":1}\n{"value":2}\n' | meshcall call demo.v1.Sum.sum '{"offset":10}' --stdin
meshcall call chat.v1.Chat.talk '{"session_id":"s1"}' --stdin   # duplex: each stdin line is sent as read
```

Flags: `--json` (machine-readable output and errors), `--timeout 10s`,
`--metadata k=v`, `--idempotency-key K`, `--idle-timeout D`, `--instance ID`
(address one instance; required for services `inspect` marks `[targeted]`,
and the error hint lists the live instance ids). The request is one JSON
object whose keys are the method's parameters. Do not write throwaway
scripts to probe a service; use these two commands.

Over plain HTTP (same port, no client needed): `curl
http://HOST:8765/v1/call/svc.v1.Name/method -d '{json}'`. Server streams
come back as NDJSON envelopes: `{"item": …}` per item, then `{"end":
{"trailers": …}}` or `{"error": …}` (read `.item` with `jq`); client streams
take an NDJSON body (first line the request); duplex is WebSocket-only.
Headers: `Authorization`, `MeshCall-Timeout-Ms` (unary calls default to 30
s), `Idempotency-Key`, `MeshCall-Instance`, `MeshCall-Meta-<key>`. Errors are
the same JSON envelope with an HTTP status (400, 401, 403, 404, 429, 503,
504…). `GET /v1/openapi.json` describes every method you may call (feed it to
Swagger UI or a client generator); `GET /v1/services` is the Registry.
`meshcall call … --http` does the same from the CLI. In a browser or edge
runtime, pass `new MeshCallHttpClient(url, { token })` from `meshcall/web`
to the generated TypeScript client instead of `MeshCallClient` (no duplex;
add `--http-cors-origin` for other origins).

## Read errors

Every error has `code`, `message`, `details.source` (`client` | `router` |
`server`) and often `details.reason`.

| code | meaning | what to do |
| --- | --- | --- |
| `invalid_argument` | your request failed validation; `details.violations` lists `{path, code, message}` per field. `reason: instance_required` means the service is targeted; `idempotency_key_on_duplex` that duplex calls take no key; `cursor_expired` that a subscription's resume cursor is older than the server's history | fix the named fields; pass `--instance` / `instance=` for targeted services; drop the key; resubscribe without a cursor |
| `method_not_found` | wrong service or method | the hint lists available methods |
| `unavailable` | no instance (`no_instances`), instance lost, every breaker open (`circuit_open`), the named instance is unknown / draining, or the Router is stopping (`router_draining`, `router_shutdown`) | `router inspect`; retry if `retryable` (another Router if you have several) |
| `resource_exhausted` | Router, connection or every instance at capacity (`overloaded`); a subscriber fell behind (`slow_subscriber`); a request, response or stream item over the frame limit (`frame_too_large`, not retryable) | back off and retry; a subscription resumes by itself; split oversized data into stream items |
| `protocol_error` | a stream item beyond the consumer's credit (`flow_control_violation`) or another frame that broke the protocol | fix the producer; regenerate a hand-patched client |
| `deadline_exceeded` | `timeout_ms` elapsed (source says who noticed) | raise the timeout or make the handler faster |
| `permission_denied` | the caller (possibly the anonymous user: no credentials sent) may not make this call; the same error whether or not the method exists | send credentials, or add `service/method` to the account, key or token |
| `internal` | the service failed; `reason: invalid_response` means its own handler broke its contract | read the service's log, fix the service |

Never guess at parameters when `invalid_argument` tells you the path.

## Write a service

**1:1 Server ↔ Client parameter lists.** Whatever business parameters a server
method declares (after stripping DI dependencies and `meta` parameters), the
generated client method in Python, TypeScript and Go takes that **exact same
parameter list and order** (0 parameters → `()`, N parameters → `(p1, p2, …)`,
client stream → `(p1, …, items)`). Service names carry a version and are
normalised (`chat` → `chat.v0`; `chat.v1`, `demo.v1.Greeting` are fine).
Docstrings/descriptions and annotations are part of the contract: tooling,
retries and LLM tool exposure depend on them.

Python (`@query`, `@stream`, `@mutates`, `DuplexChannel`, `@dependency`, `meta`):

```python
from collections.abc import AsyncIterator
from typing import Annotated
from pydantic import BaseModel
from meshcall import DuplexChannel, Router, dependency, meta, mutates, query, run, service, stream

class Message(BaseModel):
    text: str

@dependency
async def current_user(authorization: Annotated[str, meta("authorization")]) -> str:
    return authorization.removeprefix("Bearer ")

@service("chat.v1.Chat")
class Chat:
    """Chat sessions."""

    @query
    async def greet(self, name: str) -> Message:
        """Pure read-only query (`read_only=True, idempotent=True`)."""
        return Message(text=f"hello {name}")

    @mutates(idempotent=True)
    async def send(self, session_id: str, text: str, user: str = current_user) -> Message:
        """Append a message and return the reply (`user` is injected on the server only)."""
        return Message(text=f"{user}: {text}")

    @stream
    async def history(self, session_id: str) -> AsyncIterator[Message]:
        """Server stream (`AsyncIterator[Out]` return type)."""
        yield Message(text="…")

    @stream
    async def ingest(self, session_id: str, items: AsyncIterator[Message]) -> Message:
        """Client stream (ordinary parameters first, `AsyncIterator[In]` parameter last)."""
        async for item in items: ...
        return Message(text="ok")

    @mutates(reason="Interactive voice/chat turn")
    async def talk(self, session_id: str, channel: DuplexChannel[Message, Message]) -> None:
        """Duplex stream (`DuplexChannel[In, Out]` parameter, returns `None`)."""
        async for msg in channel:
            await channel.send(Message(text=f"ack {msg.text}"))

if __name__ == "__main__":
    run(Chat(), router=Router("ws://127.0.0.1:8765"))
```

TypeScript (`@query`, `@stream`, `@mutates`, `dependency`, `meta`, `serve`, `Router`):

```typescript
import { dependency, meta, mutates, query, Router, serve, service, stream, z, type DuplexChannel } from "meshcall";

const Message = z.object({ text: z.string() }).meta({ id: "Message" });

const currentUser = dependency(
  (authorization: string) => authorization.replace(/^Bearer\s+/, ""),
  { inject: [meta("authorization", { default: "" })] },
);

@service("chat.v1.Chat", { description: "Chat sessions." })
export class Chat {
  @mutates({
    description: "Append a message from the caller.", idempotent: true,
    input: { session_id: z.string(), text: z.string() }, response: Message,
    inject: [currentUser],
  })
  async send(session_id: string, text: string, user: string) {
    return { text: `${user}: ${text}` };
  }
}

await serve(new Chat(), { router: new Router("ws://127.0.0.1:8765") });
```

Go (`github.com/t4wefan/meshcall/meshcall-go`; `HandleUnary` and friends,
`Dependency`, `Meta[T]`, `WithDependencies`, `Serve`, `NewRouter`):

```go
chat := meshcall.NewService("chat.v1.Chat", meshcall.Describe("Chat sessions."))
meshcall.HandleUnary(chat, "send", func(ctx *meshcall.Context, req SendRequest) (Message, error) {
	return Message{Text: "echo " + req.Text}, nil
}, meshcall.Doc("Append a message and return the reply."), meshcall.Idempotent())

_ = meshcall.Serve(ctx, []meshcall.Service{chat}, meshcall.WithRouter(meshcall.NewRouter("ws://127.0.0.1:8765")))
```

Rules the runtime enforces or warns about (set `MESHCALL_STRICT=1` while
you edit so warnings fail fast with a `hint` that says exactly what to add):

- Every service and method has a description (docstring in Python; required
  by the decorator types in TypeScript; `meshcall.Doc(...)` in Go).
- A service whose instances hold distinct state (a session, a loaded model,
  an actor) is `targeted=True` / `targeted: true`: callers must name the
  instance (`instance="worker-3"`, generated clients require it, handlers
  read `ctx.instance_id` / `ctx.instanceId`).
- `@query` marks pure reads (`read_only=True, idempotent=True`); `@mutates(idempotent=...)`
  or `@mutates(destructive=True)` marks state-changing methods. Retry policies
  only retry idempotent calls.
- Raise `MeshCallError(code, message, retryable=…, details=…)` for
  application errors; anything else becomes a sanitised `internal`.
- Dependencies (`@dependency` / `dependency()` / `Provide()`) and `meta("key")`
  are resolved before the handler runs and are never exposed in the public
  contract or generated clients. `ctx: RpcContext` and `logger: RpcLogger`
  remain available when needed.

## Unified `Router` Object & Calling from Code

Generate a typed client instead of hand-writing frames:

```bash
uv run meshcall generate my_app.service:Chat --output generated/chat-client
uv run meshcall generate my_app.service:Chat --language typescript --output generated/chat-ts
meshcall generate service.meshcall.json --language go --output generated/chatgo
```

`Router` is the unified handle for:
1. Passing to `serve(...)` / `run(...)` when starting services,
2. Sharing a single connection across multiple generated clients (`ChatClient.connect(router)`), and
3. Calling built-in Router services (`router.list_services()`, `router.describe_service(name)`, `router.whoami()`, `router.issue_token(...)`, `router.revoke_token(...)`).

Per-call options (`timeout`, `idle_timeout`, `metadata`, `idempotency_key`, `instance`)
are configured via `.with_options(...)` / `.withOptions(...)` / `.WithOptions(...)` so
business method signatures stay 100% identical to the server handler:

```python
router = Router("ws://127.0.0.1:8765", auth=RouterCredentials.bearer("..."))
async with router:
    services = await router.list_services()
    chat = await ChatClient.connect(router)
    reply = await chat.with_options(timeout=5.0, idempotency_key="k1").send("s1", "hello")
```

TypeScript:
```typescript
const router = new Router("ws://127.0.0.1:8765", { token: "..." });
const chat = await ChatClient.connect(router);
const reply = await chat.withOptions({ timeoutMs: 5000, idempotencyKey: "k1" }).send("s1", "hello");
```

Go:
```go
router := meshcall.NewRouter("ws://127.0.0.1:8765", meshcall.WithAuthToken("..."))
defer router.Close()
chat := chatclient.NewChatClient(router)
reply, err := chat.WithOptions(meshcall.CallOptions{Timeout: 5 * time.Second}).Send(ctx, "s1", "hello")
```

## Best practices

The shape of a good MeshCall project (`best-practice/` in the MeshCall repo
is a complete example):

```text
my_app/
├── src/my_app/service.py      # @service classes and their Pydantic models; no transport code
├── src/my_app/server.py       # RpcServer + driver + signal handling, config from env
├── generated/<svc>-client/    # generated client package, checked in, regenerated on contract change
└── tests/test_service.py      # in-process Router + server on port 0, generated client against it
```

**Designing a service**

- One service per bounded capability (`chat.v1.Chat`, `billing.v1.Invoices`);
  methods are verbs on it. Bump the version segment (`chat.v2.Chat`) for an
  incompatible change and run both until clients move; never edit a shipped
  method's parameters in place. Mark replaced methods
  `deprecated="new_method"` so clients see it at compile time. *Adding* a
  method is a rolling deploy: the Router routes each method over the
  instances that have it, so new and old instances coexist.
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
- Events / notifications / live feeds: a subscription,
  `@method.subscription(cursor="seq", resume="since")` on a server stream
  whose item has an ordered `seq` and whose request has an optional
  `since`. Generated clients then resubscribe on their own after a dropped
  connection or retryable error, passing the last `seq` as `since`; the
  handler must yield only items after `since`. Fan out inside one process
  with `Topic[T]` (`Topic(history=1000, cursor=lambda e: e.seq)`,
  `topic.publish(e)`, handler `async for e in topic.subscribe(after=since):
  yield e`). Across instances, make the producing service `targeted` or
  bridge a broker into each instance's Topic; the Router is no broker.
- Duplex (`@method.duplex(reason="…")`, handler takes `channel:
  RpcDuplex[In, Out]`, returns `None`) only when output must interleave
  with input that keeps arriving *in one call* (a voice or chat turn that
  is answered while the user is still sending). It gives up retry,
  deduplication, idempotency keys, `idempotent` / `read_only`, HTTP access
  and rebalancing (one instance for the whole call). First try a server
  stream plus unary calls (`subscribe` + `send`), or a client stream. If you
  use it, read output concurrently with sending (a task per direction): a
  side that only sends stalls once the other side's credit runs out.
- Respect cancellation and deadlines: check `ctx.remaining()` before an
  expensive step; in Python an `asyncio.CancelledError` means the caller
  left, clean up and re-raise. TypeScript handlers get `ctx.signal`.
- Log with the injected `logger` (it already carries service, method,
  `call_id`); never `print`. For tracing, add the OpenTelemetry interceptors
  on both sides (`meshcall.otel.client_interceptor()` /
  `server_interceptor()` with `pip install "meshcall[otel]"`, `meshcall/otel`
  in TypeScript): trace context
  rides in call metadata across languages and the Router. Router metrics:
  `--metrics-listen 127.0.0.1:9464`. Put request-scoped facts in trailers
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

- Start a real `WebSocketRouter(host="127.0.0.1", port=0)` and register `RpcServer`
  with `WebSocketRouterServerDriver` (TypeScript: `new MeshCallServer({ services, router: { endpoint } })`)
  in a fixture, then call it through the generated client: this exercises
  routing, validation, streaming and error mapping end to end.
- Assert on error *classes* and `details`, not message text.

## Operate

- Router binary: it ships inside the SDK packages (platform wheel / npm
  platform package), so nothing to install; `meshcall-router --port 8765`
  runs it. `uv run meshcall router install` only matters on a platform
  without a bundled binary.
- Agent skill installation: `meshcall init [--agent universal|claude|cursor|copilot|windsurf|all] [--force]`
  writes `.agents/skills/meshcall/SKILL.md` and `AGENTS.md` by default, or
  the agent-specific skill/rule files (`CLAUDE.md`, `.cursor/rules/meshcall.mdc`,
  `.github/copilot-instructions.md`, `.windsurf/rules/meshcall.md`) when
  `--agent` is specified.
- Rolling restart: `server.stop(drain=True)` / `server.close({ drain: true })`
  leaves the pool first, so no request fails. Restarting a Router: send it
  SIGTERM; it refuses new calls with retryable `unavailable` /
  `router_draining`, closes idle connections with 1001 so peers move to
  another Router, and gives calls in flight `--shutdown-timeout` (default
  10 s) before ending them with `unavailable` / `router_shutdown`.
- `--max-calls-per-connection` (default 1000, `0` unlimited) bounds one
  connection's calls in flight; beyond it a call gets retryable
  `resource_exhausted` / `overloaded`.
- Several Routers: give services and clients the list of endpoints; no
  coordination is needed because the Router keeps no state peers cannot rebuild.
- Configure the Router with flags or `MESHCALL_ROUTER_<FLAG>` variables
  (`MESHCALL_ROUTER_PORT`, `MESHCALL_ROUTER_IDLE_TIMEOUT`, …); put accounts in
  `MESHCALL_ROUTER_AUTH` (the auth JSON itself) or `MESHCALL_ROUTER_AUTH_FILE`.
  Callers without credentials are the `anonymous` user (grant it `call`
  rules in the auth file for public methods); give machine callers API keys
  (`meshcall-router new-api-key ID`, sent as `Authorization: Bearer mk_…`).
  `--max-connections` (default 10000) caps WebSocket connections (HTTP calls
  are not counted); over it new connections get a retryable `503`.
  One payload or stream item must fit in a frame: `--max-frame-size`
  (default and maximum 64 MiB); send larger data as a stream.
  `--outbox-size` (default 256 MiB, at least the frame size) is how much a
  peer that stops reading may be owed before the Router disconnects it.
