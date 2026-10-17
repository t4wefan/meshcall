# Implementation Architecture

**English** | [简体中文](zh-CN/architecture.md)

## Layers

```text
Python/Pydantic service ---- contract extraction ---+
                                                    |
TypeScript schema service ---- contract export -----+--> portable contract
                                                    |       |
Go struct-tagged service ---- WriteContract --------+       +--> Python uv package
                                                            |
                                                            +--> TypeScript Yarn package
                                                            |
                                                            +--> Go client file

Python RPC event loop <---- thread-safe bridge ----> service worker event loop
        |
        v
Logical Frame connection and fair scheduler
        |
        +---- WebSocket Direct
        |
        +---- WebSocket Router ---- service instance registry
```

The contract layer has no networking dependency. Unary is the default method
shape; `@query` / `@mutates` / `@stream` (`query` / `mutates` / `stream` in
TypeScript; `HandleUnary` / `HandleServerStream` / `HandleClientStream` in Go)
select the shape and its semantics — read-only, idempotent and destructive —
and those options travel into the contract as `read_only`, `idempotent`,
`destructive`, `description` and `deprecated`. Service parameters are the
request: they are wrapped into one generated request model for validation and
transport, and a model-typed parameter is simply one field of it. A client
stream's inbound iterator comes last, after every ordinary business parameter.
Service names are normalised to carry exactly one version segment (`chat` →
`chat.v0`). The Runtime operates on logical frames and doesn't know whether the
connection is direct or routed. WebSocket drivers own connection setup, role
handshakes, TCP and local (Unix socket / Windows named pipe, `unix_path=` /
`pipe=`) endpoints, and server registration. Duplex
(`@method.duplex(reason=…)` / `@method.duplex({ reason })` / `HandleDuplex`) is
a restricted, opt-in shape of last resort: the handler gets a channel
(`RpcDuplex` / `DuplexChannel`) to iterate input and `send` output, returns
nothing, and the call is never retried, deduplicated or reachable over HTTP;
see protocol.md → Duplex calls. Subscriptions (`@method.subscription` /
`method.subscription` / `HandleSubscription`) and `Topic` remain implemented
but are **experimental** in v0.3: they are outside the recommended path and
may change.

Parameters named `logger` / typed `RpcLogger` and `ctx` / `context` / typed
`RpcContext`, values declared with `@dependency` / `dependency()` and values
declared with `meta("key")` are dependency injection, not RPC fields. They are
removed while extracting the request model and supplied by the server per call,
so generated clients do not expose them. Docstrings and the method options are
carried into the contract as `description`, `annotations` and `deprecated`.

## The 4-layer API: `Router` / `LocalRouter`, `Mesh`, and generated clients

MeshCall separates endpoint topology (Layer 2) from peer runtime (Layer 3) and business services/clients (Layer 4):

1. **Layer 2 (`Router` and `LocalRouter`)**:
   - `Router` (Python `Router`, TypeScript `Router`, Go `NewRouter` / `NewRouterWithOptions`) is a pure endpoint descriptor for a remote or external `meshcall-router`. When constructed with no arguments, it resolves `MESHCALL_ROUTER_URL` (default `ws://127.0.0.1:8765`). It accepts one or more URLs (`ws(s)://`, `http(s)://`, `unix://`, `pipe://`), `unix_path` / `unixPath` / `WithUnixSocket`, or `pipe` / `WithPipe`. It performs zero I/O and owns no network connections.
   - `LocalRouter` (Python `LocalRouter`, TypeScript `LocalRouter`, Go `NewLocalRouter` / `StartLocalRouter`) explicitly manages a local `meshcall-router` OS subprocess (`async with LocalRouter(port=0) as router:` / `await using router = await LocalRouter.start({ port: 0 })` / `StartLocalRouter(ctx, WithPort(0))`), exposing `.url`, `.http_url`, `.bound_port`, `.pid` and `.is_running`. There is no implicit auto-spawning on `Router` or `Mesh`.
2. **Layer 3 (`Mesh`)**:
   - `Mesh` (Python `Mesh(router, ...)`, TypeScript `new Mesh(router, ...)` / `Mesh.connect(router, ...)`, Go `NewMesh(router, ...)` / `ConnectMesh(ctx, router, ...)`) strictly requires a `Router` or `LocalRouter` instance and represents a peer connected to that Router.
   - **Automatic transport selection**: `Mesh` inspects the `Router`'s URL scheme and selects WebSocket (`ws(s)://`, `unix://`, `pipe://`) or HTTP (`http(s)://`) automatically (`meshcall/web` in TypeScript provides a browser-safe `fetch`-only `Mesh` and `Router`).
   - **Multiplexed client & built-in services**: Any number of generated clients (`MyClient(mesh)`) share the `Mesh`'s underlying connection. Built-in Router services are exposed as sub-namespaces on `Mesh`: `mesh.registry.list()` / `mesh.registry.describe(service)` (Go `mesh.Registry().List(ctx)` / `Describe(ctx, service)`) and `mesh.auth.whoami()` / `issue_token(...)` / `revoke_token(...)` (Go `mesh.Auth().Whoami(ctx)` / `IssueToken(...)` / `RevokeToken(...)`).
   - **Service mounting & server lifecycle**: `mesh.mount(*services, ...)` (Go `mesh.Mount(...)`) synchronously mounts local services onto the `Mesh` (failing immediately if the `Router` uses `http(s)://`). `mesh.run()` blocks until `SIGINT` / `SIGTERM` and drains in-flight calls; `mesh.serve()` (TS `mesh.serve()` / `mesh.startServing()`, Go `mesh.Serve()` / `mesh.StartServing()`) runs or starts the server asynchronously and drains on exit.

Per-call control parameters are deliberately kept out of the generated
*business* parameter list. `with_options` / `withOptions` / `WithOptions`
attach `timeout`, `idle_timeout` / `timeoutMs`, `metadata`,
`idempotency_key` / `idempotencyKey` and `instance`; generated TypeScript and
Go methods also accept an optional trailing options / `opts ...` argument for
the same purpose. A targeted service's generated client keeps `instance` a
required argument (Python keyword-only, TypeScript required option, Go
positional parameter) so the compiler enforces it.

## Dependency injection scope

Dependency injection is compiled when a service is defined and resolved per
call. Python builds a `DependencyPlan` for each provider at `@service`
registration, including nested providers and a circular-dependency check; the
same plan supports `@dependency` on a function, default-argument style
(`user: str = current_user`) and `Annotated[T, dependency(provider)]`.
TypeScript mirrors this with `dependency(provider, { inject })`,
`meta("key")`, and a `DependencyScope` created for every call. Go's
`WithDependencies(...)` registers `Dependency(...)` and `Meta[T](...)`
providers; reflected handlers whose parameter types are registered are
injected.

Each call gets a fresh scope. A provider runs **at most once per call** and its
value is cached in that scope, so two parameters that depend on the same
provider share one instance. A provider that is a generator or async generator
is entered through an exit stack: the yielded value is injected, and its
`finally` / code after `yield` runs when the call ends — on success, on error
and on cancellation. This is what makes a transaction dependency express
commit-on-success and rollback-on-failure without handlers calling
`commit()` by hand (`best-practice/src/best_practice/service.py`). Metadata
entries are resolved before regular dependencies so a missing required header
fails with `invalid_argument` before any resource is acquired. Injected
parameters are stripped from the contract; the generated clients see only
business parameters.

## Request binding and generated signatures

Every method contract stores one request JSON Schema. TypeScript's method
decorators can declare it in three binding modes; Python and Go produce the
equivalent request schema from their handler signatures:

- `request_object` — an explicit `request` schema (or the `@method` default).
  The handler receives one parsed request object.
- `positional` — `input: { a: z.string(), b: z.int() }` (or `input: {}` for no
  parameters). The named object becomes the request schema and the handler
  receives `a, b` positionally; the names and order are kept.
- `single_schema` — `input: SomeSchema`. That schema is the whole request and
  the handler receives it as one value, without unpacking its fields.

Python extracts the non-injected parameters in signature order; a single
model-typed parameter stays whole. Go reflected services use the exported
method parameters after `ctx` and after removing registered dependency types;
pointer and `omitempty` fields are optional. The code generator reads the
request schema's property order and parameter names and mirrors them 1:1 in
Python, TypeScript and Go, so the generated method takes exactly the server's
business parameter list. Client-stream methods append the inbound iterable
last; per-call options are attached with `with_options` (or the optional
trailing options argument of generated TypeScript and Go methods) and are not
treated as business parameters.

## RPC and service loop isolation

Every Python `RpcServer` owns a persistent service worker loop running in a
dedicated thread. WebSocket receive/send, call state, deadlines, cancellation,
flow-control credit, and terminal-frame arbitration remain on the RPC loop.
Pydantic request validation, the user handler, response validation, and stream
iteration run on the service worker loop.

Cross-loop operations use thread-safe future submission. A synchronously
blocking handler can delay other work assigned to the current service worker,
but it cannot prevent the RPC loop from enforcing a deadline or maintaining
other connections. The TypeScript client consumes unary, server-streaming,
client-streaming, (experimental) subscription and duplex calls; TypeScript
service handlers are still not worker-thread isolated and must avoid
synchronously blocking Node's event loop.

Access logging is also owned by the RPC loop. It is emitted at the single
terminal-frame arbitration point, after a result or error status is known, so a
streaming call produces exactly one access entry even if cancellation races
with normal completion. Unknown methods and duplicate call IDs are logged at
the `call.open` rejection point because they never create a call object.

The TypeScript runtime separates service definitions, JSON Schema validators,
per-connection sessions, credit/queue primitives, frame scheduling, and endpoint
lifecycle. Direct and Router connections share the same session implementation.
Unary, server-streaming, and client-streaming handlers receive a call-bound
logger and abort signal. Declared schemas validate requests, responses, and
stream items; raw legacy handlers without schemas remain unchecked.

TypeScript supports TCP and the local transport (`unixPath`, or `pipe` on
Windows) for clients, Direct servers, and Router service connections. The Go
runtime (`meshcall-go`) takes the same endpoints as `ws://`, `unix:///path`
and `pipe://NAME` strings; each handler runs in its own goroutine with a
`context.Context` that ends with the call. All three runtimes use the same Go
Router; its process is launched by thin Python/TypeScript wrappers (Go has no
launcher). Its registry, account authentication, scoped token RPC and failure
boundaries are described in [Router architecture](router.md).

## Call ownership

The client creates a UUID `call_id`. A Direct server stores calls per connection.
The Router stores a global route with the client connection and one selected
service instance. The route increments that instance's inflight count and is
removed exactly once by a terminal result, terminal error, or disconnect.

The Router forwards stream data without decoding Pydantic payloads. Sticky
balancing is the only policy that reads a declared request field.

## Flow control

Client-to-server and server-to-client directions each maintain independent item
credit. The receiver grants an initial window of 16 items and replenishes one
credit when application code consumes an item. A producer waits for credit before
sending the next item; an input pump may hold one prefetched item to discover
iterator exhaustion without another window grant.

The Router counts each call's credit as it forwards windows and items, and fails
a call whose producer overruns it (`flow_control_violation`) instead of
buffering the excess. Its per-connection queues are therefore bounded by bytes
alone: concurrent streams add up to what the receiving peer granted, not to a
fixed frame count.

The connection sender prioritizes cancellation, window, and Ping/Pong frames.
Other frames are scheduled round-robin by `call_id`, preventing a large stream
from monopolizing a WebSocket connection.

## Lifecycle

`RpcServer.start()` freezes service registration and obtains a generation-tagged,
exclusive Driver binding. Any startup failure rolls the Server back to `IDLE` and
releases the binding. `stop()` closes the Driver before releasing it, so a stale
task can't operate through a newly acquired generation.

`mesh.serve` (TypeScript `mesh.serve` / `mesh.startServing`, Go `mesh.Serve` / `mesh.StartServing`) wraps one server instance and a Router driver configured from the `Mesh`'s `Router` or `LocalRouter`; its `RunningServer.stop(drain=True)` / `Close()` drains the server before disconnecting. `mesh.run` wraps `mesh.serve` with signal handling: SIGINT / SIGTERM trigger the same drain path and then exit. `LocalRouter` owns its spawned OS subprocess lifecycle explicitly (`async with LocalRouter() as router:` / `await using router = await LocalRouter.start()` / `StartLocalRouter(ctx)`). Generated clients wrap a `Mesh` (`MyClient(mesh)`), sharing its multiplexed connection.

## Failure boundaries

- Service exceptions become sanitized `internal` errors and are logged locally.
- Request and input-item validation failures become `invalid_argument` with
  `details.violations` (field path, validator code, message) and never the
  rejected value; a response or output item that fails the service's own
  contract is `internal` with `reason: invalid_response` /
  `invalid_output_item`, and the violations go to the server log only.
- Clients validate responses and output items against their own contract
  (generated TypeScript clients pass `responseSchema` / `itemSchema`; Python
  clients validate through Pydantic) and raise `ProtocolError` with
  `source: client`, `reason: invalid_response` / `invalid_output_item` and
  redacted `violations` on a mismatch.
- Clients raise one subclass per protocol code (`InvalidArgumentError`,
  `DeadlineExceededError`, `UnavailableError`, …; `CallCancelledError` for
  `cancelled`), built by `MeshCallError.from_code` / `fromCode`. Unknown
  (application) codes stay plain `MeshCallError`. `error.source` and
  `error.reason` read the matching `details` keys.
- Access logs record successful calls at the configured level, caller-side
  failures at warning and `internal` / `protocol_error` at error.
- Task cancellation is propagated as `call.cancel` and `cancelled`.
- Deadline expiry cancels the service task and returns `deadline_exceeded`.
  Timeouts travel as relative `timeout_ms`; the server, the Router, and the
  client each enforce them independently on their own monotonic clocks, in that
  order, with the client waiting a configurable grace past the requested
  timeout before failing locally with `details.source = "client"`.
- `idle_timeout_ms` bounds the gap between stream items and is enforced by the
  consuming side only: the client for server-direction items, the server for
  client-direction items.
- Connecting peers send protocol `ping` frames (15 s) and drop the connection
  when a `pong` is late (10 s); accepting peers drop connections silent for
  45 s. The transport layer handles this; runtimes never see ping or pong.
- `max_inflight` caps concurrent calls per instance; the server enforces it
  itself (`resource_exhausted`, `reason: "overloaded"`) and the Router routes
  around full instances. The Router's per-instance circuit breaker excludes
  an instance after consecutive timeouts or `internal`/`unavailable` answers,
  then admits one probe call.
- A built-in `RetryPolicy` (outermost in the client interceptor chain)
  re-sends failed unary calls when the error is retryable, the code is in the
  policy's set, and the method is idempotent per the contract or the call has
  an `idempotency_key`; attempts share the call's timeout and the final error
  reports `details.attempts`. Streams are never retried; an (experimental)
  subscription is reopened from its last cursor instead (see
  [Subscriptions](protocol.md#subscriptions)).
- `stop(drain=True)` / `close({ drain: true })` first stops taking new calls
  (`server.drain` to the Router, or closing the listener) and waits up to the
  drain timeout for in-flight calls, then stops; nothing in flight fails.
- Instance disconnect terminates its routed calls with retryable `unavailable`.
- Clients and Router-mode servers accept a list of Router endpoints: clients
  fail over to the next endpoint, servers keep one registration per Router.
- Connection loss terminates all local active calls. Clients then reconnect
  lazily on the next call with exponential backoff and jitter
  (`ReconnectPolicy`), waiting at most that call's timeout. Router-mode
  servers re-register with the same backoff while staying `RUNNING`; calls
  that arrive in between fail at the Router with retryable `unavailable`.
  Calls are never replayed automatically; an (experimental) subscription is
  reopened from its last cursor instead (see
  [Subscriptions](protocol.md#subscriptions)).
- A malformed frame that still names a call fails only that call with
  `protocol_error`; a frame that cannot be attributed closes the connection.
  Oversize frames fail the sending call with `resource_exhausted`.
- Call metadata (`call.open`) and trailers (`call.result` / `call.error`) are
  string maps that reach handlers through `RpcContext` and are otherwise opaque
  to the runtime.

## Conformance suite

`meshcall-router/conformance` is the executable definition of "behaves like a
MeshCall runtime". It starts a real Router in-process, launches a *fixture
program* for the runtime under test, and plays scripted raw-frame exchanges
against it: registration shape, unary round trips and error pass-through,
item credit on both stream directions (including the one-item prefetch at
exhaustion), server-side `timeout_ms` and `idle_timeout_ms`, cancellation
reaching the handler, metadata and trailers, keepalive within the Router idle
window, re-registration after a Router restart, and drain.

The fixture contract is in `meshcall-router/conformance/FIXTURE.md`; the
reference fixtures are `meshcall-py/conformance/fixture.py`,
`meshcall-ts/conformance/fixture.ts` and `meshcall-go/conformance/fixture`.
A new language runtime is conformant when `meshcall-conformance --fixture
"<its fixture command>"` passes. `scripts/check.sh conformance` runs all
three fixtures over TCP and over the local transport (`--transport tcp` /
`local`); CI does too.
Client-side conformance (a scripted fixture *client* driven by the suite
acting as the Router) is a planned second phase.

## Contract warnings

Hard rules raise at import time (`ContractError` / a thrown `Error`): service
name shape, untyped parameters, declared-vs-inferred shape mismatches,
`read_only` together with `destructive`, and the duplex rules (declared
explicitly with a non-empty `reason`, no result value, not `idempotent` or
`read_only`). Everything else about contract
quality is an **advisory warning** with a stable code, a `hint` written as a
direct instruction, and an explicit way to silence it:

| Code | Finding | Silence |
| --- | --- | --- |
| W001 | service or method without a description | `description=""` |
| W002 | mutating-looking name (`create_*`, `delete_*`, …) with no annotation | any of `idempotent`, `read_only`, `destructive` |
| W003 | `read_only` on a mutating name, or `destructive` on a reading name | fix the name or the annotation |
| W005 | `deprecated` without a named replacement | `deprecated="…"` with text |
| W006 | sticky balancing on a `targeted` service (never used) | remove the sticky policy |

Where they surface, by language and moment:

- **TypeScript, compile time**: the method builders' overload without
  `description` is `@deprecated`, so editors strike it through and
  `@typescript-eslint/no-deprecated` can fail the build. Name shape is a type
  error via `normalizeServiceName`'s runtime check plus the documented
  template literal form.
- **Python, import time**: `warnings.warn(MeshCallWarning)` for each finding
  when the `@service` decorator runs; pytest collects them, and
  `python -W error::meshcall.MeshCallWarning` makes them fatal.
- **All three, server start**: one summarised log line per start (Go: in
  `NewServer`; `meshcall.LintService` lists the findings).
- **Strict mode**: `MESHCALL_STRICT=1` (all three runtimes),
  `MeshCallServer({ strictContracts: true })` (TS) and
  `ServerOptions{Strict: true}` (Go) turn warnings into errors. `AGENTS.md`
  tells agents to run in strict mode; CI may opt in.
- **Project-wide ignore**: `MESHCALL_IGNORE_WARNINGS=W001,W002`.

`lint_contract(contract)` / `lintService(definition)` return the findings
without side effects, for tooling such as `meshcall check`.

## Interceptors

All three runtimes expose one interception point per side, shaped the same
way in Python, TypeScript and Go (`ServerOptions.Interceptors` /
`ClientOptions.Interceptors`).

- **Server**: `RpcServer(interceptors=[...])` / `MeshCallServer({ interceptors })`.
  Each interceptor is `(ctx, request, call_next) -> result`. It runs after
  request validation, on the same loop as the handler, and `call_next()`
  resolves only once the whole call has run, including every streamed item.
  Raising a `MeshCallError` short-circuits the call; `ctx.set_trailer()` adds
  trailers; `ctx.metadata` carries what the caller sent. The built-in access
  log stays outside the chain because the authoritative status is only known
  when the terminal frame is sent.
- **Client**: `ClientBase(interceptors=[...])` / `MeshCallClient(url, peerId,
  { interceptors })`. Each interceptor is `(info, call_next) -> result` where
  `info` names the service, method and shape and holds mutable `options`
  (timeouts, metadata). `call_next(info)` returns the response for unary and
  client-stream calls, or the lazy stream object for server streams, and may be
  invoked again to retry. `info.trailers` is populated for unary and
  client-stream calls after `call_next` returns; streams expose `.trailers`
  once iteration completes.

The first interceptor in either list is outermost. Interceptors do not see
individual stream items; that remains a future extension.

## Observability

OpenTelemetry is an opt-in pair of interceptors in Python and TypeScript:
Python
`meshcall.otel.client_interceptor()` / `server_interceptor()` (install
`meshcall[otel]`), TypeScript `otelClientInterceptor()` /
`otelServerInterceptor()` from `meshcall/otel` (`@opentelemetry/api` is an
optional peer dependency; Node needs a context manager, which the
OpenTelemetry Node SDK registers). Trace context travels as call metadata
(`traceparent` / `tracestate`), so a trace continues across languages and
through the Router, which forwards metadata untouched and has no OpenTelemetry
dependency. Go has no OpenTelemetry package; a Go interceptor can carry
`traceparent` the same way. Each call gets a span named `service/method` (`rpc.system =
"meshcall"`, `rpc.service`, `rpc.method`, `rpc.meshcall.error_code`) and is
counted in `meshcall.calls` and `meshcall.call.duration` by service, method,
code and side. A server span covers the whole call; a client span covers a
unary or client-stream call and only the opening of a stream.

The Router exposes Prometheus metrics on a separate listener,
`--metrics-listen 127.0.0.1:9464`, never on the public port; see
[Router metrics](router.md#metrics).
