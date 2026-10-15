# Implementation Architecture

**English** | [简体中文](zh-CN/architecture.md)

## Layers

```text
Python/Pydantic service ---- contract extraction ---+
                                                    |
TypeScript schema service ---- contract export -----+--> portable contract
                                                            |
                                                            +--> Python uv package
                                                            |
                                                            +--> TypeScript Yarn package

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
shape; explicit stream decorators select the recommended server or client
streaming shapes. Service parameters are the request: they are wrapped into one
generated Pydantic request model for validation and transport, and a
model-typed parameter is simply one field of it. Service names are normalised
to carry exactly one version segment (`chat` → `chat.v0`). The Runtime operates on logical
frames and doesn't know whether the connection is direct or routed. WebSocket
drivers own connection setup, role handshakes, TCP and local (Unix socket /
Windows named pipe, `unix_path=` / `pipe=`) endpoints, and server
registration. Duplex (`@method.duplex(reason=…)` / `duplexMethod({ reason })`)
is a restricted, opt-in shape of last resort: the handler gets a channel
(`RpcDuplex` / `DuplexChannel`) to iterate input and `send` output, returns
nothing, and the call is never retried, deduplicated or reachable over HTTP;
see protocol.md → Duplex calls.

Parameters named `logger` / typed `RpcLogger` and `ctx` / `context` / typed
`RpcContext` are dependency injection, not RPC fields. They are removed while
extracting the request model and supplied by the server per call, so generated
clients do not expose them. Docstrings and the `read_only` / `idempotent` /
`destructive` / `deprecated` method options are carried into the contract as
`description`, `annotations` and `deprecated`.

## RPC and service loop isolation

Every Python `RpcServer` owns a persistent service worker loop running in a
dedicated thread. WebSocket receive/send, call state, deadlines, cancellation,
flow-control credit, and terminal-frame arbitration remain on the RPC loop.
Pydantic request validation, the user handler, response validation, and stream
iteration run on the service worker loop.

Cross-loop operations use thread-safe future submission. A synchronously
blocking handler can delay other work assigned to the current service worker,
but it cannot prevent the RPC loop from enforcing a deadline or maintaining
other connections. The TypeScript client consumes unary, server-streaming, and
client-streaming calls; TypeScript service handlers are still not worker-thread
isolated and must avoid synchronously blocking Node's event loop.

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
Windows) for clients, Direct servers, and Router service connections. Both languages use the same Go Router; its
process is launched by thin Python/TypeScript wrappers. Its registry, account
authentication, scoped token RPC and failure boundaries are described in
[Router architecture](router.md).

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

The connection sender prioritizes cancellation, window, and Ping/Pong frames.
Other frames are scheduled round-robin by `call_id`, preventing a large stream
from monopolizing a WebSocket connection.

## Lifecycle

`RpcServer.start()` freezes service registration and obtains a generation-tagged,
exclusive Driver binding. Any startup failure rolls the Server back to `IDLE` and
releases the binding. `stop()` closes the Driver before releasing it, so a stale
task can't operate through a newly acquired generation.

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
  reports `details.attempts`. Streams are never retried.
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
  Calls are never replayed automatically.
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
reference fixtures are `meshcall-py/conformance/fixture.py` and
`meshcall-ts/conformance/fixture.ts`. A new language runtime is conformant
when `meshcall-conformance --fixture "<its fixture command>"` passes.
`scripts/check.sh conformance` runs both reference fixtures; CI does too.
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
- **Both, server start**: one summarised log line per start.
- **Strict mode**: `MESHCALL_STRICT=1` (both), `MeshCallServer({ strictContracts: true })`
  (TS) turn warnings into errors. `AGENTS.md` tells agents to run in strict
  mode; CI may opt in.
- **Project-wide ignore**: `MESHCALL_IGNORE_WARNINGS=W001,W002`.

`lint_contract(contract)` / `lintService(definition)` return the findings
without side effects, for tooling such as `meshcall check`.

## Interceptors

Both runtimes expose one interception point per side, shaped the same way in
Python and TypeScript.

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

OpenTelemetry is an opt-in pair of interceptors per language: Python
`meshcall.otel.client_interceptor()` / `server_interceptor()` (install
`meshcall[otel]`), TypeScript `otelClientInterceptor()` /
`otelServerInterceptor()` from `meshcall/otel` (`@opentelemetry/api` is an
optional peer dependency; Node needs a context manager, which the
OpenTelemetry Node SDK registers). Trace context travels as call metadata
(`traceparent` / `tracestate`), so a trace continues across languages and
through the Router, which forwards metadata untouched and has no OpenTelemetry
dependency. Each call gets a span named `service/method` (`rpc.system =
"meshcall"`, `rpc.service`, `rpc.method`, `rpc.meshcall.error_code`) and is
counted in `meshcall.calls` and `meshcall.call.duration` by service, method,
code and side. A server span covers the whole call; a client span covers a
unary or client-stream call and only the opening of a stream.

The Router exposes Prometheus metrics on a separate listener,
`--metrics-listen 127.0.0.1:9464`, never on the public port; see
[Router metrics](router.md#metrics).
