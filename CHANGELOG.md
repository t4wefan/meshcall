# Changelog

All notable changes to MeshCall are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The packages (`meshcall` on
PyPI, `meshcall` on npm, the `meshcall-router` binary and the Go module
`github.com/t4wefan/meshcall/meshcall-go`) share one version and are
released together from a `vX.Y.Z` tag; the Go module is versioned by the
matching `meshcall-go/vX.Y.Z` tag.

## [Unreleased]

### Added

- **Four-layer peer architecture (`Service & Contract`, `Router` & `LocalRouter`, `Mesh`, `GeneratedClient`)**:
  - `Router` is a pure endpoint descriptor (`ws://`, `wss://`, `http://`, `https://`, `unix://`, `pipe://`, or a failover list of URLs).
  - `LocalRouter` manages a local `meshcall-router` subprocess (`LocalRouter(port=...)` in Python, `LocalRouter.start(...)` / `new LocalRouter(...)` in TypeScript, `meshcall.StartLocalRouter(...)` / `meshcall.NewLocalRouter(...)` in Go). Passing an unstarted `LocalRouter` to `Mesh` automatically starts the subprocess when `Mesh` serves or connects and stops it when `Mesh` closes.
  - `Mesh` is the peer runtime that mounts services (`mesh.mount(svc)`), serves them (`mesh.run()`, `mesh.serve()`, `mesh.startServing()`, `mesh.Serve()`), multiplexes a single client connection across generated clients (`ChatClient(mesh)` / `new ChatClient(mesh)` / `NewChatClient(mesh)`), and exposes the Router's built-in services on `mesh.registry` (`list`, `describe`) and `mesh.auth` (`whoami`, `issue_token`, `revoke_token`).
- **Semantic method decorators and dependency injection** (#53). `@query`, `@mutates`
  and `@stream` (Python/TypeScript) and `Query`, `Mutates`, `StreamServer`,
  `StreamClient`, `Duplex` (Go) declare all call shapes (`DuplexChannel[In, Out]` / `DuplexChannel<In, Out>` for restricted duplex methods with `@mutates(reason=...)`).
  Dependencies (`@dependency` / `dependency()` / `Provide`) and metadata
  parameters (`meta("key")` / `FromMeta`) are resolved per call, cached within
  the call, cleaned up through `yield`, and stripped from contracts and
  generated clients; a provider that yields runs its cleanup when the call
  ends (commit on success, rollback on error or cancellation).
- **1:1 generated signatures**: generated clients take the same business
  parameters as the server handler, in the same order (client streams put the
  stream last); per-call options move to `.with_options(...)` /
  `.withOptions(...)` / `.WithOptions(...)`. TypeScript services can declare
  parameters inline (`input: { session_id: z.string() }`) and Go services can
  be defined by struct reflection.
- `best-practice/` shows a transactional session-store dependency, a
  TypeScript CLI and a Go client over one Router.
- **HTTP client transport in every runtime**. Passing an `http://` or `https://`
  `Router` to `Mesh` (including browser-safe `Mesh` and `Router` in `meshcall/web`)
  speaks the Router's HTTP v1 API. Unary calls (with idempotent retry),
  server and client streams, and the built-in `mesh.registry` / `mesh.auth`
  calls work identically to WebSocket; duplex is WebSocket-only and raises
  `method_not_found` with `reason: "duplex_over_http"`.

### Changed

- **Generated clients take options only through `with_options`** (#55).
  Python generated methods no longer end with `timeout` / `idle_timeout` /
  `metadata` / `idempotency_key` keywords (a targeted service's client still
  requires `instance`, so naming an instance stays a compile-time rule).
- **TypeScript services are classes with zod schemas** (#49), like Python's
  `@service` classes with Pydantic models. `defineService`, `defineType` and
  the `unaryMethod` / `serverStreamMethod` / `clientStreamMethod` /
  `duplexMethod` builders are replaced by `@service`, `@query`, `@mutates`,
  and `@stream`. `z` is re-exported from `meshcall`.
- **Removed experimental subscriptions (`cursor` / `resume` and `Topic`)**:
  resumable stream cursor conventions are application-level parameters on
  `@stream` methods rather than a separate protocol/contract primitive.

## [0.2.1] - 2026-10-16

### Fixed

- **A frame over the size limit fails only its call** in every runtime,
  with non-retryable `resource_exhausted` (`reason: "frame_too_large"`,
  `source` the side that refused to send it), and the connection stays
  open. Before, a Python server whose response was too large sent nothing,
  so the caller waited for its deadline; a TypeScript server closed the
  connection; and the Go runtime sent the frame, so the receiving peer (the
  Router, for an instance) closed the whole connection and failed every call
  on it. An oversized input stream item also cancels the call at the peer.
  See [docs/protocol.md](docs/protocol.md#frame-size).

## [0.2.0] - 2026-10-15

### Security

- The Registry (`router inspect`, `GET /v1/services`) shows only services and
  methods the caller may call, with contract entries filtered the same way;
  describing a forbidden service is indistinguishable from describing a
  missing one. Previously any identity allowed on the Registry saw every
  registered service.

### Changed

- **Frames may be up to 64 MiB by default** (was 1 MiB), which is also the
  ceiling; `--max-frame-size` / `max_frame_size` / `maxFrameSize` /
  `MaxFrameSize` still lower it. The Router's per-peer queue is configurable
  with `--outbox-size` (Python `outbox_size`, TypeScript `outboxSize`),
  default 256 MiB and at least the maximum frame size; it replaces the fixed
  "8 MiB or twice the frame size". The limits are documented in
  [docs/protocol.md](docs/protocol.md#frame-size).
- The Router no longer caps a peer's queue at 256 frames, which dropped
  well-behaved clients with many concurrent streams: it counts each call's
  stream credit and fails an item beyond it with `protocol_error`
  (`reason: "flow_control_violation"`) and a `call.cancel` to the instance,
  without closing a connection; an overrunning instance counts as a breaker
  failure.
- Generated TypeScript clients declare server streams as `RpcStream<T>`
  (implemented by `MeshCallServerStream`) and import runtime types with
  `import type`.
- Sticky balancing uses rendezvous hashing: an instance joining or leaving
  moves only its own keys instead of remapping every key.
- **Duplex is a supported, restricted shape** (#26). It must be declared
  with a reason (`@method.duplex(reason=…)` / `duplexMethod({ reason })`),
  which goes into the contract as `duplex_reason` and shows in `router
  inspect`. The handler iterates an `RpcDuplex` / `DuplexChannel`, sends
  output on it and returns nothing: the optional result value and
  `RpcDuplexClient.result()` are gone, and the client's output iteration
  ends with the call. Duplex methods cannot be `idempotent` or `read_only`,
  take no idempotency key (`invalid_argument`, `reason:
  idempotency_key_on_duplex`, from the Router and from servers) and are
  never retried. A `RpcDuplex` parameter on a plain `@method()` is now an
  error instead of an inferred duplex.
- **Authenticate at connect, authorise at call.** With accounts configured,
  a connection or HTTP request without credentials is the reserved user
  `anonymous` instead of being refused; it may call only what its auth-file
  entry grants (nothing by default). Invalid credentials are still refused
  at connect. Every forbidden call is `permission_denied`, whether or not
  the target exists. Temporary tokens now start with `mt_`.
- `meshcall init` installs the skill at `.agents/skills/meshcall/SKILL.md`
  with a marked section in `AGENTS.md` by default (was
  `.claude/skills/…`); `--agent claude|cursor|copilot|windsurf|all` writes
  agent-specific files instead.

### Added

- **Graceful Router shutdown** (#8). On SIGTERM/SIGINT the Router drains:
  `/readyz` answers 503, new connections get HTTP 503 and new calls
  retryable `unavailable` (`reason: "router_draining"`), idle connections
  close with WebSocket 1001 at once and busy ones after their last call, and
  after `--shutdown-timeout` (default 10 s) the calls still in flight end
  with retryable `unavailable` (`reason: "router_shutdown"`) and a
  `call.cancel` to the instance. A second signal skips the wait. `Close()`
  (and a launcher closing the Router's stdin) now ends calls the same way
  instead of dropping connections. Go embedders get `Router.Shutdown()`.
- `--max-calls-per-connection` (default 1000, `0` for no limit): a further
  `call.open` on that connection fails with retryable `resource_exhausted`
  (`reason: "overloaded"`).
- Go runtime (#19): `github.com/t4wefan/meshcall/meshcall-go` — services
  (`HandleUnary`, `HandleServerStream`, `HandleClientStream`,
  `HandleSubscription`, `HandleDuplex`) with contracts and JSON Schema from
  struct tags, validation, contract warnings and strict mode, interceptors,
  trailers, deadlines, idle timeouts, max in-flight, targeted services,
  Router mode with reconnect and drain, and direct mode over TCP, Unix
  sockets and Windows named pipes; clients with typed calls, retry,
  reconnect, failover, subscriptions with resume and local validation. It
  passes the conformance suite, interoperates with Python and TypeScript
  both ways, and `meshcall generate --language go` writes typed Go clients.
- Web client (#12): `meshcall/web` exports `MeshCallHttpClient`, a client
  over HTTP v1 with `fetch` and no Node imports (browsers, Deno, Bun, edge
  runtimes, Node 18+), with the same options, errors, local validation,
  unary retry and subscription resume as `MeshCallClient`. Generated
  TypeScript clients take the `MeshCallRpc` interface both clients
  implement (services with duplex methods still take `MeshCallClient`) and
  import only types from `meshcall`, so they bundle for browsers; CI
  bundles both for the browser platform.
- Observability (#7): opt-in OpenTelemetry interceptors —
  `meshcall.otel.client_interceptor()` / `server_interceptor()`
  (`meshcall[otel]`) and `otelClientInterceptor()` /
  `otelServerInterceptor()` from `meshcall/otel` — with a span per call,
  `meshcall.calls` / `meshcall.call.duration` metrics, and W3C trace context
  carried in call metadata, so one trace spans Python, TypeScript and the
  Router. The Router serves Prometheus metrics (calls by code, durations,
  connections, routes, per-instance in-flight and breaker state, outbox
  drops) on a separate opt-in listener, `--metrics-listen`.
- Subscriptions (#14): `@method.subscription(cursor=, resume=)` /
  `subscriptionMethod({ cursor, resume })` declare a server stream that
  clients resubscribe to after a retryable failure or dropped connection,
  resuming after the last item's cursor, with no default timeouts
  (`call_subscription` / `MeshCallClient.subscribe`, generated clients
  included). The contract gains `subscription`; `router inspect` tags it.
  `Topic[T]` / `Topic<T>` fan out events in process, with a bounded queue
  per subscriber (`slow_subscriber`) and optional replay history
  (`cursor_expired` when a cursor is older than it); Python's `publish` is
  safe from any thread or loop.
- Windows named pipes (#29), Windows' local transport in place of Unix
  sockets: `meshcall-router --pipe NAME` (`MESHCALL_ROUTER_PIPE`), `pipe=` /
  `pipe:` on the Python and TypeScript Router launchers, clients and
  servers, and `--router pipe://NAME` (and `unix:///path`) in the CLI. A
  pipe admits only the Router's user, SYSTEM and Administrators. Each
  platform refuses the other's local transport with a hint. CI runs the Go,
  Python and TypeScript suites, interop and conformance on Windows, over a
  pipe and TCP; conformance also runs over the local transport elsewhere
  (`meshcall-conformance --transport local`).
- Duplex in TypeScript: `duplexMethod`, `MeshCallClient.duplex()` returning
  `MeshCallDuplexCall` (`send`, `closeSend`, async iteration, `cancel`),
  generated TypeScript clients for duplex methods, and `meshcall call
  … --stdin` for duplex calls (each line is sent as it is read). Python's
  `RpcServerStream` declares `async with` (cancel on exit), so type
  checkers accept it on every stream.
- API keys: `api_keys` in the auth file (SHA-256 digests, generated by
  `meshcall-router new-api-key ID`), sent as `Authorization: Bearer mk_…`;
  one hash lookup per check, kept across restarts.
- Successful Basic checks are cached for a minute; failures are not.
- `--max-anonymous-connections` (default 1000) caps anonymous WebSocket
  connections within `--max-connections`.
- Router configuration from the environment: every flag has a
  `MESHCALL_ROUTER_<FLAG>` variable (the command line wins), and
  `MESHCALL_ROUTER_AUTH` carries the auth document inline as an alternative
  to an auth file.
- `--max-connections` (default 10000; launchers: `max_connections` /
  `maxConnections`) caps open WebSocket connections, refusing more with
  `503` and `Retry-After`; HTTP calls are not counted. HTTP request bodies
  that stop arriving for `--idle-timeout` are cut off.
- HTTP calls through the Router (#31; `POST /v1/call/{service}/{method}` on
  the same listener), stable under `/v1`: unary calls; server streams as
  NDJSON envelopes (`{"item"}` per item, then `{"end": {"trailers"}}` or
  `{"error"}`); client streams from an NDJSON body read only as fast as the
  instance grants credit; duplex refused with `405`. Same accounts/tokens,
  permissions, deadlines, balancing, dedup and targeting as WebSocket
  calls; headers carry timeouts, idempotency keys, instance and metadata;
  errors map to HTTP statuses; closing the request cancels the call.
  `--http-default-timeout` (30 s for unary and client-stream calls without
  a timeout) and `--http-max-timeout`; `--http-max-inflight` (429 beyond,
  separate from WebSocket connections); CORS allowlist
  `--http-cors-origin`; `GET /v1/services` mirrors the Registry and
  `GET /v1/openapi.json` is OpenAPI 3.1 generated from the contracts the
  caller may call; `/healthz` and `/readyz`. `meshcall call --http`.
  Conformance scenarios drive the Python, TypeScript and Go fixtures over
  HTTP.
- Generated Python unary methods take `idempotency_key=` (TypeScript
  methods already accept `idempotencyKey` in their options), so retry-safe
  mutating calls no longer need `ClientBase.call_*`.

### Fixed

- A method with balance kind `disabled` and several live instances failed
  with reason `no_instances`; it now reports `balance_disabled`, as
  documented (still retryable: instances overlap during a handover).
- TypeScript: a draining server no longer drops a result that is still
  being sent (the caller saw `instance_lost`).
- TypeScript: one reconnect loop per Router link; two could run at once,
  registering twice and losing the drain.
- Generated TypeScript client modules import nested model types, so the
  generated package compiles.

## [0.1.0] - 2026-10-14

First public release of the `meshcall/1` protocol, the Python and TypeScript
runtimes, the standalone Go Router, and the unified `meshcall` CLI.

### Added

- **Runtimes & Transport**:
  - Python runtime (`meshcall` on PyPI): Pydantic service contracts (`@service`,
    `@method`, `@server_stream`, `@client_stream`, `@duplex`), `RpcServer`,
    generated typed clients, WebSocket Direct and Router drivers over TCP or
    Unix sockets.
  - TypeScript runtime (`meshcall` on npm): JSON Schema service contracts
    (`defineService`, `unary`, `serverStream`, `clientStream`, `duplex`),
    `MeshCallServer`, `MeshCallClient`, WebSocket Direct and Router drivers.
  - Unary, server-streaming and client-streaming calls with item credit flow
    control, cancellation and half-close; duplex as an experimental shape.
  - Relative call timeouts (`timeout_ms`) enforced by server, Router and client;
    stream idle timeouts; call metadata and response trailers; protocol-level
    keepalive; unknown-field tolerance for forward compatibility.
  - Client and server interceptor chains.
- **Standalone Go Router**:
  - Account authentication and scoped temporary tokens (`issueToken`,
    `revokeToken`), plus `meshcall.router.v1.Management` (`snapshot`, `drain`).
  - Load balancing (`round_robin`, `least_inflight`, `random`, `sticky`,
    `disabled`) with per-method routing so instances of one service may
    register supersets of each other's methods during rolling upgrades (#9).
  - Targeted calls (`instance_id` on server drivers and `call.open`,
    `@service(targeted=True)` / `defineService({ targeted: true })`,
    `meshcall call --instance ID`).
  - Multiple Router endpoints: clients fail over between Routers; Router-mode
    servers hold one registration per Router without Router-side coordination.
  - Reliability without Router state: `max_inflight` per instance enforced
    locally and announced to the Router; per-instance circuit breaker with
    half-open probing; idempotency keys on `call.open` with duplicate unary
    call join/replay; built-in client `RetryPolicy`; graceful drain
    (`server.drain` / `server.drain.ack`).
- **Contracts, Validation & Strict Mode**:
  - Service names normalised to carry a version segment (`chat` → `chat.v0`;
    `chat.v1`, `demo.v1.Greeting`).
  - Service and method `description` (from docstrings / JSDoc or explicit
    options), method `annotations` (`idempotent`, `read_only`, `destructive`)
    and `deprecated`.
  - Advisory contract warnings (`W001`–`W006`) with actionable hints, fatal
    under `MESHCALL_STRICT=1`.
  - Typed error subclasses per protocol code in Python and TypeScript with
    `source`, `reason`, and `details.violations`; both Python and TypeScript
    generated clients validate responses and stream items against contract
    schemas.
- **Unified Go CLI & Bundled Binaries**:
  - Single Go `meshcall` CLI (`generate`, `export`, `router inspect`,
    `router install`, `call`, `init`), launched transparently via
    `uv run meshcall` and `npx meshcall`.
  - `meshcall router inspect` and `meshcall call` query live contracts from
    `meshcall.router.v1.Registry` (`list`, `describe`) with contract-aware
    error hints.
  - `meshcall init` installs the first-party agent skill
    (`.claude/skills/meshcall/SKILL.md`) and `AGENTS.md` section.
  - Platform-specific `meshcall-router` and `meshcall` binaries ship inside
    PyPI platform wheels and npm `meshcall-router-<os>-<cpu>` optional
    dependencies across 14 OS/architecture targets (Linux, macOS, Windows,
    FreeBSD), falling back to a checksummed GitHub release download only on
    unbundled platforms.
- **Conformance Suite**:
  - `meshcall-conformance` runner in Go with reference fixtures for Python and
    TypeScript.

[Unreleased]: https://github.com/t4wefan/meshcall/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/t4wefan/meshcall/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/t4wefan/meshcall/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/t4wefan/meshcall/releases/tag/v0.1.0
