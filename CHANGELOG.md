# Changelog

All notable changes to MeshCall are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The three packages (`meshcall` on
PyPI, `meshcall` on npm, and the `meshcall-router` binary) share one version
and are released together from a `vX.Y.Z` tag.

## [Unreleased]

### Security

- The Registry (`router inspect`, `GET /v1/services`) shows only services and
  methods the caller may call, with contract entries filtered the same way;
  describing a forbidden service is indistinguishable from describing a
  missing one. Previously any identity allowed on the Registry saw every
  registered service.

### Changed

- Generated TypeScript clients declare server streams as `RpcStream<T>`
  (implemented by `MeshCallServerStream`) and import runtime types with
  `import type`. `scripts/regenerate.sh` builds the CLI before generating.
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

### Added

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
- HTTP v1 (#31): server streams are NDJSON envelopes (`{"item"}` per item,
  then `{"end": {"trailers"}}` or `{"error"}`), replacing bare items, SSE
  and the `MeshCall-Status` trailer; `--http-default-timeout` (30 s for
  unary and client-stream calls without a timeout) and `--http-max-timeout`;
  `--http-max-inflight` (429 beyond, separate from WebSocket connections);
  CORS allowlist `--http-cors-origin`; `GET /v1/openapi.json` (OpenAPI 3.1
  generated from the contracts the caller may call); `/healthz` and
  `/readyz`. The HTTP mapping is experimental until v0.2.0.
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
- HTTP calls through the Router (`POST /v1/call/{service}/{method}` on the
  same listener): unary calls and server streams (see HTTP v1 above for
  the stream format), client
  streams from an NDJSON body read only as fast as the instance grants
  credit, duplex refused with `405`. Same accounts/tokens, permissions,
  deadlines, balancing, dedup and targeting as WebSocket calls; headers
  carry timeouts, idempotency keys, instance and metadata; errors map to
  HTTP statuses; closing the request cancels the call; `GET /v1/services`
  mirrors the Registry. `meshcall call --http`. Conformance scenarios drive
  the Python and TypeScript fixtures over HTTP.
- Generated Python unary methods take `idempotency_key=` (TypeScript
  methods already accept `idempotencyKey` in their options), so retry-safe
  mutating calls no longer need `ClientBase.call_*`.

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

[Unreleased]: https://github.com/t4wefan/meshcall/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/t4wefan/meshcall/releases/tag/v0.1.0
