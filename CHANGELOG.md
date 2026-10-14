# Changelog

All notable changes to MeshCall are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The three packages (`meshcall` on
PyPI, `meshcall` on npm, and the `meshcall-router` binary) share one version
and are released together from a `vX.Y.Z` tag.

## [Unreleased]

### Added

- Router configuration from the environment: every flag has a
  `MESHCALL_ROUTER_<FLAG>` variable (the command line wins), and
  `MESHCALL_ROUTER_AUTH` carries the auth document inline as an alternative
  to an auth file.
- `--max-connections` (default 10000; launchers: `max_connections` /
  `maxConnections`) caps open WebSocket connections, refusing more with
  `503` and `Retry-After`; HTTP calls are not counted. HTTP request bodies
  that stop arriving for `--idle-timeout` are cut off.
- HTTP calls through the Router (`POST /v1/call/{service}/{method}` on the
  same listener): unary calls and server streams (NDJSON, or SSE with
  `Accept: text/event-stream`, plus a `MeshCall-Status` trailer), client
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
