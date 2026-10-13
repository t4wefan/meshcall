# Changelog

All notable changes to MeshCall are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The three packages (`meshcall` on
PyPI, `meshcall` on npm, and the `meshcall-router` binary) share one version
and are released together from a `vX.Y.Z` tag.

## [Unreleased]

### Changed

- **One CLI.** Client generation moved from the Python package into the Go
  `meshcall` binary (`meshcall generate <contract.json | module:Class>`,
  `meshcall export module:Class`); output is byte-identical to the previous
  generators. The Python package no longer ships `codegen*`,
  `package_codegen` or `meshcall generate-contract`; `uv run meshcall` and
  `npx meshcall` are launchers that run the Go CLI (downloading the release
  on first use). `meshcall router install` installs both binaries. The npm
  bins `meshcall-router-install` and `meshcall-init` are replaced by
  `npx meshcall router install` / `npx meshcall init`.

- **Parameters are the request.** The "single Pydantic model parameter" style
  is gone; a model-typed parameter is one field of the generated request.
  Contract documents drop `request_style`/`request_fields` and gain
  `parameters`.
- Service names are normalised to carry exactly one version segment
  (`chat` → `chat.v0`; `chat.v1`, `demo.v1.Greeting`, `chat.v1.2.0` are valid)
  and the Router rejects other shapes. `@service("chat.v1")` accepts the name
  positionally.
- `ClientBase` call entry points are `call_unary` / `call_server_stream` /
  `call_client_stream` / `call_duplex`; underscore names remain as aliases.

### Added

- Generated clients validate what they receive: TypeScript clients carry
  `responseSchema` / `itemSchema` (exported as `<Type>Schema` constants from
  the generated models) and the runtime checks results and stream items
  against them; mismatches are `protocol_error` with `source: client`,
  `reason: invalid_response` / `invalid_output_item` and redacted
  `violations`. Python clients already validate through Pydantic and now
  report the same shape.
- Router routes per method: instances of one service may register supersets
  of each other's methods (rolling upgrades); only a method re-registered
  with a different shape/balance, or a differing `targeted` flag, conflicts.
  Registry lists the instances serving each method; `router inspect` shows
  `on: …` for methods on a subset. Closes #9.
- Targeted calls: every service instance has an id (`instance_id=` on
  server drivers / `instanceId`, generated when omitted; `server.instance_id`,
  `ctx.instance_id`). `call.open` may carry `instance_id`; the Router routes
  to that instance (`instance_unknown` / `instance_draining` reasons) and
  direct servers apply the same rules. `@service(targeted=True)` /
  `defineService({ targeted: true })` requires it (`instance_required`);
  generated clients then make `instance` a required keyword / option.
  `meshcall call --instance ID`, `router inspect` shows `[targeted]`,
  warning W006 for sticky balancing on a targeted service.
- `meshcall init` (Go CLI, `uv run meshcall init`, `npx meshcall-init`):
  installs the first-party agent skill (`.claude/skills/meshcall/SKILL.md`)
  and a MeshCall section in `AGENTS.md`; idempotent, keeps local edits unless
  `--force`. The skill source is `skills/meshcall/SKILL.md`.
- Error model: typed error subclasses per protocol code in Python and
  TypeScript (`InvalidArgumentError`, `DeadlineExceededError`, …) with
  `source` / `reason` accessors; TypeScript exports `ErrorCode`.
  Request validation failures carry `details.violations` (path, code,
  message) and no longer echo the rejected input; a handler whose response
  breaks its own contract now answers `internal` (`reason: invalid_response`)
  instead of `invalid_argument`. Access logs use warning/error for failed
  calls. `meshcall call` prints the violations in its hint.

- Registration carries the service's portable contract; the Router serves
  it through `meshcall.router.v1.Registry` (`list`, `describe`). New
  `meshcall` CLI: `router inspect` and `call` with contract-aware hints.
- Multiple Router endpoints: clients fail over between Routers, Router-mode
  servers hold one registration per Router (`connected_endpoints` /
  `routerEndpoints`). No Router-side coordination.
- `max_inflight` per instance (Python `RpcServer(max_inflight=)`, TS
  `maxInflight`), enforced locally and announced to the Router, which routes
  around full instances; a per-instance circuit breaker in the Router with
  half-open probing; instance health in `Router.Snapshot()`.
- Idempotency keys on `call.open`; Router joins and replays duplicate unary
  calls and prefers another instance after `instance_lost`; built-in client
  `RetryPolicy` for unary calls; Router errors carry `details.reason`
  (`no_instances`, `instance_lost`, `overloaded`, …) and `instance_id`;
  generated clients pass the contract's `idempotent` annotation so retries
  are safe without a key.
- Protocol conformance suite (`meshcall-conformance`) with reference
  fixtures for Python and TypeScript; it found and fixed a TypeScript
  server-stream stall when the producer is exhausted exactly as credit runs
  out.
- Graceful drain: `server.drain` / `server.drain.ack` frames; Python
  `RpcServer.stop(drain=True, drain_timeout=30)` and TypeScript
  `server.close({ drain: true })` leave the Router pool (or stop accepting)
  and wait for in-flight calls before disconnecting. `hello` gains an
  optional `capabilities` list for future negotiation.
- Contract semantics: service and method `description` (from docstrings /
  JSDoc or explicit options), method `annotations` (`idempotent`,
  `read_only`, `destructive`) and `deprecated`; generated clients carry
  descriptions as docstrings / JSDoc.
- Generators name nested definitions by their own name when unambiguous and
  render each once.
- Advisory contract warnings (W001 description, W002 unannotated mutation,
  W003 contradictory annotation, W005 empty deprecation) with hints and
  explicit silencing; emitted at import (Python `MeshCallWarning`), recorded
  by `defineService` (TS, plus a `@deprecated` overload when `description` is
  missing), summarised at server start, fatal under `MESHCALL_STRICT=1`.

## [0.1.0] - 2026-10-13

First public release of the `meshcall/1` protocol and its three runtimes.

### Added

- Python runtime: Pydantic service contracts, `RpcServer`, generated typed
  clients, WebSocket Direct and Router drivers over TCP or Unix sockets.
- TypeScript runtime: JSON Schema service contracts, `MeshCallServer`,
  `MeshCallClient`, same drivers.
- Standalone Go Router with account authentication, scoped temporary tokens,
  round-robin / least-inflight / random / sticky / disabled balancing.
- Unary, server-streaming and client-streaming calls with item credit flow
  control, cancellation and half-close; duplex as an experimental shape.
- Relative call timeouts (`timeout_ms`) enforced by server, Router and client;
  stream idle timeouts; call metadata and response trailers; protocol-level
  keepalive; unknown-field tolerance for forward compatibility.
- Automatic reconnection with backoff for clients and Router-mode servers;
  malformed frames fail only the call they name.
- Client and server interceptor chains.
- `meshcall router install` (Python) and `meshcall-router-install` (npm) to
  fetch a checksummed Router binary for the current platform.

[Unreleased]: https://github.com/t4wefan/meshcall/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/t4wefan/meshcall/releases/tag/v0.1.0
