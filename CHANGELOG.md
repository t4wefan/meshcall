# Changelog

All notable changes to MeshCall are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The three packages (`meshcall` on
PyPI, `meshcall` on npm, and the `meshcall-router` binary) share one version
and are released together from a `vX.Y.Z` tag.

## [Unreleased]

### Changed

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

- Contract semantics: service and method `description` (from docstrings /
  JSDoc or explicit options), method `annotations` (`idempotent`,
  `read_only`, `destructive`) and `deprecated`; generated clients carry
  descriptions as docstrings / JSDoc.
- Generators name nested definitions by their own name when unambiguous and
  render each once.

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
