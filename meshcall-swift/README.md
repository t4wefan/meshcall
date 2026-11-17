# MeshCall for Swift

**English** | [简体中文](./README.zh-CN.md)

`meshcall-swift` is the Swift client runtime for [MeshCall](../README.md):
iOS and macOS apps call services written in Python, TypeScript or Go through
the MeshCall Router, over one WebSocket, with the same calls, errors,
deadlines, flow control, reconnect rules, tunnels and end-to-end encryption
as the other runtimes ([`docs/protocol.md`](../docs/protocol.md)). It is a
Swift 6 package with no dependencies beyond Foundation and CryptoKit
(iOS 16+, macOS 13+).

This runtime is **client side only**: Swift does not register services yet.

## Install

Add the package with a local path (a checkout or a submodule of this
repository):

```swift
// Package.swift
dependencies: [.package(name: "MeshCall", path: "../meshcall/meshcall-swift")],
targets: [.target(name: "App", dependencies: [.product(name: "MeshCall", package: "MeshCall")])]
```

## Generate a client

Generate a typed client from any service's contract, whatever language
serves it:

```bash
meshcall generate service.meshcall.json --language swift --output Sources/App/Generated
uv run meshcall generate my_app.service:Chat --language swift --output Sources/App/Generated
```

The result is one file (`ChatClient.swift`): `Codable`, `Sendable`,
`Hashable` structs for every payload and an `async` client whose methods
take the handler's parameters. Never edit it by hand; regenerate it.

```swift
import MeshCall

let mesh = try Mesh(Router("wss://router.example"), options: MeshOptions(credentials: .bearer("mk_…")))
let chat = ChatClient(mesh)

let reply = try await chat.send(session: "s1", text: "hello")           // unary
for try await event in try await chat.watch(session: "s1") {           // server stream
    render(event)
}
let total = try await chat.upload(name: "a.txt", items: chunks)         // client stream (any AsyncSequence)

let talk = try await chat.converse(session: "s1")                      // duplex
try await talk.send(Line(text: "hi"))
for try await line in talk { print(line.text) }
```

Per-call options go through `withOptions`, so method signatures stay the
handler's: `chat.withOptions(CallOptions(timeout: .seconds(5), metadata: ["tenant": "t1"]))`.
Without a generated client, `mesh.unary(service, method, request, as:)`,
`serverStream`, `clientStream` and `duplex` take any `Codable` types.

Streams are `AsyncSequence`s. Credit follows consumption: the Router only
lets the server send what this client has read (plus 16 items), so a slow
reader slows the server down instead of buffering. Leaving a `for try await`
loop early, cancelling the task, or releasing the stream cancels the call.

## Tunnels and end-to-end encryption

An issuer (your server, or Effi Cloud's `POST /api/instances/{id}/tunnel`)
answers with the Router, a tunnel credential and, for an end-to-end tunnel,
the instance's public key. `TunnelGrant` decodes that answer as is:

```swift
let grant = try JSONDecoder().decode(TunnelGrant.self, from: body)
// {router_url, credential, instance_id, tunnel_id, e2e_key}
let mesh = try Mesh(tunnel: grant)
let sessions = SessionsClient(mesh)
```

On a tunnel connection only that instance's scoped services exist and the
instance is implied. With `e2e_key`, every call is sealed with
`Noise_NK_25519_AESGCM_SHA256` towards that pinned key: the Router routes
and enforces scopes but sees no payloads, metadata or error messages. The
client never takes the key from the Router; a handshake that fails is
`unauthenticated` / `e2e_handshake_failed` and never falls back to clear.
An end-to-end credential (`mtun_e2e.…`) without its key, or a key with a
plain credential, is refused before connecting.

## Errors

Every failure is a `MeshCallError` with a stable `code`, a `message` and
`details` (`source`, `reason`, `violations`, …). Match on the code:

```swift
do {
    try await chat.send(session: "s1", text: "hi")
} catch let error as MeshCallError where error.code == .unavailable && error.retryable {
    // the connection was lost: try again
} catch let error as MeshCallError where error.reason == "tunnel_revoked" {
    // ask the issuer for a new grant
}
```

A response that does not decode into this client's types is
`protocol_error` with `reason: "invalid_response"` (version skew). Revoked or
refused credentials are kept: later calls fail at once with the same error
instead of reconnecting.

## Connections, reconnects and the app lifecycle

`Mesh` opens its WebSocket at the first call. Losing it fails the calls in
flight with a retryable `unavailable`; the next call reconnects with
exponential backoff (`MeshOptions.reconnect`), waiting at most its own
`timeout`. Unary calls that are idempotent, or carry an `idempotencyKey`,
are retried by the built-in retry policy. Streams are never resumed: watch
again after a loss.

Keepalive pings run every 15 s. An app the system suspended may come back to
a socket nobody answers; call `checkConnection()` when it returns to the
foreground, so that socket is dropped at once (its streams fail and your
watchers re-subscribe) instead of after the keepalive timeout:

```swift
.onChange(of: scenePhase) { _, phase in
    if phase == .active { Task { await mesh.checkConnection() } }
}
```

## Forwarding raw JSON

`JSONValue` is any JSON value. An app that forwards events unchanged (to a
WebView renderer, say) can stream them without decoding:
`mesh.serverStream("aa.v3.Sessions", "watch", ["id": "s1"], of: JSONValue.self)`,
then `try event.jsonData()`.

## Options

`MeshOptions`: `credentials` (`.bearer` for API keys, tokens and tunnel
credentials, `.basic` for accounts), `e2eKey`, `maxFrameSize` (64 MiB),
`handshakeTimeout` (10 s), `deadlineGrace` (`max(1 s, 10 %)`),
`pingInterval` / `pingTimeout` (15 s / 10 s), `reconnect`, `retry`,
`interceptors` (see each attempt; unary trailers are on `info.trailers`) and
default `callOptions`. Several Router URLs give failover:
`Router(urls: ["wss://a", "wss://b"])`.

Credentials travel in the `hello` frame, the path the protocol defines for
peers that cannot set `Authorization` on the upgrade.

## Not yet in this runtime

Registering services, the HTTP mapping (`https://` Routers), Unix sockets and
named pipes, OpenTelemetry, and Linux (CryptoKit and
`URLSessionWebSocketTask` are Apple frameworks).

## Tests

```bash
DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer swift test
scripts/check.sh swift   # also: iOS build, generated clients type-check, demo/ts2swift
```

The tests run against the shared client environment,
`meshcall-conformance env` (see
[`conformance/CLIENTS.md`](../meshcall-router/conformance/CLIENTS.md)): a real
Router, a proxy that can stall connections, and a conformance fixture. They
cover every client scenario of the suite, each test marked
`// conformance-scenario: <id>`: every call shape, credit, deadlines,
cancellation, metadata, frame size, tunnels, end-to-end sealing, revocation,
Router restarts and the foreground check. The Noise handshake is checked
against the same test vector as Go and TypeScript.

The fixture is Go's by default (built with `go build` when
`MESHCALL_FIXTURE_BINARY` is unset); `MESHCALL_FIXTURE` runs the same tests
against another runtime's server:

```bash
MESHCALL_FIXTURE="node meshcall-ts/dist/conformance/fixture.js" swift test
MESHCALL_FIXTURE="uv run --project meshcall-py --locked python meshcall-py/conformance/fixture.py" swift test
```

(The command runs from the repository root.)
