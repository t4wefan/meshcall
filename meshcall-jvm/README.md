# MeshCall for the JVM (Kotlin, Java, Android)

**English** | [简体中文](README.zh-CN.md)

`meshcall-jvm` is the JVM client runtime for [MeshCall](../README.md): it
calls services written in Python, TypeScript or Go through the MeshCall
Router, speaking `meshcall/1` over WebSocket exactly like the other runtimes
(same frames, error codes, `details` keys, deadlines, flow control, reconnect
and keepalive). It is written in Kotlin, with coroutines and `Flow`; Java
callers get `CompletableFuture`s and callbacks. It runs on Android
(minSdk 24) and any JVM (Java 11 bytecode; the tests run on JDK 17).

This release is the **client side**: it does not register services.

| | |
| --- | --- |
| Calls | unary, server stream, client stream, duplex; targeted calls (`instance`) |
| Router features | tunnels (`mtun_…`), end-to-end sealed tunnels (Noise NK, pinned instance key; IK / IKpsk2 when the client authenticates itself and pairs), tokens, Registry, AuthService, TunnelService |
| Transport | `ws://`, `wss://` (OkHttp), several Routers for failover |
| Codegen | `meshcall generate … --language kotlin`: kotlinx.serialization classes plus suspend / `Flow` methods and `…Async` methods for Java |
| Dependencies | kotlinx-coroutines, kotlinx-serialization-json, OkHttp, BouncyCastle (X25519 only) |

## Install

The artifact is `io.github.t4wefan:meshcall`. Until it is published, build
it from this repository (an included build):

```kotlin
// settings.gradle.kts of your app
includeBuild("path/to/meshcall/meshcall-jvm")
// build.gradle.kts
dependencies { implementation("io.github.t4wefan:meshcall:0.3.0") }
plugins { kotlin("plugin.serialization") } // generated clients use kotlinx.serialization
```

Requirements for the app: Kotlin 2.1 or later, with the Kotlin
serialization plugin of the same version; the runtime asks for
kotlinx-coroutines 1.10.2 and kotlinx-serialization-json 1.8.1 or later
(Gradle uses newer ones if the app has them). These are the oldest releases
built with Kotlin 2.1, and the runtime's metadata is pinned to Kotlin 2.2,
which a Kotlin 2.1 compiler still reads. A generated client compiled with
Kotlin 2.1.21 confirms it.

**R8 / ProGuard.** No rules are needed beyond those kotlinx.serialization,
coroutines and OkHttp ship: the runtime uses no reflection. Shrinking keeps
none of BouncyCastle's classes beyond what X25519 inlines, but its jar's
multi-release resources (about 100 KiB) are copied as-is; an Android app can
drop them:

```kotlin
android { packaging { resources.excludes += "META-INF/versions/*/org/bouncycastle/**" } }
```

## A generated client

Generate a typed client from any service's contract: a Python
`module:Class`, or a `*.meshcall.json` document that a TypeScript or Go
service exported.

```bash
uv run meshcall generate my_app.service:Chat --language kotlin \
  --package-name com.example.chat --output app/src/main/kotlin/com/example/chat
meshcall generate chat.meshcall.json --language kotlin --package-name com.example.chat --output Chat.kt
```

The result is one file, `<Name>Client.kt` (`demo.v1.Chat` → `ChatClient`), with every payload type as a
`@Serializable` class and a client class whose methods mirror the server's
business parameters 1:1:

```kotlin
val mesh = Mesh(Router("wss://router.example.com"), MeshOptions(token = "mk_…"))
val chat = ChatClient(mesh).withOptions(CallOptions.DEFAULT.withTimeoutMs(10_000))

val reply = chat.send(text = "hi")                         // unary: suspend fun
chat.watch(room = "general").collect { println(it) }        // server stream: Flow
val total = chat.upload(name = "a.txt", items = chunks)     // client stream: Flow in
chat.talk().use { call ->                                   // duplex
    launch { call.output.collect { println(it) } }          // read while sending
    call.send(Line("hello")); call.closeSend(); call.await()
}
mesh.close()
```

| JSON Schema | Kotlin |
| --- | --- |
| `integer` / `number` / `string` / `boolean` | `Long` / `Double` / `String` / `Boolean` |
| array, object with `additionalProperties` | `List<T>`, `Map<String, T>` |
| object with properties | `data class`; an inline object becomes a nested class (`Request.Diff`) |
| string `enum` | `typealias` of `String` plus an `…Values` object of known constants (a newer server may add values) |
| not required, or `null` allowed | `T? = null`; an omitted field lets the server's default apply. A parameter with a schema default takes it as its Kotlin default |
| unions of several types | `JsonElement` |

Methods of a **targeted** service take `instance: String` first, so the
compiler enforces it. Methods annotated idempotent set `idempotent` for the
retry policy. Without codegen, `Mesh` takes and returns `JsonElement`
(`mesh.unary(service, method, request)`), and the extension functions in
`io.github.t4wefan.meshcall` add serializers.

### Java

Each method also has an `…Async` variant for Java: unary and client-stream
calls return a `CompletableFuture` (cancelling it cancels the call), server
streams report to a `StreamObserver` and return a `CallHandle`, and duplex
calls return a `JavaDuplexCall` with `send` / `closeSend` futures.

```java
try (Mesh mesh = new Mesh(new Router("ws://127.0.0.1:8765"))) {
    ChatClient chat = new ChatClient(mesh);
    Reply reply = chat.sendAsync("hi").get();
    CallHandle handle = chat.watchAsync("general", new StreamObserver<Event>() {
        public void onNext(Event event) { … }
        public void onCompleted(Map<String, String> trailers) { … }
        public void onError(Throwable error) { … }
    });
}
```

Why futures and callbacks: `CompletableFuture` is in every JDK from 8 and on
Android from API 24, and callbacks need no reactive library. Flow control
holds too: the next item credit is granted only after `onNext` returns, so a
slow observer slows the producer. Interceptors are Kotlin-only (they are
suspend functions).

## Tunnels and end-to-end encryption

A tunnel credential reaches exactly one service instance; an end-to-end one
(`mtun_e2e.…`) seals every payload, metadata entry and error message between
the client and that instance, so the Router sees only ciphertext:

```kotlin
val mesh = Mesh(
    Router("wss://router.example.com"),
    MeshOptions(tunnel = credential, e2eKey = instancePublicKey), // both from your issuer
)
val sessions = SessionsClient(mesh)                // the Router supplies instance_id
```

The client starts the handshake (`Noise_NK_25519_AESGCM_SHA256` unless it
authenticates itself, below) towards the pinned key, never towards a key the
Router offers; a wrong key fails with a non-retryable
`UnauthenticatedException` (`reason = "e2e_handshake_failed"`) and never
falls back to plaintext. An end-to-end credential without `e2eKey`,
or a key with a plain credential, is refused when the `Mesh` is constructed.
When the instance restarts and loses its sessions, the client handshakes
again on a new connection and retries a unary call once
(`e2e_session_unknown`). A revoked tunnel's calls fail with
`UnauthenticatedException` and `reason = "tunnel_revoked"`, and later calls
fail at once with the same error.

### Client authentication and pairing

By default the client has no key of its own and the Router alone decides who
may connect. A client that keeps an X25519 key pair authenticates itself to
the instance too (`Noise_IK_25519_AESGCM_SHA256`), so the instance can refuse
every client it does not know, and services that require it
(`require_e2e_peer`) answer only such clients; others get
`PermissionDeniedException` with `reason = "e2e_peer_required"`.

```kotlin
val key = E2EKeyPair.generate()                // once; keep key.privateKey in the app's secure storage
val mesh = Mesh(
    Router("wss://router.example.com"),
    MeshOptions(
        tunnel = credential,
        e2eKey = instancePublicKey,            // the issuer's, or the one the app pinned itself
        e2eIdentity = E2EIdentity(key.privateKey, instanceId),
    ),
)
```

To be accepted the first time, the client pairs: the instance's owner hands it,
out of band and never through the Router (for example in a QR code on the
instance's device), the instance's id and public key, a pairing id and a
one-time 32-byte PSK. The app may use that instance id and key instead of the
issuer's; the issuer still pins the key when it opens the tunnel.

```kotlin
val identity = E2EIdentity(key.privateKey, qr.instanceId, E2EPairing(qr.pairingId, qr.psk))
val mesh = Mesh(router, MeshOptions(tunnel = credential, e2eKey = qr.instanceKey, e2eIdentity = identity))
mesh.connect()                                 // pairs, or throws
```

From Java: `new E2EIdentity(privateKey, instanceId).withPairing(new E2EPairing(id, psk))`
and `MeshOptions.DEFAULT.withTunnel(…).withE2eKey(…).withE2eIdentity(identity)`.

The first connection handshakes with `Noise_IKpsk2_25519_AESGCM_SHA256` and
confirms the pairing before any call; the instance may wait for a person to
accept the client, so the confirmation has 120 s (the handshake itself keeps
`handshakeTimeoutMs`). Once confirmed, every later handshake of the same `Mesh`
(reconnects included) uses IK with the same key. The pairing is then spent:
store that the key is paired and construct later `Mesh`es without the pairing.
A pairing that was never confirmed is tried again on the next connection until
the instance refuses it. An unknown key, a spent pairing or a wrong PSK fail
with a non-retryable `UnauthenticatedException` (`reason =
"e2e_handshake_failed"`; the instance does not say which). The client never
falls back to NK. An `e2eIdentity` without `e2eKey`, a private key or PSK that
is not 32 bytes of base64url, an empty instance id or a pairing id outside 1 to
128 bytes are refused before connecting.

X25519 comes from BouncyCastle's pure-Java RFC 7748 code, called directly
(no JCA provider is registered, so it does not clash with Android's built-in
copy). JCA's `XDH` needs Android API 33. AES-256-GCM, SHA-256 and HMAC come
from JCA, available on every Android level. The NK, IK and IKpsk2 handshakes
are checked against the same published Noise test vectors as the conformance
harness, and the MeshCall requests, session ids and pairing confirmation
against its `e2e_handshakes.json`.
BouncyCastle is kept instead of a hand-written RFC 7748: its field arithmetic
is constant-time and maintained, and R8 removes the rest of the jar (above).

## Calls, errors, deadlines

- `CallOptions` (`withTimeoutMs`, `withIdleTimeoutMs`, `withMetadata` /
  `withMetadataEntry`, `withIdempotencyKey`, `withIdempotent`, `withInstance`)
  apply per call or through `withOptions` on a `Mesh` or generated client.
  `meshcall-` metadata keys are refused locally (`reserved_metadata`).
- Every protocol code has its exception: `InvalidArgumentException` (with
  `violations`), `DeadlineExceededException`, `UnavailableException`,
  `CallCancelledException`, …; application codes stay `MeshCallException`.
  `source`, `reason`, `instanceId` and `attempts` read the `details` keys.
- `timeoutMs` covers connecting and the call. The client fails locally
  `max(1 s, 10 %)` after the server's deadline would have fired
  (`source = "client"`) and sends `call.cancel`. `idleTimeoutMs` bounds the
  gap between output items (`reason = "idle"`).
- Cancelling the calling coroutine (or leaving a `Flow` collection early)
  sends `call.cancel`. A server stream is a cold `Flow`: each collection opens
  one call, so `watch(id).retry { it is UnavailableException }` re-watches
  after a drop. `collectWithTrailers { … }` returns the server's trailers.
- The retry policy re-sends a failed **unary** call when the error is
  retryable, its code is in the policy's set (`unavailable`,
  `resource_exhausted`), and the method is idempotent or the call carries an
  idempotency key. Streams are never retried.
- Frames over `maxFrameSize` (64 MiB) are not sent; only that call fails
  (`resource_exhausted`, `frame_too_large`). OkHttp cannot send a single
  message over 16 MiB, so on the JVM that is the effective outgoing limit;
  stream larger data.

## Connections, reconnect and Android lifecycle

One `Mesh` holds one multiplexed WebSocket, opened by the first call (or
`connect()`). The client pings every 15 s and drops the connection when a
pong is 10 s late. A lost connection fails the calls on it with retryable
`unavailable`; the next call reconnects with exponential backoff and jitter
(`ReconnectPolicy`), waiting at most its own timeout. Calls are never
replayed automatically. Given several Router URLs, the client tries them in
order, starting from the last one that worked. A Router that closes the
connection with 4401 (revoked credentials) or refuses them is reported at
once and is not retried.

`close()` drops the connection, and the `Mesh` stays usable: the next call
connects again. That is the whole background/foreground recipe:

```kotlin
class SessionsViewModel(private val mesh: Mesh) : ViewModel() {
    private var watching: Job? = null

    fun onForeground() {                     // ProcessLifecycleOwner ON_START
        watching = viewModelScope.launch {
            sessions.watch(id)
                .retryWhen { e, _ -> e is UnavailableException && e.retryable }
                .collect { event -> cache.apply(event) }   // snapshot, then whole-item replacements
        }
    }

    fun onBackground() {                     // ON_STOP
        watching?.cancel()                   // call.cancel for the stream
        mesh.close()                         // release the socket; nothing is replayed later
    }
}
```

When the app returns, collecting again reconnects (and re-handshakes an
end-to-end tunnel) and the server sends a fresh snapshot. If the OS froze the
process without closing the socket, keepalive notices within
`pingIntervalMs + pingTimeoutMs`. Calling `close()` in the background avoids
that wait.

An app that keeps its streams open while briefly in the background calls
`checkConnection()` when it returns (`onResume`, or `checkConnectionAsync()`
from Java) instead. It pings the Router and waits at most `timeoutMs`
(default 3 s, capped at `pingTimeoutMs`). A connection the system cut while the
app was suspended is dropped at once, so its streams fail with a retryable
`unavailable` and the `retryWhen` above re-subscribes. It returns whether a live
connection answered, and never connects by itself.

```kotlin
override fun onResume() {
    super.onResume()
    lifecycleScope.launch { mesh.checkConnection() }
}
```

## What differs from the other runtimes

- Client only: no `mount` / `serve`.
- WebSocket only: `http(s)://` Routers (the HTTP call mapping), `unix://` and
  `pipe://` are refused with a hint.
- Outgoing frames are limited to 16 MiB by OkHttp (see above).
- Responses and items are validated by decoding them into the generated
  classes, the way Python clients validate through Pydantic. A mismatch is
  `ProtocolErrorException` with `reason = "invalid_response"` or
  `"invalid_output_item"`.

## Develop

```bash
cd meshcall-jvm
./gradlew build      # compile, unit tests, integration tests, Kotlin codegen goldens
./gradlew :examples:run   # the showcase demo's Kotlin client (MESHCALL_ROUTER_URL)
```

The integration tests run in the shared client conformance environment:
Gradle builds `meshcall-conformance` and the Go conformance fixture with
`go build`, and the tests start `meshcall-conformance env --fixture …`, which
runs a Router plus fixture and takes `restart_router` / `pause_router` /
`restart_fixture` commands. Each scenario is marked
`// conformance-scenario: <id>`, and `go test ./conformance/...` in
`meshcall-router` checks that every client scenario is covered. Set
`MESHCALL_FIXTURE` to a command to run against another fixture (for example
the Python one) and `MESHCALL_CONFORMANCE_BINARY` to use a prebuilt binary.
Without Go they are skipped. The `examples` module holds the
generated showcase client (`scripts/regenerate.sh kotlin`, drift-checked in
CI) with a Kotlin and a Java caller. The `:meshcall` tests also compile every
Kotlin golden under `meshcall-router/internal/codegen/testdata`.
