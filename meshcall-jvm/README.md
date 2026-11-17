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
| Router features | tunnels (`mtun_…`), end-to-end sealed tunnels (Noise NK, pinned instance key), tokens, Registry, AuthService, TunnelService |
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

The client starts the `Noise_NK_25519_AESGCM_SHA256` handshake towards the
pinned key, never towards a key the Router offers; a wrong key fails with a
non-retryable `UnauthenticatedException` (`reason = "e2e_handshake_failed"`)
and never falls back to plaintext. An end-to-end credential without `e2eKey`,
or a key with a plain credential, is refused when the `Mesh` is constructed.
When the instance restarts and loses its sessions, the client handshakes
again on a new connection and retries a unary call once
(`e2e_session_unknown`). A revoked tunnel's calls fail with
`UnauthenticatedException` and `reason = "tunnel_revoked"`, and later calls
fail at once with the same error.

X25519 comes from BouncyCastle's pure-Java RFC 7748 code, called directly
(no JCA provider is registered, so it does not clash with Android's built-in
copy). JCA's `XDH` needs Android API 33. AES-256-GCM, SHA-256 and HMAC come
from JCA, available on every Android level. The implementation is checked
against the same published Noise test vector as the conformance harness.

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

The integration tests build `meshcall-router` and the Go conformance fixture
with `go build`, then run the client side of the conformance scenarios
against them. Without Go they are skipped. The `examples` module holds the
generated showcase client (`scripts/regenerate.sh kotlin`, drift-checked in
CI) with a Kotlin and a Java caller. The `:meshcall` tests also compile every
Kotlin golden under `meshcall-router/internal/codegen/testdata`.
