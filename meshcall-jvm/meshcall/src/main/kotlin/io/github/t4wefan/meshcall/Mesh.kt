package io.github.t4wefan.meshcall

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationException
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.OkHttpClient
import java.util.UUID
import java.util.concurrent.CompletableFuture

/**
 * The JSON configuration generated models are read and written with: unknown
 * fields are ignored (a newer server may add some), and optional fields left
 * at their default are omitted so the server's own default applies.
 */
public val MeshCallJson: Json = Json {
    ignoreUnknownKeys = true
    encodeDefaults = false
    explicitNulls = true
}

private const val DEFAULT_ROUTER_URL = "ws://127.0.0.1:8765"

/**
 * Layer 2: where the Router is. Pure configuration (no I/O). One URL, or
 * several independent Routers tried in order for failover. `ws://` and
 * `wss://` only: the JVM client speaks WebSocket (every call shape, tunnels
 * and end-to-end sealing); `http(s)://`, `unix://` and `pipe://` are not
 * supported here.
 */
public class Router(urls: List<String>) {
    public val endpoints: List<String> = urls.toList()

    public constructor(url: String) : this(listOf(url))

    /** `MESHCALL_ROUTER_URL`, or `ws://127.0.0.1:8765`. */
    public constructor() : this(System.getenv("MESHCALL_ROUTER_URL")?.takeIf { it.isNotBlank() } ?: DEFAULT_ROUTER_URL)

    init {
        require(endpoints.isNotEmpty()) { "At least one Router endpoint is required" }
        for (url in endpoints) {
            val scheme = url.substringBefore("://", "").lowercase()
            require(scheme == "ws" || scheme == "wss") {
                when (scheme) {
                    "http", "https" -> "The JVM client connects over WebSocket: use ws:// or wss:// instead of $url"
                    "unix", "pipe" -> "Local transports (unix://, pipe://) are not supported by the JVM client; use ws://"
                    else -> "Router URLs start with ws:// or wss://, got $url"
                }
            }
        }
    }

    /** The first endpoint. */
    public val url: String get() = endpoints.first()

    override fun toString(): String = "Router(${endpoints.joinToString()})"
}

/**
 * How a [Mesh] connects. Set at most one of [token], [credentials] and
 * [tunnel]. From Java, start from [MeshOptions.DEFAULT] and use the `with…`
 * methods, or the constructor with named defaults from Kotlin.
 */
public data class MeshOptions @JvmOverloads constructor(
    /** Bearer token (`mk_…` or `mt_…`). */
    val token: String? = null,
    /** Router credentials (Bearer or Basic). */
    val credentials: RouterCredentials? = null,
    /**
     * A tunnel credential (`mtun_…`, issued with `TunnelService/open`): this
     * Mesh then reaches only the tunnel's instance.
     */
    val tunnel: String? = null,
    /**
     * The instance public key the issuer pinned for this tunnel: every call is
     * then sealed end to end, and the connection fails unless the instance
     * proves it holds the matching private key. Take it from the issuer (or
     * from where the app pinned it itself, such as a pairing QR code), never
     * from the Router.
     */
    val e2eKey: String? = null,
    val peerId: String? = null,
    /** Keepalive ping cadence; null disables pinging. */
    val pingIntervalMs: Long? = DEFAULT_PING_INTERVAL_MS,
    val pingTimeoutMs: Long = DEFAULT_PING_TIMEOUT_MS,
    val reconnect: ReconnectPolicy = ReconnectPolicy.DEFAULT,
    val retry: RetryPolicy = RetryPolicy.DEFAULT,
    /** How long past `timeoutMs` to wait for the peer's own deadline error; default `max(1 s, 10 %)`. */
    val deadlineGraceMs: Long? = null,
    val handshakeTimeoutMs: Long = 10_000,
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    /** Wraps every outgoing call; the first interceptor is outermost. */
    val interceptors: List<ClientInterceptor> = emptyList(),
    /** A shared OkHttp client (its dispatcher and connection pool are reused). */
    val httpClient: OkHttpClient? = null,
    /**
     * This client's own key on an end-to-end tunnel (needs [e2eKey]): the
     * client then authenticates itself to the instance (Noise IK), or pairs
     * with it first (IKpsk2) when the identity carries a pairing. Never falls
     * back to an unauthenticated handshake.
     */
    val e2eIdentity: E2EIdentity? = null,
) {
    public fun withToken(value: String?): MeshOptions = copy(token = value)
    public fun withCredentials(value: RouterCredentials?): MeshOptions = copy(credentials = value)
    public fun withTunnel(value: String?): MeshOptions = copy(tunnel = value)
    public fun withE2eKey(value: String?): MeshOptions = copy(e2eKey = value)
    public fun withE2eIdentity(value: E2EIdentity?): MeshOptions = copy(e2eIdentity = value)
    public fun withPeerId(value: String?): MeshOptions = copy(peerId = value)
    public fun withPingIntervalMs(value: Long?): MeshOptions = copy(pingIntervalMs = value)
    public fun withPingTimeoutMs(value: Long): MeshOptions = copy(pingTimeoutMs = value)
    public fun withReconnect(value: ReconnectPolicy): MeshOptions = copy(reconnect = value)
    public fun withRetry(value: RetryPolicy): MeshOptions = copy(retry = value)
    public fun withDeadlineGraceMs(value: Long?): MeshOptions = copy(deadlineGraceMs = value)
    public fun withHandshakeTimeoutMs(value: Long): MeshOptions = copy(handshakeTimeoutMs = value)
    public fun withMaxFrameSize(value: Int): MeshOptions = copy(maxFrameSize = value)
    public fun withInterceptors(value: List<ClientInterceptor>): MeshOptions = copy(interceptors = value)
    public fun withHttpClient(value: OkHttpClient?): MeshOptions = copy(httpClient = value)

    override fun toString(): String =
        "MeshOptions(token=${token?.let { "***" }}, tunnel=${tunnel?.let { "***" }}, e2eKey=$e2eKey, e2eIdentity=$e2eIdentity, peerId=$peerId)"

    public companion object {
        @JvmField public val DEFAULT: MeshOptions = MeshOptions()
    }
}

/** A cancellable call started from Java (a stream observed through callbacks). */
public fun interface CallHandle {
    /** Cancels the call (`call.cancel`); the observer hears nothing more. */
    public fun cancel()
}

/** Receives a stream's items from Java; called on a background thread, one call at a time. */
public interface StreamObserver<T> {
    public fun onNext(item: T)

    /** The stream ended successfully; [trailers] are the server's. */
    public fun onCompleted(trailers: Map<String, String>) {}

    public fun onError(error: Throwable)
}

/**
 * What generated clients need from a client runtime; [Mesh] implements it.
 * The raw methods take and return `JsonElement`; the typed extension functions
 * in this package add kotlinx.serialization (de)serializers.
 */
public interface MeshCallRpc {
    public suspend fun unary(
        service: String,
        method: String,
        request: JsonElement,
        options: CallOptions = CallOptions.DEFAULT,
    ): JsonElement

    /** A cold flow: each collection opens one call; leaving the collection early cancels it. */
    public fun serverStream(
        service: String,
        method: String,
        request: JsonElement,
        options: CallOptions = CallOptions.DEFAULT,
    ): Flow<JsonElement>

    public suspend fun clientStream(
        service: String,
        method: String,
        request: JsonElement,
        items: Flow<JsonElement>,
        options: CallOptions = CallOptions.DEFAULT,
    ): JsonElement

    /** Opens a duplex call; it is never retried and takes no idempotency key. */
    public suspend fun duplex(
        service: String,
        method: String,
        request: JsonElement,
        options: CallOptions = CallOptions.DEFAULT,
    ): DuplexCall<JsonElement, JsonElement>

    /** A view sharing this connection with [options] applied to every call. */
    public fun withOptions(options: CallOptions): MeshCallRpc

    /** Where Java-facing futures and observers run; cancelled by nothing but the caller. */
    public val scope: CoroutineScope
}

/**
 * Layer 3: a peer connected to a [Router]. Any number of generated clients
 * share its one multiplexed connection. The connection is made by the first
 * call (or [connect]); after a loss the next call reconnects with backoff.
 * [close] drops the connection; a later call connects again, which is how an
 * Android app releases it in the background and recovers in the foreground.
 */
public class Mesh private constructor(
    public val router: Router,
    public val options: MeshOptions,
    private val client: MeshCallClient,
    private val defaults: CallOptions,
    private val ownsClient: Boolean,
) : MeshCallRpc, AutoCloseable {

    @JvmOverloads
    public constructor(router: Router, options: MeshOptions = MeshOptions.DEFAULT) :
        this(router, options, newClient(router, options), CallOptions.DEFAULT, true)

    override val scope: CoroutineScope get() = client.scope

    /** The Router's `Registry`: the services, instances and contracts it knows. */
    public val registry: RegistryClient get() = RegistryClient(this)

    /** The Router's `AuthService`: whoami and temporary tokens. */
    public val auth: AuthClient get() = AuthClient(this)

    /** The Router's `TunnelService`: open, revoke and list tunnels. */
    public val tunnels: TunnelsClient get() = TunnelsClient(this)

    /** The endpoint of the current (or most recently used) connection. */
    public val endpoint: String get() = client.endpoint

    /** Connects now instead of on the first call; fails like a call would. */
    public suspend fun connect(): Mesh {
        client.connect()
        return this
    }

    /** [connect] for Java. */
    public fun connectAsync(): CompletableFuture<Mesh> = future { connect() }

    /**
     * Pings the Router now and waits at most [timeoutMs] for the answer. Call it
     * when the app returns to the foreground (Android: `onResume` / `onStart`):
     * a connection the system cut while the app was in the background is dropped
     * at once, so its calls fail with a retryable `unavailable` and watchers
     * re-subscribe, instead of after the next keepalive timeout. Returns whether a
     * live connection answered; it never connects (the next call does).
     */
    public suspend fun checkConnection(timeoutMs: Long = 3_000): Boolean {
        require(timeoutMs > 0) { "timeoutMs must be positive" }
        return client.checkConnection(timeoutMs)
    }

    /** [checkConnection] for Java. */
    @JvmOverloads
    public fun checkConnectionAsync(timeoutMs: Long = 3_000): CompletableFuture<Boolean> = future { checkConnection(timeoutMs) }

    /**
     * Drops the connection; calls in flight fail with `unavailable`. The Mesh
     * stays usable: the next call connects again.
     */
    override fun close() {
        if (ownsClient) client.close()
    }

    override fun withOptions(options: CallOptions): Mesh {
        options.validate()
        return Mesh(router, this.options, client, defaults.merge(options), false)
    }

    override suspend fun unary(service: String, method: String, request: JsonElement, options: CallOptions): JsonElement =
        client.unary(service, method, request, defaults.merge(options))

    override fun serverStream(service: String, method: String, request: JsonElement, options: CallOptions): Flow<JsonElement> =
        client.serverStream(service, method, request, defaults.merge(options))

    override suspend fun clientStream(
        service: String,
        method: String,
        request: JsonElement,
        items: Flow<JsonElement>,
        options: CallOptions,
    ): JsonElement = client.clientStream(service, method, request, items, defaults.merge(options))

    override suspend fun duplex(service: String, method: String, request: JsonElement, options: CallOptions): DuplexCall<JsonElement, JsonElement> {
        // Duplex calls are never retried or deduplicated: defaults set for other
        // shapes do not apply; an explicit key reaches the Router, which refuses it.
        val base = defaults.withIdempotencyKey(null).withIdempotent(false)
        return DuplexCall(client.duplex(service, method, request, base.merge(options)), { it }, { it })
    }

    override fun toString(): String = "Mesh($router)"

    public companion object {
        /** A connected Mesh (`val mesh = Mesh.connect(Router(url), options)`). */
        @JvmStatic
        @JvmOverloads
        public suspend fun connect(router: Router, options: MeshOptions = MeshOptions.DEFAULT): Mesh =
            Mesh(router, options).connect()

        private val sharedHttpClient: OkHttpClient by lazy { OkHttpClient() }

        private fun newClient(router: Router, options: MeshOptions): MeshCallClient {
            require(listOfNotNull(options.token, options.credentials, options.tunnel).size <= 1) {
                "Specify one of token, credentials or tunnel"
            }
            val bearer = options.token ?: options.credentials?.token
            require(bearer?.startsWith(TUNNEL_CREDENTIAL_PREFIX) != true) {
                "This is a tunnel credential: pass it as MeshOptions(tunnel = …)"
            }
            if (options.e2eKey != null || options.tunnel != null) {
                require(!(options.e2eKey != null && options.tunnel == null)) { "e2eKey seals the calls of a tunnel: pass tunnel too" }
                // Refused here, before any payload goes towards the Router unsealed.
                checkE2EAgreement(options.tunnel, options.e2eKey)
            }
            options.e2eKey?.let { checkE2EKey(it) }
            require(options.e2eIdentity == null || options.e2eKey != null) {
                "e2eIdentity authenticates this client to the instance whose e2eKey you pinned: pass e2eKey too"
            }
            options.tunnel?.let {
                require(it.startsWith(TUNNEL_CREDENTIAL_PREFIX) && it.none(Char::isWhitespace)) {
                    "A tunnel credential starts with 'mtun_'; it is the `credential` returned by TunnelService/open"
                }
            }
            require(options.maxFrameSize > 0 && options.handshakeTimeoutMs > 0 && options.pingTimeoutMs > 0) {
                "Limits must be positive"
            }
            require((options.pingIntervalMs ?: 0) >= 0 && (options.deadlineGraceMs ?: 0) >= 0) { "Durations must be non-negative" }
            val credentials = options.credentials
                ?: options.token?.let { RouterCredentials.bearer(it) }
                ?: options.tunnel?.let { RouterCredentials.bearer(it) }
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            return MeshCallClient(
                router.endpoints,
                options.peerId ?: UUID.randomUUID().toString().replace("-", ""),
                ClientOptions(
                    credentials = credentials,
                    maxFrameSize = options.maxFrameSize,
                    handshakeTimeoutMs = options.handshakeTimeoutMs,
                    deadlineGraceMs = options.deadlineGraceMs,
                    pingIntervalMs = options.pingIntervalMs?.takeIf { it > 0 },
                    pingTimeoutMs = options.pingTimeoutMs,
                    reconnect = options.reconnect,
                    retry = options.retry,
                    interceptors = options.interceptors.toList(),
                    e2eKey = options.e2eKey,
                    e2eIdentity = options.e2eIdentity,
                    httpClient = options.httpClient ?: sharedHttpClient,
                ),
                scope,
            )
        }
    }
}

internal const val TUNNEL_CREDENTIAL_PREFIX = "mtun_"

// --- typed calls ---------------------------------------------------------------------------

private fun <T> encode(serializer: SerializationStrategy<T>, value: T): JsonElement =
    MeshCallJson.encodeToJsonElement(serializer, value)

/** Decodes a received value; one that breaks the client's contract is `protocol_error` (version skew). */
internal fun <T> decodeReceived(deserializer: DeserializationStrategy<T>, value: JsonElement, reason: String): T = try {
    MeshCallJson.decodeFromJsonElement(deserializer, value)
} catch (error: SerializationException) {
    throw ProtocolErrorException(
        if (reason == "invalid_response") "The response does not match the client's contract"
        else "An output item does not match the client's contract",
        details = details(
            "source" to "client", "reason" to reason,
            "violations" to JsonArray(listOf(JsonObject(mapOf(
                "path" to JsonPrimitive(""), "code" to JsonPrimitive("type"),
                // The message names fields and types, never the received value.
                "message" to JsonPrimitive(error.message?.lineSequence()?.firstOrNull()?.take(300) ?: "invalid"),
            )))),
        ),
    )
} catch (error: IllegalArgumentException) {
    throw ProtocolErrorException(
        "The received value does not match the client's contract",
        details = details("source" to "client", "reason" to reason),
    )
}

public suspend fun <Req, Res> MeshCallRpc.unary(
    service: String,
    method: String,
    request: Req,
    requestSerializer: SerializationStrategy<Req>,
    responseSerializer: DeserializationStrategy<Res>,
    options: CallOptions = CallOptions.DEFAULT,
): Res = decodeReceived(responseSerializer, unary(service, method, encode(requestSerializer, request), options), "invalid_response")

public fun <Req, Item> MeshCallRpc.serverStream(
    service: String,
    method: String,
    request: Req,
    requestSerializer: SerializationStrategy<Req>,
    itemSerializer: DeserializationStrategy<Item>,
    options: CallOptions = CallOptions.DEFAULT,
): Flow<Item> = serverStream(service, method, encode(requestSerializer, request), options)
    .decoded { decodeReceived(itemSerializer, it, "invalid_output_item") }

public suspend fun <Req, Item, Res> MeshCallRpc.clientStream(
    service: String,
    method: String,
    request: Req,
    items: Flow<Item>,
    requestSerializer: SerializationStrategy<Req>,
    itemSerializer: SerializationStrategy<Item>,
    responseSerializer: DeserializationStrategy<Res>,
    options: CallOptions = CallOptions.DEFAULT,
): Res = decodeReceived(
    responseSerializer,
    clientStream(service, method, encode(requestSerializer, request), items.map { encode(itemSerializer, it) }, options),
    "invalid_response",
)

public suspend fun <Req, In, Out> MeshCallRpc.duplex(
    service: String,
    method: String,
    request: Req,
    requestSerializer: SerializationStrategy<Req>,
    inputSerializer: SerializationStrategy<In>,
    outputSerializer: DeserializationStrategy<Out>,
    options: CallOptions = CallOptions.DEFAULT,
): DuplexCall<In, Out> {
    val raw = duplex(service, method, encode(requestSerializer, request), options)
    return raw.retyped({ encode(inputSerializer, it) }, { decodeReceived(outputSerializer, it, "invalid_output_item") })
}
