package io.github.t4wefan.meshcall

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

internal class ClientOptions(
    val credentials: RouterCredentials?,
    val maxFrameSize: Int,
    val handshakeTimeoutMs: Long,
    val deadlineGraceMs: Long?,
    val pingIntervalMs: Long?,
    val pingTimeoutMs: Long,
    val reconnect: ReconnectPolicy,
    val retry: RetryPolicy,
    val interceptors: List<ClientInterceptor>,
    val e2eKey: String?,
    val httpClient: OkHttpClient,
)

/** One established connection and, on an end-to-end tunnel, its session. */
internal class Link(val connection: Connection) {
    @Volatile var e2e: E2ESession? = null
    @Volatile var ready = false

    /** Set before the link's calls are failed, so a call registered meanwhile sees it. */
    @Volatile var lost: MeshCallException? = null
}

private fun nowMs(): Long = System.nanoTime() / 1_000_000

internal fun newCallId(): String = UUID.randomUUID().toString().replace("-", "")

internal fun asMeshCallException(error: Throwable): Throwable = when (error) {
    is MeshCallException, is CancellationException, is IllegalArgumentException -> error
    else -> UnavailableException("Could not send: ${error.message}", retryable = true)
}

/**
 * The WebSocket client runtime under [Mesh]: one multiplexed connection,
 * reconnected lazily with backoff after a loss, shared by every call.
 */
internal class MeshCallClient(
    private val endpoints: List<String>,
    private val peerId: String,
    private val options: ClientOptions,
    val scope: CoroutineScope,
) {
    private val lock = Any()
    @Volatile private var current: Link? = null
    private var connecting: Deferred<Link>? = null
    @Volatile private var everConnected = false

    // The Router's 4401 answer (revoked or expired credentials): later calls
    // fail with it, reason included, instead of reconnecting.
    @Volatile private var refused: UnauthenticatedException? = null
    @Volatile private var closed = false
    private val calls = ConcurrentHashMap<String, ActiveCall>()

    /** Index of the endpoint in use (or to try first); advances on failure. */
    @Volatile private var cursor = 0

    init {
        require(endpoints.isNotEmpty()) { "At least one endpoint is required" }
    }

    val endpoint: String get() = endpoints[cursor % endpoints.size]

    // --- connecting ---------------------------------------------------------------------

    /**
     * Connects if not connected. After a loss this retries with backoff per the
     * reconnect policy; concurrent callers share one attempt sequence.
     */
    suspend fun connect(): Link {
        current?.takeIf { it.connection.isOpen }?.let { return it }
        val attempt = synchronized(lock) {
            current?.takeIf { it.connection.isOpen }?.let { return it }
            closed = false
            connecting ?: scope.async { openWithRetry() }.also { started ->
                connecting = started
                started.invokeOnCompletion { synchronized(lock) { if (connecting === started) connecting = null } }
            }
        }
        return attempt.await()
    }

    /** Pings over the current connection; false when there is none or it did not answer in time. */
    suspend fun checkConnection(timeoutMs: Long): Boolean {
        val link = current?.takeIf { it.connection.isOpen } ?: return false
        return link.connection.probe(minOf(timeoutMs, options.pingTimeoutMs))
    }

    private suspend fun openWithRetry(): Link {
        refused?.let { throw it }
        val retry = options.reconnect.enabled && everConnected
        val delays = if (retry) options.reconnect.delays() else null
        while (true) {
            try {
                val link = openAnyEndpoint()
                everConnected = true
                return link
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                // Refused credentials fail the same way again: report them at once.
                if (error is UnauthenticatedException || error is PermissionDeniedException) throw error
                val failure = error as? MeshCallException
                    ?: UnavailableException("Could not connect: ${error.message}", retryable = true)
                if (delays == null || !delays.hasNext() || closed) throw failure
                delay(delays.next())
            }
        }
    }

    /** One pass over the endpoints, starting from the last good one. */
    private suspend fun openAnyEndpoint(): Link {
        var last: Throwable? = null
        for (offset in endpoints.indices) {
            val index = (cursor + offset) % endpoints.size
            try {
                val link = open(endpoints[index])
                cursor = index
                return link
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                last = error
            }
        }
        throw last ?: UnavailableException("No endpoint", retryable = true)
    }

    private suspend fun open(endpoint: String): Link {
        val ready = CompletableDeferred<Link>()
        val handshake = arrayOfNulls<ClientHandshake>(1)
        var handshakeId: String? = null
        lateinit var link: Link

        fun fail(error: Throwable) {
            if (ready.completeExceptionally(error)) {
                link.connection.close(error as? MeshCallException ?: connectionLost(error.message))
                link.connection.closeSocket(1000, null)
            }
        }

        fun becomeReady(session: E2ESession?) {
            link.e2e = session
            link.ready = true
            synchronized(lock) { current = link }
            link.connection.startKeepalive()
            ready.complete(link)
        }

        val connection = Connection(
            scope, options.maxFrameSize, options.pingIntervalMs, options.pingTimeoutMs,
            object : Connection.Events {
                override fun onOpen(connection: Connection) {
                    connection.enqueue(Frame.Hello(PROTOCOL_VERSION, "client", peerId)).invokeOnCompletion { error ->
                        if (error != null) fail(error)
                    }
                }

                override fun onFrame(connection: Connection, frame: Frame) {
                    if (link.ready) {
                        route(link, frame)
                        return
                    }
                    try {
                        val id = handshakeId
                        if (id != null) {
                            // After hello.ack on an E2E tunnel: the instance must answer the
                            // handshake towards the pinned key before any call is made.
                            if (frame.callId != id || (frame !is Frame.CallResult && frame !is Frame.CallError)) return
                            becomeReady(handshake[0]!!.finish(frame))
                            return
                        }
                        if (frame !is Frame.HelloAck || frame.protocol != PROTOCOL_VERSION) {
                            throw ProtocolErrorException("Expected compatible hello.ack")
                        }
                        val e2eKey = options.e2eKey
                        if (e2eKey == null) {
                            becomeReady(null)
                            return
                        }
                        val callId = newCallId()
                        handshakeId = callId
                        val started = ClientHandshake(e2eKey, callId, options.credentials?.token ?: "")
                        handshake[0] = started
                        connection.enqueue(started.frame).invokeOnCompletion { error -> if (error != null) fail(error) }
                    } catch (error: Throwable) {
                        fail(error)
                    }
                }

                override fun onBadFrame(connection: Connection, error: FrameDecodeException) {
                    if (!link.ready) {
                        fail(ProtocolErrorException(error.message ?: "Invalid MeshCall frame"))
                        return
                    }
                    badFrame(link, error)
                }

                override fun onGone(connection: Connection, error: MeshCallException, upgradeRefused: Boolean) {
                    if (!link.ready) {
                        fail(error)
                        return
                    }
                    if (error is UnauthenticatedException) refused = error
                    // Start the next connect from the following endpoint: the one
                    // that just died is the least likely to come back first.
                    if (current === link && endpoints.size > 1) cursor = (cursor + 1) % endpoints.size
                    // Whether or not a newer link replaced it, its calls end here.
                    detach(link, error)
                }
            },
        )
        link = Link(connection)
        val request = try {
            Request.Builder().url(endpoint).apply {
                options.credentials?.let { header("Authorization", it.authorization) }
            }.build()
        } catch (error: IllegalArgumentException) {
            throw IllegalArgumentException("Invalid Router endpoint $endpoint: ${error.message}")
        }
        // TimeUnit, not java.time: Android has java.time only from API 26.
        val client = options.httpClient.newBuilder()
            .connectTimeout(options.handshakeTimeoutMs, TimeUnit.MILLISECONDS)
            .pingInterval(0, TimeUnit.MILLISECONDS) // keepalive is protocol-level ping/pong
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .build()
        connection.attach(client.newWebSocket(request, connection))
        try {
            return withTimeoutOrNull(options.handshakeTimeoutMs) { ready.await() }
                ?: throw DeadlineExceededException("MeshCall handshake timed out").also { fail(it) }
        } catch (error: CancellationException) {
            fail(error)
            throw error
        }
    }

    // --- incoming frames ---------------------------------------------------------------

    private fun route(link: Link, frame: Frame) {
        val callId = frame.callId ?: return
        val call = calls[callId] ?: return
        if (call.link !== link) return
        var opened = frame
        val sealed = call.sealed
        if (link.e2e != null && sealed != null) {
            opened = try {
                sealed.open(frame)
            } catch (error: Exception) {
                // A frame the instance did not seal: not from the other end. The
                // session stays; only this call ends.
                call.abort(
                    UnauthenticatedException(
                        "Sealed frame rejected: ${error.message}",
                        details = details("source" to "client", "reason" to "e2e_frame_rejected"),
                    ),
                    "e2e_frame_rejected",
                )
                return
            }
        }
        call.receive(opened)
        if (sealed != null && opened is Frame.CallError &&
            (opened.error.details?.get("reason") as? JsonPrimitive)?.content == "e2e_session_unknown"
        ) {
            // The instance lost the session: whatever the call shape, the next
            // call handshakes again on a new connection.
            resetConnection(link)
        }
    }

    private fun badFrame(link: Link, error: FrameDecodeException) {
        // A malformed frame that still names a call fails only that call.
        val call = error.callId?.let { calls[it] }
        if (call != null && call.link === link) {
            call.fail(ProtocolErrorException(error.message ?: "Invalid MeshCall frame"))
            return
        }
        val failure = ProtocolErrorException(error.message ?: "Invalid MeshCall frame")
        detach(link, failure)
        link.connection.closeSocket(1002, "Invalid MeshCall frame")
    }

    private fun detach(link: Link, error: MeshCallException) {
        synchronized(lock) {
            if (current === link) current = null
        }
        link.lost = error
        link.connection.close(error)
        for (call in calls.values) {
            if (call.link === link) call.fail(error)
        }
    }

    /** Drops the connection (its end-to-end session is stale); the next call reconnects. */
    private fun resetConnection(link: Link? = current) {
        if (link == null) return
        detach(link, UnavailableException("End-to-end session reset", retryable = true))
        link.connection.closeSocket(1000, "e2e session reset")
    }

    internal fun forget(call: ActiveCall) {
        calls.remove(call.callId, call)
    }

    /** Seals (on an E2E link) and queues a frame of [call]. */
    internal fun enqueue(call: ActiveCall, frame: Frame): CompletableDeferred<Unit> {
        val link = call.link
        val out = if (link.e2e != null) {
            val sealed = call.sealed
            if (sealed == null && (frame is Frame.StreamItem ||
                    (frame is Frame.CallOpen && !frame.service.startsWith(ROUTER_SERVICE_PREFIX)))
            ) {
                return CompletableDeferred<Unit>().also { it.completeExceptionally(CallCancelledException("Call is closed")) }
            }
            sealed?.seal(frame) ?: frame
        } else {
            frame
        }
        return link.connection.enqueue(out)
    }

    // --- calls ---------------------------------------------------------------------------

    /** Milliseconds left in the client-side budget, which started before connecting. */
    private fun localBudgetMs(options: CallOptions, startedAt: Long): Long? {
        val timeout = options.timeoutMs ?: return null
        val grace = this.options.deadlineGraceMs ?: clientDeadlineGraceMs(timeout)
        return maxOf(0, timeout + grace - (nowMs() - startedAt))
    }

    /** [connect] bounded by the caller's local budget. */
    private suspend fun connectWithin(options: CallOptions, startedAt: Long): Link {
        val budget = localBudgetMs(options, startedAt) ?: return connect()
        return withTimeoutOrNull(budget) { connect() } ?: throw localDeadlineError()
    }

    private fun register(link: Link, info: ClientCallInfo, startedAt: Long): ActiveCall {
        // Every frame of a call opened on an E2E connection is sealed under its session.
        // The Router answers its own services itself: those calls stay clear.
        val session = link.e2e
        val sealed = if (session != null && !info.service.startsWith(ROUTER_SERVICE_PREFIX)) {
            SealedCall(session, info.service, info.method)
        } else {
            null
        }
        val call = ActiveCall(this, link, newCallId(), info.service, info.method, info.stream, sealed, info)
        calls[call.callId] = call
        call.armDeadline(localBudgetMs(info.options, startedAt))
        link.lost?.let { call.fail(it) }
        return call
    }

    private fun openFrame(call: ActiveCall, request: JsonElement, options: CallOptions): Frame.CallOpen = Frame.CallOpen(
        callId = call.callId,
        service = call.service,
        method = call.method,
        payload = request,
        timeoutMs = options.timeoutMs?.coerceAtLeast(1),
        idleTimeoutMs = options.idleTimeoutMs?.coerceAtLeast(1),
        metadata = validateMetadata(options.metadata),
        idempotencyKey = options.idempotencyKey,
        instanceId = options.instance,
    )

    /** Sends `call.open`; a failure to send fails the call instead of throwing. */
    private suspend fun sendOpen(call: ActiveCall, request: JsonElement, options: CallOptions) {
        try {
            enqueue(call, openFrame(call, request, options)).await()
        } catch (error: CancellationException) {
            call.cancel("caller_cancelled")
            throw error
        } catch (error: Throwable) {
            call.fail(asMeshCallException(error))
        }
    }

    /** Awaits the terminal frame; a caller that gives up cancels the call. */
    private suspend fun awaitResult(call: ActiveCall, info: ClientCallInfo): JsonElement {
        try {
            return call.result.await().payload ?: JsonNull
        } catch (error: CancellationException) {
            call.cancel("caller_cancelled")
            throw error
        } finally {
            info.trailers = call.trailers
        }
    }

    suspend fun unary(service: String, method: String, request: JsonElement, options: CallOptions): JsonElement {
        options.validate()
        val info = ClientCallInfo(service, method, StreamShape.UNARY, options)
        return runChain(info) { attemptInfo ->
            try {
                unaryAttempt(attemptInfo, request)
            } catch (error: UnauthenticatedException) {
                if (error.reason != "e2e_session_unknown") throw error
                // The instance lost the session (it reconnected): the call never ran.
                // Handshake again on a new connection and retry once.
                resetConnection()
                unaryAttempt(attemptInfo, request)
            }
        } as JsonElement
    }

    private suspend fun unaryAttempt(info: ClientCallInfo, request: JsonElement): JsonElement {
        currentCoroutineContext().ensureActive()
        val startedAt = nowMs()
        val link = connectWithin(info.options, startedAt)
        val call = register(link, info, startedAt)
        sendOpen(call, request, info.options)
        return awaitResult(call, info)
    }

    /**
     * Sends [items] to the server and returns its final result. The server
     * grants input credit with `stream.window` frames.
     */
    suspend fun clientStream(
        service: String,
        method: String,
        request: JsonElement,
        items: Flow<JsonElement>,
        options: CallOptions,
    ): JsonElement {
        options.validate()
        val info = ClientCallInfo(service, method, StreamShape.CLIENT_STREAM, options)
        return runChain(info) { attemptInfo ->
            currentCoroutineContext().ensureActive()
            val startedAt = nowMs()
            val link = connectWithin(attemptInfo.options, startedAt)
            val call = register(link, attemptInfo, startedAt)
            sendOpen(call, request, attemptInfo.options)
            if (call.isTerminal) return@runChain awaitResult(call, attemptInfo)
            coroutineScope {
                val pump = launch { pumpInput(call, items) }
                try {
                    awaitResult(call, attemptInfo)
                } finally {
                    // The server normally answers after stream.end; if it fails
                    // early, the producer stops.
                    pump.cancel()
                }
            }
        } as JsonElement
    }

    private suspend fun pumpInput(call: ActiveCall, items: Flow<JsonElement>) {
        try {
            items.collect { item ->
                if (call.isTerminal) throw StopPump
                call.sendInput(item)
            }
            if (!call.isTerminal) call.closeInput()
        } catch (_: StopPump) {
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            if (!call.isTerminal) call.abort(error, "input_producer_failed")
        }
    }

    private object StopPump : CancellationException("call ended") {
        private fun readResolve(): Any = StopPump
    }

    /** A cold flow: each collection opens one server-stream call. */
    fun serverStream(service: String, method: String, request: JsonElement, options: CallOptions): Flow<JsonElement> = flow {
        options.validate()
        val info = ClientCallInfo(service, method, StreamShape.SERVER_STREAM, options)
        val call = runChain(info) { openStream(it, request) } as? ActiveCall
            ?: throw IllegalStateException("Client interceptors must return the stream produced by next")
        emitAll(call.outputFlow())
    }

    /**
     * Opens a duplex call (docs/protocol.md → Duplex calls). Never retried; an
     * idempotency key is refused by the Router.
     */
    suspend fun duplex(service: String, method: String, request: JsonElement, options: CallOptions): ActiveCall {
        options.validate()
        val info = ClientCallInfo(service, method, StreamShape.DUPLEX, options)
        return runChain(info) { openStream(it, request) } as? ActiveCall
            ?: throw IllegalStateException("Client interceptors must return the call produced by next")
    }

    private suspend fun openStream(info: ClientCallInfo, request: JsonElement): ActiveCall {
        currentCoroutineContext().ensureActive()
        val startedAt = nowMs()
        val link = connectWithin(info.options, startedAt)
        val call = register(link, info, startedAt)
        try {
            enqueue(call, openFrame(call, request, info.options)).await()
            call.grantOutput(INITIAL_STREAM_CREDIT)
            enqueue(call, Frame.StreamWindow(call.callId, Direction.SERVER, INITIAL_STREAM_CREDIT)).await()
            call.armIdle(info.options.idleTimeoutMs)
            return call
        } catch (error: CancellationException) {
            call.cancel("caller_cancelled")
            throw error
        } catch (error: Throwable) {
            val failure = asMeshCallException(error)
            call.fail(failure)
            throw failure
        }
    }

    /**
     * Invokes the interceptor chain; the first interceptor is outermost. The
     * built-in retry policy wraps everything so user interceptors see each
     * attempt.
     */
    private suspend fun runChain(info: ClientCallInfo, innermost: suspend (ClientCallInfo) -> Any?): Any? {
        var next = ClientCallNext { innermost(it) }
        for (interceptor in options.interceptors.asReversed()) {
            val inner = next
            next = ClientCallNext { interceptor.intercept(it, inner) }
        }
        val policy = options.retry
        if (policy.enabled && info.stream == StreamShape.UNARY) {
            val inner = next
            next = ClientCallNext { retryCall(policy, it, inner) }
        }
        return next.proceed(info)
    }

    fun close() {
        val link = synchronized(lock) {
            closed = true
            connecting?.cancel()
            connecting = null
            current.also { current = null }
        }
        val error = UnavailableException("Client is closed")
        if (link != null) {
            detach(link, error)
            link.connection.closeSocket(1000, null)
        }
        for (call in calls.values) call.fail(error)
    }
}

private suspend fun retryCall(policy: RetryPolicy, info: ClientCallInfo, next: ClientCallNext): Any? {
    val mayRetry = info.options.idempotent || info.options.idempotencyKey != null
    val deadline = info.options.timeoutMs?.let { nowMs() + it }
    var attempt = 1
    while (true) {
        try {
            return next.proceed(info)
        } catch (error: MeshCallException) {
            fun give(): Nothing {
                if (attempt > 1) {
                    throw MeshCallException.fromCode(
                        error.code, error.message ?: "", error.retryable, error.details.plus("attempts", JsonPrimitive(attempt)),
                    )
                }
                throw error
            }
            if (!mayRetry || !error.retryable || error.code !in policy.retryCodes || attempt >= policy.maxAttempts) give()
            val wait = policy.delayMs(attempt)
            if (deadline != null) {
                val remaining = deadline - nowMs()
                if (remaining <= wait) give()
                info.options = info.options.withTimeoutMs(remaining - wait)
            }
            delay(wait)
            attempt += 1
        }
    }
}
