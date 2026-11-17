package io.github.t4wefan.meshcall

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import java.util.UUID

/** Largest encoded frame, sent or received, unless configured smaller; also the Router's ceiling. */
public const val DEFAULT_MAX_FRAME_SIZE: Int = 64 * 1024 * 1024

/**
 * OkHttp refuses (and closes the socket for) a message that would take its
 * outgoing queue past 16 MiB, so no single frame can be larger than this.
 */
internal const val OKHTTP_MAX_MESSAGE: Long = 16L * 1024 * 1024

/** Keep OkHttp's own FIFO short so control frames and other calls are not stuck behind it. */
private const val OKHTTP_HIGH_WATER: Long = 1L * 1024 * 1024

internal const val DEFAULT_PING_INTERVAL_MS: Long = 15_000
internal const val DEFAULT_PING_TIMEOUT_MS: Long = 10_000

internal fun isFrameTooLarge(error: Throwable): Boolean =
    error is ResourceExhaustedException && error.reason == "frame_too_large"

private class PendingFrame(val frame: Frame, val encoded: String, val size: Long) {
    val done = CompletableDeferred<Unit>()
}

/**
 * One WebSocket connection: frame encoding and size limits, control priority
 * with round-robin scheduling by `call_id` (docs/architecture.md → Flow
 * control), and protocol-level keepalive (docs/protocol.md → Liveness).
 */
internal class Connection(
    private val scope: CoroutineScope,
    private val maxFrameSize: Int,
    private val pingIntervalMs: Long?,
    private val pingTimeoutMs: Long,
    private val events: Events,
) : WebSocketListener() {

    interface Events {
        /** The upgrade succeeded; frames may now be sent. */
        fun onOpen(connection: Connection)

        /** Every decoded frame other than keepalive, on OkHttp's reader thread, in order. */
        fun onFrame(connection: Connection, frame: Frame)

        /** A frame that does not decode; [callId] names the call when the frame still did. */
        fun onBadFrame(connection: Connection, error: FrameDecodeException)

        /** The socket is gone; [error] says why (a close status the Router chose, or a loss). */
        fun onGone(connection: Connection, error: MeshCallException, upgradeRefused: Boolean)
    }

    @Volatile private var webSocket: WebSocket? = null
    private val lock = Any()
    private val controls = ArrayDeque<PendingFrame>()
    private val calls = LinkedHashMap<String, ArrayDeque<PendingFrame>>()
    private val order = ArrayDeque<String>()
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private var closed: MeshCallException? = null
    private var writerJob: Job? = null
    private var pingJob: Job? = null
    private var pendingPong: Pair<String, CompletableDeferred<Unit>>? = null
    private val goneSignal = CompletableDeferred<MeshCallException>()

    /** Completes with the reason once the socket has closed or failed. */
    val gone: CompletableDeferred<MeshCallException> get() = goneSignal

    /** The socket OkHttp created; set as soon as it exists, before the upgrade completes. */
    fun attach(socket: WebSocket) {
        if (webSocket == null) webSocket = socket
    }

    private val socket: WebSocket get() = webSocket ?: error("socket not attached")

    /** Closes the socket (or abandons the upgrade); the loss reaches [Events.onGone]. */
    fun closeSocket(code: Int, reason: String?) {
        val ws = webSocket ?: return
        if (!ws.close(code, reason)) ws.cancel()
    }

    override fun onOpen(webSocket: WebSocket, response: Response) {
        attach(webSocket)
        writerJob = scope.launch { writeLoop() }
        events.onOpen(this)
    }

    /** Queues [frame] and suspends until OkHttp has taken it. */
    suspend fun send(frame: Frame) {
        enqueue(frame).await()
    }

    /** Queues [frame] without waiting; the result completes when it was handed to OkHttp. */
    fun enqueue(frame: Frame): CompletableDeferred<Unit> {
        val encoded = frame.encode()
        // UTF-8 is at most 3 bytes per UTF-16 unit; count exactly only near the limit.
        val size = if (encoded.length.toLong() * 3 <= maxFrameSize) encoded.length.toLong() else utf8Length(encoded)
        val pending = PendingFrame(frame, encoded, size)
        val limit = minOf(maxFrameSize.toLong(), OKHTTP_MAX_MESSAGE)
        if (size > limit) {
            // The peer would close the connection on receiving it; only this
            // frame's call fails (docs/protocol.md → Frame size).
            val why = if (size > maxFrameSize) "limit $maxFrameSize" else "OkHttp's limit $OKHTTP_MAX_MESSAGE"
            pending.done.completeExceptionally(
                ResourceExhaustedException(
                    "Not sent: ${frame.kind} is $size bytes, $why",
                    details = details("source" to "client", "reason" to "frame_too_large"),
                ),
            )
            return pending.done
        }
        synchronized(lock) {
            closed?.let {
                pending.done.completeExceptionally(it)
                return pending.done
            }
            val callId = frame.callId
            if (frame is Frame.CallCancel && cancelQueuedCall(frame.callId)) {
                pending.done.complete(Unit)
                return pending.done
            }
            if (callId == null || frame.isControl) {
                controls.addLast(pending)
            } else {
                val queue = calls.getOrPut(callId) {
                    order.addLast(callId)
                    ArrayDeque()
                }
                queue.addLast(pending)
            }
        }
        wake.trySend(Unit)
        return pending.done
    }

    /**
     * A cancel for a call whose `call.open` never left: drop the whole call
     * instead (a priority cancel cannot precede an unsent open).
     */
    private fun cancelQueuedCall(callId: String): Boolean {
        val queue = calls[callId] ?: return false
        val unopened = queue.any { it.frame is Frame.CallOpen }
        calls.remove(callId)
        order.remove(callId)
        val error = CallCancelledException("Call was cancelled before sending")
        for (pending in queue) pending.done.completeExceptionally(error)
        return unopened
    }

    private fun next(): PendingFrame? = synchronized(lock) {
        if (closed != null) return null
        controls.removeFirstOrNull()?.let { return it }
        val callId = order.removeFirstOrNull() ?: return null
        val queue = calls.getValue(callId)
        val pending = queue.removeFirst()
        if (queue.isEmpty()) calls.remove(callId) else order.addLast(callId)
        pending
    }

    private suspend fun writeLoop() {
        while (true) {
            val pending = next()
            if (pending == null) {
                if (synchronized(lock) { closed } != null) return
                wake.receive()
                continue
            }
            // OkHttp has no "drained" callback; wait for its queue to empty enough.
            while (socket.queueSize() > 0 &&
                (socket.queueSize() + pending.size > OKHTTP_MAX_MESSAGE || socket.queueSize() > OKHTTP_HIGH_WATER)
            ) {
                if (synchronized(lock) { closed } != null) break
                delay(2)
            }
            val failure = synchronized(lock) { closed }
            if (failure != null) {
                pending.done.completeExceptionally(failure)
                return
            }
            if (socket.send(pending.encoded)) {
                pending.done.complete(Unit)
            } else {
                val error = connectionLost("send failed")
                pending.done.completeExceptionally(error)
                close(error)
                return
            }
        }
    }

    /** Stops sending: every queued frame fails with [error]. Does not close the socket. */
    fun close(error: MeshCallException) {
        val failed = synchronized(lock) {
            if (closed != null) return
            closed = error
            val all = controls.toList() + calls.values.flatten()
            controls.clear()
            calls.clear()
            order.clear()
            all
        }
        for (pending in failed) pending.done.completeExceptionally(error)
        pingJob?.cancel()
        pendingPong?.second?.cancel()
        wake.trySend(Unit)
    }

    val isOpen: Boolean get() = synchronized(lock) { closed == null } && !goneSignal.isCompleted

    // --- keepalive ---------------------------------------------------------------

    fun startKeepalive() {
        val interval = pingIntervalMs ?: return
        pingJob = scope.launch {
            while (true) {
                delay(interval)
                val nonce = UUID.randomUUID().toString()
                val pong = CompletableDeferred<Unit>()
                pendingPong = nonce to pong
                enqueue(Frame.Ping(nonce)).invokeOnCompletion { if (it != null) fail("keepalive send failed") }
                if (withTimeoutOrNull(pingTimeoutMs) { pong.await() } == null) {
                    fail("keepalive timeout")
                    return@launch
                }
            }
        }
    }

    /** Ends a connection whose peer stopped answering; the loss reaches [Events.onGone]. */
    fun fail(why: String) {
        val error = connectionLost(why)
        // Report the loss now: the close handshake with a silent peer only times out.
        gone(error, upgradeRefused = false)
        val ws = webSocket ?: return
        ws.close(1001, why)
        // A peer that is truly gone never completes the close handshake.
        scope.launch {
            delay(1000)
            ws.cancel()
        }
    }

    // --- OkHttp callbacks ----------------------------------------------------------

    override fun onMessage(webSocket: WebSocket, text: String) {
        if (text.length > maxFrameSize || (text.length.toLong() * 3 > maxFrameSize && utf8Length(text) > maxFrameSize)) {
            close(connectionLost("frame over the size limit"))
            webSocket.close(1009, "Frame too large")
            return
        }
        val frame = try {
            decodeFrame(text)
        } catch (error: FrameDecodeException) {
            events.onBadFrame(this, error)
            return
        }
        when (frame) {
            is Frame.Ping -> {
                enqueue(Frame.Pong(frame.nonce))
                return
            }
            is Frame.Pong -> {
                val pending = pendingPong
                if (pending != null && pending.first == frame.nonce) {
                    pendingPong = null
                    pending.second.complete(Unit)
                }
                return
            }
            else -> events.onFrame(this, frame)
        }
    }

    override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        onMessage(webSocket, bytes.utf8())
    }

    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
        webSocket.close(1000, null)
        gone(closeError(code, reason) ?: connectionLost(), upgradeRefused = false)
    }

    override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
        gone(closeError(code, reason) ?: connectionLost(), upgradeRefused = false)
    }

    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        if (response != null && response.code != 101) {
            // The Router refused the upgrade (401, 403, 429, 503): say why.
            val body = try {
                response.body.string()
            } catch (_: Exception) {
                ""
            }
            val error = handshakeError(response.code, body)
                ?: UnavailableException("Unexpected server response: ${response.code}", retryable = true)
            gone(error, upgradeRefused = true)
            return
        }
        gone(closed ?: connectionLost(t.message), upgradeRefused = false)
    }

    private fun gone(error: MeshCallException, upgradeRefused: Boolean) {
        if (!goneSignal.complete(error)) return
        close(error)
        events.onGone(this, error, upgradeRefused)
    }
}

private fun utf8Length(text: String): Long {
    var bytes = 0L
    var i = 0
    while (i < text.length) {
        val c = text[i]
        bytes += when {
            c.code < 0x80 -> 1
            c.code < 0x800 -> 2
            Character.isHighSurrogate(c) && i + 1 < text.length && Character.isLowSurrogate(text[i + 1]) -> {
                i += 1
                4
            }
            else -> 3
        }
        i += 1
    }
    return bytes
}
