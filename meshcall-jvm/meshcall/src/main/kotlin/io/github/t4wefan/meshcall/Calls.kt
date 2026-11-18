package io.github.t4wefan.meshcall

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonElement
import java.util.concurrent.atomic.AtomicBoolean

/** Items a consumer grants when a stream opens; one more for each item it takes. */
internal const val INITIAL_STREAM_CREDIT = 16L

/** The four call shapes (docs/protocol.md → Calls). */
public enum class StreamShape(public val wire: String) {
    UNARY("unary"), SERVER_STREAM("server_stream"), CLIENT_STREAM("client_stream"), DUPLEX("duplex"),
}

/** Item credit for one direction: a producer acquires one per item; the consumer grants more. */
internal class CreditWindow {
    private var credit = 0L
    private var failure: Throwable? = null
    private val waiters = ArrayDeque<CompletableDeferred<Unit>>()

    suspend fun acquire() {
        val waiter = synchronized(this) {
            failure?.let { throw it }
            if (credit > 0) {
                credit -= 1
                return
            }
            CompletableDeferred<Unit>().also { waiters.addLast(it) }
        }
        try {
            waiter.await()
        } catch (cancelled: CancellationException) {
            synchronized(this) {
                // Granted just as the caller gave up: hand the credit back.
                if (!waiters.remove(waiter) && waiter.isCompleted && !waiter.isCancelled && failure == null) {
                    grantLocked(1)
                }
            }
            throw cancelled
        }
    }

    fun grant(amount: Long) {
        if (amount <= 0) throw ProtocolErrorException("Stream credit must be positive")
        synchronized(this) {
            if (failure != null) return
            grantLocked(amount)
        }
    }

    private fun grantLocked(amount: Long) {
        credit += amount
        while (credit > 0 && waiters.isNotEmpty()) {
            credit -= 1
            waiters.removeFirst().complete(Unit)
        }
    }

    fun close(error: Throwable) {
        val pending = synchronized(this) {
            if (failure != null) return
            failure = error
            waiters.toList().also { waiters.clear() }
        }
        for (waiter in pending) waiter.completeExceptionally(error)
    }
}

/**
 * What a client interceptor sees for one outgoing call. [options] may be
 * replaced before calling `next` to add metadata or adjust timeouts.
 * [trailers] is filled once the call has ended.
 */
public class ClientCallInfo internal constructor(
    public val service: String,
    public val method: String,
    public val stream: StreamShape,
    public var options: CallOptions,
) {
    @Volatile public var trailers: Map<String, String> = emptyMap()
        internal set
}

/** Runs the rest of the chain; may be called more than once to retry. */
public fun interface ClientCallNext {
    public suspend fun proceed(info: ClientCallInfo): Any?
}

/**
 * Wraps every outgoing call; the first interceptor is outermost. Return what
 * `next` returned: the response for unary and client-stream calls, an opaque
 * handle for streams. Kotlin only (the hook is a suspend function).
 */
public fun interface ClientInterceptor {
    public suspend fun intercept(info: ClientCallInfo, next: ClientCallNext): Any?
}

internal class Outcome(val payload: JsonElement?, val trailers: Map<String, String>)

/** The client side of one call, from `call.open` to its terminal frame. */
internal class ActiveCall(
    private val client: MeshCallClient,
    val link: Link,
    val callId: String,
    val service: String,
    val method: String,
    val shape: StreamShape,
    val sealed: SealedCall?,
    private val info: ClientCallInfo,
) {
    val result = CompletableDeferred<Outcome>()
    private val output: Channel<JsonElement>? =
        if (shape == StreamShape.SERVER_STREAM || shape == StreamShape.DUPLEX) Channel(Channel.UNLIMITED) else null
    val input: CreditWindow? =
        if (shape == StreamShape.CLIENT_STREAM || shape == StreamShape.DUPLEX) CreditWindow() else null
    private var inputSequence = 0L
    private var inputClosed = false
    private var allowance = 0L
    private var expectedSequence = 0L
    private var ended = false
    private val terminal = AtomicBoolean(false)
    private var deadlineJob: Job? = null
    private var idleJob: Job? = null
    private var idleTimeoutMs: Long? = null
    private val collected = AtomicBoolean(false)

    val isTerminal: Boolean get() = terminal.get()
    val trailers: Map<String, String> get() = info.trailers

    fun armDeadline(budgetMs: Long?) {
        if (budgetMs == null) return
        deadlineJob = client.scope.launch {
            delay(budgetMs)
            abort(localDeadlineError(), "deadline_exceeded")
        }
    }

    fun armIdle(idleMs: Long?) {
        synchronized(this) { idleTimeoutMs = idleMs }
        rearmIdle()
    }

    /** (Re)starts the gap timer; measured between arrivals, not between reads. */
    private fun rearmIdle() {
        val idle = synchronized(this) {
            idleJob?.cancel()
            idleJob = null
            if (ended || terminal.get()) return
            idleTimeoutMs ?: return
        }
        val job = client.scope.launch {
            delay(idle)
            abort(localDeadlineError(idle = true), "deadline_exceeded")
        }
        synchronized(this) { if (idleJob == null && !terminal.get()) idleJob = job else job.cancel() }
    }

    fun grantOutput(credit: Long) {
        synchronized(this) { allowance += credit }
    }

    // --- frames from the peer ------------------------------------------------------

    fun receive(frame: Frame) {
        when (frame) {
            is Frame.StreamWindow -> receiveWindow(frame)
            is Frame.StreamItem -> receiveItem(frame)
            is Frame.StreamEnd -> receiveEnd(frame)
            is Frame.CallResult -> receiveResult(frame)
            is Frame.CallError -> fail(
                MeshCallException.fromCode(frame.error.code, frame.error.message, frame.error.retryable ?: false, frame.error.details),
                frame.metadata ?: emptyMap(),
            )
            else -> {}
        }
    }

    private fun receiveWindow(frame: Frame.StreamWindow) {
        val window = input
        if (window == null || frame.direction != Direction.CLIENT) {
            fail(ProtocolErrorException(if (output == null) "Invalid input stream window" else "Unexpected output stream window"))
            return
        }
        try {
            window.grant(frame.credit)
        } catch (error: MeshCallException) {
            fail(error)
        }
    }

    private fun receiveItem(frame: Frame.StreamItem) {
        val channel = output
        if (channel == null) {
            fail(ProtocolErrorException("Unexpected output stream item"))
            return
        }
        val violation = synchronized(this) {
            when {
                terminal.get() || frame.direction != Direction.SERVER -> "Invalid output stream item"
                ended || allowance <= 0 -> "Output stream exceeded granted credit"
                frame.sequence != expectedSequence -> "Expected server sequence $expectedSequence, got ${frame.sequence}"
                else -> {
                    allowance -= 1
                    expectedSequence += 1
                    null
                }
            }
        }
        if (violation != null) {
            fail(ProtocolErrorException(violation))
            return
        }
        rearmIdle()
        channel.trySend(frame.payload)
    }

    private fun receiveEnd(frame: Frame.StreamEnd) {
        if (output == null) {
            fail(ProtocolErrorException("Unexpected output stream end"))
            return
        }
        val bad = synchronized(this) {
            if (terminal.get() || frame.direction != Direction.SERVER || ended) {
                true
            } else {
                ended = true
                false
            }
        }
        if (bad) fail(ProtocolErrorException("Invalid output stream end")) else rearmIdle()
        // The consumer sees the end once the terminal frame (with trailers) arrives.
    }

    private fun receiveResult(frame: Frame.CallResult) {
        if (output != null && !synchronized(this) { ended }) {
            fail(ProtocolErrorException("Server result arrived before output stream end"))
            return
        }
        if (!settle()) return
        info.trailers = frame.metadata ?: emptyMap()
        output?.close()
        result.complete(Outcome(frame.payload, info.trailers))
    }

    // --- endings ---------------------------------------------------------------------

    /** Marks the call ended exactly once; false when another ending won. */
    private fun settle(): Boolean {
        if (!terminal.compareAndSet(false, true)) return false
        synchronized(this) {
            deadlineJob?.cancel()
            idleJob?.cancel()
        }
        client.forget(this)
        input?.close(CallCancelledException("Call is closed"))
        return true
    }

    /** Ends the call with [error]; the peer already knows (it sent the error) or the link is gone. */
    fun fail(error: Throwable, trailers: Map<String, String> = emptyMap()): Boolean {
        if (!settle()) return false
        info.trailers = trailers
        output?.close(error)
        result.completeExceptionally(error)
        return true
    }

    /** Ends the call locally with [error] and tells the peer with `call.cancel`. */
    fun abort(error: Throwable, reason: String) {
        if (fail(error)) client.enqueue(this, Frame.CallCancel(callId, reason))
    }

    /** The caller gave up: free the call and send `call.cancel`. */
    fun cancel(reason: String) {
        abort(CallCancelledException("Call was cancelled"), reason)
    }

    // --- the consumer side -------------------------------------------------------------

    /**
     * The output items, collectable once. Taking an item grants the producer
     * one more credit; leaving early cancels the call.
     */
    fun outputFlow(): Flow<JsonElement> = flow {
        check(collected.compareAndSet(false, true)) { "The output of a call can be collected only once" }
        val channel = output ?: error("Call has no output stream")
        var completed = false
        try {
            for (item in channel) {
                consumed()
                emit(item)
            }
            completed = true
            reportTrailers(info.trailers)
        } finally {
            if (!completed) cancel("caller_cancelled")
        }
    }

    private fun consumed() {
        val grant = synchronized(this) {
            if (ended || terminal.get()) false else {
                allowance += 1
                true
            }
        }
        if (grant) {
            client.enqueue(this, Frame.StreamWindow(callId, Direction.SERVER, 1)).invokeOnCompletion { error ->
                if (error != null) abort(error, "window_send_failed")
            }
        }
    }

    /** Sends one input item once credit allows. */
    suspend fun sendInput(item: JsonElement) {
        val window = input ?: throw ProtocolErrorException("Call has no input stream")
        if (synchronized(this) { inputClosed }) throw ProtocolErrorException("Input stream is closed")
        window.acquire()
        // Claim the sequence and queue the frame under one lock (enqueue does not
        // suspend): the call's frames then leave in sequence order whatever thread
        // sends them, and closeInput's stream.end never overtakes an item.
        val sent = synchronized(this) {
            if (terminal.get()) throw CallCancelledException("Call is closed")
            if (inputClosed) throw ProtocolErrorException("Input stream is closed")
            client.enqueue(this, Frame.StreamItem(callId, Direction.CLIENT, inputSequence++, item))
        }
        try {
            sent.await()
        } catch (error: Throwable) {
            if (isFrameTooLarge(error)) abort(error, "frame_too_large") // the call is open at the peer
            throw error
        }
    }

    /** Half-closes the input; the server's iteration of it then ends. */
    suspend fun closeInput() {
        if (input == null) throw ProtocolErrorException("Call has no input stream")
        val sent = synchronized(this) {
            if (inputClosed || terminal.get()) null else {
                inputClosed = true
                client.enqueue(this, Frame.StreamEnd(callId, Direction.CLIENT))
            }
        }
        sent?.await()
    }
}

/**
 * A duplex call: [send] input items while collecting [output]. The output
 * completes when the call ends successfully; a failure is thrown from the
 * collection. Collect output concurrently with sending: a caller that only
 * sends stalls once the server's output credit runs out
 * (docs/protocol.md → Duplex calls).
 */
public class DuplexCall<Input, Output> internal constructor(
    private val call: ActiveCall,
    private val encode: (Input) -> JsonElement,
    decode: (JsonElement) -> Output,
) : AutoCloseable {
    /** The server's items; collectable once. Leaving the collection early cancels the call. */
    public val output: Flow<Output> = call.outputFlow().decoded(decode)

    /** Server trailers; filled once the call has ended. */
    public val trailers: Map<String, String> get() = call.trailers

    public suspend fun send(item: Input) {
        call.sendInput(encode(item))
    }

    /** Half-closes the input; the server's iteration of it then ends. */
    public suspend fun closeSend() {
        call.closeInput()
    }

    /** Cancels the call (`call.cancel`); a no-op once it has ended. */
    public fun cancel() {
        call.cancel("caller_cancelled")
    }

    /** Same as [cancel]: leaving a `use {}` block ends a call still running. */
    override fun close() {
        cancel()
    }

    /** Suspends until the call has ended; throws its failure. */
    public suspend fun await() {
        call.result.await()
    }

    internal fun onEnded(action: () -> Unit) {
        call.result.invokeOnCompletion { action() }
    }

    internal fun <I, O> retyped(encode: (I) -> JsonElement, decode: (JsonElement) -> O): DuplexCall<I, O> {
        @Suppress("UNCHECKED_CAST")
        val raw = this as DuplexCall<JsonElement, JsonElement>
        return DuplexCall(call, { raw.encode(encode(it)) }, decode)
    }
}

/** Maps raw items through [decode]; a value the decoder refuses ends the collection (and the call). */
internal fun <T> Flow<JsonElement>.decoded(decode: (JsonElement) -> T): Flow<T> {
    val upstream = this
    return flow { upstream.collect { emit(decode(it)) } }
}
