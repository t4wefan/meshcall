package io.github.t4wefan.meshcall

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.future.future
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

// Java callers cannot call suspend functions or collect a Flow. Every
// generated client therefore also offers `…Async` methods built on these
// helpers: unary and client-stream calls return a CompletableFuture
// (JDK 8+, Android API 24+; cancelling it cancels the call), streams report
// to a StreamObserver and return a CallHandle. Callbacks need no reactive
// library and keep flow control: the next credit is granted after onNext
// returns, so a slow observer slows the producer.

/** Runs [block] in the Mesh's scope as a CompletableFuture; cancelling the future cancels the call. */
public fun <T> MeshCallRpc.future(block: suspend () -> T): CompletableFuture<T> = scope.future { block() }

/** Collects [flow] in the Mesh's scope, reporting to [observer]. */
public fun <T> MeshCallRpc.observe(flow: Flow<T>, observer: StreamObserver<T>): CallHandle {
    val job = scope.launch(start = CoroutineStart.UNDISPATCHED) {
        try {
            val trailers = flow.collectWithTrailers { observer.onNext(it) }
            observer.onCompleted(trailers)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            observer.onError(error)
        }
    }
    return CallHandle { job.cancel() }
}

/** The input of a client stream from Java: iterated on an I/O thread, so a blocking iterator is fine. */
public fun <T> javaItems(items: Iterable<T>): Flow<T> = items.asFlow().flowOn(Dispatchers.IO)

/**
 * A duplex call driven from Java; the output goes to the observer given when
 * it was opened. [send] and [closeSend] run one at a time in the order they
 * were called, from any thread: `send(a); send(b); closeSend()` puts a, b and
 * then the end of input on the wire without waiting for the futures.
 */
public class JavaDuplexCall<Input, Output> internal constructor(
    rpc: MeshCallRpc,
    private val call: DuplexCall<Input, Output>,
) : AutoCloseable {
    private class Op(val run: suspend () -> Unit, val done: CompletableFuture<Void?>)

    private val ops = Channel<Op>(Channel.UNLIMITED)

    init {
        rpc.scope.launch {
            for (op in ops) {
                if (op.done.isDone) continue // cancelled by the caller before it ran
                try {
                    coroutineScope {
                        val running = async { op.run() }
                        op.done.whenComplete { _, _ -> if (op.done.isCancelled) running.cancel() }
                        running.await()
                    }
                    op.done.complete(null)
                } catch (cancelled: CancellationException) {
                    if (!currentCoroutineContext().isActive) {
                        op.done.cancel(false)
                        throw cancelled
                    }
                    // Only this operation's future was cancelled: run the next one.
                } catch (error: Throwable) {
                    op.done.completeExceptionally(error)
                }
            }
        }
        // Operations queued after the call ended still run, and fail as closed.
        call.onEnded { ops.close() }
    }

    private fun submit(run: suspend () -> Unit): CompletableFuture<Void?> {
        val done = CompletableFuture<Void?>()
        if (ops.trySend(Op(run, done)).isFailure) done.completeExceptionally(CallCancelledException("Call is closed"))
        return done
    }

    public fun send(item: Input): CompletableFuture<Void?> = submit { call.send(item) }
    public fun closeSend(): CompletableFuture<Void?> = submit { call.closeSend() }
    public val trailers: Map<String, String> get() = call.trailers
    public fun cancel() {
        call.cancel()
    }

    override fun close() {
        cancel()
    }
}

/** Opens a duplex call for Java: [observer] receives the output once the call is open. */
public fun <Input, Output> MeshCallRpc.duplexAsync(
    open: suspend () -> DuplexCall<Input, Output>,
    observer: StreamObserver<Output>,
): CompletableFuture<JavaDuplexCall<Input, Output>> = future {
    val call = open()
    observe(call.output, observer)
    JavaDuplexCall(this, call)
}

/** Where a server stream leaves its trailers for [collectWithTrailers]. */
internal class TrailerSink : AbstractCoroutineContextElement(Key) {
    @Volatile var trailers: Map<String, String> = emptyMap()

    companion object Key : CoroutineContext.Key<TrailerSink>
}

/**
 * Collects a server stream of this runtime and returns the server's trailers
 * once it has ended: `val trailers = client.count(3).collectWithTrailers { … }`.
 */
public suspend fun <T> Flow<T>.collectWithTrailers(action: suspend (T) -> Unit): Map<String, String> {
    val sink = TrailerSink()
    withContext(sink) { collect { action(it) } }
    return sink.trailers
}

internal suspend fun reportTrailers(trailers: Map<String, String>) {
    currentCoroutineContext()[TrailerSink]?.trailers = trailers
}
