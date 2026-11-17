package io.github.t4wefan.meshcall

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The client side of the conformance scenarios (meshcall-router/conformance),
 * played by the JVM client against a real Router and the Go fixture.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientIntegrationTest {
    private lateinit var env: Env
    private lateinit var mesh: Mesh

    @BeforeAll
    fun setUp() {
        env = Env.shared
        mesh = env.mesh()
    }

    @AfterAll
    fun tearDown() {
        if (::mesh.isInitialized) mesh.close()
    }

    private fun test(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(30_000) { block() } }

    private fun obj(vararg pairs: Pair<String, Any>): JsonObject = buildJsonObject {
        for ((k, v) in pairs) when (v) {
            is String -> put(k, v)
            is Number -> put(k, JsonPrimitive(v))
            is Boolean -> put(k, v)
            is JsonElement -> put(k, v)
        }
    }

    private val JsonElement.o get() = jsonObject
    private fun JsonElement.int(key: String) = jsonObject[key]!!.jsonPrimitive.int
    private fun JsonElement.str(key: String) = jsonObject[key]!!.jsonPrimitive.content

    @Test
    fun unaryEcho() = test {
        val result = mesh.unary(FIXTURE, "echo", obj("value" to 7, "text" to "héllo"))
        assertEquals(7, result.int("value"))
        assertEquals("héllo", result.str("text"))
    }

    @Test
    fun applicationErrorKeepsCodeAndDetails() = test {
        val error = assertFailsWith<MeshCallException> {
            mesh.unary(FIXTURE, "fail", obj("code" to "app.custom", "message" to "boom"))
        }
        assertEquals("app.custom", error.code)
        assertEquals("boom", error.message)
        assertEquals("fixture", error.details?.get("from")?.jsonPrimitive?.content)
    }

    @Test
    fun invalidArgumentCarriesViolations() = test {
        val error = assertFailsWith<InvalidArgumentException> { mesh.unary(FIXTURE, "echo", obj("value" to "x", "text" to "t")) }
        assertEquals("server", error.source)
        assertEquals("request", error.reason)
        assertTrue(error.violations.any { it.path == "value" }, error.violations.toString())
    }

    @Test
    fun invalidResponseIsTheServersFault() = test {
        val error = assertFailsWith<InternalException> { mesh.unary(FIXTURE, "bad_response", JsonObject(emptyMap())) }
        assertEquals("invalid_response", error.reason)
    }

    @Test
    fun unknownMethod() = test {
        assertFailsWith<MethodNotFoundException> { mesh.unary(FIXTURE, "nope", JsonObject(emptyMap())) }
    }

    @Test
    fun targetedCallsNameTheirInstance() = test {
        val required = assertFailsWith<InvalidArgumentException> { mesh.unary(TARGETED, "whoami", JsonObject(emptyMap())) }
        assertEquals("instance_required", required.reason)
        val result = mesh.unary(TARGETED, "whoami", JsonObject(emptyMap()), CallOptions.DEFAULT.withInstance(INSTANCE))
        assertEquals(INSTANCE, result.str("instance"))
        val unknown = assertFailsWith<UnavailableException> {
            mesh.withOptions(CallOptions.DEFAULT.withInstance("nobody")).unary(TARGETED, "whoami", JsonObject(emptyMap()))
        }
        assertEquals("instance_unknown", unknown.reason)
        assertEquals(false, unknown.retryable)
    }

    @Test
    fun serverStreamReplenishesCredit() = test {
        // More items than the initial window of 16: each taken item grants one more.
        val items = mesh.serverStream(FIXTURE, "count", obj("n" to 40)).toList()
        assertEquals((0 until 40).toList(), items.map { it.int("index") })
    }

    @Test
    fun leavingAStreamEarlyCancelsIt() = test {
        val items = mesh.serverStream(FIXTURE, "count", obj("n" to 1000, "gap_ms" to 5)).take(3).toList()
        assertEquals(listOf(0, 1, 2), items.map { it.int("index") })
        // The connection keeps serving.
        assertEquals(1, mesh.unary(FIXTURE, "echo", obj("value" to 1, "text" to "")).int("value"))
    }

    @Test
    fun concurrentStreamsShareTheConnection() = test {
        val streams = (1..4).map { n -> async { mesh.serverStream(FIXTURE, "count", obj("n" to 30 + n)).toList().size } }
        assertEquals(listOf(31, 32, 33, 34), streams.map { it.await() })
    }

    @Test
    fun clientStreamSums() = test {
        val items = (1..40).map { obj("value" to it) }.asFlow()
        val result = mesh.clientStream(FIXTURE, "sum", obj("offset" to 5), items)
        assertEquals(5 + (1..40).sum(), result.int("total"))
    }

    @Test
    fun failingProducerCancelsTheClientStream() = test {
        val items = flow<JsonElement> {
            emit(obj("value" to 1))
            throw IllegalStateException("producer broke")
        }
        val error = assertFailsWith<IllegalStateException> { mesh.clientStream(FIXTURE, "sum", JsonObject(emptyMap()), items) }
        assertEquals("producer broke", error.message)
    }

    @Test
    fun serverDeadlineWins() = test {
        val tag = "deadline-" + newCallId()
        val error = assertFailsWith<DeadlineExceededException> {
            mesh.unary(FIXTURE, "slow", obj("ms" to 5_000, "tag" to tag), CallOptions.DEFAULT.withTimeoutMs(300))
        }
        assertEquals(false, error.retryable)
        // The server layer fires first (docs/protocol.md → Timeouts).
        assertEquals("server", error.source)
    }

    @Test
    fun cancellingTheCoroutineCancelsTheCall() = test {
        val tag = "cancel-" + newCallId()
        val job = launch { mesh.unary(FIXTURE, "slow", obj("ms" to 10_000, "tag" to tag)) }
        eventually { mesh.unary(FIXTURE, "invoked_tags", JsonObject(emptyMap())).tags().contains(tag) }
        job.cancelAndJoin()
        eventually { mesh.unary(FIXTURE, "cancelled_tags", JsonObject(emptyMap())).tags().contains(tag) }
    }

    private fun JsonElement.tags() = (jsonObject["tags"] as JsonArray).map { it.jsonPrimitive.content }

    private suspend fun eventually(check: suspend () -> Boolean) {
        withTimeout(5_000) {
            while (!check()) delay(20)
        }
    }

    @Test
    fun idleTimeoutIsEnforcedByTheClient() = test {
        val error = assertFailsWith<DeadlineExceededException> {
            mesh.serverStream(FIXTURE, "count", obj("n" to 3, "gap_ms" to 2_000), CallOptions.DEFAULT.withIdleTimeoutMs(300)).toList()
        }
        assertEquals("client", error.source)
        assertEquals("idle", error.reason)
    }

    @Test
    fun metadataAndTrailers() = test {
        var trailers: Map<String, String> = emptyMap()
        val recording = Mesh(Router(env.url), MeshOptions(interceptors = listOf(ClientInterceptor { info, next ->
            next.proceed(info).also { trailers = info.trailers }
        })))
        recording.use {
            val result = it.unary(FIXTURE, "metadata_echo", JsonObject(emptyMap()), CallOptions.DEFAULT.withMetadataEntry("tenant", "t1"))
            assertEquals("t1", result.o["metadata"]!!.jsonObject["tenant"]!!.jsonPrimitive.content)
        }
        assertEquals("yes", trailers["echoed"])
        val reserved = assertFailsWith<InvalidArgumentException> {
            mesh.unary(FIXTURE, "metadata_echo", JsonObject(emptyMap()), CallOptions.DEFAULT.withMetadataEntry("meshcall-x", "1"))
        }
        assertEquals("reserved_metadata", reserved.reason)
    }

    @Test
    fun streamTrailers() = test {
        val trailers = mesh.serverStream(FIXTURE, "count", obj("n" to 2)).collectWithTrailers { }
        assertEquals(emptyMap(), trailers)
    }

    @Test
    fun duplexInterleaves() = test {
        mesh.duplex(FIXTURE, "chat", obj("prefix" to "> ")).use { call ->
            val received = async { call.output.toList().map { it.str("text") } }
            for (text in listOf("a", "b", "c")) call.send(obj("text" to text))
            call.closeSend()
            assertEquals(listOf("> a", "> b", "> c", "done:3"), received.await())
        }
    }

    @Test
    fun duplexServerHalfClose() = test {
        val call = mesh.duplex(FIXTURE, "announce", JsonObject(emptyMap()))
        val output = async { call.output.toList().map { it.str("text") } }
        repeat(20) { call.send(obj("text" to "x$it")) } // beyond the initial input credit
        call.closeSend()
        assertEquals(listOf("hello"), output.await())
        call.await()
    }

    @Test
    fun duplexRefusesAnIdempotencyKey() = test {
        val call = mesh.duplex(FIXTURE, "chat", JsonObject(emptyMap()), CallOptions.DEFAULT.withIdempotencyKey("k1"))
        val error = assertFailsWith<InvalidArgumentException> { call.output.toList() }
        assertEquals("idempotency_key_on_duplex", error.reason)
    }

    @Test
    fun frameTooLargeFailsOnlyThatCall() = test {
        Mesh(Router(env.url), MeshOptions(maxFrameSize = 64 * 1024)).use { small ->
            val error = assertFailsWith<ResourceExhaustedException> {
                small.unary(FIXTURE, "echo", obj("value" to 1, "text" to "x".repeat(100_000)))
            }
            assertEquals("frame_too_large", error.reason)
            assertEquals("client", error.source)
            assertEquals(false, error.retryable)
            assertEquals(1, small.unary(FIXTURE, "echo", obj("value" to 1, "text" to "")).int("value"))
        }
    }

    @Test
    fun largeFramesWithinTheLimit() = test {
        val big = mesh.unary(FIXTURE, "big", obj("bytes" to 4_000_000))
        assertEquals(4_000_000, big.str("text").length)
    }

    @Test
    fun retryPolicyRetriesIdempotentUnaryCalls() = test {
        val error = assertFailsWith<UnavailableException> {
            mesh.unary(
                FIXTURE, "fail", obj("code" to "unavailable", "message" to "flaky", "retryable" to true),
                CallOptions.DEFAULT.withIdempotent(true),
            )
        }
        assertEquals(3, error.attempts)
        val once = assertFailsWith<UnavailableException> {
            mesh.unary(FIXTURE, "fail", obj("code" to "unavailable", "message" to "flaky", "retryable" to true))
        }
        assertEquals(null, once.attempts) // not idempotent, no key: never re-sent
    }

    @Test
    fun registryAndWhoami() = test {
        val fixture = mesh.registry.describe(FIXTURE)
        assertTrue(fixture.methods.any { it.name == "chat" && it.stream == "duplex" })
        assertTrue(fixture.instances.any { it.id == INSTANCE })
        // Development mode has no accounts to be.
        assertFailsWith<UnauthenticatedException> { mesh.auth.whoami() }
    }
}
