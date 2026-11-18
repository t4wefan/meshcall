package io.github.t4wefan.meshcall

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Connection loss, keepalive and reconnection (docs/protocol.md → Liveness;
 * architecture → Failure boundaries), driven by the environment's
 * restart_router / pause_router / restart_fixture commands. Every test leaves
 * the environment running and resumed.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResilienceIntegrationTest {
    private lateinit var env: Env

    @BeforeAll
    fun setUp() {
        env = Env.shared
    }

    private fun test(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(60_000) { block() } }

    private val echo = buildJsonObject { put("value", 1); put("text", "") }
    private val empty = JsonObject(emptyMap())
    private val within = CallOptions.DEFAULT.withTimeoutMs(10_000)

    // conformance-scenario: reconnect.router_restart
    @Test
    fun routerRestartFailsInFlightCallsAndTheNextCallReconnects() = test {
        env.mesh().use { mesh ->
            mesh.unary(FIXTURE, "echo", echo)
            val inFlight = async { assertFailsWith<UnavailableException> { mesh.unary(FIXTURE, "slow", buildJsonObject { put("ms", 20_000) }) } }
            delay(200)
            env.command("restart_router") // answered once the fixture has registered again
            // A crash: no close frame and no call.error, so the client found the loss itself.
            val lost = inFlight.await()
            assertTrue(lost.retryable, lost.toString())
            assertTrue(lost.message!!.startsWith("Connection was lost"), lost.toString())
            assertEquals(1, mesh.unary(FIXTURE, "echo", echo, within).jsonObject["value"]!!.jsonPrimitive.int)
        }
    }

    // conformance-scenario: reconnect.dead_socket
    @Test
    fun keepaliveDropsASilentConnectionAndTheNextCallReconnects() = test {
        env.mesh(MeshOptions(pingIntervalMs = 200, pingTimeoutMs = 300)).use { mesh ->
            mesh.unary(FIXTURE, "echo", echo)
            val inFlight = async { assertFailsWith<UnavailableException> { mesh.unary(FIXTURE, "slow", buildJsonObject { put("ms", 20_000) }) } }
            delay(100)
            env.command("pause_router")
            try {
                val lost = withTimeout(5_000) { inFlight.await() }
                assertTrue(lost.retryable, lost.toString())
            } finally {
                env.command("resume_router")
            }
            assertEquals(1, mesh.unary(FIXTURE, "echo", echo, within).jsonObject["value"]!!.jsonPrimitive.int)
        }
    }

    // conformance-scenario: deadline.handshake
    @Test
    fun theBudgetCoversAHandshakeNobodyAnswers() = test {
        env.command("pause_router")
        try {
            env.mesh().use { mesh ->
                val started = System.nanoTime()
                val error = assertFailsWith<DeadlineExceededException> {
                    mesh.unary(FIXTURE, "echo", echo, CallOptions.DEFAULT.withTimeoutMs(300))
                }
                val elapsedMs = (System.nanoTime() - started) / 1_000_000
                assertEquals("client", error.source)
                // timeout plus the client grace (max(1 s, 10 %)), with slack for scheduling
                assertTrue(elapsedMs < 300 + 1000 + 500, "took $elapsedMs ms")
            }
        } finally {
            env.command("resume_router")
        }
    }

    // conformance-scenario: deadline.connect
    @Test
    fun theBudgetCoversConnectingToNothing() = test {
        Mesh(Router("ws://127.0.0.1:1")).use { mesh ->
            val error = assertFailsWith<UnavailableException> { mesh.unary(FIXTURE, "echo", echo, CallOptions.DEFAULT.withTimeoutMs(2_000)) }
            assertTrue(error.retryable)
        }
    }

    @Test
    fun closeReleasesTheConnectionAndALaterCallReconnects() = test {
        // What an Android app does in onStop / onStart.
        val mesh = env.mesh()
        val watching = async {
            assertFailsWith<UnavailableException> {
                mesh.serverStream(FIXTURE, "count", buildJsonObject { put("n", 1000); put("gap_ms", 50) }).toList()
            }
        }
        delay(300)
        mesh.close()
        assertEquals("Client is closed", watching.await().message)
        val again = mesh.serverStream(FIXTURE, "count", buildJsonObject { put("n", 3) }).toList()
        assertEquals(3, again.size)
        mesh.close()
    }

    @Test
    fun checkConnectionDropsAConnectionThatStoppedAnswering() = test {
        // What an Android app does in onResume. Keepalive is off, so only the check can notice.
        env.mesh(MeshOptions(pingIntervalMs = null)).use { mesh ->
            assertEquals(false, mesh.checkConnection(), "no connection yet")
            mesh.unary(FIXTURE, "echo", echo)
            assertEquals(true, mesh.checkConnection())
            val watching = async {
                assertFailsWith<UnavailableException> {
                    mesh.serverStream(FIXTURE, "count", buildJsonObject { put("n", 1000); put("gap_ms", 50) }).toList()
                }
            }
            delay(200)
            env.command("pause_router")
            try {
                val started = System.nanoTime()
                assertEquals(false, mesh.checkConnection(500))
                assertTrue((System.nanoTime() - started) / 1_000_000 < 1_500)
                assertTrue(withTimeout(2_000) { watching.await() }.retryable)
            } finally {
                env.command("resume_router")
            }
            assertEquals(1, mesh.unary(FIXTURE, "echo", echo, within).jsonObject["value"]!!.jsonPrimitive.int)
            assertEquals(true, mesh.checkConnectionAsync().get())
        }
    }

    @Test
    fun closeWhileConnectingFailsTheCallAsUnavailable() = test {
        // A listener that accepts and never answers: the WebSocket upgrade hangs.
        java.net.ServerSocket(0).use { silent ->
            val accepted = mutableListOf<java.net.Socket>()
            val acceptor = Thread { runCatching { while (true) accepted += silent.accept() } }.apply { isDaemon = true; start() }
            val mesh = Mesh(Router("ws://127.0.0.1:${silent.localPort}"))
            val pending = async { runCatching { mesh.unary(FIXTURE, "echo", echo) }.exceptionOrNull() }
            delay(300)
            mesh.close()
            val error = withTimeout(2_000) { pending.await() }
            assertTrue(error is UnavailableException, "got $error")
            assertEquals("Client is closed", error.message)
            accepted.forEach { it.close() }
            acceptor.interrupt()
        }
    }

    // conformance-scenario: reconnect.failover
    @Test
    fun endpointsFailOver() = test {
        Mesh(Router(listOf("ws://127.0.0.1:1", env.url))).use { mesh ->
            assertEquals(1, mesh.unary(FIXTURE, "echo", echo).jsonObject["value"]!!.jsonPrimitive.int)
            assertEquals(env.url, mesh.endpoint)
        }
    }

    // conformance-scenario: e2e.session_lost
    @Test
    fun aRestartedInstanceMakesTheClientHandshakeAgain() = test {
        val tunnel = env.mesh().use { it.tunnels.open(env.instance, listOf(TunnelScope(FIXTURE)), "user:bob", e2eKey = env.e2eKey) }
        env.mesh(MeshOptions(tunnel = tunnel.credential, e2eKey = env.e2eKey)).use { mesh ->
            assertEquals(1, mesh.unary(FIXTURE, "echo", echo).jsonObject["value"]!!.jsonPrimitive.int)
            // Same identity and key: the Router offers the tunnel again (resumed).
            env.command("restart_fixture")
            // The new process has no sessions: e2e_session_unknown, a new handshake
            // on a new connection, and the unary call is retried once, unseen.
            val back = withTimeout(10_000) {
                var result: kotlinx.serialization.json.JsonElement? = null
                while (result == null) {
                    try {
                        result = mesh.unary(FIXTURE, "echo", echo)
                    } catch (error: UnavailableException) {
                        assertEquals("instance_offline", error.reason, error.toString()) // the re-offer may still be on its way
                        delay(100)
                    }
                }
                result
            }
            assertEquals(1, back.jsonObject["value"]!!.jsonPrimitive.int)
            assertEquals(true, mesh.unary(FIXTURE, "tunnel_info", empty).jsonObject["sealed"]!!.jsonPrimitive.content.toBoolean())
        }
    }
}
