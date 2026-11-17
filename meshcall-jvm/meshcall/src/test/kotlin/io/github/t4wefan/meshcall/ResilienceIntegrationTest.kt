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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Connection loss, keepalive and reconnection (docs/protocol.md → Liveness; architecture → Failure boundaries). */
class ResilienceIntegrationTest {
    private fun test(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(60_000) { block() } }

    private val echo = buildJsonObject { put("value", 1); put("text", "") }
    private val empty = JsonObject(emptyMap())

    @Test
    fun routerRestartFailsInFlightCallsAndTheNextCallReconnects() = test {
        val env = Env.start(instance = "restart-1")
        try {
            val port = env.router.port
            val mesh = env.mesh()
            mesh.unary(FIXTURE, "echo", echo)
            val inFlight = async { assertFailsWith<UnavailableException> { mesh.unary(FIXTURE, "slow", buildJsonObject { put("ms", 20_000) }) } }
            delay(200)
            env.router.close()
            val lost = inFlight.await()
            assertTrue(lost.retryable)
            // The same address comes back; the fixture re-registers there by itself.
            val restarted = RouterProcess.start(port = port)
            try {
                env.awaitRegistered()
                assertEquals(1, mesh.unary(FIXTURE, "echo", echo, CallOptions.DEFAULT.withTimeoutMs(10_000)).jsonObject["value"]!!.jsonPrimitive.int)
            } finally {
                mesh.close()
                restarted.close()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun keepaliveDetectsASilentPeerAndTheNextCallReconnects() = test {
        val env = Env.shared
        FreezableProxy(env.router.port).use { proxy ->
            Mesh(Router(proxy.url), MeshOptions(pingIntervalMs = 200, pingTimeoutMs = 300)).use { mesh ->
                mesh.unary(FIXTURE, "echo", echo)
                val inFlight = async { assertFailsWith<UnavailableException> { mesh.unary(FIXTURE, "slow", buildJsonObject { put("ms", 20_000) }) } }
                delay(100)
                proxy.frozen = true
                val lost = withTimeout(5_000) { inFlight.await() }
                assertTrue(lost.retryable, lost.toString())
                proxy.reset()
                assertEquals(1, mesh.unary(FIXTURE, "echo", echo, CallOptions.DEFAULT.withTimeoutMs(10_000)).jsonObject["value"]!!.jsonPrimitive.int)
            }
        }
    }

    @Test
    fun closeReleasesTheConnectionAndALaterCallReconnects() = test {
        // What an Android app does in onStop / onStart.
        val mesh = Env.shared.mesh()
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
    fun endpointsFailOver() = test {
        val dead = "ws://127.0.0.1:1"
        Mesh(Router(listOf(dead, Env.shared.url))).use { mesh ->
            assertEquals(1, mesh.unary(FIXTURE, "echo", echo).jsonObject["value"]!!.jsonPrimitive.int)
            assertEquals(Env.shared.url, mesh.endpoint)
        }
    }

    @Test
    fun firstConnectFailureIsReportedAtOnce() = test {
        Mesh(Router("ws://127.0.0.1:1")).use { mesh ->
            val error = assertFailsWith<UnavailableException> { mesh.unary(FIXTURE, "echo", echo) }
            assertTrue(error.retryable)
        }
    }

    @Test
    fun instanceOfflineThenResumedRenegotiatesTheE2ESession() = test {
        val env = Env.start(instance = "offline-1")
        try {
            val tunnel = env.mesh().use { it.tunnels.open("offline-1", listOf(TunnelScope(FIXTURE)), "user:bob", e2eKey = Env.E2E_PUBLIC) }
            env.mesh(MeshOptions(tunnel = tunnel.credential, e2eKey = Env.E2E_PUBLIC)).use { mesh ->
                assertEquals(1, mesh.unary(FIXTURE, "echo", echo).jsonObject["value"]!!.jsonPrimitive.int)
                env.fixture.destroyForcibly().waitFor()
                val offline = assertFailsWith<UnavailableException> { awaitOffline(mesh) }
                assertEquals("instance_offline", offline.reason)
                assertTrue(offline.retryable)
                // Same identity and key: the Router offers the tunnel again (resumed).
                env.startFixture()
                env.awaitRegistered()
                // The restarted instance has no sessions: e2e_session_unknown, a new
                // handshake on a new connection, and the unary call is retried once.
                val back = withTimeout(10_000) {
                    while (true) {
                        try {
                            return@withTimeout mesh.unary(FIXTURE, "echo", echo)
                        } catch (error: UnavailableException) {
                            delay(100) // the re-offer may still be on its way
                        }
                    }
                    @Suppress("UNREACHABLE_CODE") error("unreachable")
                }
                assertEquals(1, back.jsonObject["value"]!!.jsonPrimitive.int)
                assertEquals(true, mesh.unary(FIXTURE, "tunnel_info", empty).jsonObject["sealed"]!!.jsonPrimitive.content.toBoolean())
            }
        } finally {
            env.close()
        }
    }

    private suspend fun awaitOffline(mesh: Mesh): Nothing {
        withTimeout(10_000) {
            while (true) {
                try {
                    mesh.unary(FIXTURE, "echo", echo)
                    delay(50)
                } catch (error: UnavailableException) {
                    if (error.reason == "instance_offline") throw error
                    delay(50)
                }
            }
        }
        error("unreachable")
    }
}
