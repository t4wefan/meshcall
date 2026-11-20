package io.github.t4wefan.meshcall

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull
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
import kotlin.test.assertNotNull

/**
 * Clients that authenticate themselves end to end (docs/protocol.md → Client
 * authentication): IK with a key the fixture accepts, pairing with IKpsk2,
 * and `conformance.v1.Paired`, which serves only such clients. Runs in
 * [Env.peers], an environment of their own with the same fixture. Each
 * pairing test spends its own one-time pairing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PeerIntegrationTest {
    private lateinit var env: Env

    @BeforeAll
    fun setUp() {
        env = Env.peers
    }

    private fun test(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(60_000) { block() } }

    private val empty = JsonObject(emptyMap())

    private suspend fun open(): OpenedTunnel = env.mesh().use { issuer ->
        issuer.tunnels.open(
            env.instance, listOf(TunnelScope(FIXTURE), TunnelScope(PAIRED)), "user:alice/device:phone",
            ttlSeconds = 600, e2eKey = env.e2eKey,
        )
    }

    private fun client(tunnel: OpenedTunnel, identity: E2EIdentity?, options: MeshOptions = MeshOptions.DEFAULT): Mesh =
        env.mesh(options.copy(tunnel = tunnel.credential, e2eKey = env.e2eKey, e2eIdentity = identity))

    private fun identity(privateKey: String, pairing: E2EPairing? = null) = E2EIdentity(privateKey, env.instance, pairing)

    /** What `conformance.v1.Paired/peer` saw of this call's end-to-end session. */
    private suspend fun peer(mesh: Mesh): Map<String, String?> = mesh.unary(PAIRED, "peer", empty).jsonObject
        .mapValues { (_, value) -> if (value is JsonNull) null else value.jsonPrimitive.content }

    private fun pairing(index: Int): E2EPairing = env.e2ePairings.getOrNull(index)
        ?: error("the environment has no pairing $index (env.ready e2e_pairings)")

    private fun assertHandshakeRefused(error: UnauthenticatedException) {
        assertEquals("e2e_handshake_failed", error.reason, error.toString())
        assertEquals(false, error.retryable)
    }

    // conformance-scenario: e2e.ik
    @Test
    fun aKnownClientKeyReachesPairedWithIK() = test {
        val key = assertNotNull(env.e2eClientKey, "env.ready has no e2e_client_key")
        client(open(), identity(key)).use { mesh ->
            assertEquals(
                mapOf("protocol" to NOISE_IK, "peer_key" to E2EKeyPair.publicKey(key), "pairing_id" to null),
                peer(mesh),
            )
            // The same session seals ordinary calls too.
            val echo = mesh.unary(FIXTURE, "echo", buildJsonObject { put("value", 2); put("text", "ik") })
            assertEquals(2, echo.jsonObject["value"]!!.jsonPrimitive.int)
        }
    }

    // conformance-scenario: e2e.ik_unknown_key
    @Test
    fun anUnknownClientKeyFailsTheHandshake() = test {
        val tunnel = open()
        client(tunnel, identity(E2EKeyPair.generate().privateKey)).use { mesh ->
            val started = System.nanoTime()
            assertHandshakeRefused(assertFailsWith<UnauthenticatedException> { mesh.unary(FIXTURE, "echo", empty) })
            // Not retried: no reconnect backoff before the answer.
            assert((System.nanoTime() - started) / 1_000_000 < 5_000)
            // It never falls back to NK: the next call fails the same way.
            assertHandshakeRefused(assertFailsWith<UnauthenticatedException> { mesh.unary(PAIRED, "peer", empty) })
        }
    }

    // conformance-scenario: e2e.peer_required
    @Test
    fun aClientWithoutAKeyIsRefusedByPairedOnly() = test {
        client(open(), null).use { mesh ->
            val refused = assertFailsWith<PermissionDeniedException> { mesh.unary(PAIRED, "peer", empty) }
            assertEquals("e2e_peer_required", refused.reason)
            val echo = mesh.unary(FIXTURE, "echo", buildJsonObject { put("value", 1); put("text", "nk") })
            assertEquals(1, echo.jsonObject["value"]!!.jsonPrimitive.int)
        }
    }

    // conformance-scenario: e2e.pairing
    @Test
    fun aPairingAuthenticatesTheClientFromThenOn() = test {
        val tunnel = open()
        val pairing = pairing(0)
        val key = E2EKeyPair.generate()
        client(tunnel, identity(key.privateKey, pairing)).use { mesh ->
            assertEquals(
                mapOf("protocol" to NOISE_IKPSK2, "peer_key" to key.publicKey, "pairing_id" to pairing.id),
                peer(mesh),
            )
            assertEquals(NOISE_IKPSK2, peer(mesh)["protocol"], "the confirmed session stays in use")
        }
        // The pairing is spent: another key cannot use it.
        client(tunnel, identity(E2EKeyPair.generate().privateKey, pairing)).use { mesh ->
            assertHandshakeRefused(assertFailsWith<UnauthenticatedException> { mesh.unary(PAIRED, "peer", empty) })
        }
        // The paired key is known now: a new client without the pairing uses IK.
        client(tunnel, identity(key.privateKey)).use { mesh ->
            assertEquals(mapOf("protocol" to NOISE_IK, "peer_key" to key.publicKey, "pairing_id" to null), peer(mesh))
        }
    }

    // conformance-scenario: e2e.pairing_wrong_psk
    @Test
    fun aWrongPskFailsWithoutSpendingThePairing() = test {
        val tunnel = open()
        val pairing = pairing(1)
        val key = E2EKeyPair.generate()
        val wrong = E2EPairing(pairing.id, b64encode(ByteArray(32) { 3 }))
        client(tunnel, identity(key.privateKey, wrong)).use { mesh ->
            assertHandshakeRefused(assertFailsWith<UnauthenticatedException> { mesh.unary(PAIRED, "peer", empty) })
        }
        client(tunnel, identity(key.privateKey, pairing)).use { mesh ->
            assertEquals(
                mapOf("protocol" to NOISE_IKPSK2, "peer_key" to key.publicKey, "pairing_id" to pairing.id),
                peer(mesh),
            )
        }
    }

    // conformance-scenario: e2e.pairing_reconnect
    @Test
    fun aConfirmedPairingHandshakesWithIKAfterReconnecting() = test {
        val tunnel = open()
        val pairing = pairing(2)
        val key = E2EKeyPair.generate()
        // Keepalive off: only the connection check notices the paused Router.
        client(tunnel, identity(key.privateKey, pairing), MeshOptions(pingIntervalMs = null)).use { mesh ->
            assertEquals(NOISE_IKPSK2, peer(mesh)["protocol"])
            env.command("pause_router")
            try {
                assertEquals(false, mesh.checkConnection(500))
            } finally {
                env.command("resume_router")
            }
            // The same Mesh reconnects with IK: the pairing is spent, the key is known.
            assertEquals(mapOf("protocol" to NOISE_IK, "peer_key" to key.publicKey, "pairing_id" to null), peer(mesh))
        }
    }
}
