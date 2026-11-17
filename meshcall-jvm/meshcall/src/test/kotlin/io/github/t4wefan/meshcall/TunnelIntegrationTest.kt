package io.github.t4wefan.meshcall

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
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

/** Tunnels and end-to-end sealing (docs/protocol.md → Tunnels), against the Go fixture. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TunnelIntegrationTest {
    private lateinit var env: Env

    @BeforeAll
    fun setUp() {
        env = Env.shared
    }

    private fun test(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(30_000) { block() } }

    private val empty = JsonObject(emptyMap())
    private fun JsonElement.str(key: String) = jsonObject[key]!!.jsonPrimitive.content

    private suspend fun open(
        e2eKey: String? = null,
        principal: String = "user:alice/device:phone",
        scopes: List<TunnelScope> = listOf(TunnelScope(FIXTURE)),
    ): OpenedTunnel = env.mesh().use { issuer ->
        issuer.tunnels.open(env.instance, scopes, principal, ttlSeconds = 600, e2eKey = e2eKey)
    }

    // conformance-scenario: tunnel.scope
    // conformance-scenario: e2e.required
    @Test
    fun plainTunnelReachesOnlyItsInstance() = test {
        val tunnel = open()
        env.mesh(MeshOptions(tunnel = tunnel.credential)).use { mesh ->
            val seen = mesh.unary(FIXTURE, "tunnel_info", empty).jsonObject
            assertEquals(tunnel.tunnelId, seen["tunnel_id"]!!.jsonPrimitive.content)
            assertEquals("user:alice/device:phone", seen["principal"]!!.jsonPrimitive.content)
            assertEquals(false, seen["sealed"]!!.jsonPrimitive.boolean)
            val outside = assertFailsWith<PermissionDeniedException> { mesh.unary(TARGETED, "whoami", empty) }
            assertEquals("tunnel_scope", outside.reason)
        }
        // The instance requires end-to-end tunnels for the Vault: a plain one is refused.
        val vault = assertFailsWith<PermissionDeniedException> { open(scopes = listOf(TunnelScope(VAULT))) }
        assertEquals("tunnel_rejected", vault.reason)
    }

    // conformance-scenario: e2e.all_shapes
    @Test
    fun e2eTunnelSealsEveryShape() = test {
        val tunnel = open(e2eKey = env.e2eKey, scopes = listOf(TunnelScope(FIXTURE), TunnelScope(VAULT)))
        assertTrue(tunnel.credential.startsWith("mtun_e2e."))
        env.mesh(MeshOptions(tunnel = tunnel.credential, e2eKey = env.e2eKey)).use { mesh ->
            val seen = mesh.unary(FIXTURE, "tunnel_info", empty).jsonObject
            assertEquals(true, seen["sealed"]!!.jsonPrimitive.boolean)
            assertEquals(true, mesh.unary(VAULT, "peek", empty).jsonObject["sealed"]!!.jsonPrimitive.boolean)
            val echo = mesh.unary(FIXTURE, "echo", buildJsonObject { put("value", 3); put("text", "secret") })
            assertEquals("secret", echo.str("text"))
            val counted = mesh.serverStream(FIXTURE, "count", buildJsonObject { put("n", 25) }).toList()
            assertEquals(25, counted.size)
            val total = mesh.clientStream(
                FIXTURE, "sum", empty,
                kotlinx.coroutines.flow.flowOf(*(1..20).map { buildJsonObject { put("value", it) } }.toTypedArray()),
            )
            assertEquals(210, total.jsonObject["total"]!!.jsonPrimitive.int)
            mesh.duplex(FIXTURE, "chat", buildJsonObject { put("prefix", "e2e:") }).use { call ->
                val out = async { call.output.toList().map { it.str("text") } }
                call.send(buildJsonObject { put("text", "x") })
                call.closeSend()
                assertEquals(listOf("e2e:x", "done:1"), out.await())
            }
            // Sealed application errors keep their message and details end to end.
            val error = assertFailsWith<MeshCallException> {
                mesh.unary(FIXTURE, "fail", buildJsonObject { put("code", "app.sealed"); put("message", "hidden from the Router") })
            }
            assertEquals("hidden from the Router", error.message)
            assertEquals("fixture", error.details?.get("from")?.jsonPrimitive?.content)
            // Metadata travels sealed too; trailers come back sealed.
            val md = mesh.unary(FIXTURE, "metadata_echo", empty, CallOptions.DEFAULT.withMetadataEntry("k", "v"))
            assertEquals("v", md.jsonObject["metadata"]!!.jsonObject["k"]!!.jsonPrimitive.content)
            // The Router's own services stay clear on an E2E connection.
            assertTrue(mesh.registry.list().any { it.name == FIXTURE })
        }
    }

    // conformance-scenario: e2e.wrong_key
    @Test
    fun aWrongPinnedKeyNeverFallsBackToPlaintext() = test {
        val tunnel = open(e2eKey = env.e2eKey)
        val impostorKey = b64encode(x25519Public(ByteArray(32) { 9 }))
        env.mesh(MeshOptions(tunnel = tunnel.credential, e2eKey = impostorKey)).use { mesh ->
            val error = assertFailsWith<UnauthenticatedException> { mesh.unary(FIXTURE, "echo", empty) }
            assertEquals("e2e_handshake_failed", error.reason)
            assertEquals(false, error.retryable)
        }
    }

    // conformance-scenario: e2e.agreement
    @Test
    fun credentialsAndKeysAreCheckedBeforeConnecting() {
        assertFailsWith<IllegalArgumentException> { env.mesh(MeshOptions(tunnel = "mtun_e2e.abc")) }
        assertFailsWith<IllegalArgumentException> { env.mesh(MeshOptions(tunnel = "mtun_abc", e2eKey = env.e2eKey)) }
        assertFailsWith<IllegalArgumentException> { env.mesh(MeshOptions(token = "mtun_abc")) }
        assertFailsWith<IllegalArgumentException> { env.mesh(MeshOptions(e2eKey = env.e2eKey)) }
    }

    // conformance-scenario: tunnel.revoked
    @Test
    fun revokedTunnelFailsAtOnceAndStaysFailed() = test {
        val tunnel = open()
        env.mesh(MeshOptions(tunnel = tunnel.credential)).use { mesh ->
            mesh.unary(FIXTURE, "echo", buildJsonObject { put("value", 1); put("text", "") })
            val stream = async {
                assertFailsWith<UnauthenticatedException> {
                    mesh.serverStream(FIXTURE, "count", buildJsonObject { put("n", 100); put("gap_ms", 100) }).toList()
                }
            }
            kotlinx.coroutines.delay(300)
            env.mesh().use { issuer -> assertTrue(issuer.tunnels.revoke(tunnel.tunnelId)) }
            assertEquals("tunnel_revoked", stream.await().reason)
            // Later calls fail with the same answer without reconnecting.
            val later = assertFailsWith<UnauthenticatedException> { mesh.unary(FIXTURE, "echo", empty) }
            assertEquals("tunnel_revoked", later.reason)
            assertEquals("router", later.source)
        }
    }

    @Test
    fun refusedTunnelsAndUnknownCredentials() = test {
        val refused = assertFailsWith<PermissionDeniedException> { open(principal = "refuse:me") }
        assertEquals("tunnel_rejected", refused.reason)
        env.mesh(MeshOptions(tunnel = "mtun_notarealcredential")).use { mesh ->
            val error = assertFailsWith<UnauthenticatedException> { mesh.unary(FIXTURE, "echo", JsonNull) }
            assertEquals("router", error.source)
        }
    }
}
