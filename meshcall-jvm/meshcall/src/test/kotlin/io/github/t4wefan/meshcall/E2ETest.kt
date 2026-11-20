package io.github.t4wefan.meshcall

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class E2ETest {
    private fun hex(value: String): ByteArray = value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
    private fun hex(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it.toInt() and 0xff) }

    private fun testdata(name: String): JsonObject {
        val root = System.getProperty("meshcall.repoRoot") ?: "../.."
        return Json.parseToJsonElement(File(root, "meshcall-router/conformance/testdata/$name").readText()).jsonObject
    }

    /** The same published vectors the conformance harness checks itself against. */
    // conformance-scenario: e2e.test_vector
    @Test
    fun noiseMatchesTheCacophonyVectors() {
        for (name in listOf("nk", "ik", "ikpsk2")) {
            val vector = testdata("noise_${name}_25519_aesgcm_sha256.json")
            fun field(key: String) = vector[key]?.jsonPrimitive?.content?.let(::hex)
            val messages = (vector["messages"] as JsonArray).map { it.jsonObject }
            fun msg(i: Int, key: String) = messages[i][key]!!.jsonPrimitive.content
            val protocol = vector["protocol_name"]!!.jsonPrimitive.content
            val psk = (vector["init_psks"] as? JsonArray)?.firstOrNull()?.jsonPrimitive?.content?.let(::hex)

            val initiator = NoiseHandshake(
                protocol, initiator = true, prologue = field("init_prologue")!!, s = field("init_static"),
                rs = field("init_remote_static"), psk = psk, ephemeral = field("init_ephemeral"),
            )
            val responder = NoiseHandshake(
                protocol, initiator = false, prologue = field("resp_prologue")!!, s = field("resp_static"),
                psk = (vector["resp_psks"] as? JsonArray)?.firstOrNull()?.jsonPrimitive?.content?.let(::hex),
                ephemeral = field("resp_ephemeral"),
            )
            val m1 = initiator.writeMessage(hex(msg(0, "payload")))
            assertEquals(msg(0, "ciphertext"), hex(m1), "$name message 1")
            assertEquals(msg(0, "payload"), hex(responder.readMessage(m1)), "$name read 1")
            field("init_static")?.let { assertContentEquals(x25519Public(it), responder.remoteStatic, "$name: client key") }
            // The responder side reproduces message 2 from its own keys.
            val m2 = responder.writeMessage(hex(msg(1, "payload")))
            assertEquals(msg(1, "ciphertext"), hex(m2), "$name message 2")
            assertEquals(msg(1, "payload"), hex(initiator.readMessage(m2)), "$name read 2")
            val session = initiator.split()
            assertEquals(vector["handshake_hash"]!!.jsonPrimitive.content, hex(session.hash), "$name hash")
            assertEquals(session.id, responder.split().id)

            // Transport messages alternate, initiator first, each direction with its own counter.
            var sent = 0L
            var receivedN = 0L
            for (i in 2 until messages.size) {
                if ((i - 2) % 2 == 0) {
                    assertEquals(msg(i, "ciphertext"), hex(aeadEncrypt(session.sendKey, sent++, hex(msg(i, "payload")), ByteArray(0))))
                } else {
                    assertEquals(msg(i, "payload"), hex(aeadDecrypt(session.recvKey, receivedN++, hex(msg(i, "ciphertext")), ByteArray(0))))
                }
            }
        }
    }

    /**
     * The MeshCall layer on top (testdata/e2e_handshakes.json): the client's
     * request JSON, session id and pairing confirmation, byte for byte; a
     * test-side responder checks the answer and opens the confirmation.
     */
    // conformance-scenario: e2e.test_vector
    @Test
    fun clientHandshakesMatchTheMeshCallVectors() {
        val vector = testdata("e2e_handshakes.json")
        fun field(key: String) = vector[key]!!.jsonPrimitive.content
        val pairing = vector["pairing"]!!.jsonObject.let {
            E2EPairing(it["id"]!!.jsonPrimitive.content, it["psk"]!!.jsonPrimitive.content)
        }
        val credential = field("credential")
        assertEquals(field("credential_digest"), credentialDigest(credential))
        assertEquals(field("instance_public_key"), E2EKeyPair.publicKey(field("instance_private_key")))
        val identity = E2EIdentity(field("client_private_key"), field("instance_id"))
        assertEquals(field("client_public_key"), identity.publicKey)
        fun encoded(element: JsonElement) = Json.encodeToString(JsonElement.serializer(), element)
        val cases = (vector["cases"] as JsonArray).map { it.jsonObject }
        assertEquals(listOf(NOISE_NK, NOISE_IK, NOISE_IKPSK2), cases.map { it["protocol"]!!.jsonPrimitive.content })
        for (case in cases) {
            val protocol = case["protocol"]!!.jsonPrimitive.content
            val clientIdentity = when (protocol) {
                NOISE_NK -> null
                NOISE_IK -> identity
                else -> identity.withPairing(pairing)
            }
            val client = ClientHandshake(
                field("instance_public_key"), "h1", credential, clientIdentity,
                ephemeral = b64decode(field("client_ephemeral")),
            )
            assertEquals(protocol, client.protocol)
            assertEquals(encoded(case["request"]!!), encoded(client.frame.payload), "$protocol request")
            assertEquals(TUNNEL_HOST_SERVICE, client.frame.service)
            assertEquals(TUNNEL_HANDSHAKE_METHOD, client.frame.method)

            val prologue = case["prologue"]!!.jsonPrimitive.content.encodeToByteArray()
            val digest = credentialDigest(credential)
            assertContentEquals(
                prologue,
                if (protocol == NOISE_NK) tunnelPrologue(digest) else peerPrologue(digest, field("instance_id")),
            )
            val responder = NoiseHandshake(
                protocol, initiator = false, prologue = prologue, s = b64decode(field("instance_private_key")),
                psk = if (protocol == NOISE_IKPSK2) b64decode(pairing.psk) else null,
                ephemeral = b64decode(field("instance_ephemeral")),
            )
            val request = client.frame.payload.jsonObject
            responder.readMessage(b64decode(request["message"]!!.jsonPrimitive.content))
            if (protocol != NOISE_NK) assertEquals(field("client_public_key"), b64encode(responder.remoteStatic!!))
            val answer = b64encode(responder.writeMessage())
            assertEquals(case["answer"]!!.jsonObject["message"]!!.jsonPrimitive.content, answer, "$protocol answer")
            val instanceSession = responder.split()

            val session = client.finish(Frame.CallResult("h1", buildJsonObject { put("message", JsonPrimitive(answer)) }))
            assertEquals(case["session"]!!.jsonPrimitive.content, session.id)
            assertEquals(case["handshake_hash"]!!.jsonPrimitive.content, hex(session.hash))
            assertEquals(instanceSession.id, session.id)
            assertEquals(protocol == NOISE_IKPSK2, client.confirms)
            val confirmRequest = case["confirm_request"]
            if (confirmRequest != null) {
                val confirm = client.confirmFrame("h2", session)
                assertEquals(encoded(confirmRequest), encoded(confirm.payload), "confirm request")
                assertEquals(E2E_CONFIRM_TIMEOUT_MS, confirm.timeoutMs)
                val sealed = confirm.payload.jsonObject["confirm"]!!.jsonPrimitive.content
                assertEquals("{}", instanceSession.open(sealed, confirmAd(session.id)).decodeToString())
                client.confirmed(Frame.CallResult("h2", JsonObject(emptyMap())))
            }
        }
        // Once paired, the same identity sends the IK request: never the pairing again.
        val again = ClientHandshake(
            field("instance_public_key"), "h3", credential, identity.withPairing(pairing), paired = true,
            ephemeral = b64decode(field("client_ephemeral")),
        )
        assertEquals(NOISE_IK, again.protocol)
        assertEquals(encoded(cases[1]["request"]!!), encoded(again.frame.payload))
    }

    @Test
    fun handshakeRefusalsAreNotRetryable() {
        val instance = E2EKeyPair.generate()
        val client = ClientHandshake(instance.publicKey, "h1", "mtun_e2e.x", E2EIdentity(E2EKeyPair.generate().privateKey, "fixture-1"))
        // The instance's own refusal keeps its message and details.
        val refused = assertFailsWith<UnauthenticatedException> {
            client.finish(Frame.CallError("h1", ErrorPayload("unauthenticated", "End-to-end handshake failed",
                details = buildJsonObject { put("source", JsonPrimitive("server")); put("reason", JsonPrimitive("e2e_handshake_failed")) })))
        }
        assertEquals("e2e_handshake_failed", refused.reason)
        assertEquals(false, refused.retryable)
        // Anything else (a Router error, a message 2 from another key) is the client's own refusal.
        val other = assertFailsWith<UnauthenticatedException> {
            client.finish(Frame.CallError("h1", ErrorPayload("deadline_exceeded", "late")))
        }
        assertEquals("e2e_handshake_failed", other.reason)
        assertEquals("client", other.source)
        val forged = assertFailsWith<UnauthenticatedException> {
            client.finish(Frame.CallResult("h1", buildJsonObject { put("message", JsonPrimitive(b64encode(ByteArray(48)))) }))
        }
        assertEquals("e2e_handshake_failed", forged.reason)
    }

    @Test
    fun identitiesAreCheckedBeforeConnecting() {
        val key = E2EKeyPair.generate()
        assertEquals(key.publicKey, E2EKeyPair.publicKey(key.privateKey))
        assertFailsWith<IllegalArgumentException> { E2EIdentity("short", "fixture-1") }
        assertFailsWith<IllegalArgumentException> { E2EIdentity(key.privateKey, "") }
        assertFailsWith<IllegalArgumentException> { E2EPairing("", b64encode(ByteArray(32))) }
        assertFailsWith<IllegalArgumentException> { E2EPairing("x".repeat(129), b64encode(ByteArray(32))) }
        E2EPairing("x".repeat(128), b64encode(ByteArray(32)))
        assertFailsWith<IllegalArgumentException> { E2EPairing("p", b64encode(ByteArray(16))) }
        val identity = E2EIdentity(key.privateKey, "fixture-1", E2EPairing("p", b64encode(ByteArray(32) { 1 })))
        // Secrets never show up in logs.
        assert(key.privateKey !in identity.toString() && identity.pairing!!.psk !in identity.toString())
        assert(key.privateKey !in key.toString())
        // An identity needs the pinned instance key (and so an end-to-end tunnel).
        assertFailsWith<IllegalArgumentException> {
            Mesh(Router("ws://127.0.0.1:1"), MeshOptions(tunnel = "mtun_e2e.abc", e2eIdentity = identity))
        }
        Mesh(Router("ws://127.0.0.1:1"), MeshOptions(tunnel = "mtun_e2e.abc", e2eKey = key.publicKey, e2eIdentity = identity)).close()
        assert(key.privateKey !in MeshOptions(e2eIdentity = identity).toString())
    }

    private fun pair(): Pair<E2ESession, E2ESession> {
        val instancePrivate = ByteArray(32) { (it + 1).toByte() }
        val prologue = tunnelPrologue(credentialDigest("mtun_e2e.x"))
        val initiator = NoiseHandshake(NOISE_NK, initiator = true, prologue = prologue, rs = x25519Public(instancePrivate))
        val responder = NoiseHandshake(NOISE_NK, initiator = false, prologue = prologue, s = instancePrivate)
        responder.readMessage(initiator.writeMessage())
        initiator.readMessage(responder.writeMessage())
        return initiator.split() to responder.split()
    }

    @Test
    fun sealedFramesRoundTripAndHideContent() {
        val (client, instance) = pair()
        val c = SealedCall(client, "svc.v1", "m")
        val i = SealedCall(instance, "svc.v1", "m")
        val open = Frame.CallOpen("c1", "svc.v1", "m", buildJsonObject { put("secret", JsonPrimitive(SECRET)) }, metadata = mapOf("k" to "v"))
        val sealed = c.seal(open) as Frame.CallOpen
        assertEquals(JsonNull, sealed.payload)
        assertNull(sealed.metadata)
        assertEquals(client.id, sealed.e2eSession)
        // Base64 ciphertext can contain any short string by chance; a long one it cannot.
        assert(!sealed.encode().contains(SECRET) && !sealed.encode().contains("secret"))
        val restored = i.open(sealed) as Frame.CallOpen
        assertEquals(open.payload, restored.payload)
        assertEquals(mapOf("k" to "v"), restored.metadata)

        val error = Frame.CallError("c1", ErrorPayload("app.oops", "private message", false, buildJsonObject { put("x", JsonPrimitive(1)) }))
        val sealedError = i.seal(error) as Frame.CallError
        assertEquals("sealed", sealedError.error.message)
        assertNull(sealedError.error.details)
        val openedError = c.open(sealedError) as Frame.CallError
        assertEquals("private message", openedError.error.message)
        assertEquals(error.error.details, openedError.error.details)
    }

    @Test
    fun replayedMovedAndClearFramesAreRefused() {
        val (client, instance) = pair()
        val c = SealedCall(client, "svc.v1", "m")
        val i = SealedCall(instance, "svc.v1", "m")
        val item = i.seal(Frame.StreamItem("c1", Direction.SERVER, 0, JsonPrimitive(1)))
        c.open(item)
        assertFailsWith<IllegalArgumentException> { c.open(item) } // replay
        val moved = (i.seal(Frame.StreamItem("c1", Direction.SERVER, 1, JsonPrimitive(2))) as Frame.StreamItem).copy(sequence = 7)
        assertFailsWith<IllegalArgumentException> { c.open(moved) }
        assertFailsWith<IllegalArgumentException> { c.open(Frame.CallResult("c1", JsonPrimitive(1))) } // clear content
        // A clear call.error is the Router's own and is kept.
        val routerError = Frame.CallError("c1", ErrorPayload("unavailable", "instance_offline"))
        assertEquals(routerError, c.open(routerError))
        // Out-of-order arrival within the window is fine.
        val a = i.seal(Frame.StreamItem("c1", Direction.SERVER, 2, JsonPrimitive(3)))
        val b = i.seal(Frame.StreamItem("c1", Direction.SERVER, 3, JsonPrimitive(4)))
        c.open(b)
        c.open(a)
    }

    @Test
    fun credentialsAndKeysMustAgree() {
        val key = b64encode(ByteArray(32))
        assertFailsWith<IllegalArgumentException> { checkE2EAgreement("mtun_e2e.abc", null) }
        assertFailsWith<IllegalArgumentException> { checkE2EAgreement("mtun_abc", key) }
        checkE2EAgreement("mtun_e2e.abc", key)
        assertFailsWith<IllegalArgumentException> { checkE2EKey("short") }
        assertContentEquals(ByteArray(32) { 7 }, b64decode(b64encode(ByteArray(32) { 7 })))
        assertEquals(43, key.length) // base64url without padding
    }

    private companion object {
        const val SECRET = "plaintext-that-must-not-leak"
    }
}
