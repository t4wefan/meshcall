package io.github.t4wefan.meshcall

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
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

    /** The same published vector the conformance harness checks itself against. */
    @Test
    fun noiseMatchesTheCacophonyVector() {
        val root = System.getProperty("meshcall.repoRoot") ?: "../.."
        val vector = Json.parseToJsonElement(
            File(root, "meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json").readText(),
        ).jsonObject
        fun field(name: String) = vector[name]!!.jsonPrimitive.content
        val messages = (vector["messages"] as JsonArray).map { it.jsonObject }
        fun msg(i: Int, key: String) = messages[i][key]!!.jsonPrimitive.content

        val initiator = NKInitiator(
            hex(field("init_remote_static")), prologue = hex(field("init_prologue")),
            ephemeral = hex(field("init_ephemeral")), payload = hex(msg(0, "payload")),
        )
        assertEquals(msg(0, "ciphertext"), hex(initiator.message))

        // The responder side reproduces message 2 from its own keys.
        val (reply, received, responderSession) = nkRespond(
            hex(field("resp_static")), initiator.message, prologue = hex(field("resp_prologue")),
            ephemeral = hex(field("resp_ephemeral")), payload = hex(msg(1, "payload")),
        )
        assertEquals(msg(0, "payload"), hex(received))
        assertEquals(msg(1, "ciphertext"), hex(reply))

        val (payload, session) = initiator.finish(hex(msg(1, "ciphertext")))
        assertEquals(msg(1, "payload"), hex(payload))
        assertEquals(field("handshake_hash"), hex(session.hash))
        assertEquals(session.id, responderSession.id)

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

    private fun pair(): Pair<E2ESession, E2ESession> {
        val instancePrivate = ByteArray(32) { (it + 1).toByte() }
        val initiator = NKInitiator(x25519Public(instancePrivate), prologue = tunnelPrologue(credentialDigest("mtun_e2e.x")))
        val (reply, _, instance) = nkRespond(instancePrivate, initiator.message, prologue = tunnelPrologue(credentialDigest("mtun_e2e.x")))
        return initiator.finish(reply).second to instance
    }

    @Test
    fun sealedFramesRoundTripAndHideContent() {
        val (client, instance) = pair()
        val c = SealedCall(client, "svc.v1", "m")
        val i = SealedCall(instance, "svc.v1", "m")
        val open = Frame.CallOpen("c1", "svc.v1", "m", buildJsonObject { put("secret", JsonPrimitive("s3")) }, metadata = mapOf("k" to "v"))
        val sealed = c.seal(open) as Frame.CallOpen
        assertEquals(JsonNull, sealed.payload)
        assertNull(sealed.metadata)
        assertEquals(client.id, sealed.e2eSession)
        assert(!sealed.encode().contains("s3"))
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
}
