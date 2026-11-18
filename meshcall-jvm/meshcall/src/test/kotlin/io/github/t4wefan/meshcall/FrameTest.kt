package io.github.t4wefan.meshcall

import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class FrameTest {
    @Test
    fun framesRoundTrip() {
        val frames = listOf(
            Frame.Hello(PROTOCOL_VERSION, "client", "p1"),
            Frame.CallOpen("c", "s.v1", "m", buildJsonObject { put("a", JsonPrimitive(1)) }, timeoutMs = 5, metadata = mapOf("k" to "v"), instanceId = "i"),
            Frame.CallResult("c", JsonNull),
            Frame.CallResult("c"),
            Frame.CallError("c", ErrorPayload("unavailable", "x", true, buildJsonObject { put("reason", JsonPrimitive("r")) })),
            Frame.CallCancel("c", "why"),
            Frame.StreamItem("c", Direction.SERVER, 3, JsonPrimitive("x")),
            Frame.StreamEnd("c", Direction.CLIENT),
            Frame.StreamWindow("c", Direction.SERVER, 16),
            Frame.Ping("n"),
            Frame.Pong("n"),
        )
        for (frame in frames) assertEquals(frame, decodeFrame(frame.encode()))
    }

    @Test
    fun unknownFieldsAreIgnoredAndUnknownKindsRejected() {
        val frame = decodeFrame("""{"kind":"call.result","call_id":"c","payload":1,"future":true}""")
        assertIs<Frame.CallResult>(frame)
        val unknown = assertFailsWith<FrameDecodeException> { decodeFrame("""{"kind":"call.teleport","call_id":"c"}""") }
        assertNull(unknown.callId)
    }

    @Test
    fun invalidFramesNameTheirCall() {
        val bad = assertFailsWith<FrameDecodeException> { decodeFrame("""{"kind":"stream.window","call_id":"c","direction":"server","credit":0}""") }
        assertEquals("c", bad.callId)
        assertFailsWith<FrameDecodeException> { decodeFrame("""{"kind":"stream.item","call_id":"c","direction":"up","sequence":0,"payload":1}""") }
        assertFailsWith<FrameDecodeException> { decodeFrame("""{"kind":"call.open","call_id":"c","service":"s","method":"m"}""") }
        assertFailsWith<FrameDecodeException> { decodeFrame("""{"kind":"call.error","call_id":"c","error":{"code":"x"}}""") }
        assertFailsWith<FrameDecodeException> { decodeFrame("not json") }
    }

    @Test
    fun closeStatusesMapToErrors() {
        val revoked = closeError(4401, "tunnel_revoked")
        assertIs<UnauthenticatedException>(revoked)
        assertEquals("tunnel_revoked", revoked.reason)
        assertEquals("router", revoked.source)
        assertNull(closeError(4401, "something else")!!.reason)
        assertEquals("policy_violation", closeError(1008, "nope")!!.reason)
        val busy = closeError(4429, "tunnel connection limit reached")
        assertIs<ResourceExhaustedException>(busy)
        assert(busy.retryable)
        assertNull(closeError(1001, "going away"))
        assertIs<UnavailableException>(handshakeError(503))
    }

    @Test
    fun reservedMetadataIsRefused() {
        val reserved = assertFailsWith<InvalidArgumentException> { validateMetadata(mapOf("MeshCall-Trace" to "x")) }
        assertEquals("reserved_metadata", reserved.reason)
        assertEquals("client", reserved.source)
        assertEquals("invalid_metadata", assertFailsWith<InvalidArgumentException> { validateMetadata(mapOf("" to "x")) }.reason)
    }
}
