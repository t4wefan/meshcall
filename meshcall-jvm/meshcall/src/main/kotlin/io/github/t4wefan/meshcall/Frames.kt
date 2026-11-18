package io.github.t4wefan.meshcall

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull

/** The protocol version every connection negotiates in `hello`. */
public const val PROTOCOL_VERSION: String = "meshcall/1"

/** The direction of stream data: produced by the client, or by the server. */
internal enum class Direction(val wire: String) {
    CLIENT("client"), SERVER("server");

    companion object {
        fun of(value: String): Direction? = entries.firstOrNull { it.wire == value }
    }
}

internal data class ErrorPayload(
    val code: String,
    val message: String,
    val retryable: Boolean? = null,
    val details: JsonObject? = null,
)

/**
 * One frame (docs/protocol.md → Frames). Decoding ignores unknown fields and
 * rejects unknown kinds (the meshcall/1 compatibility rule).
 */
internal sealed class Frame(val kind: String) {
    open val callId: String? get() = null

    data class Hello(
        val protocol: String,
        val role: String,
        val peerId: String,
        val instanceId: String? = null,
        val capabilities: List<String>? = null,
        val authorization: String? = null,
    ) : Frame("hello")

    data class HelloAck(val protocol: String, val connectionId: String) : Frame("hello.ack")

    /** Frames a client never acts on but must still accept as known kinds. */
    data class Other(val name: String) : Frame(name)

    data class CallOpen(
        override val callId: String,
        val service: String,
        val method: String,
        val payload: JsonElement,
        val timeoutMs: Long? = null,
        val idleTimeoutMs: Long? = null,
        val metadata: Map<String, String>? = null,
        val idempotencyKey: String? = null,
        val instanceId: String? = null,
        val e2eSession: String? = null,
        val sealed: String? = null,
    ) : Frame("call.open")

    /** `payload` null means absent (a duplex or sealed result has none in clear). */
    data class CallResult(
        override val callId: String,
        val payload: JsonElement? = null,
        val metadata: Map<String, String>? = null,
        val sealed: String? = null,
    ) : Frame("call.result")

    data class CallError(
        override val callId: String,
        val error: ErrorPayload,
        val metadata: Map<String, String>? = null,
        val sealed: String? = null,
    ) : Frame("call.error")

    data class CallCancel(override val callId: String, val reason: String? = null) : Frame("call.cancel")

    data class StreamItem(
        override val callId: String,
        val direction: Direction,
        val sequence: Long,
        val payload: JsonElement,
        val sealed: String? = null,
    ) : Frame("stream.item")

    data class StreamEnd(override val callId: String, val direction: Direction) : Frame("stream.end")

    data class StreamWindow(override val callId: String, val direction: Direction, val credit: Long) : Frame("stream.window")

    data class Ping(val nonce: String) : Frame("ping")

    data class Pong(val nonce: String) : Frame("pong")

    /** Cancels, windows and keepalives jump the per-call round robin. */
    val isControl: Boolean get() = this is CallCancel || this is StreamWindow || this is Ping || this is Pong

    fun encode(): String = Json.encodeToString(JsonObject.serializer(), toJson())

    fun toJson(): JsonObject = buildJsonObject {
        put("kind", JsonPrimitive(kind))
        when (val f = this@Frame) {
            is Hello -> {
                put("protocol", JsonPrimitive(f.protocol))
                put("role", JsonPrimitive(f.role))
                put("peer_id", JsonPrimitive(f.peerId))
                f.instanceId?.let { put("instance_id", JsonPrimitive(it)) }
                f.capabilities?.let { caps -> put("capabilities", JsonArray(caps.map(::JsonPrimitive))) }
                f.authorization?.let { put("authorization", JsonPrimitive(it)) }
            }
            is HelloAck -> {
                put("protocol", JsonPrimitive(f.protocol))
                put("connection_id", JsonPrimitive(f.connectionId))
            }
            is Other -> {}
            is CallOpen -> {
                put("call_id", JsonPrimitive(f.callId))
                put("service", JsonPrimitive(f.service))
                put("method", JsonPrimitive(f.method))
                put("payload", f.payload)
                f.timeoutMs?.let { put("timeout_ms", JsonPrimitive(it)) }
                f.idleTimeoutMs?.let { put("idle_timeout_ms", JsonPrimitive(it)) }
                f.metadata?.let { put("metadata", metadataJson(it)) }
                f.idempotencyKey?.let { put("idempotency_key", JsonPrimitive(it)) }
                f.instanceId?.let { put("instance_id", JsonPrimitive(it)) }
                f.e2eSession?.let { put("e2e_session", JsonPrimitive(it)) }
                f.sealed?.let { put("sealed", JsonPrimitive(it)) }
            }
            is CallResult -> {
                put("call_id", JsonPrimitive(f.callId))
                f.payload?.let { put("payload", it) }
                f.metadata?.let { put("metadata", metadataJson(it)) }
                f.sealed?.let { put("sealed", JsonPrimitive(it)) }
            }
            is CallError -> {
                put("call_id", JsonPrimitive(f.callId))
                put("error", buildJsonObject {
                    put("code", JsonPrimitive(f.error.code))
                    put("message", JsonPrimitive(f.error.message))
                    f.error.retryable?.let { put("retryable", JsonPrimitive(it)) }
                    f.error.details?.let { put("details", it) }
                })
                f.metadata?.let { put("metadata", metadataJson(it)) }
                f.sealed?.let { put("sealed", JsonPrimitive(it)) }
            }
            is CallCancel -> {
                put("call_id", JsonPrimitive(f.callId))
                f.reason?.let { put("reason", JsonPrimitive(it)) }
            }
            is StreamItem -> {
                put("call_id", JsonPrimitive(f.callId))
                put("direction", JsonPrimitive(f.direction.wire))
                put("sequence", JsonPrimitive(f.sequence))
                put("payload", f.payload)
                f.sealed?.let { put("sealed", JsonPrimitive(it)) }
            }
            is StreamEnd -> {
                put("call_id", JsonPrimitive(f.callId))
                put("direction", JsonPrimitive(f.direction.wire))
            }
            is StreamWindow -> {
                put("call_id", JsonPrimitive(f.callId))
                put("direction", JsonPrimitive(f.direction.wire))
                put("credit", JsonPrimitive(f.credit))
            }
            is Ping -> put("nonce", JsonPrimitive(f.nonce))
            is Pong -> put("nonce", JsonPrimitive(f.nonce))
        }
    }
}

internal fun metadataJson(metadata: Map<String, String>): JsonObject =
    JsonObject(metadata.mapValues { JsonPrimitive(it.value) })

/**
 * A frame failed validation. [callId] is set when the frame still named a
 * call, so the receiver can fail that call instead of the whole connection.
 */
internal class FrameDecodeException(message: String, val callId: String?) : Exception(message)

private const val MAX_SAFE_INTEGER = 9007199254740991L

internal fun decodeFrame(text: String): Frame {
    val value = try {
        Json.parseToJsonElement(text)
    } catch (_: Exception) {
        throw FrameDecodeException("Invalid MeshCall frame", null)
    }
    val obj = value as? JsonObject ?: throw FrameDecodeException("Invalid MeshCall frame", null)
    val kind = (obj["kind"] as? JsonPrimitive)?.takeIf { it.isString }?.content
        ?: throw FrameDecodeException("Invalid MeshCall frame", null)
    val callId = (obj["call_id"] as? JsonPrimitive)?.takeIf { it.isString && it.content.isNotEmpty() }?.content
    val reader = FrameReader(obj, kind, callId)
    return when (kind) {
        "hello" -> Frame.Hello(
            reader.string("protocol"), reader.enum("role", setOf("client", "server")), reader.string("peer_id"),
            reader.optString("instance_id"), reader.optStrings("capabilities"), reader.optString("authorization"),
        )
        "hello.ack" -> Frame.HelloAck(reader.string("protocol"), reader.string("connection_id"))
        "server.drain", "server.drain.ack", "server.register", "server.register.ack" -> {
            if (kind == "server.register.ack" || kind == "server.register") reader.string("instance_id")
            if (kind == "server.drain.ack") reader.optString("instance_id")
            Frame.Other(kind)
        }
        "call.open" -> Frame.CallOpen(
            reader.string("call_id"), reader.string("service"), reader.string("method"), reader.required("payload"),
            reader.optPositive("timeout_ms"), reader.optPositive("idle_timeout_ms"), reader.metadata(),
            reader.optString("idempotency_key")?.also { if (it.isEmpty() || it.codePointCount(0, it.length) > 128) reader.invalid() },
            reader.optString("instance_id")?.also { if (it.isEmpty()) reader.invalid() },
            reader.optString("e2e_session"), reader.optString("sealed"),
        )
        "call.result" -> Frame.CallResult(
            reader.string("call_id"), obj["payload"], reader.metadata(), reader.optString("sealed"),
        )
        "call.error" -> {
            val error = obj["error"] as? JsonObject ?: reader.invalid()
            val inner = FrameReader(error, kind, callId)
            val retryable = error["retryable"]?.let { (it as? JsonPrimitive)?.takeIf { p -> !p.isString }?.booleanOrNull ?: reader.invalid() }
            val details = when (val d = error["details"]) {
                null, JsonNull -> null
                is JsonObject -> d
                else -> reader.invalid()
            }
            Frame.CallError(
                reader.string("call_id"), ErrorPayload(inner.string("code"), inner.string("message"), retryable, details),
                reader.metadata(), reader.optString("sealed"),
            )
        }
        "call.cancel" -> Frame.CallCancel(
            reader.string("call_id"),
            obj["reason"]?.let { (it as? JsonPrimitive)?.takeIf { p -> p.isString }?.content ?: reader.invalid() },
        )
        "stream.item" -> Frame.StreamItem(
            reader.string("call_id"), reader.direction(), reader.integer("sequence", 0), reader.required("payload"),
            reader.optString("sealed"),
        )
        "stream.end" -> Frame.StreamEnd(reader.string("call_id"), reader.direction())
        "stream.window" -> Frame.StreamWindow(reader.string("call_id"), reader.direction(), reader.integer("credit", 1))
        "ping" -> Frame.Ping(reader.string("nonce"))
        "pong" -> Frame.Pong(reader.string("nonce"))
        else -> throw FrameDecodeException("Unknown MeshCall frame kind $kind", null)
    }
}

private class FrameReader(private val obj: JsonObject, private val kind: String, private val callId: String?) {
    fun invalid(): Nothing = throw FrameDecodeException("Invalid MeshCall $kind frame", callId)

    fun required(key: String): JsonElement = obj[key] ?: invalid()

    fun string(key: String): String =
        (obj[key] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: invalid()

    fun optString(key: String): String? = when (val v = obj[key]) {
        null, JsonNull -> null
        is JsonPrimitive -> if (v.isString) v.content else invalid()
        else -> invalid()
    }

    fun optStrings(key: String): List<String>? = when (val v = obj[key]) {
        null, JsonNull -> null
        is JsonArray -> v.map { (it as? JsonPrimitive)?.takeIf { p -> p.isString }?.content ?: invalid() }
        else -> invalid()
    }

    fun enum(key: String, values: Set<String>): String = string(key).also { if (it !in values) invalid() }

    fun direction(): Direction = Direction.of(string("direction")) ?: invalid()

    fun integer(key: String, minimum: Long): Long = integerOf(obj[key] ?: invalid(), minimum)

    fun optPositive(key: String): Long? = when (val v = obj[key]) {
        null, JsonNull -> null
        else -> integerOf(v, 1)
    }

    private fun integerOf(value: JsonElement, minimum: Long): Long {
        val p = value as? JsonPrimitive ?: invalid()
        if (p.isString) invalid()
        val n = p.longOrNull ?: p.doubleOrNull?.takeIf { it % 1.0 == 0.0 && Math.abs(it) <= MAX_SAFE_INTEGER }?.toLong() ?: invalid()
        if (n < minimum || n > MAX_SAFE_INTEGER) invalid()
        return n
    }

    fun metadata(): Map<String, String>? = when (val v = obj["metadata"]) {
        null, JsonNull -> null
        is JsonObject -> v.mapValues { (key, item) ->
            if (key.isEmpty()) invalid()
            (item as? JsonPrimitive)?.takeIf { it.isString }?.content ?: invalid()
        }
        else -> invalid()
    }
}
