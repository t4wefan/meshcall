package io.github.t4wefan.meshcall

import kotlinx.coroutines.CopyableThrowable
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull

/** Protocol error codes; see docs/protocol.md → Errors. */
public object ErrorCode {
    public const val INVALID_ARGUMENT: String = "invalid_argument"
    public const val METHOD_NOT_FOUND: String = "method_not_found"
    public const val UNAUTHENTICATED: String = "unauthenticated"
    public const val PERMISSION_DENIED: String = "permission_denied"
    public const val DEADLINE_EXCEEDED: String = "deadline_exceeded"
    public const val CANCELLED: String = "cancelled"
    public const val UNAVAILABLE: String = "unavailable"
    public const val RESOURCE_EXHAUSTED: String = "resource_exhausted"
    public const val PROTOCOL_ERROR: String = "protocol_error"
    public const val INTERNAL: String = "internal"
}

/** One field-level validation finding; never carries the input value. */
public data class Violation(
    /** Dotted path of the offending field; `""` for the root. */
    val path: String,
    /** Short validator keyword: `type`, `missing`, … */
    val code: String,
    val message: String,
)

/**
 * An RPC error: a stable [code], a message and optional [details].
 *
 * Errors received from a peer are built through [MeshCallException.fromCode],
 * which picks the subclass for the protocol code so callers can catch
 * `DeadlineExceededException` instead of comparing strings. Application codes
 * stay plain [MeshCallException].
 */
@OptIn(ExperimentalCoroutinesApi::class)
public open class MeshCallException(
    public val code: String,
    message: String,
    public val retryable: Boolean = false,
    public val details: JsonObject? = null,
) : Exception(message), CopyableThrowable<MeshCallException> {

    /** `details.source`: `"client"`, `"router"` or `"server"`. */
    public val source: String? get() = detail("source")

    /** `details.reason` when the sender supplied one. */
    public val reason: String? get() = detail("reason")

    /** `details.instance_id` when the error names a service instance. */
    public val instanceId: String? get() = detail("instance_id")

    /** `details.attempts` when the retry policy made more than one attempt. */
    public val attempts: Int? get() = (details?.get("attempts") as? JsonPrimitive)?.contentOrNull?.toIntOrNull()

    private fun detail(key: String): String? =
        (details?.get(key) as? JsonPrimitive)?.takeIf { it.isString }?.content

    // Coroutines' stack-trace recovery would rebuild the exception through a
    // reflective constructor and drop `details`; the original is exact already.
    override fun createCopy(): MeshCallException? = null

    public companion object {
        /** The exception for a received `error{code, message, retryable, details}`. */
        @JvmStatic
        @JvmOverloads
        public fun fromCode(
            code: String,
            message: String,
            retryable: Boolean = false,
            details: JsonObject? = null,
        ): MeshCallException = when (code) {
            ErrorCode.INVALID_ARGUMENT -> InvalidArgumentException(message, retryable, details)
            ErrorCode.METHOD_NOT_FOUND -> MethodNotFoundException(message, retryable, details)
            ErrorCode.UNAUTHENTICATED -> UnauthenticatedException(message, retryable, details)
            ErrorCode.PERMISSION_DENIED -> PermissionDeniedException(message, retryable, details)
            ErrorCode.DEADLINE_EXCEEDED -> DeadlineExceededException(message, retryable, details)
            ErrorCode.CANCELLED -> CallCancelledException(message, retryable, details)
            ErrorCode.UNAVAILABLE -> UnavailableException(message, retryable, details)
            ErrorCode.RESOURCE_EXHAUSTED -> ResourceExhaustedException(message, retryable, details)
            ErrorCode.PROTOCOL_ERROR -> ProtocolErrorException(message, retryable, details)
            ErrorCode.INTERNAL -> InternalException(message, retryable, details)
            else -> MeshCallException(code, message, retryable, details)
        }
    }
}

public class InvalidArgumentException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.INVALID_ARGUMENT, message, retryable, details) {
    /** Field-level findings when the server validated the request. */
    public val violations: List<Violation> get() = violationsOf(details)
}

public class MethodNotFoundException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.METHOD_NOT_FOUND, message, retryable, details)

public class UnauthenticatedException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.UNAUTHENTICATED, message, retryable, details)

public class PermissionDeniedException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.PERMISSION_DENIED, message, retryable, details)

public class DeadlineExceededException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.DEADLINE_EXCEEDED, message, retryable, details)

/**
 * The call was cancelled by the caller or a peer. Not a coroutine
 * `CancellationException`: a peer's cancellation is an error the caller sees.
 */
public class CallCancelledException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.CANCELLED, message, retryable, details)

public class UnavailableException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.UNAVAILABLE, message, retryable, details)

public class ResourceExhaustedException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.RESOURCE_EXHAUSTED, message, retryable, details)

/** `protocol_error`: a frame violated docs/protocol.md, or a value broke the client's contract. */
public class ProtocolErrorException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.PROTOCOL_ERROR, message, retryable, details) {
    /** Redacted findings when the client's own contract rejected a value. */
    public val violations: List<Violation> get() = violationsOf(details)
}

public class InternalException(message: String, retryable: Boolean = false, details: JsonObject? = null) :
    MeshCallException(ErrorCode.INTERNAL, message, retryable, details)

private fun violationsOf(details: JsonObject?): List<Violation> {
    val raw = details?.get("violations") as? JsonArray ?: return emptyList()
    return raw.mapNotNull { item ->
        val record = item as? JsonObject ?: return@mapNotNull null
        fun text(key: String) = (record[key] as? JsonPrimitive)?.contentOrNull ?: ""
        Violation(text("path"), text("code"), text("message"))
    }
}

internal fun details(vararg pairs: Pair<String, Any?>): JsonObject = buildJsonObject {
    for ((key, value) in pairs) {
        when (value) {
            null -> {}
            is String -> put(key, JsonPrimitive(value))
            is Number -> put(key, JsonPrimitive(value))
            is Boolean -> put(key, JsonPrimitive(value))
            is JsonElement -> put(key, value)
            else -> put(key, JsonPrimitive(value.toString()))
        }
    }
}

internal fun JsonObject?.plus(key: String, value: JsonElement): JsonObject =
    JsonObject((this ?: JsonObject(emptyMap())) + (key to value))

/** WebSocket close status for credentials that are not, or no longer, valid. */
internal const val CLOSE_UNAUTHENTICATED = 4401

/** Close reasons that name an error reason (docs/protocol.md → Tunnels, Temporary tokens). */
private val CLOSE_REASONS = setOf("tunnel_revoked", "tunnel_closed", "key_changed", "token_revoked")

/**
 * The error a Router close status stands for, or `null` for a plain loss:
 * 4401 the credentials are not (or no longer) valid, 1008 the Router refused
 * the connection's request, 4403/4429/4503 credentials sent in `hello`
 * refused like their HTTP counterparts.
 */
internal fun closeError(code: Int, reason: String): MeshCallException? {
    if (code == CLOSE_UNAUTHENTICATED) {
        return UnauthenticatedException(
            "The Router closed the connection: " + reason.ifEmpty { "unauthenticated" },
            details = if (reason in CLOSE_REASONS) details("source" to "router", "reason" to reason) else details("source" to "router"),
        )
    }
    if (code == 1008) {
        return PermissionDeniedException(
            "The Router refused the connection: " + reason.ifEmpty { "policy violation" },
            details = details("source" to "router", "reason" to "policy_violation"),
        )
    }
    if (code == 4403 || code == 4429 || code == 4503) return handshakeError(code - 4000, reason)
    return null
}

/** The error an HTTP status refusing a WebSocket upgrade stands for. */
internal fun handshakeError(status: Int, body: String = ""): MeshCallException? {
    val text = " (HTTP $status)" + if (body.isBlank()) "" else ": " + body.trim()
    val details = details("source" to "router")
    return when (status) {
        401 -> UnauthenticatedException("The Router refused the credentials$text", details = details)
        403 -> PermissionDeniedException("The Router refused the connection$text", details = details)
        429 -> ResourceExhaustedException("The Router refused the connection$text", retryable = true, details = details)
        503 -> UnavailableException("The Router is busy$text", retryable = true, details = details)
        else -> null
    }
}

internal fun localDeadlineError(idle: Boolean = false): MeshCallException = DeadlineExceededException(
    if (idle) "Output stream idle timeout" else "Call deadline exceeded",
    details = if (idle) details("source" to "client", "reason" to "idle") else details("source" to "client"),
)

internal fun connectionLost(why: String? = null): UnavailableException =
    UnavailableException(if (why == null) "Connection was lost" else "Connection was lost: $why", retryable = true)
