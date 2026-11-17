package io.github.t4wefan.meshcall

import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

/** Keys beginning with this, in any letter case, are set only by the runtime and the Router. */
public const val RESERVED_METADATA_PREFIX: String = "meshcall-"

internal fun isReservedKey(key: String): Boolean =
    key.length >= RESERVED_METADATA_PREFIX.length &&
        key.substring(0, RESERVED_METADATA_PREFIX.length).lowercase() == RESERVED_METADATA_PREFIX

/**
 * Exponential backoff with jitter for re-establishing a lost connection.
 * In-flight calls fail with a retryable `unavailable`; the next call
 * reconnects lazily, waiting at most its own timeout. `maxAttempts` null
 * keeps trying until the client is closed or the call's timeout expires.
 */
public data class ReconnectPolicy @JvmOverloads constructor(
    val enabled: Boolean = true,
    val initialDelayMs: Long = 100,
    val maxDelayMs: Long = 30_000,
    val multiplier: Double = 2.0,
    val jitter: Double = 0.2,
    val maxAttempts: Int? = null,
) {
    init {
        require(initialDelayMs > 0 && maxDelayMs >= initialDelayMs && multiplier >= 1 && jitter in 0.0..1.0 &&
            (maxAttempts == null || maxAttempts >= 1)) { "Invalid reconnect policy" }
    }

    internal fun delays(): Iterator<Long> = iterator {
        var delay = initialDelayMs.toDouble()
        var attempt = 0
        while (maxAttempts == null || attempt < maxAttempts) {
            val spread = delay * jitter
            yield(max(0.0, delay + (Random.nextDouble() * 2 - 1) * spread).toLong())
            delay = min(maxDelayMs.toDouble(), delay * multiplier)
            attempt += 1
        }
    }

    public companion object {
        @JvmField public val DEFAULT: ReconnectPolicy = ReconnectPolicy()
    }
}

/**
 * Client-side retry for failed unary calls. Only `retryable` errors with a
 * code in [retryCodes] are retried, and a method not declared idempotent only
 * when the call carries an idempotency key. Attempts share the call's
 * timeout. Streams are never retried.
 */
public data class RetryPolicy @JvmOverloads constructor(
    val enabled: Boolean = true,
    val maxAttempts: Int = 3,
    val initialDelayMs: Long = 100,
    val maxDelayMs: Long = 2_000,
    val multiplier: Double = 2.0,
    val jitter: Double = 0.2,
    val retryCodes: Set<String> = setOf(ErrorCode.UNAVAILABLE, ErrorCode.RESOURCE_EXHAUSTED),
) {
    init {
        require(maxAttempts >= 1 && initialDelayMs >= 0 && maxDelayMs >= initialDelayMs && multiplier >= 1 && jitter in 0.0..1.0) {
            "Invalid retry policy"
        }
    }

    internal fun delayMs(attempt: Int): Long {
        val base = min(maxDelayMs.toDouble(), initialDelayMs * multiplier.pow(attempt - 1))
        return max(0.0, base + (Random.nextDouble() * 2 - 1) * base * jitter).toLong()
    }

    public companion object {
        @JvmField public val DEFAULT: RetryPolicy = RetryPolicy()

        /** No retries at all. */
        @JvmField public val DISABLED: RetryPolicy = RetryPolicy(enabled = false)
    }
}

/** Extra time a client waits past `timeoutMs` for the peer's own deadline error. */
internal fun clientDeadlineGraceMs(timeoutMs: Long): Long = max(1000L, timeoutMs / 10)

/**
 * Per-call options. Immutable; derive variants with the `with…` methods
 * (`CallOptions.DEFAULT.withTimeoutMs(5_000).withInstance("pc-1")`), which
 * read the same from Kotlin and Java.
 */
public class CallOptions private constructor(
    /** Total budget for the call, including connecting. */
    public val timeoutMs: Long?,
    /** Longest allowed gap between consecutive stream items. */
    public val idleTimeoutMs: Long?,
    /** Request metadata delivered to the service handler. */
    public val metadata: Map<String, String>?,
    /**
     * Lets the Router join or replay duplicate unary calls and lets the retry
     * policy re-send a non-idempotent method safely. 1–128 bytes.
     */
    public val idempotencyKey: String?,
    /** The method's own semantics; generated clients set this from the contract. */
    public val idempotent: Boolean,
    /** Address one service instance by id; required for targeted services. */
    public val instance: String?,
) {
    public fun withTimeoutMs(value: Long?): CallOptions = copy(timeoutMs = value)
    public fun withIdleTimeoutMs(value: Long?): CallOptions = copy(idleTimeoutMs = value)

    /** Replaces the metadata; [withMetadataEntry] adds one entry. */
    public fun withMetadata(value: Map<String, String>?): CallOptions = copy(metadata = value?.toMap())
    public fun withMetadataEntry(key: String, value: String): CallOptions = copy(metadata = (metadata ?: emptyMap()) + (key to value))
    public fun withIdempotencyKey(value: String?): CallOptions = copy(idempotencyKey = value)
    public fun withIdempotent(value: Boolean): CallOptions = copy(idempotent = value)
    public fun withInstance(value: String?): CallOptions = copy(instance = value)

    /** These options overridden by every value [other] sets; metadata maps are merged. */
    public fun merge(other: CallOptions): CallOptions = CallOptions(
        other.timeoutMs ?: timeoutMs,
        other.idleTimeoutMs ?: idleTimeoutMs,
        when {
            metadata != null && other.metadata != null -> metadata + other.metadata
            else -> other.metadata ?: metadata
        },
        other.idempotencyKey ?: idempotencyKey,
        other.idempotent || idempotent,
        other.instance ?: instance,
    )

    internal fun copy(
        timeoutMs: Long? = this.timeoutMs,
        idleTimeoutMs: Long? = this.idleTimeoutMs,
        metadata: Map<String, String>? = this.metadata,
        idempotencyKey: String? = this.idempotencyKey,
        idempotent: Boolean = this.idempotent,
        instance: String? = this.instance,
    ): CallOptions = CallOptions(timeoutMs, idleTimeoutMs, metadata, idempotencyKey, idempotent, instance)

    internal fun validate() {
        timeoutMs?.let { require(it > 0) { "timeoutMs must be positive" } }
        idleTimeoutMs?.let { require(it > 0) { "idleTimeoutMs must be positive" } }
        validateMetadata(metadata)
        instance?.let { require(it.isNotEmpty()) { "instance must not be empty" } }
        idempotencyKey?.let {
            val bytes = it.encodeToByteArray().size
            require(bytes in 1..128) { "idempotencyKey must be 1..128 bytes" }
        }
    }

    override fun equals(other: Any?): Boolean = other is CallOptions && timeoutMs == other.timeoutMs &&
        idleTimeoutMs == other.idleTimeoutMs && metadata == other.metadata && idempotencyKey == other.idempotencyKey &&
        idempotent == other.idempotent && instance == other.instance

    override fun hashCode(): Int = listOf(timeoutMs, idleTimeoutMs, metadata, idempotencyKey, idempotent, instance).hashCode()

    override fun toString(): String =
        "CallOptions(timeoutMs=$timeoutMs, idleTimeoutMs=$idleTimeoutMs, metadata=$metadata, " +
            "idempotencyKey=$idempotencyKey, idempotent=$idempotent, instance=$instance)"

    public companion object {
        @JvmField public val DEFAULT: CallOptions = CallOptions(null, null, null, null, false, null)
    }
}

internal fun validateMetadata(metadata: Map<String, String>?): Map<String, String>? {
    if (metadata == null) return null
    for (key in metadata.keys) {
        if (key.isEmpty()) {
            throw InvalidArgumentException(
                "metadata keys must not be empty", details = details("source" to "client", "reason" to "invalid_metadata"),
            )
        }
        if (isReservedKey(key)) {
            throw InvalidArgumentException(
                "metadata key \"$key\" uses the reserved prefix \"meshcall-\"",
                details = details("source" to "client", "reason" to "reserved_metadata"),
            )
        }
    }
    return metadata.toMap()
}

/** Router credentials: a Bearer token (`mk_…`, `mt_…`, `mtun_…`) or an account's username and password. */
public sealed class RouterCredentials {
    internal abstract val authorization: String

    /** The Bearer token, or null for Basic credentials. */
    public abstract val token: String?

    public class Bearer internal constructor(override val token: String) : RouterCredentials() {
        override val authorization: String get() = "Bearer $token"
    }

    public class Basic internal constructor(public val username: String, internal val password: String) : RouterCredentials() {
        override val token: String? get() = null
        override val authorization: String
            get() = "Basic " + okio.ByteString.Companion.run { "$username:$password".encodeUtf8().base64() }
    }

    public companion object {
        @JvmStatic
        public fun bearer(token: String): RouterCredentials {
            require(token.isNotEmpty() && '\r' !in token && '\n' !in token) { "Set either a token or username/password" }
            return Bearer(token)
        }

        @JvmStatic
        public fun basic(username: String, password: String): RouterCredentials {
            require(username.isNotEmpty() && password.isNotEmpty() && username.none { it == ':' || it == '\r' || it == '\n' }) {
                "A username and password are required"
            }
            return Basic(username, password)
        }
    }
}
