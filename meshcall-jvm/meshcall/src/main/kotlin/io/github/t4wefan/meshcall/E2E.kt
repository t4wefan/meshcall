package io.github.t4wefan.meshcall

// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).
//
// A Noise handshake per client connection (NK, or IK / IKpsk2 when the
// client authenticates itself with an E2EIdentity), then the payloads, metadata and error messages of every call travel in a `sealed`
// field while the Router routes on the clear header. X25519 comes from
// BouncyCastle's pure-Java RFC 7748 code (JCA's XDH needs JDK 11 / Android
// API 33); AES-GCM, SHA-256 and HMAC from JCA, available on every Android level.

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import okio.ByteString.Companion.decodeBase64
import okio.ByteString.Companion.toByteString
import org.bouncycastle.math.ec.rfc7748.X25519
import java.math.BigInteger
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

internal const val E2E_PROLOGUE = "meshcall-e2e/1"

/** How far behind the newest counter a sealed frame may arrive. */
internal const val E2E_WINDOW = 1024
internal const val TUNNEL_HOST_SERVICE = "meshcall.router.v1.TunnelHost"
internal const val TUNNEL_HANDSHAKE_METHOD = "handshake"

/** A pairing confirmation may wait for a person at the instance. */
internal const val E2E_CONFIRM_TIMEOUT_MS = 120_000L
internal const val MAX_PAIRING_ID_BYTES = 128

/** End-to-end tunnel credentials say so ("." never occurs in a plain one). */
internal const val E2E_CREDENTIAL_PREFIX = "mtun_e2e."

/** The Router answers these services itself: calls to them are never sealed. */
internal const val ROUTER_SERVICE_PREFIX = "meshcall.router."
private const val SEALED_NOTICE = "sealed"
private val random = SecureRandom()

internal fun isE2ECredential(credential: String?): Boolean = credential?.startsWith(E2E_CREDENTIAL_PREFIX) == true

/**
 * A credential and a key must agree before anything is sent: an end-to-end
 * tunnel never sees an unsealed payload by mistake.
 */
internal fun checkE2EAgreement(credential: String?, e2eKey: String?) {
    require(!(isE2ECredential(credential) && e2eKey == null)) {
        "This is an end-to-end tunnel credential: pass the instance's e2eKey the issuer gave you"
    }
    require(e2eKey == null || isE2ECredential(credential)) {
        "e2eKey needs the credential of an end-to-end tunnel (mtun_e2e.…)"
    }
}

internal fun b64encode(bytes: ByteArray): String = bytes.toByteString().base64Url().trimEnd('=')

internal fun b64decode(value: String): ByteArray {
    require(value.all { it in 'A'..'Z' || it in 'a'..'z' || it in '0'..'9' || it == '-' || it == '_' }) { "not base64url" }
    return value.decodeBase64()?.toByteArray() ?: throw IllegalArgumentException("not base64url")
}

internal fun keyBytes(value: String, what: String): ByteArray {
    val raw = try {
        b64decode(value)
    } catch (_: IllegalArgumentException) {
        ByteArray(0)
    }
    require(raw.size == 32) { "$what is 32 bytes, base64url without padding" }
    return raw
}

/** Checks an instance public key as the issuer pins it; returns it unchanged. */
internal fun checkE2EKey(value: String): String {
    keyBytes(value, "An E2E key (the instance's X25519 public key)")
    return value
}

/**
 * An X25519 key pair for end-to-end tunnels, base64url without padding. A
 * client that authenticates itself generates one once ([generate]), keeps
 * [privateKey] in the app's own secure storage and passes it as
 * [E2EIdentity.privateKey]; [publicKey] is what the instance learns in the
 * handshake (and may be registered with it out of band).
 */
public data class E2EKeyPair(val privateKey: String, val publicKey: String) {
    override fun toString(): String = "E2EKeyPair(publicKey=$publicKey)"

    public companion object {
        /** A new random key pair. */
        @JvmStatic
        public fun generate(): E2EKeyPair {
            val private = newPrivateKey()
            return E2EKeyPair(b64encode(private), b64encode(x25519Public(private)))
        }

        /** The public key of [privateKey] (base64url). */
        @JvmStatic
        public fun publicKey(privateKey: String): String = b64encode(x25519Public(keyBytes(privateKey, "An E2E private key")))
    }
}

/**
 * A one-time pairing the instance's owner handed the client out of band, never
 * through the Router (for example in a QR code shown on the instance's
 * device): its [id] (1 to 128 bytes, not a secret) and 32-byte [psk]
 * (base64url without padding).
 */
public data class E2EPairing(val id: String, val psk: String) {
    init {
        require(id.encodeToByteArray().size in 1..MAX_PAIRING_ID_BYTES) { "A pairing id is 1 to $MAX_PAIRING_ID_BYTES bytes" }
        keyBytes(psk, "A pairing PSK")
    }

    override fun toString(): String = "E2EPairing(id=$id, psk=***)"
}

/**
 * This client's own key for an end-to-end tunnel (`MeshOptions.e2eIdentity`):
 * with it the client authenticates itself to the instance (Noise IK), and the
 * instance may refuse every client it does not know.
 *
 * [privateKey] is this client's static key ([E2EKeyPair.generate], kept by the
 * app); [instanceId] the instance whose key the client pinned in
 * `MeshOptions.e2eKey`, bound in the handshake. Both may come from where the
 * app pinned them itself (a QR code) rather than from the issuer. [pairing]
 * makes the first handshake a pairing (IKpsk2), confirmed before the first
 * call; once the instance accepted it, every later handshake of the same
 * [Mesh] uses IK.
 */
public data class E2EIdentity @JvmOverloads constructor(
    val privateKey: String,
    val instanceId: String,
    val pairing: E2EPairing? = null,
) {
    init {
        keyBytes(privateKey, "An E2E private key")
        require(instanceId.isNotEmpty()) { "E2EIdentity needs the instanceId whose key you pinned" }
    }

    /** This client's public key (base64url), as the instance sees it. */
    public val publicKey: String get() = E2EKeyPair.publicKey(privateKey)

    public fun withPairing(value: E2EPairing?): E2EIdentity = copy(pairing = value)

    override fun toString(): String = "E2EIdentity(publicKey=$publicKey, instanceId=$instanceId, pairing=$pairing)"
}

private fun hex(bytes: ByteArray): String = bytes.joinToString("") { "%02x".format(it.toInt() and 0xff) }

internal fun sha256(vararg parts: ByteArray): ByteArray {
    val digest = MessageDigest.getInstance("SHA-256")
    for (part in parts) digest.update(part)
    return digest.digest()
}

/** The hex SHA-256 of a tunnel credential, which the handshake prologue binds. */
internal fun credentialDigest(credential: String): String = hex(sha256(credential.encodeToByteArray()))

/** The handshake prologue binding a session to the tunnel with this credential digest. */
internal fun tunnelPrologue(digest: String): ByteArray = "$E2E_PROLOGUE|$digest".encodeToByteArray()

/** The IK / IKpsk2 prologue: the tunnel, and the instance whose key the client pinned. */
internal fun peerPrologue(digest: String, instanceId: String): ByteArray =
    "$E2E_PROLOGUE|$digest|$instanceId".encodeToByteArray()

/** The associated data of a pairing confirmation. */
internal fun confirmAd(sessionId: String): ByteArray = "$E2E_PROLOGUE|confirm|$sessionId".encodeToByteArray()

private fun hmac(key: ByteArray, vararg data: ByteArray): ByteArray {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(key, "HmacSHA256"))
    for (part in data) mac.update(part)
    return mac.doFinal()
}

private fun hkdf2(ck: ByteArray, ikm: ByteArray): Pair<ByteArray, ByteArray> {
    val temp = hmac(ck, ikm)
    val a = hmac(temp, byteArrayOf(1))
    return a to hmac(temp, a, byteArrayOf(2))
}

private fun hkdf3(ck: ByteArray, ikm: ByteArray): Triple<ByteArray, ByteArray, ByteArray> {
    val temp = hmac(ck, ikm)
    val a = hmac(temp, byteArrayOf(1))
    val b = hmac(temp, a, byteArrayOf(2))
    return Triple(a, b, hmac(temp, b, byteArrayOf(3)))
}

private fun nonce(n: Long): ByteArray = ByteBuffer.allocate(12).putInt(0).putLong(n).array()

/** AES-256-GCM with the Noise nonce (4 zero bytes, 8-byte big-endian counter). */
internal fun aeadEncrypt(key: ByteArray, n: Long, plaintext: ByteArray, ad: ByteArray): ByteArray {
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, nonce(n)))
    cipher.updateAAD(ad)
    return cipher.doFinal(plaintext)
}

internal fun aeadDecrypt(key: ByteArray, n: Long, ciphertext: ByteArray, ad: ByteArray): ByteArray {
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.DECRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(128, nonce(n)))
    cipher.updateAAD(ad)
    return cipher.doFinal(ciphertext)
}

internal fun x25519Public(privateKey: ByteArray): ByteArray =
    ByteArray(32).also { X25519.generatePublicKey(privateKey, 0, it, 0) }

private fun dh(privateKey: ByteArray, publicKey: ByteArray): ByteArray {
    val out = ByteArray(32)
    // A low-order public key yields all zeros: no shared secret, so no session.
    check(X25519.calculateAgreement(privateKey, 0, publicKey, 0, out, 0)) { "X25519 produced no shared secret" }
    return out
}

private fun newPrivateKey(): ByteArray = ByteArray(32).also { X25519.generatePrivateKey(random, it) }

// --- Noise NK, IK, IKpsk2 ----------------------------------------------------

internal const val NOISE_NK = "Noise_NK_25519_AESGCM_SHA256"
internal const val NOISE_IK = "Noise_IK_25519_AESGCM_SHA256"
internal const val NOISE_IKPSK2 = "Noise_IKpsk2_25519_AESGCM_SHA256"

/** The tokens of each message, after the responder's static key (`<- s`), which all three know beforehand. */
private val PATTERNS: Map<String, List<List<String>>> = mapOf(
    NOISE_NK to listOf(listOf("e", "es"), listOf("e", "ee")),
    NOISE_IK to listOf(listOf("e", "es", "s", "ss"), listOf("e", "ee", "se")),
    NOISE_IKPSK2 to listOf(listOf("e", "es", "s", "ss"), listOf("e", "ee", "se", "psk")),
)

private class SymmetricState(protocol: String, prologue: ByteArray) {
    var h: ByteArray
    private var ck: ByteArray
    private var k: ByteArray? = null
    private var n = 0L
    val hasKey: Boolean get() = k != null

    init {
        // Every name here is at most HASHLEN, so it is zero-padded, not hashed.
        val name = ByteArray(32)
        protocol.encodeToByteArray().copyInto(name)
        h = name
        ck = name
        mixHash(prologue)
    }

    fun mixHash(data: ByteArray) {
        h = sha256(h, data)
    }

    fun mixKey(ikm: ByteArray) {
        val (nextCk, key) = hkdf2(ck, ikm)
        ck = nextCk
        k = key
        n = 0
    }

    fun mixKeyAndHash(ikm: ByteArray) {
        val (nextCk, tempH, key) = hkdf3(ck, ikm)
        ck = nextCk
        mixHash(tempH)
        k = key
        n = 0
    }

    fun encryptAndHash(plaintext: ByteArray): ByteArray {
        val key = k
        val out = if (key == null) plaintext else aeadEncrypt(key, n++, plaintext, h)
        mixHash(out)
        return out
    }

    fun decryptAndHash(ciphertext: ByteArray): ByteArray {
        val key = k
        val out = if (key == null) ciphertext else aeadDecrypt(key, n++, ciphertext, h)
        mixHash(ciphertext)
        return out
    }

    fun split(): Pair<ByteArray, ByteArray> = hkdf2(ck, ByteArray(0))
}

/**
 * One end of a two-message Noise handshake towards (or as) a responder whose
 * static key the initiator knows: NK, IK or IKpsk2. [s] is this end's static
 * private key (the responder's; the initiator's for IK and IKpsk2), [rs] the
 * responder's public key on the initiator, [psk] the pairing secret for
 * IKpsk2. [ephemeral] is for test vectors. The client is the initiator; the
 * responder side is here for tests.
 */
internal class NoiseHandshake(
    val protocol: String,
    private val initiator: Boolean,
    prologue: ByteArray,
    private val s: ByteArray? = null,
    rs: ByteArray? = null,
    private val psk: ByteArray? = null,
    ephemeral: ByteArray? = null,
) {
    private val sym = SymmetricState(protocol, prologue)
    private val patterns = PATTERNS[protocol] ?: throw IllegalArgumentException("unknown handshake $protocol")
    private var e: ByteArray? = ephemeral
    private var re: ByteArray? = null
    private var turn = 0

    /** The other end's static public key (the client's, on the responder, once message 1 is read). */
    var remoteStatic: ByteArray? = rs
        private set

    init {
        require((protocol == NOISE_IKPSK2) == (psk != null)) { "IKpsk2, and only IKpsk2, takes a PSK" }
        require(psk == null || psk.size == 32) { "A pairing PSK is 32 bytes" }
        require(s != null || (initiator && protocol == NOISE_NK)) { "$protocol needs this end's static key" }
        val responderStatic = if (initiator) requireNotNull(rs) { "the initiator needs the responder's key" } else x25519Public(s!!)
        require(responderStatic.size == 32) { "an X25519 key is 32 bytes" }
        sym.mixHash(responderStatic)
    }

    private fun mixEphemeral(pub: ByteArray) {
        sym.mixHash(pub)
        // In psk mode every `e` token also keys the cipher.
        if (psk != null) sym.mixKey(pub)
    }

    fun writeMessage(payload: ByteArray = ByteArray(0)): ByteArray {
        val tokens = patterns[turn++]
        var out = ByteArray(0)
        for (token in tokens) {
            when (token) {
                "e" -> {
                    val pub = x25519Public(e ?: newPrivateKey().also { e = it })
                    out += pub
                    mixEphemeral(pub)
                }
                "s" -> out += sym.encryptAndHash(x25519Public(s!!))
                "psk" -> sym.mixKeyAndHash(psk!!)
                else -> sym.mixKey(tokenDh(token))
            }
        }
        return out + sym.encryptAndHash(payload)
    }

    fun readMessage(message: ByteArray): ByteArray {
        val tokens = patterns[turn++]
        var at = 0
        for (token in tokens) {
            when (token) {
                "e" -> {
                    require(message.size - at >= 32) { "short handshake message" }
                    val pub = message.copyOfRange(at, at + 32)
                    at += 32
                    re = pub
                    mixEphemeral(pub)
                }
                "s" -> {
                    val size = if (sym.hasKey) 48 else 32
                    require(message.size - at >= size) { "short handshake message" }
                    remoteStatic = sym.decryptAndHash(message.copyOfRange(at, at + size))
                    at += size
                }
                "psk" -> sym.mixKeyAndHash(psk!!)
                else -> sym.mixKey(tokenDh(token))
            }
        }
        require(!sym.hasKey || message.size - at >= 16) { "short handshake message" }
        return sym.decryptAndHash(message.copyOfRange(at, message.size))
    }

    private fun tokenDh(token: String): ByteArray {
        // The first letter is the initiator's key, the second the responder's.
        val mine = if (initiator) token[0] else token[1]
        val theirs = if (initiator) token[1] else token[0]
        val local = if (mine == 'e') e else s
        val remote = if (theirs == 'e') re else remoteStatic
        return dh(checkNotNull(local), checkNotNull(remote))
    }

    /** The session once both messages are done. */
    fun split(): E2ESession {
        val (toResponder, toInitiator) = sym.split()
        return if (initiator) E2ESession(sym.h, toResponder, toInitiator) else E2ESession(sym.h, toInitiator, toResponder)
    }
}

// --- sessions and sealed frames ---------------------------------------------

/**
 * One end of an E2E session: a sending key and counter, a receiving key with
 * a replay window (the Router may drop or reorder frames, never replay them).
 */
internal class E2ESession(val hash: ByteArray, val sendKey: ByteArray, val recvKey: ByteArray) {
    val id: String = hex(hash.copyOfRange(0, 16))
    private var sendN = 0L
    private var top = 0L // highest counter received + 1
    private var window: BigInteger = BigInteger.ZERO // bit i: counter top-1-i seen

    @Synchronized
    fun seal(plaintext: ByteArray, ad: ByteArray): String {
        val n = sendN++
        val counter = ByteBuffer.allocate(8).putLong(n).array()
        return b64encode(counter + aeadEncrypt(sendKey, n, plaintext, ad))
    }

    @Synchronized
    fun open(sealed: String, ad: ByteArray): ByteArray {
        val raw = try {
            b64decode(sealed)
        } catch (_: IllegalArgumentException) {
            throw IllegalArgumentException("malformed sealed field")
        }
        require(raw.size >= 24) { "malformed sealed field" }
        val n = ByteBuffer.wrap(raw, 0, 8).long
        val plaintext = try {
            aeadDecrypt(recvKey, n, raw.copyOfRange(8, raw.size), ad)
        } catch (_: Exception) {
            throw IllegalArgumentException("sealed field does not open")
        }
        require(accept(n)) { "sealed field replayed or too old" }
        return plaintext
    }

    private fun accept(n: Long): Boolean {
        if (n < 0) return false
        if (n >= top) {
            val shift = n + 1 - top
            window = if (shift >= E2E_WINDOW) BigInteger.ONE
            else window.shiftLeft(shift.toInt()).or(BigInteger.ONE).and(WINDOW_MASK)
            top = n + 1
            return true
        }
        val age = top - 1 - n
        if (age >= E2E_WINDOW || window.testBit(age.toInt())) return false
        window = window.setBit(age.toInt())
        return true
    }

    private companion object {
        val WINDOW_MASK: BigInteger = BigInteger.ONE.shiftLeft(E2E_WINDOW).subtract(BigInteger.ONE)
    }
}

private fun withoutReserved(metadata: JsonElement?): Map<String, String>? {
    val obj = metadata as? JsonObject ?: return null
    return obj.entries.mapNotNull { (key, value) ->
        val text = (value as? JsonPrimitive)?.takeIf { it.isString }?.content
        if (isReservedKey(key) || text == null) null else key to text
    }.toMap()
}

/** What both ends know of one sealed call: its session, service and method. */
internal class SealedCall(val session: E2ESession, val service: String, val method: String) {
    private fun ad(kind: String, callId: String, direction: Direction?, sequence: Long?): ByteArray = listOf(
        E2E_PROLOGUE, kind, callId, service, method, direction?.wire ?: "", sequence?.toString() ?: "", session.id,
    ).joinToString("|").encodeToByteArray()

    private fun ad(frame: Frame): ByteArray = when (frame) {
        is Frame.StreamItem -> ad(frame.kind, frame.callId, frame.direction, frame.sequence)
        else -> ad(frame.kind, frame.callId ?: "", null, null)
    }

    private fun sealParts(frame: Frame, parts: JsonObject): String =
        session.seal(Json.encodeToString(JsonObject.serializer(), parts).encodeToByteArray(), ad(frame))

    /** Moves [frame]'s content into its sealed field. */
    fun seal(frame: Frame): Frame = when (frame) {
        is Frame.CallOpen -> {
            val out = frame.copy(payload = JsonNull, metadata = null, e2eSession = session.id)
            out.copy(sealed = sealParts(out, buildJsonObject {
                put("payload", frame.payload)
                frame.metadata?.let { put("metadata", metadataJson(it)) }
            }))
        }
        is Frame.StreamItem -> {
            val out = frame.copy(payload = JsonNull)
            out.copy(sealed = sealParts(out, buildJsonObject { put("payload", frame.payload) }))
        }
        is Frame.CallResult -> {
            val out = frame.copy(payload = JsonNull, metadata = null)
            out.copy(sealed = sealParts(out, buildJsonObject {
                frame.payload?.let { put("payload", it) }
                frame.metadata?.let { put("metadata", metadataJson(it)) }
            }))
        }
        is Frame.CallError -> {
            val out = frame.copy(error = frame.error.copy(message = SEALED_NOTICE, details = null), metadata = null)
            out.copy(sealed = sealParts(out, buildJsonObject {
                put("message", JsonPrimitive(frame.error.message))
                frame.error.details?.let { put("details", it) }
                frame.metadata?.let { put("metadata", metadataJson(it)) }
            }))
        }
        else -> frame // windows, ends and cancels carry no content
    }

    /**
     * Restores [frame]'s content. Content frames in clear did not come from
     * the other end and are refused; a clear `call.error` (the Router's own)
     * is kept.
     */
    fun open(frame: Frame): Frame {
        val sealed = when (frame) {
            is Frame.CallOpen -> frame.sealed
            is Frame.CallResult -> frame.sealed
            is Frame.CallError -> frame.sealed
            is Frame.StreamItem -> frame.sealed
            else -> null
        }
        if (sealed.isNullOrEmpty()) {
            require(frame !is Frame.CallOpen && frame !is Frame.CallResult && frame !is Frame.StreamItem) {
                "a frame of a sealed call arrived in clear"
            }
            return frame
        }
        val parts = try {
            Json.parseToJsonElement(session.open(sealed, ad(frame)).decodeToString()) as? JsonObject
        } catch (error: IllegalArgumentException) {
            throw error
        } catch (_: Exception) {
            null
        } ?: throw IllegalArgumentException("sealed field is not a sealed object")
        val metadata = withoutReserved(parts["metadata"])
        return when (frame) {
            is Frame.CallError -> frame.copy(
                error = frame.error.copy(
                    message = (parts["message"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: "",
                    details = parts["details"] as? JsonObject,
                ),
                metadata = metadata,
                sealed = null,
            )
            is Frame.CallResult -> frame.copy(payload = parts["payload"] ?: JsonNull, metadata = metadata, sealed = null)
            is Frame.StreamItem -> frame.copy(payload = parts["payload"] ?: JsonNull, sealed = null)
            is Frame.CallOpen -> {
                // The Router's clear keys (the tunnel identity) win over sealed ones.
                val merged = (metadata ?: emptyMap()) + (frame.metadata ?: emptyMap())
                frame.copy(payload = parts["payload"] ?: JsonNull, metadata = merged.ifEmpty { null }, sealed = null)
            }
            else -> frame
        }
    }
}

internal fun handshakeFailed(why: String, source: String = "client"): UnauthenticatedException =
    UnauthenticatedException(
        "End-to-end handshake failed: $why",
        details = details("source" to source, "reason" to "e2e_handshake_failed"),
    )

/** The handshake's answer (`{message}` or `{}`), or the error it came back with. */
private fun handshakeAnswer(reply: Frame): JsonObject {
    if (reply is Frame.CallError) {
        val details = reply.error.details
        if ((details?.get("reason") as? JsonPrimitive)?.content == "e2e_handshake_failed") {
            // The instance's own answer: it refused the handshake.
            throw UnauthenticatedException(reply.error.message, details = details)
        }
        throw handshakeFailed(reply.error.code + ": " + reply.error.message)
    }
    if (reply !is Frame.CallResult) throw handshakeFailed("unexpected " + reply.kind)
    return reply.payload as? JsonObject ?: JsonObject(emptyMap())
}

/**
 * The client's handshake on a fresh connection to an E2E tunnel: [frame] is
 * the `TunnelHost/handshake` call to send; [finish] reads its answer. With an
 * [identity] the client authenticates itself: IK, or IKpsk2 while its pairing
 * is not [paired] yet, which [confirmFrame] / [confirmed] then confirm. It
 * never falls back to NK.
 */
internal class ClientHandshake(
    pinnedKey: String,
    callId: String,
    credential: String,
    identity: E2EIdentity? = null,
    paired: Boolean = false,
    ephemeral: ByteArray? = null,
) {
    private val pairing: E2EPairing? = identity?.pairing?.takeIf { !paired }
    val protocol: String = when {
        identity == null -> NOISE_NK
        pairing != null -> NOISE_IKPSK2
        else -> NOISE_IK
    }
    private val handshake = NoiseHandshake(
        protocol,
        initiator = true,
        prologue = if (identity == null) tunnelPrologue(credentialDigest(credential))
        else peerPrologue(credentialDigest(credential), identity.instanceId),
        s = identity?.let { keyBytes(it.privateKey, "An E2E private key") },
        rs = keyBytes(pinnedKey, "An E2E key (the instance's X25519 public key)"),
        psk = pairing?.let { keyBytes(it.psk, "A pairing PSK") },
        ephemeral = ephemeral,
    )

    val frame: Frame.CallOpen = Frame.CallOpen(
        callId, TUNNEL_HOST_SERVICE, TUNNEL_HANDSHAKE_METHOD,
        buildJsonObject {
            put("message", JsonPrimitive(b64encode(handshake.writeMessage())))
            if (protocol != NOISE_NK) put("protocol", JsonPrimitive(protocol))
            pairing?.let { put("pairing_id", JsonPrimitive(it.id)) }
        },
    )

    /** A pairing: the session [finish] returns is usable only once [confirmed]. */
    val confirms: Boolean get() = pairing != null

    fun finish(reply: Frame): E2ESession {
        val message = (handshakeAnswer(reply)["message"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: ""
        return try {
            handshake.readMessage(b64decode(message))
            handshake.split()
        } catch (_: Exception) {
            // Only the holder of the pinned private key can write message 2.
            throw handshakeFailed("message 2 is not from the pinned instance key")
        }
    }

    /**
     * The pairing confirmation `{session, confirm}`: `{}` sealed with the first
     * counter of the client's direction, proving the client holds the PSK.
     */
    fun confirmFrame(callId: String, session: E2ESession): Frame.CallOpen = Frame.CallOpen(
        callId, TUNNEL_HOST_SERVICE, TUNNEL_HANDSHAKE_METHOD,
        buildJsonObject {
            put("session", JsonPrimitive(session.id))
            put("confirm", JsonPrimitive(session.seal("{}".encodeToByteArray(), confirmAd(session.id))))
        },
        timeoutMs = E2E_CONFIRM_TIMEOUT_MS,
    )

    /** Reads the confirmation's answer: the instance accepted the pairing, or this throws. */
    fun confirmed(reply: Frame) {
        handshakeAnswer(reply)
    }
}
