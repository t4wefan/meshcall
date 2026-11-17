package io.github.t4wefan.meshcall

// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).
//
// A Noise_NK_25519_AESGCM_SHA256 handshake per client connection, then the
// payloads, metadata and error messages of every call travel in a `sealed`
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

/** End-to-end tunnel credentials say so ("." never occurs in a plain one). */
internal const val E2E_CREDENTIAL_PREFIX = "mtun_e2e."

/** The Router answers these services itself: calls to them are never sealed. */
internal const val ROUTER_SERVICE_PREFIX = "meshcall.router."
private const val NOISE_PROTOCOL = "Noise_NK_25519_AESGCM_SHA256"
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

// --- Noise NK ---------------------------------------------------------------

private class SymmetricState(prologue: ByteArray) {
    var h: ByteArray
    private var ck: ByteArray
    private var k: ByteArray? = null
    private var n = 0L

    init {
        val name = ByteArray(32) // shorter than HASHLEN: zero-padded
        NOISE_PROTOCOL.encodeToByteArray().copyInto(name)
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

/** The initiator of a Noise NK handshake: message 1 towards the pinned key, then [finish] with message 2. */
internal class NKInitiator(
    remoteStatic: ByteArray,
    prologue: ByteArray = E2E_PROLOGUE.encodeToByteArray(),
    ephemeral: ByteArray? = null,
    payload: ByteArray = ByteArray(0),
) {
    private val sym = SymmetricState(prologue)
    private val e = ephemeral ?: newPrivateKey()
    val message: ByteArray

    init {
        require(remoteStatic.size == 32)
        sym.mixHash(remoteStatic)
        val ePub = x25519Public(e)
        sym.mixHash(ePub)
        sym.mixKey(dh(e, remoteStatic))
        message = ePub + sym.encryptAndHash(payload)
    }

    /** Reads message 2; returns its payload and the established session. */
    fun finish(message2: ByteArray): Pair<ByteArray, E2ESession> {
        require(message2.size >= 48) { "short handshake message" }
        val re = message2.copyOfRange(0, 32)
        sym.mixHash(re)
        sym.mixKey(dh(e, re))
        val payload = sym.decryptAndHash(message2.copyOfRange(32, message2.size))
        val (send, recv) = sym.split()
        return payload to E2ESession(sym.h, send, recv)
    }
}

/** The responder (instance) side, for tests: reads message 1, writes message 2. */
internal fun nkRespond(
    staticPrivate: ByteArray,
    message: ByteArray,
    prologue: ByteArray = E2E_PROLOGUE.encodeToByteArray(),
    ephemeral: ByteArray? = null,
    payload: ByteArray = ByteArray(0),
): Triple<ByteArray, ByteArray, E2ESession> {
    require(message.size >= 48) { "short handshake message" }
    val sym = SymmetricState(prologue)
    sym.mixHash(x25519Public(staticPrivate))
    val re = message.copyOfRange(0, 32)
    sym.mixHash(re)
    sym.mixKey(dh(staticPrivate, re))
    val received = sym.decryptAndHash(message.copyOfRange(32, message.size))
    val e = ephemeral ?: newPrivateKey()
    val ePub = x25519Public(e)
    sym.mixHash(ePub)
    sym.mixKey(dh(e, re))
    val reply = ePub + sym.encryptAndHash(payload)
    val (toResponder, toInitiator) = sym.split()
    return Triple(reply, received, E2ESession(sym.h, toInitiator, toResponder))
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

/**
 * The client's handshake on a fresh connection to an E2E tunnel: [frame] is
 * the `TunnelHost/handshake` call to send; [finish] reads its answer.
 */
internal class ClientHandshake(pinnedKey: String, callId: String, credential: String) {
    private val initiator = NKInitiator(
        keyBytes(pinnedKey, "An E2E key (the instance's X25519 public key)"),
        prologue = tunnelPrologue(credentialDigest(credential)),
    )

    val frame: Frame.CallOpen = Frame.CallOpen(
        callId, TUNNEL_HOST_SERVICE, TUNNEL_HANDSHAKE_METHOD,
        buildJsonObject { put("message", JsonPrimitive(b64encode(initiator.message))) },
    )

    fun finish(reply: Frame): E2ESession {
        if (reply is Frame.CallError) {
            val details = reply.error.details
            if ((details?.get("reason") as? JsonPrimitive)?.content == "e2e_handshake_failed") {
                // The instance's own answer: it could not open message 1.
                throw UnauthenticatedException(reply.error.message, details = details)
            }
            throw handshakeFailed(reply.error.code + ": " + reply.error.message)
        }
        if (reply !is Frame.CallResult) throw handshakeFailed("unexpected " + reply.kind)
        val message = ((reply.payload as? JsonObject)?.get("message") as? JsonPrimitive)?.takeIf { it.isString }?.content ?: ""
        return try {
            initiator.finish(b64decode(message)).second
        } catch (_: Exception) {
            // Only the holder of the pinned private key can write message 2.
            throw handshakeFailed("message 2 is not from the pinned instance key")
        }
    }
}
