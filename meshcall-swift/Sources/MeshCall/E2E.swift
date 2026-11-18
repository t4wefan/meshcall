// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).
//
// A Noise_NK_25519_AESGCM_SHA256 handshake per client connection, then the
// payloads, metadata and error messages of every call travel in a `sealed`
// field while the Router routes on the clear header.

import CryptoKit
import Foundation

let e2ePrologue = "meshcall-e2e/1"
/// How far behind the newest counter a sealed frame may arrive.
let e2eWindow: UInt64 = 1024
let tunnelHandshakeMethod = "handshake"
let tunnelHostService = "meshcall.router.v1.TunnelHost"
/// End-to-end tunnel credentials say so ("." never occurs in a plain one).
let e2eCredentialPrefix = "mtun_e2e."
/// The Router answers these services itself: calls to them are never sealed.
let routerServicePrefix = "meshcall.router."

private let noiseProtocol = "Noise_NK_25519_AESGCM_SHA256"
private let sealedNotice = "sealed"

func isE2ECredential(_ credential: String?) -> Bool {
    credential?.hasPrefix(e2eCredentialPrefix) == true
}

/// A credential and a key must agree before anything is sent: an end-to-end
/// tunnel never sees an unsealed payload by mistake.
func checkE2EAgreement(credential: String?, e2eKey: String?) throws {
    if isE2ECredential(credential) && e2eKey == nil {
        throw ConfigurationError(
            "This is an end-to-end tunnel credential: pass the instance's e2eKey the issuer gave you")
    }
    if e2eKey != nil && !isE2ECredential(credential) {
        throw ConfigurationError("e2eKey needs the credential of an end-to-end tunnel (mtun_e2e.…)")
    }
}

// MARK: - base64url and keys

func base64URLEncode(_ data: Data) -> String {
    data.base64EncodedString()
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "/", with: "_")
        .replacingOccurrences(of: "=", with: "")
}

func base64URLDecode(_ value: String) -> Data? {
    guard value.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "-" || $0 == "_") }) else {
        return nil
    }
    var text = value.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
    text += String(repeating: "=", count: (4 - text.count % 4) % 4)
    return Data(base64Encoded: text)
}

func keyBytes(_ value: String, _ what: String) throws -> Data {
    guard let raw = base64URLDecode(value), raw.count == 32 else {
        throw ConfigurationError(what + " is 32 bytes, base64url without padding")
    }
    return raw
}

/// An instance key pair for end-to-end tunnels, base64url without padding.
/// The instance is configured with `privateKey`; the issuer pins `publicKey`.
public struct E2EKeyPair: Sendable, Hashable {
    public let privateKey: String
    public let publicKey: String

    /// A new X25519 key pair.
    public static func generate() -> E2EKeyPair {
        let key = Curve25519.KeyAgreement.PrivateKey()
        return E2EKeyPair(
            privateKey: base64URLEncode(key.rawRepresentation),
            publicKey: base64URLEncode(key.publicKey.rawRepresentation))
    }

    /// The public key of an instance private key.
    public static func publicKey(of privateKey: String) throws -> String {
        let raw = try keyBytes(privateKey, "An E2E private key")
        return base64URLEncode(try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: raw).publicKey.rawRepresentation)
    }
}

/// The hex SHA-256 of a tunnel credential, which the handshake prologue binds.
func credentialDigest(_ credential: String) -> String {
    hex(Data(SHA256.hash(data: Data(credential.utf8))))
}

func tunnelPrologue(digest: String) -> Data {
    Data((e2ePrologue + "|" + digest).utf8)
}

func hex(_ data: Data) -> String {
    data.map { String(format: "%02x", $0) }.joined()
}

// MARK: - Noise NK

private func sha256(_ data: Data) -> Data { Data(SHA256.hash(data: data)) }

private func hmac(_ key: Data, _ data: Data) -> Data {
    Data(HMAC<SHA256>.authenticationCode(for: data, using: SymmetricKey(data: key)))
}

private func hkdf2(_ chainingKey: Data, _ ikm: Data) -> (Data, Data) {
    let temp = hmac(chainingKey, ikm)
    let first = hmac(temp, Data([1]))
    return (first, hmac(temp, first + Data([2])))
}

private func dh(_ privateKey: Curve25519.KeyAgreement.PrivateKey, _ publicKey: Curve25519.KeyAgreement.PublicKey) throws
    -> Data
{
    try privateKey.sharedSecretFromKeyAgreement(with: publicKey).withUnsafeBytes { Data($0) }
}

/// The Noise nonce: four zero bytes, then the 8-byte big-endian counter.
private func gcmNonce(_ counter: UInt64) throws -> AES.GCM.Nonce {
    var bytes = Data(count: 4)
    withUnsafeBytes(of: counter.bigEndian) { bytes.append(contentsOf: $0) }
    return try AES.GCM.Nonce(data: bytes)
}

/// AES-256-GCM, ciphertext followed by the 16-byte tag.
func aeadEncrypt(_ key: SymmetricKey, _ counter: UInt64, _ plaintext: Data, _ ad: Data) throws -> Data {
    let box = try AES.GCM.seal(plaintext, using: key, nonce: gcmNonce(counter), authenticating: ad)
    return box.ciphertext + box.tag
}

func aeadDecrypt(_ key: SymmetricKey, _ counter: UInt64, _ ciphertext: Data, _ ad: Data) throws -> Data {
    guard ciphertext.count >= 16 else { throw E2EError.doesNotOpen }
    let box = try AES.GCM.SealedBox(
        nonce: gcmNonce(counter), ciphertext: ciphertext.prefix(ciphertext.count - 16),
        tag: ciphertext.suffix(16))
    return try AES.GCM.open(box, using: key, authenticating: ad)
}

enum E2EError: Error, CustomStringConvertible {
    case shortMessage, doesNotOpen, malformed, replayed, clear, notAnObject

    var description: String {
        switch self {
        case .shortMessage: return "short handshake message"
        case .doesNotOpen: return "sealed field does not open"
        case .malformed: return "malformed sealed field"
        case .replayed: return "sealed field replayed or too old"
        case .clear: return "a frame of a sealed call arrived in clear"
        case .notAnObject: return "sealed field is not a sealed object"
        }
    }
}

private struct SymmetricState {
    var h: Data
    var ck: Data
    var k: SymmetricKey?
    var n: UInt64 = 0

    init(prologue: Data) {
        var name = Data(noiseProtocol.utf8)  // shorter than HASHLEN: zero-padded
        name.append(Data(count: 32 - name.count))
        h = name
        ck = name
        mixHash(prologue)
    }

    mutating func mixHash(_ data: Data) { h = sha256(h + data) }

    mutating func mixKey(_ ikm: Data) {
        let (chaining, key) = hkdf2(ck, ikm)
        ck = chaining
        k = SymmetricKey(data: key)
        n = 0
    }

    mutating func encryptAndHash(_ plaintext: Data) throws -> Data {
        var out = plaintext
        if let k {
            out = try aeadEncrypt(k, n, plaintext, h)
            n += 1
        }
        mixHash(out)
        return out
    }

    mutating func decryptAndHash(_ ciphertext: Data) throws -> Data {
        var out = ciphertext
        if let k {
            out = try aeadDecrypt(k, n, ciphertext, h)
            n += 1
        }
        mixHash(ciphertext)
        return out
    }

    func split() -> (Data, Data) { hkdf2(ck, Data()) }
}

/// The client side of a handshake after message 1, waiting for message 2.
struct NKInitiator {
    fileprivate var sym: SymmetricState
    let ephemeral: Curve25519.KeyAgreement.PrivateKey

    func finish(_ message2: Data) throws -> (payload: Data, session: E2ESession) {
        guard message2.count >= 48 else { throw E2EError.shortMessage }
        var sym = sym
        let reBytes = message2.prefix(32)
        let re = try Curve25519.KeyAgreement.PublicKey(rawRepresentation: reBytes)
        sym.mixHash(reBytes)
        sym.mixKey(try dh(ephemeral, re))
        let payload = try sym.decryptAndHash(message2.dropFirst(32))
        let (send, receive) = sym.split()
        return (payload, E2ESession(hash: sym.h, send: send, receive: receive))
    }
}

/// Message 1 towards the pinned key. `ephemeral` is fixed only by test vectors.
func nkInitiate(
    remoteStatic: Data, prologue: Data, ephemeral: Curve25519.KeyAgreement.PrivateKey? = nil, payload: Data = Data()
) throws -> (NKInitiator, Data) {
    let rs = try Curve25519.KeyAgreement.PublicKey(rawRepresentation: remoteStatic)
    var sym = SymmetricState(prologue: prologue)
    sym.mixHash(remoteStatic)
    let e = ephemeral ?? Curve25519.KeyAgreement.PrivateKey()
    let ePub = e.publicKey.rawRepresentation
    sym.mixHash(ePub)
    sym.mixKey(try dh(e, rs))
    let message = ePub + (try sym.encryptAndHash(payload))
    return (NKInitiator(sym: sym, ephemeral: e), message)
}

/// The instance side: read message 1, write message 2 (tests and test vectors).
func nkRespond(
    staticPrivate: Curve25519.KeyAgreement.PrivateKey, prologue: Data, message: Data,
    ephemeral: Curve25519.KeyAgreement.PrivateKey? = nil, payload: Data = Data()
) throws -> (reply: Data, received: Data, session: E2ESession) {
    guard message.count >= 48 else { throw E2EError.shortMessage }
    var sym = SymmetricState(prologue: prologue)
    sym.mixHash(staticPrivate.publicKey.rawRepresentation)
    let reBytes = message.prefix(32)
    let re = try Curve25519.KeyAgreement.PublicKey(rawRepresentation: reBytes)
    sym.mixHash(reBytes)
    sym.mixKey(try dh(staticPrivate, re))
    let received = try sym.decryptAndHash(message.dropFirst(32))
    let e = ephemeral ?? Curve25519.KeyAgreement.PrivateKey()
    let ePub = e.publicKey.rawRepresentation
    sym.mixHash(ePub)
    sym.mixKey(try dh(e, re))
    let reply = ePub + (try sym.encryptAndHash(payload))
    let (toResponder, toInitiator) = sym.split()
    return (reply, received, E2ESession(hash: sym.h, send: toInitiator, receive: toResponder))
}

// MARK: - sessions and sealed frames

/// One end of an E2E session: a sending key and counter, a receiving key with
/// a replay window (the Router may drop or reorder frames, never replay them).
final class E2ESession {
    let id: String
    let hash: Data
    let sendKey: SymmetricKey
    private let receiveKey: SymmetricKey
    private var sendCounter: UInt64 = 0
    /// Highest counter received + 1.
    private var top: UInt64 = 0
    /// Bit `n % 1024`: counter n seen, for the counters within the window.
    private var seen = [UInt64](repeating: 0, count: Int(e2eWindow / 64))

    init(hash: Data, send: Data, receive: Data) {
        self.hash = hash
        self.id = hex(hash.prefix(16))
        self.sendKey = SymmetricKey(data: send)
        self.receiveKey = SymmetricKey(data: receive)
    }

    func seal(_ plaintext: Data, ad: Data) throws -> String {
        let counter = sendCounter
        sendCounter += 1
        var out = Data()
        withUnsafeBytes(of: counter.bigEndian) { out.append(contentsOf: $0) }
        out.append(try aeadEncrypt(sendKey, counter, plaintext, ad))
        return base64URLEncode(out)
    }

    func open(_ sealed: String, ad: Data) throws -> Data {
        guard let raw = base64URLDecode(sealed), raw.count >= 24 else { throw E2EError.malformed }
        let counter = raw.prefix(8).reduce(UInt64(0)) { $0 << 8 | UInt64($1) }
        let plaintext: Data
        do {
            plaintext = try aeadDecrypt(receiveKey, counter, Data(raw.dropFirst(8)), ad)
        } catch {
            throw E2EError.doesNotOpen
        }
        guard accept(counter) else { throw E2EError.replayed }
        return plaintext
    }

    private func bit(_ n: UInt64) -> (Int, UInt64) {
        let index = Int(n % e2eWindow)
        return (index / 64, UInt64(1) << UInt64(index % 64))
    }

    private func accept(_ n: UInt64) -> Bool {
        if n >= top {
            if n - top >= e2eWindow {
                seen = [UInt64](repeating: 0, count: seen.count)
            } else {
                var skipped = top
                while skipped < n {
                    let (word, mask) = bit(skipped)
                    seen[word] &= ~mask
                    skipped += 1
                }
            }
            let (word, mask) = bit(n)
            seen[word] |= mask
            top = n + 1
            return true
        }
        if top - 1 - n >= e2eWindow { return false }
        let (word, mask) = bit(n)
        if seen[word] & mask != 0 { return false }
        seen[word] |= mask
        return true
    }
}

/// What both ends know of one sealed call: its session, service and method.
struct SealedCall {
    let session: E2ESession
    let service: String
    let method: String

    private func ad(_ frame: Frame) -> Data {
        let sequence = frame["sequence"]?.intValue.map(String.init) ?? ""
        return Data(
            [
                e2ePrologue, frame.kind, frame.callID ?? "", service, method, frame.direction ?? "", sequence,
                session.id,
            ].joined(separator: "|").utf8)
    }

    /// Move `frame`'s content into its sealed field.
    func seal(_ frame: Frame) throws -> Frame {
        var out = frame
        var parts: [String: JSONValue] = [:]
        switch frame.kind {
        case "call.open", "call.result", "stream.item":
            if let payload = frame["payload"] { parts["payload"] = payload }
            if frame.kind != "stream.item", let metadata = frame["metadata"], !metadata.isNull {
                parts["metadata"] = metadata
            }
            out["payload"] = .null
            out.fields.removeValue(forKey: "metadata")
            if frame.kind == "call.open" { out["e2e_session"] = .string(session.id) }
        case "call.error":
            var error = frame["error"]?.objectValue ?? [:]
            parts["message"] = error["message"] ?? ""
            if let details = error["details"], !details.isNull { parts["details"] = details }
            if let metadata = frame["metadata"], !metadata.isNull { parts["metadata"] = metadata }
            error["message"] = .string(sealedNotice)
            error.removeValue(forKey: "details")
            out["error"] = .object(error)
            out.fields.removeValue(forKey: "metadata")
        default:
            return frame  // windows, ends and cancels carry no content
        }
        out["sealed"] = .string(try session.seal(try JSONValue.object(parts).jsonData(), ad: ad(out)))
        return out
    }

    /// Restore `frame`'s content. Content frames in clear did not come from the
    /// other end and are refused; a clear `call.error` (the Router's own) is kept.
    func open(_ frame: Frame) throws -> Frame {
        guard let sealed = frame["sealed"]?.stringValue, !sealed.isEmpty else {
            if ["call.open", "call.result", "stream.item"].contains(frame.kind) { throw E2EError.clear }
            return frame
        }
        guard case .object(let parts) = try JSONValue(json: try session.open(sealed, ad: ad(frame))) else {
            throw E2EError.notAnObject
        }
        let metadata = parts["metadata"]?.objectValue?.filter { !isReservedKey($0.key) && $0.value.stringValue != nil }
        var out = frame
        out.fields.removeValue(forKey: "sealed")
        func setMetadata(_ value: [String: JSONValue]?) {
            if let value { out["metadata"] = .object(value) } else { out.fields.removeValue(forKey: "metadata") }
        }
        if frame.kind == "call.error" {
            var error = frame["error"]?.objectValue ?? [:]
            error["message"] = .string(parts["message"]?.stringValue ?? "")
            if let details = parts["details"], details.objectValue != nil {
                error["details"] = details
            } else {
                error.removeValue(forKey: "details")
            }
            out["error"] = .object(error)
            setMetadata(metadata)
            return out
        }
        out["payload"] = parts["payload"] ?? .null
        if frame.kind == "call.open" {
            // The Router's clear keys (the tunnel identity) win over sealed ones.
            let merged = (metadata ?? [:]).merging(frame["metadata"]?.objectValue ?? [:]) { _, clear in clear }
            setMetadata(merged.isEmpty ? nil : merged)
        } else if frame.kind == "call.result" {
            setMetadata(metadata)
        }
        return out
    }
}

func handshakeFailed(_ why: String) -> MeshCallError {
    .detailed(.unauthenticated, "End-to-end handshake failed: " + why, source: "client", reason: "e2e_handshake_failed")
}

/// The client's handshake on a fresh connection to an E2E tunnel: `frame` is
/// the `TunnelHost/handshake` call to send; `finish` reads its answer.
struct ClientHandshake {
    let frame: Frame
    private let initiator: NKInitiator

    init(pinnedKey: String, callID: String, credential: String) throws {
        let remote = try keyBytes(pinnedKey, "An E2E key (the instance's X25519 public key)")
        // The prologue binds the session to this credential's tunnel.
        let (initiator, message) = try nkInitiate(
            remoteStatic: remote, prologue: tunnelPrologue(digest: credentialDigest(credential)))
        self.initiator = initiator
        frame = Frame(
            "call.open",
            [
                "call_id": .string(callID), "service": .string(tunnelHostService),
                "method": .string(tunnelHandshakeMethod), "payload": ["message": .string(base64URLEncode(message))],
            ])
    }

    func finish(_ reply: Frame) throws -> E2ESession {
        if reply.kind == "call.error" {
            let error = reply["error"]?.objectValue ?? [:]
            let details = error["details"]?.objectValue ?? [:]
            if details["reason"]?.stringValue == "e2e_handshake_failed" {
                // The instance's own answer: it could not open message 1.
                throw MeshCallError(.unauthenticated, error["message"]?.stringValue ?? "", details: details)
            }
            throw handshakeFailed(
                (error["code"]?.stringValue ?? "") + ": " + (error["message"]?.stringValue ?? ""))
        }
        guard reply.kind == "call.result" else { throw handshakeFailed("unexpected " + reply.kind) }
        do {
            guard let message = reply.payload["message"]?.stringValue, let raw = base64URLDecode(message) else {
                throw E2EError.malformed
            }
            return try initiator.finish(raw).session
        } catch {
            // Only the holder of the pinned private key can write message 2.
            throw handshakeFailed("message 2 is not from the pinned instance key")
        }
    }
}
