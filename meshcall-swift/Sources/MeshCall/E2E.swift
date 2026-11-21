// End-to-end sealed tunnels (docs/protocol.md → End-to-end encryption).
//
// A Noise handshake per client connection (NK, or IK / IKpsk2 when the
// client authenticates itself; docs/protocol.md → Client authentication),
// then the payloads, metadata and error messages of every call travel in a
// `sealed` field while the Router routes on the clear header.

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

/// A pairing confirmation may wait for a person at the instance.
let e2eConfirmTimeout: Duration = .seconds(120)
let maxPairingIDBytes = 128
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

/// An X25519 key pair for end-to-end tunnels, base64url without padding: an
/// instance's (configured with `privateKey`; the issuer pins `publicKey`) or a
/// client's own (`E2EIdentity.privateKey`, kept by the app).
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

    /// The public key of a private key (an instance's or a client's).
    public static func publicKey(of privateKey: String) throws -> String {
        let raw = try keyBytes(privateKey, "An E2E private key")
        return base64URLEncode(try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: raw).publicKey.rawRepresentation)
    }
}

/// A one-time pairing the instance's owner handed the client out of band
/// (never through the Router, e.g. in a QR code): its id and 32-byte PSK,
/// base64url without padding.
public struct E2EPairing: Sendable, Hashable, CustomStringConvertible, CustomDebugStringConvertible {
    public var id: String
    public var psk: String

    public init(id: String, psk: String) {
        self.id = id
        self.psk = psk
    }

    public var description: String { "E2EPairing(id: \(id))" }
    public var debugDescription: String { description }
}

/// This client's own key for end-to-end tunnels: with it the client
/// authenticates itself to the instance (Noise IK), so the instance can refuse
/// clients it does not know even when the Router lets them through.
///
/// `privateKey` is this client's static X25519 key (`E2EKeyPair.generate()`,
/// kept by the app, never sent anywhere); `instanceID` the instance whose key
/// the client pinned, bound in the handshake. `pairing` makes the first
/// handshake a pairing (IKpsk2), confirmed before the first call; once the
/// instance accepted it, every later handshake of the same `Mesh` uses IK.
public struct E2EIdentity: Sendable, Hashable, CustomStringConvertible, CustomDebugStringConvertible {
    public var privateKey: String
    public var instanceID: String
    public var pairing: E2EPairing?

    public init(privateKey: String, instanceID: String, pairing: E2EPairing? = nil) {
        self.privateKey = privateKey
        self.instanceID = instanceID
        self.pairing = pairing
    }

    /// This client's public key, as the instance sees it.
    public var publicKey: String {
        get throws { try E2EKeyPair.publicKey(of: privateKey) }
    }

    public var description: String {
        "E2EIdentity(instanceID: \(instanceID), pairing: \(pairing.map(\.description) ?? "nil"))"
    }
    public var debugDescription: String { description }

    func validate() throws {
        _ = try keyBytes(privateKey, "An E2E private key (E2EIdentity.privateKey)")
        if instanceID.isEmpty { throw ConfigurationError("E2EIdentity needs the instanceID whose key you pinned") }
        if let pairing {
            if pairing.id.utf8.isEmpty || pairing.id.utf8.count > maxPairingIDBytes {
                throw ConfigurationError("A pairing id is 1 to \(maxPairingIDBytes) bytes")
            }
            _ = try keyBytes(pairing.psk, "A pairing PSK")
        }
    }
}

/// The hex SHA-256 of a tunnel credential, which the handshake prologue binds.
func credentialDigest(_ credential: String) -> String {
    hex(Data(SHA256.hash(data: Data(credential.utf8))))
}

func tunnelPrologue(digest: String) -> Data {
    Data((e2ePrologue + "|" + digest).utf8)
}

/// The IK / IKpsk2 prologue: the tunnel, and the instance the client pinned the key of.
func peerPrologue(digest: String, instanceID: String) -> Data {
    Data((e2ePrologue + "|" + digest + "|" + instanceID).utf8)
}

/// The associated data of a pairing confirmation.
func confirmAD(_ sessionID: String) -> Data {
    Data((e2ePrologue + "|confirm|" + sessionID).utf8)
}

func hex(_ data: Data) -> String {
    data.map { String(format: "%02x", $0) }.joined()
}

// MARK: - Noise NK, IK, IKpsk2

private func sha256(_ data: Data) -> Data { Data(SHA256.hash(data: data)) }

private func hmac(_ key: Data, _ data: Data) -> Data {
    Data(HMAC<SHA256>.authenticationCode(for: data, using: SymmetricKey(data: key)))
}

/// Noise HKDF with `count` (2 or 3) outputs.
private func hkdf(_ chainingKey: Data, _ ikm: Data, _ count: Int) -> [Data] {
    let temp = hmac(chainingKey, ikm)
    var out: [Data] = []
    var previous = Data()
    for index in 1...count {
        previous = hmac(temp, previous + Data([UInt8(index)]))
        out.append(previous)
    }
    return out
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

/// The handshakes of docs/protocol.md: NK (the client has no key of its own),
/// IK (a client the instance knows) and IKpsk2 (pairing).
enum NoiseProtocol: String, Sendable, CaseIterable {
    case nk = "Noise_NK_25519_AESGCM_SHA256"
    case ik = "Noise_IK_25519_AESGCM_SHA256"
    case ikpsk2 = "Noise_IKpsk2_25519_AESGCM_SHA256"

    enum Token { case e, s, es, ss, ee, se, psk }

    /// The two message patterns after the responder's static key (`<- s`),
    /// which the initiator knows beforehand in all three.
    var messages: [[Token]] {
        switch self {
        case .nk: return [[.e, .es], [.e, .ee]]
        case .ik: return [[.e, .es, .s, .ss], [.e, .ee, .se]]
        case .ikpsk2: return [[.e, .es, .s, .ss], [.e, .ee, .se, .psk]]
        }
    }
}

private struct SymmetricState {
    var h: Data
    var ck: Data
    var k: SymmetricKey?
    var n: UInt64 = 0

    init(protocolName: String, prologue: Data) {
        var name = Data(protocolName.utf8)  // every name here is shorter than HASHLEN: zero-padded
        name.append(Data(count: 32 - name.count))
        h = name
        ck = name
        mixHash(prologue)
    }

    mutating func mixHash(_ data: Data) { h = sha256(h + data) }

    mutating func mixKey(_ ikm: Data) {
        let out = hkdf(ck, ikm, 2)
        ck = out[0]
        k = SymmetricKey(data: out[1])
        n = 0
    }

    mutating func mixKeyAndHash(_ ikm: Data) {
        let out = hkdf(ck, ikm, 3)
        ck = out[0]
        mixHash(out[1])
        k = SymmetricKey(data: out[2])
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

    func split() -> (Data, Data) {
        let out = hkdf(ck, Data(), 2)
        return (out[0], out[1])
    }
}

/// One end of a two-message Noise handshake towards (or as) a responder whose
/// static key the initiator knows: NK, IK or IKpsk2. The client is always the
/// initiator; the responder side serves tests and test vectors.
struct NoiseHandshake {
    typealias PrivateKey = Curve25519.KeyAgreement.PrivateKey
    typealias PublicKey = Curve25519.KeyAgreement.PublicKey

    let noiseProtocol: NoiseProtocol
    let initiator: Bool
    private var sym: SymmetricState
    private let s: PrivateKey?
    /// The other end's static public key (the client's, on the responder, once message 1 is read).
    private(set) var rs: PublicKey?
    private let psk: Data?
    private var e: PrivateKey?
    private var re: PublicKey?
    private var turn = 0

    /// `s` is this end's static key (the responder's; the initiator's for IK
    /// and IKpsk2), `rs` the responder's public key on the initiator, `psk` the
    /// pairing secret of IKpsk2. `ephemeral` is fixed only by test vectors.
    init(
        _ noiseProtocol: NoiseProtocol, initiator: Bool, prologue: Data, s: PrivateKey? = nil, rs: PublicKey? = nil,
        psk: Data? = nil, ephemeral: PrivateKey? = nil
    ) throws {
        guard (noiseProtocol == .ikpsk2) == (psk != nil) else {
            throw ConfigurationError("IKpsk2, and only IKpsk2, takes a PSK")
        }
        if let psk, psk.count != 32 { throw ConfigurationError("A pairing PSK is 32 bytes") }
        if noiseProtocol != .nk || !initiator, s == nil {
            throw ConfigurationError("\(noiseProtocol.rawValue) needs this end's static key")
        }
        let responderStatic = initiator ? rs : s?.publicKey
        guard let responderStatic else { throw ConfigurationError("The initiator needs the responder's static key") }
        self.noiseProtocol = noiseProtocol
        self.initiator = initiator
        self.s = s
        self.rs = rs
        self.psk = psk
        self.e = ephemeral
        sym = SymmetricState(protocolName: noiseProtocol.rawValue, prologue: prologue)
        sym.mixHash(responderStatic.rawRepresentation)
    }

    private mutating func nextPattern() -> [NoiseProtocol.Token] {
        let tokens = noiseProtocol.messages[turn]
        turn += 1
        return tokens
    }

    mutating func writeMessage(_ payload: Data = Data()) throws -> Data {
        var out = Data()
        for token in nextPattern() {
            switch token {
            case .e:
                let key = e ?? PrivateKey()
                e = key
                let pub = key.publicKey.rawRepresentation
                out.append(pub)
                sym.mixHash(pub)
                if psk != nil { sym.mixKey(pub) }
            case .s:
                guard let s else { throw ConfigurationError("No static key") }
                out.append(try sym.encryptAndHash(s.publicKey.rawRepresentation))
            case .psk:
                sym.mixKeyAndHash(psk ?? Data())
            default:
                sym.mixKey(try tokenDH(token))
            }
        }
        out.append(try sym.encryptAndHash(payload))
        return out
    }

    mutating func readMessage(_ message: Data) throws -> Data {
        var rest = Data(message)
        for token in nextPattern() {
            switch token {
            case .e:
                guard rest.count >= 32 else { throw E2EError.shortMessage }
                let raw = Data(rest.prefix(32))
                re = try PublicKey(rawRepresentation: raw)
                sym.mixHash(raw)
                if psk != nil { sym.mixKey(raw) }
                rest = Data(rest.dropFirst(32))
            case .s:
                let size = sym.k != nil ? 48 : 32
                guard rest.count >= size else { throw E2EError.shortMessage }
                rs = try PublicKey(rawRepresentation: try sym.decryptAndHash(Data(rest.prefix(size))))
                rest = Data(rest.dropFirst(size))
            case .psk:
                sym.mixKeyAndHash(psk ?? Data())
            default:
                sym.mixKey(try tokenDH(token))
            }
        }
        if sym.k != nil, rest.count < 16 { throw E2EError.shortMessage }
        return try sym.decryptAndHash(rest)
    }

    /// The first letter is the initiator's key, the second the responder's.
    private func tokenDH(_ token: NoiseProtocol.Token) throws -> Data {
        let (first, second): (Bool, Bool)  // true: ephemeral
        switch token {
        case .ee: (first, second) = (true, true)
        case .es: (first, second) = (true, false)
        case .se: (first, second) = (false, true)
        case .ss: (first, second) = (false, false)
        default: throw E2EError.malformed
        }
        let (mine, theirs) = initiator ? (first, second) : (second, first)
        guard let local = mine ? e : s, let remote = theirs ? re : rs else { throw E2EError.shortMessage }
        return try dh(local, remote)
    }

    /// The session once both messages are done.
    func split() -> E2ESession {
        let (toResponder, toInitiator) = sym.split()
        return initiator
            ? E2ESession(hash: sym.h, send: toResponder, receive: toInitiator)
            : E2ESession(hash: sym.h, send: toInitiator, receive: toResponder)
    }
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

/// The payload of a `TunnelHost/handshake` answer, or why there is none.
private func handshakeAnswer(_ reply: Frame) throws -> JSONValue {
    if reply.kind == "call.error" {
        let error = reply["error"]?.objectValue ?? [:]
        let details = error["details"]?.objectValue ?? [:]
        if details["reason"]?.stringValue == "e2e_handshake_failed" {
            // The instance's own answer: it refused the handshake.
            throw MeshCallError(.unauthenticated, error["message"]?.stringValue ?? "", details: details)
        }
        throw handshakeFailed((error["code"]?.stringValue ?? "") + ": " + (error["message"]?.stringValue ?? ""))
    }
    guard reply.kind == "call.result" else { throw handshakeFailed("unexpected " + reply.kind) }
    return reply.payload
}

private func handshakeCall(_ callID: String, _ payload: [String: JSONValue], timeout: Duration? = nil) -> Frame {
    var fields: [String: JSONValue] = [
        "call_id": .string(callID), "service": .string(tunnelHostService),
        "method": .string(tunnelHandshakeMethod), "payload": .object(payload),
    ]
    if let timeout { fields["timeout_ms"] = .int(timeout.roundedMilliseconds) }
    return Frame("call.open", fields)
}

/// The client's handshake on a fresh connection to an E2E tunnel: `frame` is
/// the `TunnelHost/handshake` call to send; `finish` reads its answer. With
/// an identity it is IK, or IKpsk2 while `pairing` is set (the caller then
/// sends `confirmFrame` and reads its answer with `finishConfirm`). It never
/// falls back to a weaker handshake.
struct ClientHandshake {
    let frame: Frame
    let pairing: E2EPairing?
    private let noise: NoiseHandshake

    /// `paired`: this client completed its pairing already, so it uses IK.
    init(
        pinnedKey: String, callID: String, credential: String, identity: E2EIdentity? = nil, paired: Bool = false,
        ephemeral: Curve25519.KeyAgreement.PrivateKey? = nil
    ) throws {
        let remote = try Curve25519.KeyAgreement.PublicKey(
            rawRepresentation: keyBytes(pinnedKey, "An E2E key (the instance's X25519 public key)"))
        let digest = credentialDigest(credential)
        var payload: [String: JSONValue] = [:]
        var noise: NoiseHandshake
        if let identity {
            let pairing = paired ? nil : identity.pairing
            let noiseProtocol: NoiseProtocol = pairing != nil ? .ikpsk2 : .ik
            // The prologue binds the session to this credential's tunnel and to the pinned instance.
            noise = try NoiseHandshake(
                noiseProtocol, initiator: true, prologue: peerPrologue(digest: digest, instanceID: identity.instanceID),
                s: Curve25519.KeyAgreement.PrivateKey(
                    rawRepresentation: keyBytes(identity.privateKey, "An E2E private key (E2EIdentity.privateKey)")),
                rs: remote, psk: try pairing.map { try keyBytes($0.psk, "A pairing PSK") }, ephemeral: ephemeral)
            payload["protocol"] = .string(noiseProtocol.rawValue)
            if let pairing { payload["pairing_id"] = .string(pairing.id) }
            self.pairing = pairing
        } else {
            // The prologue binds the session to this credential's tunnel.
            noise = try NoiseHandshake(
                .nk, initiator: true, prologue: tunnelPrologue(digest: digest), rs: remote, ephemeral: ephemeral)
            self.pairing = nil
        }
        payload["message"] = .string(base64URLEncode(try noise.writeMessage()))
        self.noise = noise
        frame = handshakeCall(callID, payload)
    }

    func finish(_ reply: Frame) throws -> E2ESession {
        let answer = try handshakeAnswer(reply)
        do {
            guard let message = answer["message"]?.stringValue, let raw = base64URLDecode(message) else {
                throw E2EError.malformed
            }
            var noise = noise
            _ = try noise.readMessage(raw)
            return noise.split()
        } catch {
            // Only the holder of the pinned private key (and, pairing, of the PSK) can write message 2.
            throw handshakeFailed("message 2 is not from the pinned instance key")
        }
    }

    /// The pairing confirmation `{session, confirm}`: proves the PSK, so the
    /// instance accepts this client only now. Sealed with the session's first
    /// client counter.
    static func confirmFrame(session: E2ESession, callID: String) throws -> Frame {
        let confirm = try session.seal(Data("{}".utf8), ad: confirmAD(session.id))
        return handshakeCall(
            callID, ["session": .string(session.id), "confirm": .string(confirm)], timeout: e2eConfirmTimeout)
    }

    static func finishConfirm(_ reply: Frame) throws {
        _ = try handshakeAnswer(reply)
    }
}
