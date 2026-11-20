import CryptoKit
import Foundation
import XCTest

@testable import MeshCall

/// The repository root, from this file's path.
let repositoryRoot = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
    .deletingLastPathComponent().deletingLastPathComponent()

private func unhex(_ text: String) -> Data {
    var data = Data()
    var index = text.startIndex
    while index < text.endIndex {
        let next = text.index(index, offsetBy: 2)
        data.append(UInt8(text[index..<next], radix: 16)!)
        index = next
    }
    return data
}

final class E2ETests: XCTestCase {
    private func testdata(_ name: String) throws -> Data {
        try Data(contentsOf: repositoryRoot.appendingPathComponent("meshcall-router/conformance/testdata/" + name))
    }

    private func privateKey(hex text: String) throws -> Curve25519.KeyAgreement.PrivateKey {
        try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: unhex(text))
    }

    private func privateKey(base64URL text: String) throws -> Curve25519.KeyAgreement.PrivateKey {
        try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: XCTUnwrap(base64URLDecode(text)))
    }

    /// The cacophony test vectors every runtime checks: NK, IK and IKpsk2.
    // conformance-scenario: e2e.test_vector
    func testNoiseMatchesTheTestVectors() throws {
        struct Vector: Decodable {
            struct Message: Decodable {
                let payload: String
                let ciphertext: String
            }
            let protocol_name: String
            let init_prologue: String
            let init_static: String?
            let init_ephemeral: String
            let init_remote_static: String
            let init_psks: [String]?
            let resp_static: String
            let resp_ephemeral: String
            let handshake_hash: String
            let messages: [Message]
        }
        for name in ["nk", "ik", "ikpsk2"] {
            let v = try JSONDecoder().decode(Vector.self, from: testdata("noise_\(name)_25519_aesgcm_sha256.json"))
            let noiseProtocol = try XCTUnwrap(NoiseProtocol(rawValue: v.protocol_name))
            let prologue = unhex(v.init_prologue)
            let psk = v.init_psks?.first.map(unhex)
            let responderKey = try privateKey(hex: v.resp_static)
            XCTAssertEqual(hex(responderKey.publicKey.rawRepresentation), v.init_remote_static)

            var initiator = try NoiseHandshake(
                noiseProtocol, initiator: true, prologue: prologue, s: v.init_static.map { try privateKey(hex: $0) },
                rs: Curve25519.KeyAgreement.PublicKey(rawRepresentation: unhex(v.init_remote_static)), psk: psk,
                ephemeral: privateKey(hex: v.init_ephemeral))
            var responder = try NoiseHandshake(
                noiseProtocol, initiator: false, prologue: prologue, s: responderKey, psk: psk,
                ephemeral: privateKey(hex: v.resp_ephemeral))

            let message1 = try initiator.writeMessage(unhex(v.messages[0].payload))
            XCTAssertEqual(hex(message1), v.messages[0].ciphertext, name + " message 1")
            XCTAssertEqual(hex(try responder.readMessage(message1)), v.messages[0].payload, name)
            if let initStatic = v.init_static {
                XCTAssertEqual(
                    responder.rs?.rawRepresentation, try privateKey(hex: initStatic).publicKey.rawRepresentation, name)
            }
            let message2 = try responder.writeMessage(unhex(v.messages[1].payload))
            XCTAssertEqual(hex(message2), v.messages[1].ciphertext, name + " message 2")
            XCTAssertEqual(hex(try initiator.readMessage(message2)), v.messages[1].payload, name)
            let client = initiator.split()
            let instance = responder.split()
            XCTAssertEqual(hex(client.hash), v.handshake_hash, name)
            XCTAssertEqual(client.id, instance.id)

            // Transport messages alternate initiator, responder, counting from 0 per direction.
            var counters: [UInt64] = [0, 0]
            for (index, message) in v.messages.dropFirst(2).enumerated() {
                let from = [client, instance][index % 2]
                let sealed = try aeadEncrypt(from.sendKey, counters[index % 2], unhex(message.payload), Data())
                counters[index % 2] += 1
                XCTAssertEqual(hex(sealed), message.ciphertext, "\(name) transport message \(index)")
            }
        }
    }

    /// The MeshCall layer: prologues, the handshake request this client sends,
    /// the session id and the pairing confirmation, byte for byte from fixed
    /// keys; a responder written here answers as the instance would.
    // conformance-scenario: e2e.test_vector
    func testClientHandshakesMatchTheMeshCallVectors() throws {
        struct Vectors: Decodable {
            struct Case: Decodable {
                let `protocol`: String
                let prologue: String
                let request: [String: String]
                let answer: [String: String]
                let handshake_hash: String
                let session: String
                let confirm_request: [String: String]?
            }
            struct Pairing: Decodable {
                let id: String
                let psk: String
            }
            let credential: String
            let credential_digest: String
            let instance_id: String
            let instance_private_key: String
            let instance_public_key: String
            let client_private_key: String
            let client_public_key: String
            let instance_ephemeral: String
            let client_ephemeral: String
            let pairing: Pairing
            let cases: [Case]
        }
        let v = try JSONDecoder().decode(Vectors.self, from: testdata("e2e_handshakes.json"))
        XCTAssertEqual(credentialDigest(v.credential), v.credential_digest)
        XCTAssertEqual(try E2EKeyPair.publicKey(of: v.instance_private_key), v.instance_public_key)
        let identity = E2EIdentity(
            privateKey: v.client_private_key, instanceID: v.instance_id,
            pairing: E2EPairing(id: v.pairing.id, psk: v.pairing.psk))
        XCTAssertEqual(try identity.publicKey, v.client_public_key)
        XCTAssertEqual(v.cases.map(\.protocol), NoiseProtocol.allCases.map(\.rawValue))

        for c in v.cases {
            let noiseProtocol = try XCTUnwrap(NoiseProtocol(rawValue: c.protocol))
            // NK has no identity; IK is a client that paired already; IKpsk2 pairs.
            let handshake = try ClientHandshake(
                pinnedKey: v.instance_public_key, callID: "hs", credential: v.credential,
                identity: noiseProtocol == .nk ? nil : identity, paired: noiseProtocol == .ik,
                ephemeral: privateKey(base64URL: v.client_ephemeral))
            XCTAssertEqual(handshake.frame.kind, "call.open")
            XCTAssertEqual(handshake.frame["service"]?.stringValue, tunnelHostService)
            XCTAssertEqual(handshake.frame["method"]?.stringValue, "handshake")
            XCTAssertEqual(handshake.frame.payload, .object(c.request.mapValues { .string($0) }), c.protocol)
            XCTAssertEqual(handshake.pairing?.id, noiseProtocol == .ikpsk2 ? v.pairing.id : nil)

            // The instance's side.
            let digest = credentialDigest(v.credential)
            let prologue =
                noiseProtocol == .nk
                ? tunnelPrologue(digest: digest) : peerPrologue(digest: digest, instanceID: v.instance_id)
            XCTAssertEqual(String(decoding: prologue, as: UTF8.self), c.prologue)
            var responder = try NoiseHandshake(
                noiseProtocol, initiator: false, prologue: prologue, s: privateKey(base64URL: v.instance_private_key),
                psk: noiseProtocol == .ikpsk2 ? base64URLDecode(v.pairing.psk) : nil,
                ephemeral: privateKey(base64URL: v.instance_ephemeral))
            _ = try responder.readMessage(XCTUnwrap(base64URLDecode(XCTUnwrap(c.request["message"]))))
            if noiseProtocol != .nk {
                XCTAssertEqual(responder.rs.map { base64URLEncode($0.rawRepresentation) }, v.client_public_key)
            }
            let answer = base64URLEncode(try responder.writeMessage())
            XCTAssertEqual(answer, c.answer["message"], c.protocol)
            let instance = responder.split()

            let session = try handshake.finish(Frame("call.result", ["call_id": "hs", "payload": ["message": .string(answer)]]))
            XCTAssertEqual(hex(session.hash), c.handshake_hash, c.protocol)
            XCTAssertEqual(session.id, c.session)
            XCTAssertEqual(instance.id, c.session)

            if let confirmRequest = c.confirm_request {
                let confirm = try ClientHandshake.confirmFrame(session: session, callID: "confirm")
                XCTAssertEqual(confirm.payload, .object(confirmRequest.mapValues { .string($0) }))
                XCTAssertEqual(confirm["timeout_ms"], .int(120_000))
                let sealed = try XCTUnwrap(confirm.payload["confirm"]?.stringValue)
                XCTAssertEqual(try instance.open(sealed, ad: confirmAD(c.session)), Data("{}".utf8))
                XCTAssertNoThrow(try ClientHandshake.finishConfirm(Frame("call.result", ["call_id": "confirm", "payload": [:]])))
            } else {
                XCTAssertNil(handshake.pairing)
            }
        }
    }

    func testAnAnswerNotFromThePinnedKeyOrPSKFailsTheHandshake() throws {
        let instance = E2EKeyPair.generate()
        let identity = E2EIdentity(
            privateKey: E2EKeyPair.generate().privateKey, instanceID: "i-1",
            pairing: E2EPairing(id: "p", psk: base64URLEncode(Data(repeating: 7, count: 32))))
        let handshake = try ClientHandshake(
            pinnedKey: instance.publicKey, callID: "hs", credential: "mtun_e2e.x", identity: identity)
        // The instance answers with another PSK: message 2 does not open.
        var responder = try NoiseHandshake(
            .ikpsk2, initiator: false, prologue: peerPrologue(digest: credentialDigest("mtun_e2e.x"), instanceID: "i-1"),
            s: privateKey(base64URL: instance.privateKey), psk: Data(repeating: 8, count: 32))
        _ = try responder.readMessage(XCTUnwrap(base64URLDecode(XCTUnwrap(handshake.frame.payload["message"]?.stringValue))))
        let answer = Frame("call.result", ["call_id": "hs", "payload": ["message": .string(base64URLEncode(try responder.writeMessage()))]])
        XCTAssertThrowsError(try handshake.finish(answer)) {
            XCTAssertEqual(($0 as? MeshCallError)?.reason, "e2e_handshake_failed")
            XCTAssertEqual(($0 as? MeshCallError)?.source, "client")
        }
        // The instance's own refusal keeps its message and details.
        let refused = Frame(
            "call.error",
            [
                "call_id": "hs",
                "error": [
                    "code": "unauthenticated", "message": "End-to-end handshake failed", "retryable": false,
                    "details": ["source": "server", "reason": "e2e_handshake_failed"],
                ],
            ])
        XCTAssertThrowsError(try ClientHandshake.finishConfirm(refused)) {
            XCTAssertEqual(($0 as? MeshCallError)?.code, .unauthenticated)
            XCTAssertEqual(($0 as? MeshCallError)?.reason, "e2e_handshake_failed")
            XCTAssertEqual(($0 as? MeshCallError)?.source, "server")
        }
    }

    func testIdentityMisconfigurationFailsBeforeConnecting() throws {
        let instanceKey = E2EKeyPair.generate().publicKey
        let client = E2EKeyPair.generate().privateKey
        let psk = base64URLEncode(Data(repeating: 1, count: 32))
        func options(_ identity: E2EIdentity, key: String? = instanceKey) -> MeshOptions {
            MeshOptions(credentials: .bearer("mtun_e2e.abc"), e2eKey: key, e2eIdentity: identity)
        }
        XCTAssertNoThrow(try options(E2EIdentity(privateKey: client, instanceID: "i")).validate())
        XCTAssertNoThrow(
            try options(E2EIdentity(privateKey: client, instanceID: "i", pairing: E2EPairing(id: "p", psk: psk))).validate())
        for bad in [
            E2EIdentity(privateKey: "short", instanceID: "i"),
            E2EIdentity(privateKey: client, instanceID: ""),
            E2EIdentity(privateKey: client, instanceID: "i", pairing: E2EPairing(id: "", psk: psk)),
            E2EIdentity(privateKey: client, instanceID: "i", pairing: E2EPairing(id: String(repeating: "x", count: 129), psk: psk)),
            E2EIdentity(privateKey: client, instanceID: "i", pairing: E2EPairing(id: "p", psk: "short")),
        ] {
            XCTAssertThrowsError(try options(bad).validate()) { XCTAssertTrue($0 is ConfigurationError) }
        }
        // An identity authenticates towards a pinned key: never without one (no NK fallback).
        XCTAssertThrowsError(
            try MeshOptions(credentials: .bearer("mtun_abc"), e2eIdentity: E2EIdentity(privateKey: client, instanceID: "i"))
                .validate())
        XCTAssertThrowsError(
            try Mesh(
                tunnel: TunnelGrant(routerURL: "ws://127.0.0.1:1", credential: "mtun_plain", instanceID: "i"),
                identity: E2EIdentity(privateKey: client, instanceID: "i"))
        ) { XCTAssertTrue($0 is ConfigurationError) }
        // Secrets stay out of descriptions.
        let identity = E2EIdentity(privateKey: client, instanceID: "i", pairing: E2EPairing(id: "p", psk: psk))
        XCTAssertFalse("\(identity)".contains(client))
        XCTAssertFalse(String(reflecting: identity).contains(psk))
        XCTAssertTrue("\(identity)".contains("E2EPairing(id: p)"))
    }

    private func sessionPair() throws -> (client: E2ESession, instance: E2ESession) {
        let pair = E2EKeyPair.generate()
        let instanceKey = try privateKey(base64URL: pair.privateKey)
        let prologue = Data(e2ePrologue.utf8)
        var initiator = try NoiseHandshake(.nk, initiator: true, prologue: prologue, rs: instanceKey.publicKey)
        var responder = try NoiseHandshake(.nk, initiator: false, prologue: prologue, s: instanceKey)
        _ = try responder.readMessage(try initiator.writeMessage())
        _ = try initiator.readMessage(try responder.writeMessage())
        return (initiator.split(), responder.split())
    }

    func testSealedFramesRoundTripAndBindTheirCall() throws {
        let (client, instance) = try sessionPair()
        let clientCall = SealedCall(session: client, service: "chat.v1", method: "send")
        let instanceCall = SealedCall(session: instance, service: "chat.v1", method: "send")

        let open = Frame(
            "call.open",
            [
                "call_id": "c1", "service": "chat.v1", "method": "send", "payload": ["text": "hi"],
                "metadata": ["trace": "1"],
            ])
        let sealed = try clientCall.seal(open)
        XCTAssertEqual(sealed["payload"], .null)
        XCTAssertNil(sealed["metadata"])
        XCTAssertEqual(sealed["e2e_session"]?.stringValue, client.id)
        let opened = try instanceCall.open(sealed)
        XCTAssertEqual(opened.payload, ["text": "hi"])
        XCTAssertEqual(opened.metadata, ["trace": "1"])

        // Replayed: refused.
        XCTAssertThrowsError(try instanceCall.open(sealed))
        // Moved to another method: refused.
        let other = SealedCall(session: instance, service: "chat.v1", method: "delete")
        XCTAssertThrowsError(try other.open(try clientCall.seal(open)))

        // An error's message and details are sealed; its code stays clear.
        let error = Frame(
            "call.error",
            [
                "call_id": "c1",
                "error": ["code": "app.v1.nope", "message": "secret", "retryable": false, "details": ["x": 1]],
            ])
        let sealedError = try instanceCall.seal(error)
        XCTAssertEqual(sealedError["error"]?["message"], "sealed")
        XCTAssertNil(sealedError["error"]?["details"])
        XCTAssertEqual(sealedError["error"]?["code"], "app.v1.nope")
        let openedError = try clientCall.open(sealedError)
        XCTAssertEqual(openedError["error"]?["message"], "secret")
        XCTAssertEqual(openedError["error"]?["details"], ["x": 1])

        // A content frame in clear is not from the other end; a clear error (the Router's) is kept.
        XCTAssertThrowsError(try clientCall.open(Frame("call.result", ["call_id": "c1", "payload": 1])))
        XCTAssertNoThrow(try clientCall.open(error))
    }

    func testReplayWindowAcceptsReorderingOnce() throws {
        let (client, instance) = try sessionPair()
        let ad = Data("ad".utf8)
        let sealed = try (0..<1100).map { try client.seal(Data("\($0)".utf8), ad: ad) }
        XCTAssertNoThrow(try instance.open(sealed[5], ad: ad))
        XCTAssertNoThrow(try instance.open(sealed[3], ad: ad))  // reordered: fine
        XCTAssertThrowsError(try instance.open(sealed[3], ad: ad))  // replayed
        XCTAssertNoThrow(try instance.open(sealed[1099], ad: ad))
        XCTAssertThrowsError(try instance.open(sealed[4], ad: ad))  // older than the window
        XCTAssertNoThrow(try instance.open(sealed[1000], ad: ad))
        XCTAssertThrowsError(try instance.open(sealed[1000], ad: ad))
    }

    func testCredentialAndKeyMustAgree() {
        let key = E2EKeyPair.generate().publicKey
        XCTAssertThrowsError(try MeshOptions(credentials: .bearer("mtun_e2e.abc")).validate())
        XCTAssertThrowsError(try MeshOptions(credentials: .bearer("mtun_abc"), e2eKey: key).validate())
        XCTAssertNoThrow(try MeshOptions(credentials: .bearer("mtun_e2e.abc"), e2eKey: key).validate())
        XCTAssertThrowsError(try MeshOptions(credentials: .bearer("mtun_e2e.abc"), e2eKey: "short").validate())
    }

    func testPublicKeyOfPrivateKey() throws {
        let pair = E2EKeyPair.generate()
        XCTAssertEqual(try E2EKeyPair.publicKey(of: pair.privateKey), pair.publicKey)
        XCTAssertEqual(credentialDigest("mtun_x").count, 64)
    }
}
