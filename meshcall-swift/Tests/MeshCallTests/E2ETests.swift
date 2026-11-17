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
    /// The cacophony test vector the Go and TypeScript runtimes check too.
    func testNoiseNKMatchesTheTestVector() throws {
        struct Vector: Decodable {
            struct Message: Decodable {
                let payload: String
                let ciphertext: String
            }
            let init_prologue: String
            let init_ephemeral: String
            let init_remote_static: String
            let resp_static: String
            let resp_ephemeral: String
            let handshake_hash: String
            let messages: [Message]
        }
        let path = repositoryRoot.appendingPathComponent(
            "meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json")
        let v = try JSONDecoder().decode(Vector.self, from: Data(contentsOf: path))
        let key = { (text: String) in try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: unhex(text)) }
        let prologue = unhex(v.init_prologue)

        let (initiator, message1) = try nkInitiate(
            remoteStatic: unhex(v.init_remote_static), prologue: prologue, ephemeral: key(v.init_ephemeral),
            payload: unhex(v.messages[0].payload))
        XCTAssertEqual(hex(message1), v.messages[0].ciphertext)
        let responded = try nkRespond(
            staticPrivate: key(v.resp_static), prologue: prologue, message: message1, ephemeral: key(v.resp_ephemeral),
            payload: unhex(v.messages[1].payload))
        XCTAssertEqual(hex(responded.reply), v.messages[1].ciphertext)
        XCTAssertEqual(hex(responded.received), v.messages[0].payload)
        let (payload2, client) = try initiator.finish(responded.reply)
        XCTAssertEqual(hex(payload2), v.messages[1].payload)
        XCTAssertEqual(hex(client.hash), v.handshake_hash)
        XCTAssertEqual(client.id, responded.session.id)

        // Transport messages alternate initiator, responder, counting from 0 per direction.
        var counters: [UInt64] = [0, 0]
        for (index, message) in v.messages.dropFirst(2).enumerated() {
            let from = [client, responded.session][index % 2]
            let sealed = try aeadEncrypt(from.sendKey, counters[index % 2], unhex(message.payload), Data())
            counters[index % 2] += 1
            XCTAssertEqual(hex(sealed), message.ciphertext, "transport message \(index)")
        }
    }

    private func sessionPair() throws -> (client: E2ESession, instance: E2ESession) {
        let pair = E2EKeyPair.generate()
        let instanceKey = try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: base64URLDecode(pair.privateKey)!)
        let prologue = Data(e2ePrologue.utf8)
        let (initiator, message1) = try nkInitiate(remoteStatic: base64URLDecode(pair.publicKey)!, prologue: prologue)
        let responded = try nkRespond(staticPrivate: instanceKey, prologue: prologue, message: message1)
        return (try initiator.finish(responded.reply).session, responded.session)
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
