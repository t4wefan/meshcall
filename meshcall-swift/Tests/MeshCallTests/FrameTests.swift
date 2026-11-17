import Foundation
import XCTest

@testable import MeshCall

final class FrameTests: XCTestCase {
    private func decode(_ text: String) throws -> Frame { try Frame.decode(Data(text.utf8)) }

    func testUnknownFieldsAreKeptAndUnknownKindsRejected() throws {
        let frame = try decode(#"{"kind":"call.result","call_id":"a","payload":{"x":1},"future":true}"#)
        XCTAssertEqual(frame.payload, ["x": 1])
        XCTAssertEqual(frame["future"], true)
        XCTAssertThrowsError(try decode(#"{"kind":"call.teleport","call_id":"a"}"#)) { error in
            XCTAssertNil((error as? FrameDecodeError)?.callID)
        }
    }

    func testAnInvalidFrameNamesItsCall() {
        XCTAssertThrowsError(try decode(#"{"kind":"stream.window","call_id":"a","direction":"server","credit":0}"#)) {
            XCTAssertEqual(($0 as? FrameDecodeError)?.callID, "a")
        }
        XCTAssertThrowsError(
            try decode(#"{"kind":"stream.item","call_id":"a","direction":"up","sequence":0,"payload":1}"#))
        XCTAssertThrowsError(try decode(#"{"kind":"call.error","call_id":"a","error":{"code":"x"}}"#))
        XCTAssertThrowsError(try decode("[1]"))
        XCTAssertNoThrow(try decode(#"{"kind":"call.result","call_id":"a","payload":null,"metadata":null}"#))
    }

    func testEncodingRoundTrips() throws {
        let frame = Frame("call.open", ["call_id": "a", "service": "s.v1", "method": "m", "payload": ["t": "héllo 😀"]])
        let again = try Frame.decode(Data(try frame.encoded().utf8))
        XCTAssertEqual(again.kind, "call.open")
        XCTAssertEqual(again.payload, ["t": "héllo 😀"])
    }

    func testCloseStatusesMapToErrors() {
        XCTAssertEqual(closeError(code: 4401, reason: "tunnel_revoked")?.reason, "tunnel_revoked")
        XCTAssertEqual(closeError(code: 4401, reason: "who knows")?.code, .unauthenticated)
        XCTAssertNil(closeError(code: 4401, reason: "who knows")?.reason)
        XCTAssertEqual(closeError(code: 1008, reason: "no")?.reason, "policy_violation")
        XCTAssertEqual(closeError(code: 4429, reason: "full")?.retryable, true)
        XCTAssertEqual(closeError(code: 4503, reason: "")?.code, .unavailable)
        XCTAssertNil(closeError(code: 1001, reason: "going away"))
    }

    func testMetadataRules() {
        XCTAssertThrowsError(try CallOptions(metadata: ["Meshcall-Tunnel-Id": "x"]).validate()) {
            XCTAssertEqual(($0 as? MeshCallError)?.reason, "reserved_metadata")
        }
        XCTAssertThrowsError(try CallOptions(metadata: ["": "x"]).validate()) {
            XCTAssertEqual(($0 as? MeshCallError)?.reason, "invalid_metadata")
        }
        XCTAssertThrowsError(try CallOptions(idempotencyKey: String(repeating: "k", count: 129)).validate())
    }

    func testScopeServicesGetAVersion() {
        XCTAssertEqual(normalizeScopeService("chat"), "chat.v0")
        XCTAssertEqual(normalizeScopeService("aa.v3.Sessions"), "aa.v3.Sessions")
        XCTAssertEqual(normalizeScopeService("*"), "*")
    }

    func testTunnelGrantDecodesTheControlPlaneAnswer() throws {
        let json =
            #"{"router_url":"wss://r.example","credential":"mtun_e2e.x","instance_id":"dev-1","tunnel_id":"t1","e2e_key":"k"}"#
        let grant = try JSONDecoder().decode(TunnelGrant.self, from: Data(json.utf8))
        XCTAssertEqual(grant.instanceID, "dev-1")
        XCTAssertEqual(grant.e2eKey, "k")
    }

    func testRouterURLsMustBeWebSockets() {
        XCTAssertThrowsError(try Router("http://127.0.0.1:8765"))
        XCTAssertNoThrow(try Router("wss://router.example"))
    }
}
