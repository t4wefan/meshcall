import Foundation
import XCTest

@testable import MeshCall

/// Records what is sent; the first send waits until `release()` so frames
/// queue up behind it.
private final class GatedSocket: WebSocketConnection, @unchecked Sendable {
    private let lock = NSLock()
    private var sent: [String] = []
    private var gate: CheckedContinuation<Void, Never>?
    private var gated = true
    private var released = false

    func open() async throws {}

    func send(_ text: String) async throws {
        let wait = lock.withLock { () -> Bool in
            defer { gated = false }
            return gated && !released
        }
        if wait {
            await withCheckedContinuation { continuation in
                let resume = lock.withLock { () -> Bool in
                    if released { return true }
                    gate = continuation
                    return false
                }
                if resume { continuation.resume() }
            }
        }
        lock.withLock { sent.append(text) }
    }

    func receive() async throws -> Data { try await Task.sleep(for: .seconds(60)); return Data() }
    func close(code: Int, reason: String) {}

    func release() {
        let gate = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            released = true
            defer { self.gate = nil }
            return self.gate
        }
        gate?.resume()
    }

    var callIDs: [String] {
        lock.withLock { sent }.compactMap { text in
            (try? JSONValue(json: Data(text.utf8)))?["call_id"]?.stringValue.map { id in
                id + ":" + ((try? JSONValue(json: Data(text.utf8)))?["payload"]?["n"]?.intValue.map(String.init) ?? "")
            }
        }
    }
}

final class WriterTests: XCTestCase {
    private func item(_ call: String, _ n: Int) -> Frame {
        Frame("stream.item", ["call_id": .string(call), "direction": "client", "payload": ["n": .int(Int64(n))]])
    }

    private func order(ordered: Bool) async throws -> [String] {
        let socket = GatedSocket()
        let writer = FrameWriter(socket: socket, maxFrameSize: defaultMaxFrameSize, ordered: ordered)
        var tickets = [try writer.enqueue(item("a", 1))]
        // a:1 is now waiting in the socket; the rest queue behind it.
        try await Task.sleep(for: .milliseconds(50))
        for frame in [item("a", 2), item("a", 3), item("b", 1)] { tickets.append(try writer.enqueue(frame)) }
        socket.release()
        for ticket in tickets { try await ticket.wait() }
        return socket.callIDs
    }

    func testCallsTakeTurns() async throws {
        let sent = try await order(ordered: false)
        XCTAssertEqual(sent, ["a:1", "a:2", "b:1", "a:3"])
    }

    /// End to end, each frame was sealed with the next counter when queued:
    /// they must leave in that order or the peer's replay window refuses them.
    func testSealedCallFramesLeaveInTheOrderTheyWereQueued() async throws {
        let sent = try await order(ordered: true)
        XCTAssertEqual(sent, ["a:1", "a:2", "a:3", "b:1"])
    }
}
