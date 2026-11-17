#if os(macOS)
    import Foundation
    import XCTest

    @testable import MeshCall

    // The client half of the conformance suite's scenarios (scenarios.go,
    // tunnel.go, e2e.go), played by this runtime against the Go fixture.

    struct Echo: Codable, Sendable, Equatable {
        var value: Int
        var text: String
    }
    struct WrongEcho: Codable, Sendable {
        var value: String
        var text: String
    }
    struct Fail: Codable, Sendable {
        var code: String
        var message: String
        var retryable = false
    }
    struct Slow: Codable, Sendable {
        var ms: Int
        var tag = ""
    }
    struct Tag: Codable, Sendable { var tag: String }
    struct Tags: Codable, Sendable { var tags: [String] }
    struct Count: Codable, Sendable {
        var n: Int
        var gap_ms = 0
    }
    struct Index: Codable, Sendable { var index: Int }
    struct SumRequest: Codable, Sendable { var offset = 0 }
    struct Value: Codable, Sendable { var value: Int }
    struct Total: Codable, Sendable { var total: Int }
    struct MetadataEcho: Codable, Sendable { var metadata: [String: String] }
    struct Text: Codable, Sendable, Equatable { var text: String }
    struct ChatRequest: Codable, Sendable { var prefix = "" }
    struct Ok: Codable, Sendable { var ok: Bool }
    struct Big: Codable, Sendable { var bytes: Int }
    struct TunnelSeen: Codable, Sendable {
        var tunnel_id: String?
        var principal: String?
        var sealed: Bool
    }
    struct WhoAmI: Codable, Sendable { var instance: String }
    struct Peek: Codable, Sendable { var sealed: Bool }

    let fixture = LiveMesh.service
    let targeted = "conformance.v1.Targeted"
    let vault = "conformance.v1.Vault"

    /// One Router and fixture per test class.
    class LiveTestCase: XCTestCase {
        nonisolated(unsafe) private static var shared: [ObjectIdentifier: LiveMesh] = [:]
        private static let lock = NSLock()
        var live: LiveMesh!
        var mesh: Mesh!

        override func setUp() async throws {
            let key = ObjectIdentifier(Self.self)
            let existing = Self.lock.withLock { Self.shared[key] }
            if let existing {
                live = existing
            } else {
                live = try LiveMesh()
                Self.lock.withLock { Self.shared[key] = live }
            }
            mesh = try live.mesh()
        }

        override func tearDown() async throws {
            await mesh?.close()
        }

        override class func tearDown() {
            lock.withLock { shared.removeValue(forKey: ObjectIdentifier(self)) }?.stop()
            super.tearDown()
        }
    }

    final class CallTests: LiveTestCase {
        // conformance-scenario: unary.round_trip
        func testUnaryRoundTrip() async throws {
            let echo = Echo(value: 7, text: "héllo 😀")
            let result = try await mesh.unary(fixture, "echo", echo, as: Echo.self)
            XCTAssertEqual(result, echo)
        }

        // conformance-scenario: unary.invalid_request
        func testAnInvalidRequestFailsOnlyThatCall() async throws {
            let error = await meshError {
                _ = try await self.mesh.unary(fixture, "echo", WrongEcho(value: "x", text: "y"), as: Echo.self)
            }
            XCTAssertEqual(error?.code, .invalidArgument)
            XCTAssertEqual(error?.source, "server")
            XCTAssertEqual(error?.reason, "request")
            XCTAssertEqual(error?.violations.first?.path, "value")
            let again = try await mesh.unary(fixture, "echo", Echo(value: 1, text: "still here"), as: Echo.self)
            XCTAssertEqual(again.value, 1)
        }

        // conformance-scenario: unary.invalid_response
        func testAnInvalidResponseIsTheServersFault() async throws {
            let error = await meshError { _ = try await self.mesh.unary(fixture, "bad_response", Empty(), as: Ok.self) }
            XCTAssertEqual(error?.code, .internal)
            XCTAssertEqual(error?.reason, "invalid_response")
        }

        // conformance-scenario: unary.undecodable_response
        func testAResponseThisClientCannotDecodeIsAProtocolError() async throws {
            let error = await meshError {
                _ = try await self.mesh.unary(fixture, "echo", Echo(value: 1, text: "x"), as: WrongEcho.self)
            }
            XCTAssertEqual(error?.code, .protocolError)
            XCTAssertEqual(error?.source, "client")
            XCTAssertEqual(error?.reason, "invalid_response")
            XCTAssertEqual(error?.violations.first?.path, "value")
        }

        // conformance-scenario: unary.application_error
        func testApplicationErrorsPassThrough() async throws {
            let error = await meshError {
                _ = try await self.mesh.unary(
                    fixture, "fail", Fail(code: "app.v1.quota_exceeded", message: "no more", retryable: true),
                    as: Empty.self)
            }
            XCTAssertEqual(error?.code, ErrorCode("app.v1.quota_exceeded"))
            XCTAssertEqual(error?.message, "no more")
            XCTAssertEqual(error?.retryable, true)
            XCTAssertEqual(error?.details["from"], "fixture")
            XCTAssertNil(error?.attempts, "a call without an idempotency key is not retried")
        }

        // conformance-scenario: unary.retry
        func testRetryableErrorsAreRetriedWithAnIdempotencyKey() async throws {
            let error = await meshError {
                _ = try await self.mesh.unary(
                    fixture, "fail", Fail(code: "unavailable", message: "busy", retryable: true), as: Empty.self,
                    options: CallOptions(idempotencyKey: newID()))
            }
            XCTAssertEqual(error?.code, .unavailable)
            XCTAssertEqual(error?.attempts, 3)
        }

        // conformance-scenario: targeted.instance
        func testTargetedCalls() async throws {
            let missing = await meshError { _ = try await self.mesh.unary(targeted, "whoami", Empty(), as: WhoAmI.self) }
            XCTAssertEqual(missing?.code, .invalidArgument)
            XCTAssertEqual(missing?.reason, "instance_required")
            let found = try await mesh.unary(
                targeted, "whoami", Empty(), as: WhoAmI.self, options: CallOptions(instance: LiveMesh.instance))
            XCTAssertEqual(found.instance, LiveMesh.instance)
            let unknown = await meshError {
                _ = try await self.mesh.unary(
                    targeted, "whoami", Empty(), as: WhoAmI.self, options: CallOptions(instance: "no-such-instance"))
            }
            XCTAssertEqual(unknown?.code, .unavailable)
            XCTAssertEqual(unknown?.reason, "instance_unknown")
        }

        // conformance-scenario: router.method_not_found
        func testUnknownMethod() async throws {
            let error = await meshError { _ = try await self.mesh.unary(fixture, "nope", Empty(), as: Empty.self) }
            XCTAssertEqual(error?.code, .methodNotFound)
        }

        // conformance-scenario: stream.server_credit
        func testServerStreamBeyondTheInitialCredit() async throws {
            var seen: [Int] = []
            for try await item in try await mesh.serverStream(fixture, "count", Count(n: 40), of: Index.self) {
                seen.append(item.index)
            }
            XCTAssertEqual(seen, Array(0..<40))
        }

        // conformance-scenario: stream.concurrent
        func testConcurrentStreamsKeepTheirOwnCredit() async throws {
            let mesh = self.mesh!
            try await withThrowingTaskGroup(of: Int.self) { group in
                for _ in 0..<4 {
                    group.addTask {
                        var count = 0
                        for try await _ in try await mesh.serverStream(
                            fixture, "count", Count(n: 50), of: Index.self)
                        {
                            count += 1
                        }
                        return count
                    }
                }
                for try await count in group { XCTAssertEqual(count, 50) }
            }
        }

        // conformance-scenario: stream.early_exit
        func testLeavingAStreamEarlyCancelsIt() async throws {
            var seen = 0
            for try await _ in try await mesh.serverStream(fixture, "count", Count(n: 1000, gap_ms: 5), of: Index.self) {
                seen += 1
                if seen == 3 { break }
            }
            XCTAssertEqual(seen, 3)
            // The connection is still good.
            let echo = try await mesh.unary(fixture, "echo", Echo(value: 2, text: "after"), as: Echo.self)
            XCTAssertEqual(echo.value, 2)
        }

        // conformance-scenario: stream.client_stream
        func testClientStream() async throws {
            let values = (1...40).map { Value(value: $0) }
            let total = try await mesh.clientStream(
                fixture, "sum", SumRequest(offset: 10), items: items(values), as: Total.self)
            XCTAssertEqual(total.total, 10 + 820)
        }

        // conformance-scenario: deadline.server
        func testTheServerEnforcesTheTimeout() async throws {
            let error = await meshError {
                _ = try await self.mesh.unary(
                    fixture, "slow", Slow(ms: 2000, tag: "timeout"), as: Tag.self,
                    options: CallOptions(timeout: .milliseconds(100)))
            }
            XCTAssertEqual(error?.code, .deadlineExceeded)
            XCTAssertEqual(error?.retryable, false)
        }

        // conformance-scenario: deadline.idle
        func testTheClientEnforcesTheOutputIdleTimeout() async throws {
            let error = await meshError {
                for try await _ in try await self.mesh.serverStream(
                    fixture, "count", Count(n: 3, gap_ms: 800), of: Index.self,
                    options: CallOptions(idleTimeout: .milliseconds(200)))
                {}
            }
            XCTAssertEqual(error?.code, .deadlineExceeded)
            XCTAssertEqual(error?.source, "client")
            XCTAssertEqual(error?.reason, "idle")
        }

        // conformance-scenario: cancel.caller
        func testCancellingTheTaskCancelsTheCall() async throws {
            let tag = "cancel-" + newID()
            let mesh = self.mesh!
            let call = Task { try await mesh.unary(fixture, "slow", Slow(ms: 5000, tag: tag), as: Tag.self) }
            try await Task.sleep(for: .milliseconds(300))
            call.cancel()
            let error = await meshError { _ = try await call.value }
            XCTAssertEqual(error?.code, .cancelled)
            for _ in 0..<50 {
                let cancelled = try await mesh.unary(fixture, "cancelled_tags", Empty(), as: Tags.self)
                if cancelled.tags.contains(tag) { return }
                try await Task.sleep(for: .milliseconds(50))
            }
            XCTFail("the handler never saw the cancel")
        }

        // conformance-scenario: metadata.round_trip
        func testMetadataAndTrailers() async throws {
            let seen = TrailerLog()
            let mesh = try live.mesh(
                MeshOptions(interceptors: [
                    { info, next in
                        try await next(info)
                        seen.record(info.trailers)
                    }
                ]))
            defer { Task { await mesh.close() } }
            let echoed = try await mesh.unary(
                fixture, "metadata_echo", Empty(), as: MetadataEcho.self,
                options: CallOptions(metadata: ["a": "1", "traceparent": "00-abc"]))
            XCTAssertEqual(echoed.metadata["a"], "1")
            XCTAssertEqual(echoed.metadata["traceparent"], "00-abc")
            XCTAssertEqual(seen.last?["echoed"], "yes")
            XCTAssertEqual(seen.last?["count"], "2")
        }

        // conformance-scenario: duplex.interleaved
        func testDuplexInterleavesAndHalfCloses() async throws {
            let chat = try await mesh.duplex(fixture, "chat", ChatRequest(prefix: "~"), input: Text.self, output: Text.self)
            var output = chat.makeAsyncIterator()
            try await chat.send(Text(text: "a"))
            let first = try await output.next()
            XCTAssertEqual(first?.text, "~a")
            try await chat.send(Text(text: "b"))
            let second = try await output.next()
            XCTAssertEqual(second?.text, "~b")
            try await chat.closeSend()
            let done = try await output.next()
            XCTAssertEqual(done?.text, "done:2")
            let end = try await output.next()
            XCTAssertNil(end)
        }

        // conformance-scenario: duplex.server_half_close
        func testDuplexOutputEndsWhileInputStaysOpen() async throws {
            let call = try await mesh.duplex(fixture, "announce", Empty(), input: Text.self, output: Text.self)
            var output = call.makeAsyncIterator()
            let hello = try await output.next()
            XCTAssertEqual(hello?.text, "hello")
            try await call.send(Text(text: "still listening"))
            try await call.closeSend()
            let end = try await output.next()
            XCTAssertNil(end)
        }

        // conformance-scenario: frame_size.request
        func testAnOversizedRequestFailsOnlyItsCall() async throws {
            let mesh = try live.mesh(MeshOptions(maxFrameSize: 4096))
            defer { Task { await mesh.close() } }
            let error = await meshError {
                _ = try await mesh.unary(
                    fixture, "echo", Echo(value: 1, text: String(repeating: "x", count: 8192)), as: Echo.self)
            }
            XCTAssertEqual(error?.code, .resourceExhausted)
            XCTAssertEqual(error?.reason, "frame_too_large")
            XCTAssertEqual(error?.source, "client")
            let small = try await mesh.unary(fixture, "echo", Echo(value: 3, text: "small"), as: Echo.self)
            XCTAssertEqual(small.value, 3)
        }

        // conformance-scenario: metadata.reserved
        func testReservedMetadataIsRefusedBeforeSending() async throws {
            let error = await meshError {
                _ = try await self.mesh.unary(
                    fixture, "echo", Echo(value: 1, text: "x"), as: Echo.self,
                    options: CallOptions(metadata: ["meshcall-tunnel-id": "forged"]))
            }
            XCTAssertEqual(error?.reason, "reserved_metadata")
        }

        func testRawJSONPassesThroughUnchanged() async throws {
            // An app that forwards events as they are (to a WebView) never decodes them.
            let raw: JSONValue = ["value": 9, "text": "raw", "ignored": ["nested": true]]
            let result = try await mesh.unary(fixture, "echo", raw, as: JSONValue.self)
            XCTAssertEqual(result, ["value": 9, "text": "raw"])
        }
    }

    final class TrailerLog: @unchecked Sendable {
        private let lock = NSLock()
        private var entries: [[String: String]] = []
        func record(_ trailers: [String: String]) { lock.withLock { entries.append(trailers) } }
        var last: [String: String]? { lock.withLock { entries.last } }
    }
#endif
