#if os(macOS)
    import Foundation
    import XCTest

    @testable import MeshCall

    /// A socket whose upgrade fails, as a Router refusing it would.
    private final class FailingSocket: WebSocketConnection, @unchecked Sendable {
        let error: MeshCallError
        init(_ error: MeshCallError) { self.error = error }
        func open() async throws { throw error }
        func send(_ text: String) async throws { throw error }
        func receive() async throws -> Data { throw error }
        func close(code: Int, reason: String) {}
    }

    private final class Counter: @unchecked Sendable {
        private let lock = NSLock()
        private var value = 0
        func increment() { lock.withLock { value += 1 } }
        var count: Int { lock.withLock { value } }
    }

    private func realSocket(_ url: URL, _ options: MeshOptions) -> WebSocketConnection {
        URLSessionWebSocket(url: url, maxFrameSize: options.maxFrameSize, handshakeTimeout: options.handshakeTimeout)
    }

    private struct DescribeRequest: Codable, Sendable { var service: String }
    private struct Described: Codable, Sendable {
        struct Instance: Codable, Sendable { var id: String; var inflight: Int }
        var instances: [Instance]
    }

    extension LiveTestCase {
        /// The fixture instance's calls in flight, as the Router counts them.
        func inflight() async throws -> Int {
            let described = try await mesh.unary(
                "meshcall.router.v1.Registry", "describe", DescribeRequest(service: fixture), as: Described.self)
            return described.instances.first { $0.id == LiveMesh.instance }?.inflight ?? -1
        }

        /// Waits until the Router has seen every call to the fixture end (a
        /// cancel reached it), failing after two seconds.
        func waitForNoCallsInFlight(file: StaticString = #filePath, line: UInt = #line) async throws {
            for _ in 0..<100 {
                if try await inflight() == 0 { return }
                try await Task.sleep(for: .milliseconds(20))
            }
            XCTFail("the fixture still has calls in flight: \(try await inflight())", file: file, line: line)
        }
    }

    /// How the client manages its connection, its calls and its lifetime.
    final class LifecycleTests: LiveTestCase {
        /// A refusal from one Router does not end the pass: the credentials
        /// may be good on the next (as TypeScript and Go).
        func testARefusingEndpointDoesNotStopThePass() async throws {
            let refusing = URL(string: "ws://refusing.invalid")!
            let core = try ClientCore(endpoints: [refusing, URL(string: live.routerURL)!], options: MeshOptions()) {
                url, options in
                url == refusing
                    ? FailingSocket(MeshCallError(.unauthenticated, "refused", details: ["source": "router"]))
                    : realSocket(url, options)
            }
            let both = Mesh(core: core)
            defer { Task { await both.close() } }
            let echo = try await both.unary(fixture, "echo", Echo(value: 4, text: "second"), as: Echo.self)
            XCTAssertEqual(echo.value, 4)

            // Refused everywhere: the refusal, not a retry loop.
            let onlyRefusing = Mesh(
                core: try ClientCore(endpoints: [refusing], options: MeshOptions()) { _, _ in
                    FailingSocket(MeshCallError(.unauthenticated, "refused", details: ["source": "router"]))
                })
            let refused = await meshError { _ = try await onlyRefusing.unary(fixture, "echo", Echo(value: 1, text: ""), as: Echo.self) }
            XCTAssertEqual(refused?.code, .unauthenticated)
        }

        func testAnInterceptorThatSkipsNextFailsTheCall() async throws {
            let skipping = try live.mesh(MeshOptions(interceptors: [{ _, _ in }]))
            defer { Task { await skipping.close() } }
            let unary = await meshError { _ = try await skipping.unary(fixture, "echo", Echo(value: 1, text: ""), as: Echo.self) }
            XCTAssertEqual(unary?.code, .internal)
            XCTAssertEqual(unary?.reason, "interceptor_skipped_next")
            let stream = await meshError { _ = try await skipping.serverStream(fixture, "count", Count(n: 1), of: Index.self) }
            XCTAssertEqual(stream?.reason, "interceptor_skipped_next")
        }

        func testMetadataAnInterceptorAddsIsCheckedToo() async throws {
            let reserved = try live.mesh(
                MeshOptions(interceptors: [
                    { info, next in
                        info.options.metadata["meshcall-forged"] = "1"
                        try await next(info)
                    }
                ]))
            defer { Task { await reserved.close() } }
            let error = await meshError { _ = try await reserved.unary(fixture, "echo", Echo(value: 1, text: ""), as: Echo.self) }
            XCTAssertEqual(error?.code, .invalidArgument)
            XCTAssertEqual(error?.reason, "reserved_metadata")
            XCTAssertEqual(error?.source, "client")
        }

        /// Leaving the loop cancels the call even while the stream object is
        /// kept elsewhere, and the cancel reaches the server.
        func testLeavingTheLoopCancelsAStreamThatIsStillHeld() async throws {
            let stream = try await mesh.serverStream(fixture, "count", Count(n: 1000, gap_ms: 20), of: Index.self)
            var seen = 0
            for try await _ in stream {
                seen += 1
                if seen == 2 { break }
            }
            XCTAssertEqual(seen, 2)
            try await waitForNoCallsInFlight()
            withExtendedLifetime(stream) {}
        }

        /// A Mesh released while it reconnects stops trying.
        func testAReleasedMeshStopsReconnecting() async throws {
            let opened = Counter()
            let router = URL(string: live.routerURL)!
            let fast = ReconnectPolicy(initialDelay: .milliseconds(20), maxDelay: .milliseconds(50))
            var released: Mesh? = Mesh(
                core: try ClientCore(endpoints: [router], options: MeshOptions(reconnect: fast)) { url, options in
                    opened.increment()
                    // The first connection is real; after it is lost, the Router is unreachable.
                    return opened.count == 1
                        ? realSocket(url, options) : FailingSocket(MeshCallError(.unavailable, "unreachable"))
                })
            _ = try await released!.unary(fixture, "echo", Echo(value: 1, text: ""), as: Echo.self)
            try live.restartRouter()
            try await Task.sleep(for: .milliseconds(200))
            // This call starts the reconnect loop and gives up on it; the loop goes on.
            _ = await meshError {
                _ = try await released!.unary(
                    fixture, "echo", Echo(value: 2, text: ""), as: Echo.self, options: CallOptions(timeout: .milliseconds(200)))
            }
            XCTAssertGreaterThan(opened.count, 2)
            released = nil
            try await Task.sleep(for: .milliseconds(200))
            let after = opened.count
            try await Task.sleep(for: .milliseconds(500))
            XCTAssertEqual(opened.count, after, "still reconnecting after the Mesh was released")
        }
    }
#endif
