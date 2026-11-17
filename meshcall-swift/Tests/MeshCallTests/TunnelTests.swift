#if os(macOS)
    import Foundation
    import XCTest

    @testable import MeshCall

    // The client half of conformance/tunnel.go and conformance/e2e.go.
    final class TunnelTests: LiveTestCase {
        private func open(
            _ services: [String] = [fixture], principal: String = "user:alice/device:phone", e2e: Bool = false
        ) async throws -> TunnelGrant {
            let opened = try await mesh.tunnels.open(
                OpenTunnelRequest(
                    instance: LiveMesh.instance, scopes: services.map { TunnelScope(service: $0) },
                    principal: principal, ttlSeconds: 600, e2eKey: e2e ? live.e2eKey.publicKey : nil))
            return opened.grant(routerURL: live.routerURL)
        }

        func testATunnelReachesOnlyItsInstanceAndCarriesItsIdentity() async throws {
            let grant = try await open()
            XCTAssertTrue(grant.credential.hasPrefix("mtun_"))
            XCTAssertNil(grant.e2eKey)
            let tunnel = try Mesh(tunnel: grant)
            defer { Task { await tunnel.close() } }

            let echo = try await tunnel.unary(fixture, "echo", Echo(value: 5, text: "through"), as: Echo.self)
            XCTAssertEqual(echo.value, 5)
            let seen = try await tunnel.unary(fixture, "tunnel_info", Empty(), as: TunnelSeen.self)
            XCTAssertEqual(seen.tunnel_id, grant.tunnelID)
            XCTAssertEqual(seen.principal, "user:alice/device:phone")
            XCTAssertFalse(seen.sealed)

            var count = 0
            for try await _ in try await tunnel.serverStream(fixture, "count", Count(n: 20), of: Index.self) { count += 1 }
            XCTAssertEqual(count, 20)

            let outside = await meshError {
                _ = try await tunnel.unary(targeted, "whoami", Empty(), as: WhoAmI.self)
            }
            XCTAssertEqual(outside?.code, .permissionDenied)
            XCTAssertEqual(outside?.reason, "tunnel_scope")

            let identity = try await tunnel.auth.whoami()
            XCTAssertEqual(identity.tunnel?.tunnelID, grant.tunnelID)
        }

        func testEveryShapeIsSealedEndToEnd() async throws {
            let grant = try await open([fixture, vault], e2e: true)
            XCTAssertTrue(grant.credential.hasPrefix("mtun_e2e."))
            XCTAssertEqual(grant.e2eKey, live.e2eKey.publicKey)
            let tunnel = try Mesh(tunnel: grant)
            defer { Task { await tunnel.close() } }

            let echo = try await tunnel.unary(fixture, "echo", Echo(value: 11, text: "sealed 🔒"), as: Echo.self)
            XCTAssertEqual(echo, Echo(value: 11, text: "sealed 🔒"))
            let seen = try await tunnel.unary(fixture, "tunnel_info", Empty(), as: TunnelSeen.self)
            XCTAssertTrue(seen.sealed)
            XCTAssertEqual(seen.principal, "user:alice/device:phone")
            let peek = try await tunnel.unary(vault, "peek", Empty(), as: Peek.self)
            XCTAssertTrue(peek.sealed)

            var indices: [Int] = []
            for try await item in try await tunnel.serverStream(fixture, "count", Count(n: 30), of: Index.self) {
                indices.append(item.index)
            }
            XCTAssertEqual(indices, Array(0..<30))

            let total = try await tunnel.clientStream(
                fixture, "sum", SumRequest(offset: 1), items: items((1...20).map { Value(value: $0) }), as: Total.self)
            XCTAssertEqual(total.total, 211)

            let chat = try await tunnel.duplex(fixture, "chat", ChatRequest(prefix: ">"), input: Text.self, output: Text.self)
            var output = chat.makeAsyncIterator()
            try await chat.send(Text(text: "x"))
            let reply = try await output.next()
            XCTAssertEqual(reply?.text, ">x")
            try await chat.closeSend()
            let done = try await output.next()
            XCTAssertEqual(done?.text, "done:1")

            // Sealed error messages and details open again here.
            let failure = await meshError {
                _ = try await tunnel.unary(
                    fixture, "fail", Fail(code: "app.v1.secret", message: "only for you"), as: Empty.self)
            }
            XCTAssertEqual(failure?.code, ErrorCode("app.v1.secret"))
            XCTAssertEqual(failure?.message, "only for you")
            XCTAssertEqual(failure?.details["from"], "fixture")
        }

        func testAWrongPinnedKeyNeverSendsACall() async throws {
            var grant = try await open(e2e: true)
            grant.e2eKey = E2EKeyPair.generate().publicKey
            let tunnel = try Mesh(tunnel: grant)
            defer { Task { await tunnel.close() } }
            let error = await meshError { _ = try await tunnel.unary(fixture, "echo", Echo(value: 1, text: "x"), as: Echo.self) }
            XCTAssertEqual(error?.code, .unauthenticated)
            XCTAssertEqual(error?.reason, "e2e_handshake_failed")
        }

        func testAPlainTunnelRequiredToBeSealedIsRefusedAtOpen() async throws {
            let error = await meshError { _ = try await self.open([vault]) }
            XCTAssertEqual(error?.code, .permissionDenied)
            XCTAssertEqual(error?.reason, "tunnel_rejected")
        }

        func testAnEndToEndCredentialWithoutItsKeyIsRefusedBeforeConnecting() async throws {
            var grant = try await open(e2e: true)
            grant.e2eKey = nil
            XCTAssertThrowsError(try Mesh(tunnel: grant)) { XCTAssertTrue($0 is ConfigurationError) }
        }

        func testAnInstanceThatLostTheSessionGetsANewHandshake() async throws {
            let grant = try await open(e2e: true)
            let tunnel = try Mesh(tunnel: grant)
            defer { Task { await tunnel.close() } }
            _ = try await tunnel.unary(fixture, "echo", Echo(value: 1, text: "first"), as: Echo.self)

            try live.restartFixture()
            try await live.waitForFixture()
            // The tunnel resumes; the instance answers e2e_session_unknown, and the
            // client handshakes again on a new connection and retries once.
            let echo = try await tunnel.unary(
                fixture, "echo", Echo(value: 2, text: "second"), as: Echo.self, options: CallOptions(timeout: .seconds(10)))
            XCTAssertEqual(echo.value, 2)
        }

        func testRevokingEndsCallsAndLaterCallsKeepTheAnswer() async throws {
            let grant = try await open(e2e: true)
            let tunnel = try Mesh(tunnel: grant)
            defer { Task { await tunnel.close() } }
            let stream = try await tunnel.serverStream(fixture, "count", Count(n: 1000, gap_ms: 20), of: Index.self)
            var iterator = stream.makeAsyncIterator()
            _ = try await iterator.next()

            let revoked = try await mesh.tunnels.revoke(grant.tunnelID!)
            XCTAssertTrue(revoked)
            let ended = await meshError { while try await iterator.next() != nil {} }
            XCTAssertEqual(ended?.code, .unauthenticated)
            XCTAssertEqual(ended?.reason, "tunnel_revoked")
            XCTAssertEqual(ended?.source, "router")

            let later = await meshError { _ = try await tunnel.unary(fixture, "echo", Echo(value: 1, text: "x"), as: Echo.self) }
            XCTAssertEqual(later?.code, .unauthenticated)
            XCTAssertEqual(later?.reason, "tunnel_revoked")
        }
    }

    // Connection loss, reconnect and the foreground check, against Routers
    // that stop and restart under the client.
    final class ReconnectTests: LiveTestCase {
        func testCallsAfterARouterRestartReconnect() async throws {
            _ = try await mesh.unary(fixture, "echo", Echo(value: 1, text: "before"), as: Echo.self)
            let stream = try await mesh.serverStream(fixture, "count", Count(n: 1000, gap_ms: 20), of: Index.self)
            var iterator = stream.makeAsyncIterator()
            _ = try await iterator.next()

            try live.restartRouter()
            // A lost connection fails the calls on it with a retryable unavailable.
            let lost = await meshError { while try await iterator.next() != nil {} }
            XCTAssertEqual(lost?.code, .unavailable)
            XCTAssertEqual(lost?.retryable, true)

            try await live.waitForFixture()
            // The next call reconnects (with backoff) within its own timeout.
            let echo = try await mesh.unary(
                fixture, "echo", Echo(value: 2, text: "after"), as: Echo.self, options: CallOptions(timeout: .seconds(10)))
            XCTAssertEqual(echo.value, 2)
        }

        func testCheckConnectionDropsAConnectionThatStoppedAnswering() async throws {
            _ = try await mesh.unary(fixture, "echo", Echo(value: 1, text: "x"), as: Echo.self)
            let alive = await mesh.checkConnection(timeout: .seconds(2))
            XCTAssertTrue(alive)

            let stream = try await mesh.serverStream(fixture, "count", Count(n: 1000, gap_ms: 20), of: Index.self)
            var iterator = stream.makeAsyncIterator()
            _ = try await iterator.next()
            // What an app finds after the system suspended it: a socket nobody answers.
            live.pauseRouter()
            let dead = await mesh.checkConnection(timeout: .milliseconds(300))
            live.resumeRouter()
            XCTAssertFalse(dead)
            let lost = await meshError { while try await iterator.next() != nil {} }
            XCTAssertEqual(lost?.code, .unavailable)
            XCTAssertEqual(lost?.retryable, true)

            let echo = try await mesh.unary(
                fixture, "echo", Echo(value: 3, text: "again"), as: Echo.self, options: CallOptions(timeout: .seconds(10)))
            XCTAssertEqual(echo.value, 3)
        }

        func testCheckConnectionWithoutAConnection() async {
            let fresh = try! live.mesh()
            let connected = await fresh.checkConnection()
            XCTAssertFalse(connected)
        }

        func testTheClientDeadlineCoversAHandshakeNobodyAnswers() async throws {
            let fresh = try live.mesh(MeshOptions(retry: .disabled))
            live.pauseRouter()
            defer { live.resumeRouter() }
            let started = ContinuousClock.now
            let error = await meshError {
                _ = try await fresh.unary(
                    fixture, "echo", Echo(value: 1, text: "x"), as: Echo.self,
                    options: CallOptions(timeout: .milliseconds(200)))
            }
            XCTAssertEqual(error?.code, .deadlineExceeded)
            XCTAssertEqual(error?.source, "client")
            XCTAssertLessThan(ContinuousClock.now - started, .seconds(3), "timeout + grace (1 s), not the handshake timeout")
        }

        func testTheClientDeadlineCoversConnecting() async throws {
            // Nothing listens on this port: connecting fails fast and retryably.
            let nowhere = try Mesh(Router("ws://127.0.0.1:1"), options: MeshOptions(retry: .disabled))
            let error = await meshError {
                _ = try await nowhere.unary(fixture, "echo", Echo(value: 1, text: "x"), as: Echo.self)
            }
            XCTAssertEqual(error?.code, .unavailable)
            XCTAssertEqual(error?.retryable, true)
        }

        func testFailoverToTheNextEndpoint() async throws {
            let mesh = try Mesh(Router(urls: ["ws://127.0.0.1:1", live.routerURL]))
            defer { Task { await mesh.close() } }
            let echo = try await mesh.unary(fixture, "echo", Echo(value: 4, text: "second"), as: Echo.self)
            XCTAssertEqual(echo.value, 4)
            let endpoint = await mesh.endpoint
            XCTAssertEqual(endpoint.absoluteString, live.routerURL)
        }
    }
#endif
