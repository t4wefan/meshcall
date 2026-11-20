#if os(macOS)
    import Foundation
    import XCTest

    @testable import MeshCall

    /// A client environment (meshcall-router/conformance/CLIENTS.md): a Router
    /// and a fixture service behind `meshcall-conformance env`, which also
    /// restarts or stalls them on command. The fixture is the Go one unless
    /// `MESHCALL_FIXTURE` names another runtime's (any FIXTURE.md program).
    final class LiveMesh: @unchecked Sendable {
        let routerURL: String
        /// The fixture's end-to-end public key.
        let e2eKey: String
        /// A client private key the fixture accepts (IK).
        let e2eClientKey: String
        /// One-time pairings the fixture accepts (IKpsk2); each test uses its own.
        let e2ePairings: [E2EPairing]
        private let process: Process
        private let commands: FileHandle
        private let answers: FileHandle
        private let lock = NSLock()

        static let instance = "fixture-1"
        static let service = "conformance.v1.Fixture"

        init() throws {
            let environment = ProcessInfo.processInfo.environment
            let conformance = try Self.binary(
                env: "MESHCALL_CONFORMANCE_BINARY", built: "meshcall-router/bin/meshcall-conformance",
                module: "meshcall-router", package: "./cmd/meshcall-conformance")
            let fixture =
                try environment["MESHCALL_FIXTURE"].flatMap { $0.isEmpty ? nil : $0 }
                ?? Self.binary(
                    env: "MESHCALL_FIXTURE_BINARY", built: "meshcall-go/bin/conformance-fixture", module: "meshcall-go",
                    package: "./conformance/fixture")
            process = Process()
            process.executableURL = URL(fileURLWithPath: conformance)
            process.arguments = ["env", "--fixture", fixture]
            process.currentDirectoryURL = repositoryRoot
            let input = Pipe()
            let output = Pipe()
            process.standardInput = input
            process.standardOutput = output
            process.standardError = FileHandle.standardError
            try process.run()
            commands = input.fileHandleForWriting
            answers = output.fileHandleForReading
            guard let ready = try? JSONValue(json: Self.readLine(answers)), ready["kind"] == "env.ready",
                let url = ready["router_url"]?.stringValue, let key = ready["e2e_key"]?.stringValue
            else {
                process.terminate()
                throw MeshCallError(.unavailable, "meshcall-conformance env did not start")
            }
            routerURL = url
            e2eKey = key
            e2eClientKey = ready["e2e_client_key"]?.stringValue ?? ""
            e2ePairings = (ready["e2e_pairings"]?.arrayValue ?? []).compactMap { pairing in
                guard let id = pairing["id"]?.stringValue, let psk = pairing["psk"]?.stringValue else { return nil }
                return E2EPairing(id: id, psk: psk)
            }
        }

        deinit { stop() }

        func stop() {
            guard process.isRunning else { return }
            try? commands.close()  // the environment stops when its stdin ends
            process.waitUntilExit()
        }

        /// Run one environment command; it returns once the command is done
        /// (a restarted Router or fixture has the fixture registered again).
        private func run(_ command: String) throws {
            try lock.withLock {
                commands.write(Data(("{\"command\":\"\(command)\"}\n").utf8))
                let answer = try JSONValue(json: Self.readLine(answers))
                guard answer["ok"] == true else {
                    throw MeshCallError(.internal, "\(command): \(answer["error"]?.stringValue ?? "failed")")
                }
            }
        }

        /// The Router stops at once (no drain) and a new one takes its place.
        func restartRouter() throws { try run("restart_router") }
        /// The fixture is killed and started again: it has lost its E2E sessions.
        func restartFixture() throws { try run("restart_fixture") }
        /// Client connections stall: open, but nothing arrives either way.
        func pauseRouter() { try? run("pause_router") }
        func resumeRouter() { try? run("resume_router") }

        func mesh(_ options: MeshOptions = MeshOptions()) throws -> Mesh {
            try Mesh(Router(routerURL), options: options)
        }

        private static func readLine(_ handle: FileHandle) throws -> Data {
            var line = Data()
            while let byte = try handle.read(upToCount: 1), !byte.isEmpty, byte != Data("\n".utf8) {
                line.append(byte)
            }
            return line
        }

        private static func binary(env: String, built: String, module: String, package: String) throws -> String {
            if let path = ProcessInfo.processInfo.environment[env], !path.isEmpty { return path }
            let path = repositoryRoot.appendingPathComponent(built).path
            let build = Process()
            build.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            build.arguments = ["go", "build", "-o", path, package]
            build.currentDirectoryURL = repositoryRoot.appendingPathComponent(module)
            try build.run()
            build.waitUntilExit()
            guard build.terminationStatus == 0, FileManager.default.isExecutableFile(atPath: path) else {
                throw XCTSkip("Go is needed to build \(built) (or set \(env))")
            }
            return path
        }
    }

    /// An async sequence of the given values, for client streams.
    func items<T: Sendable>(_ values: [T]) -> AsyncStream<T> {
        AsyncStream { continuation in
            for value in values { continuation.yield(value) }
            continuation.finish()
        }
    }

    /// `body` throws a `MeshCallError`; returns it.
    func meshError(_ body: () async throws -> Void, file: StaticString = #filePath, line: UInt = #line) async
        -> MeshCallError?
    {
        do {
            try await body()
            XCTFail("expected a MeshCallError", file: file, line: line)
            return nil
        } catch let error as MeshCallError {
            return error
        } catch {
            XCTFail("expected a MeshCallError, got \(error)", file: file, line: line)
            return nil
        }
    }
#endif
