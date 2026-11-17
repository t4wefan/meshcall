#if os(macOS)
import Foundation
import XCTest

@testable import MeshCall

/// A Router and the Go conformance fixture (meshcall-router/conformance/FIXTURE.md),
/// both real processes, for the client to talk to. Built with `go build` when
/// the binaries are missing; `MESHCALL_ROUTER_BINARY` and
/// `MESHCALL_FIXTURE_BINARY` point at prebuilt ones.
final class LiveMesh: @unchecked Sendable {
    let routerURL: String
    let port: Int
    let e2eKey = E2EKeyPair.generate()
    private var router: Process
    private var fixture: Process
    private let routerBinary: String
    private let fixtureBinary: String
    private let logs = Logs()

    static let instance = "fixture-1"
    static let service = "conformance.v1.Fixture"

    init(port requested: Int = 0) throws {
        routerBinary = try Self.binary(
            env: "MESHCALL_ROUTER_BINARY", built: "meshcall-router/bin/meshcall-router", module: "meshcall-router",
            package: "./cmd/meshcall-router")
        fixtureBinary = try Self.binary(
            env: "MESHCALL_FIXTURE_BINARY", built: "meshcall-go/bin/conformance-fixture", module: "meshcall-go",
            package: "./conformance/fixture")
        (router, port) = try Self.startRouter(routerBinary, port: requested, logs: logs)
        routerURL = "ws://127.0.0.1:\(port)"
        fixture = Process()
        fixture = try startFixture()
    }

    deinit { stop() }

    func stop() {
        for process in [fixture, router] where process.isRunning {
            process.terminate()
            process.waitUntilExit()
        }
    }

    /// Kill the Router at once (no drain) and, after `pause`, start a new one on the same port.
    func restartRouter() throws {
        kill(router.processIdentifier, SIGKILL)
        router.waitUntilExit()
        (router, _) = try Self.startRouter(routerBinary, port: port, logs: logs)
    }

    /// Kill the fixture and start it again: it re-registers and loses its E2E sessions.
    func restartFixture() throws {
        kill(fixture.processIdentifier, SIGKILL)
        fixture.waitUntilExit()
        fixture = try startFixture()
    }

    func pauseRouter() { kill(router.processIdentifier, SIGSTOP) }
    func resumeRouter() { kill(router.processIdentifier, SIGCONT) }

    func mesh(_ options: MeshOptions = MeshOptions()) throws -> Mesh {
        try Mesh(Router(routerURL), options: options)
    }

    /// Wait until the fixture registered with the (current) Router.
    func waitForFixture(timeout: Duration = .seconds(20)) async throws {
        let mesh = try mesh(MeshOptions(retry: .disabled))
        defer { Task { await mesh.close() } }
        let deadline = ContinuousClock.now + timeout
        while ContinuousClock.now < deadline {
            struct Listed: Decodable { let services: [Service] }
            struct Service: Decodable { let name: String }
            if let listed = try? await mesh.unary(
                "meshcall.router.v1.Registry", "list", Empty(), as: Listed.self,
                options: CallOptions(timeout: .seconds(2))),
                listed.services.contains(where: { $0.name == Self.service })
            {
                return
            }
            try await Task.sleep(for: .milliseconds(100))
        }
        throw MeshCallError(.unavailable, "the fixture did not register:\n" + logs.text)
    }

    private func startFixture() throws -> Process {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: fixtureBinary)
        var environment = ProcessInfo.processInfo.environment
        environment["MESHCALL_ROUTER_URL"] = routerURL
        environment["MESHCALL_CONFORMANCE_INSTANCE"] = Self.instance
        environment["MESHCALL_PING_INTERVAL_MS"] = "1000"
        environment["MESHCALL_E2E_PRIVATE_KEY"] = e2eKey.privateKey
        environment["MESHCALL_STRICT"] = "1"
        process.environment = environment
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe
        logs.follow(pipe, prefix: "fixture")
        try process.run()
        return process
    }

    private static func startRouter(_ binary: String, port: Int, logs: Logs) throws -> (Process, Int) {
        for attempt in 0..<20 {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: binary)
            process.arguments = ["-port", String(port)]
            let out = Pipe()
            let err = Pipe()
            process.standardOutput = out
            process.standardError = err
            logs.follow(err, prefix: "router")
            try process.run()
            // The first line is {"kind":"router.ready",...,"port":N}.
            var line = Data()
            while let byte = try out.fileHandleForReading.read(upToCount: 1), !byte.isEmpty, byte != Data("\n".utf8) {
                line.append(byte)
            }
            if let ready = try? JSONValue(json: line), let chosen = ready["port"]?.intValue {
                return (process, Int(chosen))
            }
            // The old Router's port may not be free yet.
            process.waitUntilExit()
            Thread.sleep(forTimeInterval: 0.1 * Double(attempt + 1))
        }
        throw MeshCallError(.unavailable, "the Router did not start:\n" + logs.text)
    }

    private static func binary(env: String, built: String, module: String, package: String) throws -> String {
        if let path = ProcessInfo.processInfo.environment[env], !path.isEmpty { return path }
        let path = repositoryRoot.appendingPathComponent(built).path
        let build = Process()
        build.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        build.arguments = ["go", "build", "-o", repositoryRoot.appendingPathComponent(built).path, package]
        build.currentDirectoryURL = repositoryRoot.appendingPathComponent(module)
        try build.run()
        build.waitUntilExit()
        guard build.terminationStatus == 0, FileManager.default.isExecutableFile(atPath: path) else {
            throw XCTSkip("Go is needed to build \(built) (or set \(env))")
        }
        return path
    }
}

private final class Logs: @unchecked Sendable {
    private let lock = NSLock()
    private var lines: [String] = []

    func follow(_ pipe: Pipe, prefix: String) {
        pipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else {
                handle.readabilityHandler = nil
                return
            }
            self?.lock.withLock { self?.lines.append(prefix + ": " + String(decoding: data, as: UTF8.self)) }
        }
    }

    var text: String { lock.withLock { lines.suffix(80).joined() } }
}

/// An async sequence of the given values, for client streams.
func items<T: Sendable>(_ values: [T]) -> AsyncStream<T> {
    AsyncStream { continuation in
        for value in values { continuation.yield(value) }
        continuation.finish()
    }
}

/// `body` throws a `MeshCallError`; returns it.
func meshError(_ body: () async throws -> Void, file: StaticString = #filePath, line: UInt = #line) async -> MeshCallError?
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
