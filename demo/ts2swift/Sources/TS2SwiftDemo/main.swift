// Build and start the TypeScript service, call it through the generated
// Swift client (TypeScriptGreetingServiceClient.swift), and stop it.
import Foundation
import MeshCall

let demo = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
    .deletingLastPathComponent()
let repository = demo.deletingLastPathComponent().deletingLastPathComponent()
let runtime = repository.appendingPathComponent("meshcall-ts")
let server = repository.appendingPathComponent("demo/ts2py/ts-server")

func run(_ arguments: [String], in directory: URL) throws {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = arguments
    process.currentDirectoryURL = directory
    process.standardOutput = FileHandle.nullDevice
    try process.run()
    process.waitUntilExit()
    guard process.terminationStatus == 0 else {
        throw ConfigurationError("\(arguments.joined(separator: " ")) failed in \(directory.path)")
    }
}

try run(["yarn", "-s", "install", "--frozen-lockfile"], in: runtime)
try run(["yarn", "-s", "run", "build"], in: runtime)
try run(["yarn", "-s", "install", "--frozen-lockfile", "--check-files"], in: server)
try run(["yarn", "-s", "run", "build"], in: server)

// The service starts its own Router and prints {"url": ...}; a line on stdin stops it.
let process = Process()
process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
process.arguments = ["node", "dist/server.js"]
process.currentDirectoryURL = server
let input = Pipe()
let output = Pipe()
process.standardInput = input
process.standardOutput = output
try process.run()
var line = Data()
while let byte = try output.fileHandleForReading.read(upToCount: 1), !byte.isEmpty, byte != Data("\n".utf8) {
    line.append(byte)
}
guard let url = try JSONValue(json: line)["url"]?.stringValue else {
    throw ConfigurationError("the TypeScript service did not report its Router")
}

do {
    let mesh = try Mesh(Router(url))
    let client = TypeScriptGreetingServiceClient(mesh)

    let greeting = try await client.greet(name: "Swift", repeat: 2)
    var items: [NumberItem] = []
    for try await item in try await client.count(count: 40) { items.append(item) }
    let upload = AsyncStream { continuation in
        for item in items { continuation.yield(item) }
        continuation.finish()
    }
    let total = try await client.sum(offset: 10, items: upload)
    await mesh.close()

    precondition(greeting.handledBy == "typescript")
    precondition(items.count == 40 && total.total == 790)
    print("TypeScript service -> Swift client: \(greeting.message) (handled by \(greeting.handledBy))")
    print("Streaming: received \(items.count) items, uploaded total=\(total.total)")
}

input.fileHandleForWriting.write(Data("\n".utf8))
process.waitUntilExit()
