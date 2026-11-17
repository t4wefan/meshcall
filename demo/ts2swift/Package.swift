// swift-tools-version:6.0
import PackageDescription

// A TypeScript service (../ts2py/ts-server) called from its generated Swift client.
let package = Package(
    name: "TS2SwiftDemo",
    platforms: [.macOS(.v13)],
    dependencies: [.package(name: "MeshCall", path: "../../meshcall-swift")],
    targets: [
        .executableTarget(
            name: "TS2SwiftDemo",
            dependencies: [.product(name: "MeshCall", package: "MeshCall")]
        ),
    ]
)
