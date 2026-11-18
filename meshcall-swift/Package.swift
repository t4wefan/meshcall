// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "MeshCall",
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        .library(name: "MeshCall", targets: ["MeshCall"]),
    ],
    targets: [
        .target(name: "MeshCall"),
        .testTarget(
            name: "MeshCallTests",
            dependencies: ["MeshCall"]
        ),
    ]
)
