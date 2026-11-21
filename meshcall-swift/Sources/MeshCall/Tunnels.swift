import Foundation

let tunnelService = "meshcall.router.v1.TunnelService"
let routerAuthService = "meshcall.router.v1.AuthService"

/// One service of the target instance and the methods a tunnel allows (`"*"`: all).
public struct TunnelScope: Codable, Sendable, Hashable {
    public var service: String
    public var methods: [String]

    public init(service: String, methods: [String] = ["*"]) {
        self.service = service
        self.methods = methods
    }
}

/// A tunnel as the Router lists it (never with its credential).
public struct TunnelInfo: Codable, Sendable, Hashable {
    public var tunnelID: String
    public var instanceID: String
    public var principal: String
    public var scopes: [TunnelScope]
    public var expiresAtUnixMs: Int64
    public var maxConnections: Int?
    /// The instance public key the issuer pinned: the tunnel carries sealed calls only.
    public var e2eKey: String?
    public var connections: Int
    public var online: Bool

    enum CodingKeys: String, CodingKey {
        case tunnelID = "tunnel_id", instanceID = "instance_id", principal, scopes
        case expiresAtUnixMs = "expires_at_unix_ms", maxConnections = "max_connections", e2eKey = "e2e_key"
        case connections, online
    }
}

/// `tunnels.open`'s answer: hand `credential` (and `e2eKey`) to the client once.
public struct OpenedTunnel: Codable, Sendable, Hashable {
    public var credential: String
    public var tunnelID: String
    public var instanceID: String
    public var principal: String
    public var scopes: [TunnelScope]
    public var expiresAtUnixMs: Int64
    public var maxConnections: Int?
    public var e2eKey: String?

    enum CodingKeys: String, CodingKey {
        case credential, tunnelID = "tunnel_id", instanceID = "instance_id", principal, scopes
        case expiresAtUnixMs = "expires_at_unix_ms", maxConnections = "max_connections", e2eKey = "e2e_key"
    }

    /// The grant a client needs to reach the instance through `routerURL`.
    public func grant(routerURL: String) -> TunnelGrant {
        TunnelGrant(
            routerURL: routerURL, credential: credential, instanceID: instanceID, tunnelID: tunnelID, e2eKey: e2eKey)
    }
}

public struct OpenTunnelRequest: Sendable, Hashable {
    public var instance: String
    public var scopes: [TunnelScope]
    public var principal: String
    /// Lifetime for new connections, 1–86400 s.
    public var ttlSeconds: Int
    public var maxConnections: Int?
    /// Pin the instance's public key (the one you provisioned it with, not one
    /// learnt from the Router): every call on the tunnel is then sealed end to end.
    public var e2eKey: String?

    public init(
        instance: String, scopes: [TunnelScope], principal: String, ttlSeconds: Int = 3600,
        maxConnections: Int? = nil, e2eKey: String? = nil
    ) {
        self.instance = instance
        self.scopes = scopes
        self.principal = principal
        self.ttlSeconds = ttlSeconds
        self.maxConnections = maxConnections
        self.e2eKey = e2eKey
    }
}

/// The Router normalises a service name without a version segment to `.v0`.
func normalizeScopeService(_ service: String) -> String {
    if service == "*" || service.isEmpty { return service }
    let segments = service.split(separator: ".")
    let versioned = segments.contains { $0.count > 1 && $0.first == "v" && $0.dropFirst().first?.isNumber == true }
    return versioned ? service : service + ".v0"
}

/// The Router's `TunnelService` (docs/protocol.md → Tunnels). Issuers are
/// accounts or API keys; apps usually receive a `TunnelGrant` instead.
public struct TunnelsClient: Sendable {
    let mesh: Mesh

    init(_ mesh: Mesh) { self.mesh = mesh }

    private struct OpenWire: Encodable, Sendable {
        let instance_id: String
        let scopes: [TunnelScope]
        let principal: String
        let ttl_seconds: Int
        let max_connections: Int?
        let e2e_key: String?
    }

    public func open(_ request: OpenTunnelRequest, options: CallOptions = CallOptions()) async throws -> OpenedTunnel {
        if let key = request.e2eKey { _ = try keyBytes(key, "An E2E key (the instance's X25519 public key)") }
        let wire = OpenWire(
            instance_id: request.instance,
            scopes: request.scopes.map { TunnelScope(service: normalizeScopeService($0.service), methods: $0.methods) },
            principal: request.principal, ttl_seconds: request.ttlSeconds, max_connections: request.maxConnections,
            e2e_key: request.e2eKey)
        return try await mesh.unary(tunnelService, "open", wire, as: OpenedTunnel.self, options: options)
    }

    public func revoke(_ tunnelID: String, options: CallOptions = CallOptions()) async throws -> Bool {
        struct Revoked: Decodable, Sendable { let revoked: Bool }
        return try await mesh.unary(tunnelService, "revoke", ["tunnel_id": tunnelID], as: Revoked.self, options: options)
            .revoked
    }

    public func list(options: CallOptions = CallOptions()) async throws -> [TunnelInfo] {
        struct Listed: Decodable, Sendable { let tunnels: [TunnelInfo]? }
        return try await mesh.unary(tunnelService, "list", Empty(), as: Listed.self, options: options).tunnels ?? []
    }
}

/// Who this connection is, as the Router sees it.
public struct RouterIdentity: Decodable, Sendable, Hashable {
    public var username: String
    public var roles: [String]
    public var register: [String]
    public var call: [String]
    public var expiresAtUnixMs: Int64?
    public var instanceIDs: [String]?
    /// The issuer-chosen principal of a temporary token, when set.
    public var principal: String?
    /// The tunnel this connection belongs to, on a tunnel connection.
    public var tunnel: TunnelInfo?

    enum CodingKeys: String, CodingKey {
        case username, roles, register, call, expiresAtUnixMs = "expires_at_unix_ms", instanceIDs = "instance_ids"
        case principal, tunnel
    }
}

/// The Router's `AuthService`; this runtime exposes `whoami` (token issuing
/// stays with server-side issuers).
public struct RouterAuthClient: Sendable {
    let mesh: Mesh

    init(_ mesh: Mesh) { self.mesh = mesh }

    public func whoami(options: CallOptions = CallOptions()) async throws -> RouterIdentity {
        try await mesh.unary(routerAuthService, "whoami", Empty(), as: RouterIdentity.self, options: options)
    }
}
