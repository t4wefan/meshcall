import Foundation

/// One call's options. Generated clients set `idempotent` and `instance` from
/// the contract; callers add timeouts, metadata and idempotency keys.
public struct CallOptions: Sendable, Hashable {
    /// Total budget for the call, including connecting.
    public var timeout: Duration?
    /// Longest allowed gap between consecutive stream items.
    public var idleTimeout: Duration?
    /// Request metadata delivered to the service handler.
    public var metadata: [String: String]
    /// Lets the Router join or replay duplicate unary calls and lets the retry
    /// policy re-send a non-idempotent method safely. 1–128 bytes.
    public var idempotencyKey: String?
    /// The method's own semantics; generated clients set this from the contract.
    public var idempotent: Bool
    /// Address one service instance by id; required for targeted services
    /// (except on a tunnel connection, where the instance is implied).
    public var instance: String?

    public init(
        timeout: Duration? = nil, idleTimeout: Duration? = nil, metadata: [String: String] = [:],
        idempotencyKey: String? = nil, idempotent: Bool = false, instance: String? = nil
    ) {
        self.timeout = timeout
        self.idleTimeout = idleTimeout
        self.metadata = metadata
        self.idempotencyKey = idempotencyKey
        self.idempotent = idempotent
        self.instance = instance
    }

    /// `extra` over `self`: set values win, metadata is merged.
    public func merged(with extra: CallOptions) -> CallOptions {
        CallOptions(
            timeout: extra.timeout ?? timeout, idleTimeout: extra.idleTimeout ?? idleTimeout,
            metadata: metadata.merging(extra.metadata) { _, new in new },
            idempotencyKey: extra.idempotencyKey ?? idempotencyKey, idempotent: extra.idempotent || idempotent,
            instance: extra.instance ?? instance)
    }

    func validate() throws {
        for (name, value) in [("timeout", timeout), ("idleTimeout", idleTimeout)] {
            if let value, value <= .zero { throw ConfigurationError(name + " must be positive") }
        }
        try validateMetadata(metadata)
        if instance == "" { throw ConfigurationError("instance must not be empty") }
        if let key = idempotencyKey, key.utf8.isEmpty || key.utf8.count > 128 {
            throw ConfigurationError("idempotencyKey must be 1..128 bytes")
        }
    }
}

/// The shape of a method (docs/protocol.md → Calls).
public enum StreamShape: String, Sendable, Hashable {
    case unary
    case serverStream = "server_stream"
    case clientStream = "client_stream"
    case duplex
}

/// What a client interceptor sees for one outgoing call. `options` may be
/// replaced before calling `next`; `trailers` is filled for unary and
/// client-stream calls once `next` returns (streams expose their own).
public final class ClientCallInfo: @unchecked Sendable {
    public let service: String
    public let method: String
    public let shape: StreamShape
    private let lock = NSLock()
    private var _options: CallOptions
    private var _trailers: [String: String] = [:]

    init(service: String, method: String, shape: StreamShape, options: CallOptions) {
        self.service = service
        self.method = method
        self.shape = shape
        self._options = options
    }

    public var options: CallOptions {
        get { lock.withLock { _options } }
        set { lock.withLock { _options = newValue } }
    }

    public var trailers: [String: String] {
        get { lock.withLock { _trailers } }
        set { lock.withLock { _trailers = newValue } }
    }
}

/// Runs the rest of the chain; it may be called more than once to retry.
public typealias ClientCallNext = @Sendable (ClientCallInfo) async throws -> Void
/// Wraps every outgoing call; the first interceptor is outermost.
public typealias ClientInterceptor = @Sendable (ClientCallInfo, ClientCallNext) async throws -> Void

/// Router credentials: an API key, a temporary token or a tunnel credential
/// (all Bearer), or an account's username and password.
public enum RouterCredentials: Sendable, Hashable {
    case bearer(String)
    case basic(username: String, password: String)

    func authorization() throws -> String {
        switch self {
        case .bearer(let token):
            if token.isEmpty || token.contains(where: { $0 == "\r" || $0 == "\n" }) {
                throw ConfigurationError("A token is required")
            }
            return "Bearer " + token
        case .basic(let username, let password):
            if username.isEmpty || password.isEmpty || username.contains(where: { ":\r\n".contains($0) }) {
                throw ConfigurationError("A username and password are required")
            }
            return "Basic " + Data((username + ":" + password).utf8).base64EncodedString()
        }
    }

    var token: String? {
        if case .bearer(let token) = self { return token }
        return nil
    }
}

/// Connection options of a `Mesh`. The defaults match every other runtime.
public struct MeshOptions: Sendable {
    public var credentials: RouterCredentials?
    /// Seal every call end to end towards the instance holding this pinned
    /// public key (an end-to-end tunnel; docs/protocol.md → End-to-end encryption).
    public var e2eKey: String?
    /// This client's own key: it authenticates itself to the instance whose
    /// key is pinned in `e2eKey` (Noise IK; IKpsk2 to pair first). Without it
    /// the handshake is NK and the Router alone decides who may connect.
    public var e2eIdentity: E2EIdentity?
    public var peerID: String
    public var maxFrameSize: Int
    public var handshakeTimeout: Duration
    /// How long past `timeout` to wait for the peer's own deadline error
    /// before failing locally. Defaults to `max(1s, 10%)`.
    public var deadlineGrace: Duration?
    /// Keepalive ping cadence; `nil` disables pinging.
    public var pingInterval: Duration?
    public var pingTimeout: Duration
    /// How a lost connection is re-established. In-flight calls fail with a
    /// retryable `unavailable`; the next call reconnects lazily with backoff,
    /// waiting at most its own `timeout`.
    public var reconnect: ReconnectPolicy
    /// Built-in retry for unary calls.
    public var retry: RetryPolicy
    public var interceptors: [ClientInterceptor]
    /// Default options for every call.
    public var callOptions: CallOptions

    public init(
        credentials: RouterCredentials? = nil, e2eKey: String? = nil, e2eIdentity: E2EIdentity? = nil,
        peerID: String = newID(),
        maxFrameSize: Int = defaultMaxFrameSize, handshakeTimeout: Duration = .seconds(10),
        deadlineGrace: Duration? = nil, pingInterval: Duration? = .seconds(15), pingTimeout: Duration = .seconds(10),
        reconnect: ReconnectPolicy = .default, retry: RetryPolicy = .default, interceptors: [ClientInterceptor] = [],
        callOptions: CallOptions = CallOptions()
    ) {
        self.credentials = credentials
        self.e2eKey = e2eKey
        self.e2eIdentity = e2eIdentity
        self.peerID = peerID
        self.maxFrameSize = maxFrameSize
        self.handshakeTimeout = handshakeTimeout
        self.deadlineGrace = deadlineGrace
        self.pingInterval = pingInterval
        self.pingTimeout = pingTimeout
        self.reconnect = reconnect
        self.retry = retry
        self.interceptors = interceptors
        self.callOptions = callOptions
    }

    func validate() throws {
        try checkE2EAgreement(credential: credentials?.token, e2eKey: e2eKey)
        if let e2eKey { _ = try keyBytes(e2eKey, "An E2E key (the instance's X25519 public key)") }
        if let e2eIdentity {
            guard e2eKey != nil else {
                throw ConfigurationError(
                    "e2eIdentity authenticates this client to the instance whose key you pinned: pass e2eKey too")
            }
            try e2eIdentity.validate()
        }
        _ = try credentials?.authorization()
        if maxFrameSize <= 0 || handshakeTimeout <= .zero || pingTimeout <= .zero {
            throw ConfigurationError("Limits must be positive")
        }
        if let pingInterval, pingInterval <= .zero { throw ConfigurationError("pingInterval must be positive") }
        if let deadlineGrace, deadlineGrace < .zero { throw ConfigurationError("deadlineGrace must not be negative") }
        try reconnect.validate()
        try callOptions.validate()
    }
}

/// A random 32-character hex id (peer and call ids).
public func newID() -> String {
    UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
}
