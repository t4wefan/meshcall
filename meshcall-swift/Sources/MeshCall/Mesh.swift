import Foundation

/// Where the Router is: one or more `ws://` / `wss://` endpoints, tried in
/// order starting from the last one that worked (failover without any
/// Router-side coordination). Performs no I/O.
public struct Router: Sendable, Hashable {
    public static let defaultURL = "ws://127.0.0.1:8765"

    public let urls: [URL]

    public init(_ url: String = Router.defaultURL) throws {
        try self.init(urls: [url])
    }

    public init(urls: [String]) throws {
        guard !urls.isEmpty else { throw ConfigurationError("At least one Router endpoint is required") }
        self.urls = try urls.map { text in
            guard let url = URL(string: text), let scheme = url.scheme?.lowercased() else {
                throw ConfigurationError("Not a Router URL: \(text)")
            }
            guard scheme == "ws" || scheme == "wss" else {
                // Apps hold one WebSocket per Router; the HTTP mapping and local
                // sockets are not part of this runtime.
                throw ConfigurationError("Router URL must use ws:// or wss://, got \(text)")
            }
            return url
        }
    }

    public var url: URL { urls[0] }
}

/// What a tunnel issuer hands a client: Effi Cloud's
/// `POST /api/instances/{id}/tunnel` answer, or any control plane's. Decodes
/// from its snake_case JSON as is.
public struct TunnelGrant: Codable, Sendable, Hashable {
    public var routerURL: String
    /// `mtun_…` (`mtun_e2e.…` for an end-to-end tunnel), sent as a Bearer token.
    public var credential: String
    public var instanceID: String
    public var tunnelID: String?
    /// The instance's pinned public key; set for end-to-end tunnels.
    public var e2eKey: String?

    public init(routerURL: String, credential: String, instanceID: String, tunnelID: String? = nil, e2eKey: String? = nil)
    {
        self.routerURL = routerURL
        self.credential = credential
        self.instanceID = instanceID
        self.tunnelID = tunnelID
        self.e2eKey = e2eKey
    }

    enum CodingKeys: String, CodingKey {
        case routerURL = "router_url"
        case credential
        case instanceID = "instance_id"
        case tunnelID = "tunnel_id"
        case e2eKey = "e2e_key"
    }
}

/// Options for a duplex call: it is never retried or deduplicated, so it takes
/// no idempotency key and no `idempotent` flag.
public struct DuplexCallOptions: Sendable, Hashable {
    public var timeout: Duration?
    public var idleTimeout: Duration?
    public var metadata: [String: String]
    public var instance: String?

    public init(
        timeout: Duration? = nil, idleTimeout: Duration? = nil, metadata: [String: String] = [:], instance: String? = nil
    ) {
        self.timeout = timeout
        self.idleTimeout = idleTimeout
        self.metadata = metadata
        self.instance = instance
    }

    var callOptions: CallOptions {
        CallOptions(timeout: timeout, idleTimeout: idleTimeout, metadata: metadata, instance: instance)
    }
}

/// What generated clients call: `Mesh` implements it.
public protocol MeshCallRPC: Sendable {
    func unary<Request: Encodable & Sendable, Response: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, as: Response.Type, options: CallOptions
    ) async throws -> Response
    func serverStream<Request: Encodable & Sendable, Item: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, of: Item.Type, options: CallOptions
    ) async throws -> ServerStream<Item>
    func clientStream<Request: Encodable & Sendable, Items: AsyncSequence & Sendable, Response: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, items: Items, as: Response.Type, options: CallOptions
    ) async throws -> Response where Items.Element: Encodable & Sendable
    func duplex<Request: Encodable & Sendable, Input: Encodable & Sendable, Output: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, input: Input.Type, output: Output.Type,
        options: DuplexCallOptions
    ) async throws -> DuplexCall<Input, Output>
    /// A view sharing this connection with `options` applied to every call.
    func withOptions(_ options: CallOptions) -> Self
}

/// A MeshCall client connected to a Router. One `Mesh` holds one WebSocket,
/// opened by the first call and re-opened by the next call after a loss.
///
///     let mesh = try Mesh(tunnel: grant)          // from the issuer
///     let sessions = SessionsClient(mesh)          // generated
///     for try await event in try await sessions.watch(id: "s1") { … }
public final class Mesh: MeshCallRPC, @unchecked Sendable {
    let core: ClientCore
    private let defaults: CallOptions
    private let owner: Bool

    public init(_ router: Router, options: MeshOptions = MeshOptions()) throws {
        core = try ClientCore(endpoints: router.urls, options: options) { url, options in
            URLSessionWebSocket(url: url, maxFrameSize: options.maxFrameSize, handshakeTimeout: options.handshakeTimeout)
        }
        defaults = options.callOptions
        owner = true
    }

    /// A Mesh that reaches only the tunnel's instance, sealing every call end
    /// to end when the grant carries the instance's key.
    public convenience init(tunnel grant: TunnelGrant, options: MeshOptions = MeshOptions()) throws {
        guard grant.credential.hasPrefix("mtun_"), !grant.credential.contains(where: \.isWhitespace) else {
            throw ConfigurationError("A tunnel credential starts with 'mtun_'; it is the credential the issuer returned")
        }
        var options = options
        options.credentials = .bearer(grant.credential)
        options.e2eKey = grant.e2eKey
        try self.init(Router(grant.routerURL), options: options)
    }

    init(core: ClientCore, defaults: CallOptions, owner: Bool) {
        self.core = core
        self.defaults = defaults
        self.owner = owner
    }

    init(core: ClientCore) {
        self.core = core
        self.defaults = core.options.callOptions
        self.owner = true
    }

    public func withOptions(_ options: CallOptions) -> Self {
        Self(core: core, defaults: defaults.merged(with: options), owner: false)
    }

    /// The Router's `TunnelService`: open, revoke and list tunnels.
    public var tunnels: TunnelsClient { TunnelsClient(self) }
    /// The Router's `AuthService`.
    public var auth: RouterAuthClient { RouterAuthClient(self) }

    /// Connect now instead of at the first call.
    public func connect() async throws {
        try await core.connect()
    }

    /// The endpoint of the current (or most recently used) connection.
    public var endpoint: URL {
        get async { await core.endpoint }
    }

    /// Ping the Router now and wait at most `timeout` for the answer. Call it
    /// when the app returns to the foreground: a connection the system cut
    /// while suspended is dropped at once (its calls fail with a retryable
    /// `unavailable`, so watchers re-subscribe) instead of after the next
    /// keepalive timeout. Returns whether a live connection answered.
    @discardableResult
    public func checkConnection(timeout: Duration = .seconds(3)) async -> Bool {
        await core.checkConnection(timeout: timeout)
    }

    /// Close the connection; calls in flight fail with `unavailable`. A later
    /// call connects again.
    public func close() async {
        guard owner else { return }
        await core.close()
    }

    // MARK: calls

    public func unary<Request: Encodable & Sendable, Response: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, as: Response.Type = Response.self,
        options: CallOptions = CallOptions()
    ) async throws -> Response {
        let merged = defaults.merged(with: options)
        try merged.validate()
        let payload = try JSONValue(encoding: request)
        let info = ClientCallInfo(service: service, method: method, shape: .unary, options: merged)
        let box = Box<Response>()
        try await runChain(info) { [core] info in
            let value: JSONValue
            do {
                value = try await Self.unaryAttempt(core, info, payload)
            } catch where ClientCore.lostSession(error) {
                // The instance lost the session (it reconnected): the call
                // never ran. Handshake again on a new connection and retry once.
                value = try await Self.unaryAttempt(core, info, payload)
            }
            box.value = try decodeReceived(value, as: Response.self, reason: "invalid_response")
        }
        return box.value!
    }

    private static func unaryAttempt(_ core: ClientCore, _ info: ClientCallInfo, _ payload: JSONValue) async throws
        -> JSONValue
    {
        if Task.isCancelled { throw MeshCallError.callCancelled() }
        let pending = try await core.openCall(
            service: info.service, method: info.method, request: payload, options: info.options, input: false)
        defer { info.trailers = pending.trailers }
        return try await Self.awaitResult(core, pending)
    }

    private static func awaitResult(_ core: ClientCore, _ pending: PendingCall) async throws -> JSONValue {
        try await withTaskCancellationHandler {
            try await core.result(of: pending)
        } onCancel: {
            Task { await core.cancel(pending, MeshCallError.callCancelled(), reason: "caller_cancelled") }
        }
    }

    public func serverStream<Request: Encodable & Sendable, Item: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, of: Item.Type = Item.self,
        options: CallOptions = CallOptions()
    ) async throws -> ServerStream<Item> {
        let merged = defaults.merged(with: options)
        try merged.validate()
        let payload = try JSONValue(encoding: request)
        let info = ClientCallInfo(service: service, method: method, shape: .serverStream, options: merged)
        let box = Box<ServerStream<Item>>()
        try await runChain(info) { [core] info in
            let state = try await core.openStream(
                service: info.service, method: info.method, request: payload, options: info.options, duplex: false)
            box.value = ServerStream(core: core, state: state)
        }
        return box.value!
    }

    public func clientStream<
        Request: Encodable & Sendable, Items: AsyncSequence & Sendable, Response: Decodable & Sendable
    >(
        _ service: String, _ method: String, _ request: Request, items: Items, as: Response.Type = Response.self,
        options: CallOptions = CallOptions()
    ) async throws -> Response where Items.Element: Encodable & Sendable {
        let merged = defaults.merged(with: options)
        try merged.validate()
        let payload = try JSONValue(encoding: request)
        let info = ClientCallInfo(service: service, method: method, shape: .clientStream, options: merged)
        let box = Box<Response>()
        try await runChain(info) { [core] info in
            let pending = try await core.openCall(
                service: info.service, method: info.method, request: payload, options: info.options, input: true)
            defer { info.trailers = pending.trailers }
            // The server normally answers after stream.end; if it fails early
            // the producer is stopped.
            let pump = Task {
                do {
                    for try await item in items {
                        try await core.sendInput(pending, try JSONValue(encoding: item))
                    }
                    await core.closeInput(pending)
                } catch {
                    await core.cancel(pending, error, reason: "input_producer_failed")
                }
            }
            defer { pump.cancel() }
            let value = try await Self.awaitResult(core, pending)
            box.value = try decodeReceived(value, as: Response.self, reason: "invalid_response")
        }
        return box.value!
    }

    public func duplex<Request: Encodable & Sendable, Input: Encodable & Sendable, Output: Decodable & Sendable>(
        _ service: String, _ method: String, _ request: Request, input: Input.Type = Input.self,
        output: Output.Type = Output.self, options: DuplexCallOptions = DuplexCallOptions()
    ) async throws -> DuplexCall<Input, Output> {
        var base = defaults
        base.idempotencyKey = nil
        base.idempotent = false
        let merged = base.merged(with: options.callOptions)
        try merged.validate()
        let payload = try JSONValue(encoding: request)
        let info = ClientCallInfo(service: service, method: method, shape: .duplex, options: merged)
        let box = Box<DuplexCall<Input, Output>>()
        try await runChain(info) { [core] info in
            let state = try await core.openStream(
                service: info.service, method: info.method, request: payload, options: info.options, duplex: true)
            box.value = DuplexCall(core: core, state: state)
        }
        return box.value!
    }

    /// The interceptor chain, first interceptor outermost; the retry policy
    /// wraps everything so interceptors see each attempt.
    private func runChain(_ info: ClientCallInfo, _ innermost: @escaping ClientCallNext) async throws {
        var next = innermost
        for interceptor in core.options.interceptors.reversed() {
            let inner = next
            next = { info in try await interceptor(info, inner) }
        }
        let policy = core.options.retry
        if policy.enabled, info.shape == .unary {
            let inner = next
            next = { info in try await retry(policy, info, inner) }
        }
        try await next(info)
    }
}

private final class Box<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: Value?
    var value: Value? {
        get { lock.withLock { _value } }
        set { lock.withLock { _value = newValue } }
    }
}

/// Re-send a failed unary call only for a `retryable` error whose code is in
/// `retryCodes`, when the method is idempotent or the call carries an
/// idempotency key, within the call's timeout. The final error carries
/// `details.attempts` when more than one attempt was made.
private func retry(_ policy: RetryPolicy, _ info: ClientCallInfo, _ next: ClientCallNext) async throws {
    let options = info.options
    let mayRetry = options.idempotent || options.idempotencyKey != nil
    let deadline = options.timeout.map { ContinuousClock.now + $0 }
    var attempt = 1
    while true {
        do {
            try await next(info)
            return
        } catch let error as MeshCallError {
            func give() -> MeshCallError {
                attempt > 1 ? error.with(details: ["attempts": .int(Int64(attempt))]) : error
            }
            guard mayRetry, error.retryable, policy.retryCodes.contains(error.code), attempt < policy.maxAttempts else {
                throw give()
            }
            let delay = policy.delay(attempt: attempt)
            if let deadline {
                let remaining = deadline - ContinuousClock.now
                if remaining <= delay { throw give() }
                info.options.timeout = remaining - delay
            }
            do {
                try await Task.sleep(for: delay)
            } catch {
                throw MeshCallError.callCancelled()
            }
            attempt += 1
        }
    }
}
