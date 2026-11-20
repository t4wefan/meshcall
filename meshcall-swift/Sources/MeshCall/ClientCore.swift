import Foundation

// State owned by `ClientCore`. These classes are only touched on the actor;
// they are `@unchecked Sendable` so handles can carry them back to it.

/// A one-shot result several tasks may wait for.
final class Promise<Value: Sendable>: @unchecked Sendable {
    private(set) var result: Result<Value, Error>?
    private var waiters: [CheckedContinuation<Value, Error>] = []

    func settle(_ outcome: Result<Value, Error>) {
        guard result == nil else { return }
        result = outcome
        let waiters = self.waiters
        self.waiters = []
        for waiter in waiters { waiter.resume(with: outcome) }
    }

    func wait(_ continuation: CheckedContinuation<Value, Error>) {
        if let result {
            continuation.resume(with: result)
        } else {
            waiters.append(continuation)
        }
    }
}

/// Item credit for one input direction: `acquire` waits for a `stream.window`.
final class CreditWindow: @unchecked Sendable {
    private var credit: Int64 = 0
    private var failure: Error?
    private var waiters: [CheckedContinuation<Void, Error>] = []

    func acquire(_ continuation: CheckedContinuation<Void, Error>) {
        if let failure {
            continuation.resume(throwing: failure)
        } else if credit > 0 {
            credit -= 1
            continuation.resume()
        } else {
            waiters.append(continuation)
        }
    }

    func grant(_ amount: Int64) throws {
        guard amount > 0 else { throw MeshCallError.protocolViolation("Stream credit must be positive") }
        guard failure == nil else { return }
        credit += amount
        while credit > 0, !waiters.isEmpty {
            credit -= 1
            waiters.removeFirst().resume()
        }
    }

    func close(_ error: Error) {
        guard failure == nil else { return }
        failure = error
        let waiters = self.waiters
        self.waiters = []
        for waiter in waiters { waiter.resume(throwing: error) }
    }
}

final class InputState: @unchecked Sendable {
    let credit = CreditWindow()
    var sequence: Int64 = 0
    var closed = false
}

/// A unary or client-stream call waiting for its terminal frame.
final class PendingCall: @unchecked Sendable {
    let callID: String
    let promise = Promise<JSONValue>()
    var trailers: [String: String] = [:]
    var timer: Task<Void, Never>?
    let input: InputState?
    var closed = false

    init(callID: String, input: InputState?) {
        self.callID = callID
        self.input = input
    }
}

/// The client's side of a server stream or duplex call.
final class StreamState: @unchecked Sendable {
    let callID: String
    let input: InputState?
    var items: [JSONValue] = []
    var waiters: [CheckedContinuation<JSONValue?, Error>] = []
    /// The queue's end: `.success` once the result arrived, `.failure` on error.
    var outcome: Result<Void, Error>?
    var allowance: Int64 = 0
    var expectedSequence: Int64 = 0
    /// The server half-closed its output.
    var ended = false
    var terminal = false
    var trailers: [String: String] = [:]
    var timer: Task<Void, Never>?
    var idleTimer: Task<Void, Never>?
    var idleTimeout: Duration?

    init(callID: String, duplex: Bool) {
        self.callID = callID
        self.input = duplex ? InputState() : nil
    }

    func push(_ item: JSONValue) {
        guard outcome == nil else { return }
        if waiters.isEmpty {
            items.append(item)
        } else {
            waiters.removeFirst().resume(returning: item)
        }
    }

    func finish(_ result: Result<Void, Error>) {
        guard outcome == nil else { return }
        outcome = result
        guard items.isEmpty else { return }
        let waiters = self.waiters
        self.waiters = []
        for waiter in waiters {
            switch result {
            case .success: waiter.resume(returning: nil)
            case .failure(let error): waiter.resume(throwing: error)
            }
        }
    }

    func next(_ continuation: CheckedContinuation<JSONValue?, Error>) {
        if !items.isEmpty {
            continuation.resume(returning: items.removeFirst())
            return
        }
        switch outcome {
        case .success?: continuation.resume(returning: nil)
        case .failure(let error)?: continuation.resume(throwing: error)
        case nil: waiters.append(continuation)
        }
    }
}

/// One established connection: socket, writer, keepalive and E2E session.
final class Connection: @unchecked Sendable {
    let socket: WebSocketConnection
    let writer: FrameWriter
    let e2e: E2ESession?
    var reader: Task<Void, Never>?
    var pingTask: Task<Void, Never>?
    var pongTask: Task<Void, Never>?
    var pendingNonce: String?
    var probes: [CheckedContinuation<Bool, Never>] = []

    init(socket: WebSocketConnection, writer: FrameWriter, e2e: E2ESession?) {
        self.socket = socket
        self.writer = writer
        self.e2e = e2e
    }
}

typealias SocketFactory = @Sendable (URL, MeshOptions) -> WebSocketConnection

/// The first of several outcomes wins; the rest are dropped.
private final class Race: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Error>?
    private var early: Result<Void, Error>?
    private var settled = false

    func start(_ continuation: CheckedContinuation<Void, Error>) {
        lock.lock()
        if let early {
            lock.unlock()
            continuation.resume(with: early)
            return
        }
        self.continuation = continuation
        lock.unlock()
    }

    func finish(_ outcome: Result<Void, Error>) {
        lock.lock()
        if settled {
            lock.unlock()
            return
        }
        settled = true
        guard let continuation else {
            early = outcome  // cancelled before the continuation existed
            lock.unlock()
            return
        }
        self.continuation = nil
        lock.unlock()
        continuation.resume(with: outcome)
    }
}

private final class Flag: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false
    func set() { lock.withLock { value = true } }
    var isSet: Bool { lock.withLock { value } }
}

/// The connection and every call on it (docs/protocol.md). Mirrors
/// `meshcall-ts/src/client.ts`: synchronous actor methods are the atomic
/// steps the TypeScript event loop gives that implementation.
actor ClientCore {
    let endpoints: [URL]
    let options: MeshOptions
    private let authorization: String?
    private let socketFactory: SocketFactory

    /// Index of the endpoint in use (or to try first); advances on failure.
    private var cursor = 0
    private var connection: Connection?
    private var connecting: Task<Void, Error>?
    private var everConnected = false
    // The Router's 4401 answer (revoked or expired credentials): later calls
    // fail with it, reason included, instead of reconnecting.
    private var refused: MeshCallError?
    private var closed = false
    private var calls: [String: PendingCall] = [:]
    private var streams: [String: StreamState] = [:]
    private var sealedCalls: [String: SealedCall] = [:]
    /// Set once the instance confirmed this client's pairing: every later
    /// handshake (reconnects, new connections) uses IK with the same key.
    private var paired = false

    init(endpoints: [URL], options: MeshOptions, socketFactory: @escaping SocketFactory) throws {
        guard !endpoints.isEmpty else { throw ConfigurationError("At least one endpoint is required") }
        try options.validate()
        self.endpoints = endpoints
        self.options = options
        self.authorization = try options.credentials?.authorization()
        self.socketFactory = socketFactory
    }

    deinit {
        connection?.socket.close(code: 1000, reason: "client released")
    }

    var endpoint: URL { endpoints[cursor % endpoints.count] }

    var isConnected: Bool { connection != nil }

    // MARK: connecting

    /// Connect if not connected. After a loss this retries with backoff per
    /// the reconnect policy; concurrent callers share one attempt sequence.
    func connect() async throws {
        if connection != nil { return }
        if let connecting {
            try await connecting.value
            return
        }
        closed = false
        let attempt = Task { try await self.openWithRetry() }
        connecting = attempt
        defer { if connecting == attempt { connecting = nil } }
        try await attempt.value
    }

    private func openWithRetry() async throws {
        if let refused { throw refused }
        let retry = options.reconnect.enabled && everConnected
        let delays = retry ? options.reconnect.delays() : nil
        while true {
            do {
                try await openAnyEndpoint()
                everConnected = true
                return
            } catch let error as MeshCallError {
                // Refused credentials fail the same way again: report them at once.
                if error.code == .unauthenticated || error.code == .permissionDenied { throw error }
                guard let delay = delays?.next(), !closed else { throw error }
                do {
                    try await Task.sleep(for: delay)
                } catch {
                    throw MeshCallError(.unavailable, "Client is closed")
                }
            }
        }
    }

    /// One pass over the endpoints, starting from the last good one. A
    /// refusal from one Router does not stop the pass: the credentials may be
    /// good on the next (openWithRetry stops on a refusal after the pass).
    private func openAnyEndpoint() async throws {
        var last: MeshCallError = .lost("Could not connect")
        for offset in 0..<endpoints.count {
            let index = (cursor + offset) % endpoints.count
            do {
                try await open(endpoints[index])
                cursor = index
                return
            } catch let error as MeshCallError {
                last = error
            }
        }
        throw last
    }

    private func open(_ url: URL) async throws {
        let socket = socketFactory(url, options)
        let writer = FrameWriter(socket: socket, maxFrameSize: options.maxFrameSize, ordered: options.e2eKey != nil)
        let timedOut = Flag()
        func arm(_ timeout: Duration) -> Task<Void, Never> {
            Task {
                try? await Task.sleep(for: timeout)
                if Task.isCancelled { return }
                timedOut.set()
                socket.close(code: 1000, reason: "handshake timed out")
            }
        }
        var watchdog = arm(options.handshakeTimeout)
        var confirming = false
        defer { watchdog.cancel() }
        do {
            try await socket.open()
            var hello: [String: JSONValue] = [
                "protocol": .string(protocolVersion), "role": "client", "peer_id": .string(options.peerID),
            ]
            // URLSession may not set Authorization on an upgrade: credentials
            // travel in hello, the path the protocol defines for such peers.
            if let authorization { hello["authorization"] = .string(authorization) }
            try await writer.send(Frame("hello", hello))
            let ack = try await Self.receiveHandshakeFrame(socket, writer)
            guard ack.kind == "hello.ack", ack["protocol"]?.stringValue == protocolVersion else {
                throw MeshCallError.protocolViolation("Expected compatible hello.ack")
            }
            var session: E2ESession?
            if let key = options.e2eKey {
                // On an E2E tunnel the instance must answer the handshake
                // towards the pinned key before any call is made.
                let callID = newID()
                let handshake = try ClientHandshake(
                    pinnedKey: key, callID: callID, credential: options.credentials?.token ?? "",
                    identity: options.e2eIdentity, paired: paired)
                try await writer.send(handshake.frame)
                let established = try handshake.finish(try await Self.receiveAnswer(callID, socket, writer))
                if handshake.pairing != nil {
                    // Prove the PSK; the instance accepts this client only then,
                    // possibly after asking a person: a longer timeout.
                    watchdog.cancel()
                    confirming = true
                    watchdog = arm(e2eConfirmTimeout)
                    let confirmID = newID()
                    try await writer.send(try ClientHandshake.confirmFrame(session: established, callID: confirmID))
                    try ClientHandshake.finishConfirm(try await Self.receiveAnswer(confirmID, socket, writer))
                    paired = true
                }
                session = established
            }
            if closed { throw MeshCallError(.unavailable, "Client is closed") }
            install(Connection(socket: socket, writer: writer, e2e: session))
        } catch {
            let failure =
                timedOut.isSet
                ? MeshCallError(
                    .deadlineExceeded,
                    confirming ? "End-to-end pairing confirmation timed out" : "MeshCall handshake timed out")
                : Self.connectFailure(error)
            writer.close(failure)
            socket.close(code: 1000, reason: "")
            throw failure
        }
    }

    /// The answer to the `TunnelHost/handshake` call `callID`.
    private static func receiveAnswer(_ callID: String, _ socket: WebSocketConnection, _ writer: FrameWriter)
        async throws -> Frame
    {
        while true {
            let frame = try await receiveHandshakeFrame(socket, writer)
            if frame.callID == callID { return frame }
        }
    }

    private static func receiveHandshakeFrame(_ socket: WebSocketConnection, _ writer: FrameWriter) async throws -> Frame {
        while true {
            let frame: Frame
            do {
                frame = try Frame.decode(try await socket.receive())
            } catch let error as FrameDecodeError {
                throw MeshCallError.protocolViolation(error.message)
            }
            if frame.kind == "ping" {
                _ = try? writer.enqueue(Frame("pong", ["nonce": frame["nonce"] ?? ""]))
                continue
            }
            if frame.kind == "pong" { continue }
            return frame
        }
    }

    private static func connectFailure(_ error: Error) -> MeshCallError {
        if let error = error as? MeshCallError { return error }
        if let closed = error as? SocketClosed {
            if let status = closed.status {
                // The Router refused the upgrade (401, 403, 429, 503): say why.
                return handshakeError(status: status)
                    ?? MeshCallError(.unavailable, "Unexpected server response: \(status)", retryable: true)
            }
            return closeError(code: closed.code, reason: closed.reason)
                ?? MeshCallError(
                    .unavailable, "Could not connect: " + (closed.underlying ?? "connection closed"), retryable: true)
        }
        return MeshCallError(.unavailable, "Could not connect: \(error)", retryable: true)
    }

    private func install(_ conn: Connection) {
        connection = conn
        conn.reader = Task { [weak self] in
            while true {
                do {
                    let data = try await conn.socket.receive()
                    guard let self else { return }
                    await self.handleMessage(data, conn)
                } catch {
                    await self?.connectionLost(conn, connectionError(error))
                    return
                }
            }
        }
        schedulePing(conn)
    }

    /// `connect()` bounded by the caller's local budget and its task's
    /// cancellation. The attempt itself is shared and keeps going for others.
    private func connect(within budget: Duration?) async throws {
        if connection != nil { return }
        let race = Race()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                race.start(continuation)
                Task {
                    do {
                        try await self.connect()
                        race.finish(.success(()))
                    } catch {
                        race.finish(.failure(error))
                    }
                }
                if let budget {
                    Task {
                        try? await Task.sleep(for: budget)
                        race.finish(.failure(MeshCallError.localDeadline()))
                    }
                }
            }
        } onCancel: {
            race.finish(.failure(MeshCallError.callCancelled()))
        }
    }

    private func connectionLost(_ conn: Connection, _ error: MeshCallError) {
        guard connection === conn else { return }
        connection = nil
        stopLiveness(conn, alive: false)
        conn.writer.close(error)
        if error.code == .unauthenticated { refused = error }
        rejectAll(error)
        sealedCalls.removeAll()
        // Start the next connect from the following endpoint: the one that
        // just died is the least likely to come back first.
        if endpoints.count > 1 { cursor = (cursor + 1) % endpoints.count }
    }

    /// Drop the connection (its end-to-end session is stale); the next call reconnects.
    private func resetConnection() {
        guard let conn = connection else { return }
        connectionLost(conn, MeshCallError(.unavailable, "End-to-end session reset", retryable: true))
        conn.socket.close(code: 1000, reason: "e2e session reset")
    }

    func close() {
        closed = true
        connecting?.cancel()
        if let conn = connection {
            connectionLost(conn, MeshCallError(.unavailable, "Client is closed"))
            conn.socket.close(code: 1000, reason: "client closed")
        }
        rejectAll(MeshCallError(.unavailable, "Client is closed"))
    }

    // MARK: keepalive

    private func schedulePing(_ conn: Connection) {
        guard let interval = options.pingInterval, conn.pingTask == nil, connection === conn else { return }
        conn.pingTask = Task { [weak self] in
            try? await Task.sleep(for: interval)
            if Task.isCancelled { return }
            await self?.sendPing(conn, timeout: nil)
        }
    }

    private func sendPing(_ conn: Connection, timeout: Duration?) {
        conn.pingTask = nil
        guard connection === conn, conn.pendingNonce == nil else { return }
        let nonce = newID()
        conn.pendingNonce = nonce
        let wait = timeout ?? options.pingTimeout
        conn.pongTask = Task { [weak self] in
            try? await Task.sleep(for: wait)
            if Task.isCancelled { return }
            await self?.keepaliveFailed(conn, "keepalive timeout")
        }
        do {
            _ = try conn.writer.enqueue(Frame("ping", ["nonce": .string(nonce)]))
        } catch {
            keepaliveFailed(conn, "keepalive send failed")
        }
    }

    private func receivePong(_ conn: Connection, nonce: String?) {
        guard nonce != nil, nonce == conn.pendingNonce else { return }  // pongs nobody asked for are ignored
        conn.pendingNonce = nil
        conn.pongTask?.cancel()
        conn.pongTask = nil
        for probe in conn.probes { probe.resume(returning: true) }
        conn.probes = []
        schedulePing(conn)
    }

    private func keepaliveFailed(_ conn: Connection, _ reason: String) {
        guard connection === conn else { return }
        connectionLost(conn, .lost("Connection was lost: " + reason))
        conn.socket.close(code: 1001, reason: reason)
    }

    private func stopLiveness(_ conn: Connection, alive: Bool) {
        conn.pingTask?.cancel()
        conn.pongTask?.cancel()
        conn.pingTask = nil
        conn.pongTask = nil
        for probe in conn.probes { probe.resume(returning: alive) }
        conn.probes = []
    }

    /// Ping now and wait for the pong: `false` (and the connection dropped,
    /// failing its calls with a retryable `unavailable`) when none arrives in
    /// time or there is no connection. The next call reconnects.
    func checkConnection(timeout: Duration) async -> Bool {
        guard let conn = connection else { return false }
        if conn.pendingNonce == nil {
            conn.pingTask?.cancel()
            sendPing(conn, timeout: min(timeout, options.pingTimeout))
        }
        guard connection === conn else { return false }
        return await withCheckedContinuation { continuation in conn.probes.append(continuation) }
    }

    // MARK: receiving

    private func handleMessage(_ data: Data, _ conn: Connection) {
        guard connection === conn else { return }
        let frame: Frame
        do {
            frame = try Frame.decode(data)
        } catch {
            // A malformed frame that still names a call fails only that call.
            let decodeError = error as? FrameDecodeError
            if let callID = decodeError?.callID, calls[callID] != nil || streams[callID] != nil {
                let failure = MeshCallError.protocolViolation(decodeError?.message ?? "Invalid MeshCall frame")
                rejectPending(callID, failure)
                if let stream = streams[callID] { fail(stream, failure) }
                return
            }
            connectionLost(conn, MeshCallError.protocolViolation(decodeError?.message ?? "Invalid MeshCall frame"))
            conn.socket.close(code: 1002, reason: "Invalid MeshCall frame")
            return
        }
        switch frame.kind {
        case "ping":
            _ = try? conn.writer.enqueue(Frame("pong", ["nonce": frame["nonce"] ?? ""]))
            return
        case "pong":
            receivePong(conn, nonce: frame["nonce"]?.stringValue)
            return
        default: break
        }
        if conn.e2e != nil, let callID = frame.callID, let sealed = sealedCalls[callID] {
            let opened: Frame
            do {
                opened = try sealed.open(frame)
            } catch {
                // A frame the instance did not seal: not from the other end.
                // The session stays; only this call ends.
                let failure = MeshCallError.detailed(
                    .unauthenticated, "Sealed frame rejected: \(error)", source: "client", reason: "e2e_frame_rejected")
                cancelPending(callID, failure, reason: "e2e_frame_rejected")
                if let stream = streams[callID] { abort(stream, failure, reason: "e2e_frame_rejected") }
                return
            }
            route(opened)
            if opened.kind == "call.error", opened["error"]?["details"]?["reason"]?.stringValue == "e2e_session_unknown" {
                // The instance lost the session: whatever the call shape, the
                // next call handshakes again on a new connection.
                resetConnection()
            }
            return
        }
        route(frame)
    }

    private func route(_ frame: Frame) {
        guard let callID = frame.callID else { return }
        switch frame.kind {
        case "stream.window": receiveWindow(frame, callID)
        case "stream.item": receiveItem(frame, callID)
        case "stream.end": receiveEnd(frame, callID)
        case "call.result": receiveResult(frame, callID)
        case "call.error": receiveError(frame, callID)
        default: break
        }
    }

    private func receiveWindow(_ frame: Frame, _ callID: String) {
        let credit = frame["credit"]?.intValue ?? 0
        if let pending = calls[callID] {
            guard frame.direction == "client", let input = pending.input else {
                rejectPending(callID, MeshCallError.protocolViolation("Invalid input stream window"))
                return
            }
            do { try input.credit.grant(credit) } catch { rejectPending(callID, error) }
            return
        }
        guard let stream = streams[callID] else { return }
        guard let input = stream.input, frame.direction == "client" else {
            fail(stream, MeshCallError.protocolViolation("Unexpected output stream window"))
            return
        }
        do { try input.credit.grant(credit) } catch { fail(stream, error) }
    }

    private func receiveItem(_ frame: Frame, _ callID: String) {
        guard let stream = streams[callID] else {
            if calls[callID] != nil { rejectPending(callID, MeshCallError.protocolViolation("Unexpected output stream item")) }
            return
        }
        guard !stream.terminal, frame.direction == "server" else {
            fail(stream, MeshCallError.protocolViolation("Invalid output stream item"))
            return
        }
        guard !stream.ended, stream.allowance > 0 else {
            fail(stream, MeshCallError.protocolViolation("Output stream exceeded granted credit"))
            return
        }
        let sequence = frame["sequence"]?.intValue ?? -1
        guard sequence == stream.expectedSequence else {
            fail(stream, MeshCallError.protocolViolation("Expected server sequence \(stream.expectedSequence), got \(sequence)"))
            return
        }
        stream.allowance -= 1
        stream.expectedSequence += 1
        armIdle(stream)
        stream.push(frame.payload)
    }

    private func receiveEnd(_ frame: Frame, _ callID: String) {
        guard let stream = streams[callID] else {
            if calls[callID] != nil { rejectPending(callID, MeshCallError.protocolViolation("Unexpected output stream end")) }
            return
        }
        guard !stream.terminal, frame.direction == "server", !stream.ended else {
            fail(stream, MeshCallError.protocolViolation("Invalid output stream end"))
            return
        }
        stream.ended = true
        armIdle(stream)
        // The consumer sees the end once the terminal frame (with trailers) arrives.
    }

    private func receiveResult(_ frame: Frame, _ callID: String) {
        if let stream = streams[callID] {
            guard !stream.terminal else { return }
            guard stream.ended else {
                fail(stream, MeshCallError.protocolViolation("Server result arrived before output stream end"))
                return
            }
            stream.terminal = true
            stream.trailers = frame.metadata
            cleanup(stream)
            stream.finish(.success(()))
            return
        }
        guard let pending = takePending(callID, closing: MeshCallError.callCancelled("Call is complete")) else { return }
        pending.trailers = frame.metadata
        pending.promise.settle(.success(frame.payload))
    }

    private func receiveError(_ frame: Frame, _ callID: String) {
        let payload = frame["error"]?.objectValue ?? [:]
        let error = MeshCallError(
            ErrorCode(payload["code"]?.stringValue ?? "internal"), payload["message"]?.stringValue ?? "",
            retryable: payload["retryable"]?.boolValue ?? false, details: payload["details"]?.objectValue ?? [:])
        if let stream = streams[callID] {
            fail(stream, error, trailers: frame.metadata)
            return
        }
        guard let pending = takePending(callID, closing: error) else { return }
        pending.trailers = frame.metadata
        pending.promise.settle(.failure(error))
    }

    // MARK: sending

    private func send(_ frame: Frame) throws -> FrameWriter.Ticket {
        guard let conn = connection else { throw MeshCallError(.unavailable, "Client is not connected", retryable: true) }
        guard conn.e2e != nil else { return try conn.writer.enqueue(frame) }
        // Taken now: a call settling meanwhile must not let its content out in clear.
        let sealed = frame.callID.flatMap { sealedCalls[$0] }
        guard let sealed else {
            let service = frame["service"]?.stringValue ?? ""
            if frame.kind == "stream.item" || (frame.kind == "call.open" && !service.hasPrefix(routerServicePrefix)) {
                throw MeshCallError.callCancelled("Call is closed")
            }
            return try conn.writer.enqueue(frame)
        }
        return try conn.writer.enqueue(try sealed.seal(frame))
    }

    private func sendBestEffort(_ frame: Frame) {
        _ = try? send(frame)
    }

    private func openFrame(
        _ callID: String, _ service: String, _ method: String, _ request: JSONValue, _ options: CallOptions
    ) -> Frame {
        // Every frame of a call opened on an E2E connection is sealed under its
        // session. The Router answers its own services itself: those stay clear.
        if let session = connection?.e2e, !service.hasPrefix(routerServicePrefix) {
            sealedCalls[callID] = SealedCall(session: session, service: service, method: method)
        }
        var fields: [String: JSONValue] = [
            "call_id": .string(callID), "service": .string(service), "method": .string(method), "payload": request,
        ]
        if let timeout = options.timeout { fields["timeout_ms"] = .int(max(1, timeout.roundedMilliseconds)) }
        if let instance = options.instance { fields["instance_id"] = .string(instance) }
        if let idle = options.idleTimeout { fields["idle_timeout_ms"] = .int(max(1, idle.roundedMilliseconds)) }
        if !options.metadata.isEmpty { fields["metadata"] = .object(options.metadata.mapValues { .string($0) }) }
        if let key = options.idempotencyKey { fields["idempotency_key"] = .string(key) }
        return Frame("call.open", fields)
    }

    /// Time left in the client-side budget, which started before connecting.
    private func localBudget(_ options: CallOptions, since start: ContinuousClock.Instant) -> Duration? {
        guard let timeout = options.timeout else { return nil }
        let grace = self.options.deadlineGrace ?? clientDeadlineGrace(timeout)
        return max(.zero, timeout + grace - (ContinuousClock.now - start))
    }

    // MARK: unary and client-stream calls

    /// Open a unary or client-stream call; the result is awaited with `result(of:)`.
    func openCall(
        service: String, method: String, request: JSONValue, options: CallOptions, input: Bool
    ) async throws -> PendingCall {
        // Again here: interceptors may have added metadata after Mesh validated it.
        try validateMetadata(options.metadata)
        let start = ContinuousClock.now
        try await connect(within: localBudget(options, since: start))
        if Task.isCancelled { throw MeshCallError.callCancelled() }
        let callID = newID()
        let pending = PendingCall(callID: callID, input: input ? InputState() : nil)
        if let budget = localBudget(options, since: start) {
            pending.timer = Task { [weak self] in
                try? await Task.sleep(for: budget)
                if Task.isCancelled { return }
                await self?.cancelPending(callID, MeshCallError.localDeadline(), reason: "deadline_exceeded")
            }
        }
        calls[callID] = pending
        do {
            _ = try send(openFrame(callID, service, method, request, options))
        } catch {
            rejectPending(callID, error)
        }
        return pending
    }

    func result(of pending: PendingCall) async throws -> JSONValue {
        try await withCheckedThrowingContinuation { pending.promise.wait($0) }
    }

    /// Send one input item once the server granted credit for it.
    func sendInput(_ pending: PendingCall, _ item: JSONValue) async throws {
        guard let input = pending.input, !pending.closed else { throw MeshCallError.callCancelled("Call is closed") }
        try await withCheckedThrowingContinuation { input.credit.acquire($0) }
        guard !pending.closed else { throw MeshCallError.callCancelled("Call is closed") }
        let sequence = input.sequence
        input.sequence += 1
        do {
            _ = try send(
                Frame(
                    "stream.item",
                    [
                        "call_id": .string(pending.callID), "direction": "client", "sequence": .int(sequence),
                        "payload": item,
                    ]))
        } catch let error as MeshCallError where error.isFrameTooLarge {
            cancelPending(pending.callID, error, reason: "frame_too_large")  // the call is open at the peer
            throw error
        }
    }

    func closeInput(_ pending: PendingCall) {
        guard let input = pending.input, !pending.closed, !input.closed else { return }
        input.closed = true
        sendBestEffort(Frame("stream.end", ["call_id": .string(pending.callID), "direction": "client"]))
    }

    func cancel(_ pending: PendingCall, _ error: Error, reason: String) {
        cancelPending(pending.callID, error, reason: reason)
    }

    private func cancelPending(_ callID: String, _ error: Error, reason: String) {
        guard let pending = takePending(callID, closing: error) else { return }
        sendBestEffort(Frame("call.cancel", ["call_id": .string(callID), "reason": .string(reason)]))
        pending.promise.settle(.failure(error))
    }

    private func rejectPending(_ callID: String, _ error: Error) {
        takePending(callID, closing: error)?.promise.settle(.failure(error))
    }

    private func takePending(_ callID: String, closing error: Error) -> PendingCall? {
        guard let pending = calls.removeValue(forKey: callID) else { return nil }
        sealedCalls.removeValue(forKey: callID)
        pending.closed = true
        pending.input?.credit.close(error)
        pending.timer?.cancel()
        return pending
    }

    private func rejectAll(_ error: Error) {
        for callID in Array(calls.keys) { rejectPending(callID, error) }
        for stream in Array(streams.values) { fail(stream, error) }
    }

    // MARK: server streams and duplex calls

    func openStream(
        service: String, method: String, request: JSONValue, options: CallOptions, duplex: Bool
    ) async throws -> StreamState {
        try validateMetadata(options.metadata)
        let start = ContinuousClock.now
        try await connect(within: localBudget(options, since: start))
        if Task.isCancelled { throw MeshCallError.callCancelled() }
        let callID = newID()
        let stream = StreamState(callID: callID, duplex: duplex)
        streams[callID] = stream
        do {
            _ = try send(openFrame(callID, service, method, request, options))
            stream.allowance += initialStreamCredit
            _ = try send(
                Frame(
                    "stream.window",
                    ["call_id": .string(callID), "direction": "server", "credit": .int(initialStreamCredit)]))
        } catch {
            fail(stream, error)
            throw error
        }
        if let budget = localBudget(options, since: start) {
            stream.timer = Task { [weak self] in
                try? await Task.sleep(for: budget)
                if Task.isCancelled { return }
                await self?.abort(stream, MeshCallError.localDeadline(), reason: "deadline_exceeded")
            }
        }
        stream.idleTimeout = options.idleTimeout
        armIdle(stream)
        return stream
    }

    /// The next output item, or `nil` once the call ended successfully.
    func next(_ stream: StreamState) async throws -> JSONValue? {
        let item = try await withCheckedThrowingContinuation { stream.next($0) }
        if item != nil, !stream.ended, !stream.terminal {
            // Credit follows consumption: a slow reader slows the server down.
            stream.allowance += 1
            do {
                _ = try send(Frame("stream.window", ["call_id": .string(stream.callID), "direction": "server", "credit": 1]))
            } catch {
                abort(stream, error, reason: "window_send_failed")
                throw error
            }
        }
        return item
    }

    func trailers(_ stream: StreamState) -> [String: String] { stream.trailers }

    func sendInput(_ stream: StreamState, _ item: JSONValue) async throws {
        guard let input = stream.input else { throw MeshCallError.protocolViolation("Call has no input stream") }
        guard !input.closed else { throw MeshCallError.protocolViolation("Input stream is closed") }
        try await withCheckedThrowingContinuation { input.credit.acquire($0) }
        guard !stream.terminal else { throw MeshCallError.callCancelled("Call is closed") }
        // Claimed before sending: concurrent sends stay in order.
        let sequence = input.sequence
        input.sequence += 1
        do {
            _ = try send(
                Frame(
                    "stream.item",
                    [
                        "call_id": .string(stream.callID), "direction": "client", "sequence": .int(sequence),
                        "payload": item,
                    ]))
        } catch let error as MeshCallError where error.isFrameTooLarge {
            abort(stream, error, reason: "frame_too_large")  // the call is open at the peer
            throw error
        }
    }

    func closeInput(_ stream: StreamState) throws {
        guard let input = stream.input else { throw MeshCallError.protocolViolation("Call has no input stream") }
        guard !input.closed, !stream.terminal else { return }
        input.closed = true
        _ = try send(Frame("stream.end", ["call_id": .string(stream.callID), "direction": "client"]))
    }

    /// The caller is done with the stream: end it and tell the server.
    func cancel(_ stream: StreamState, reason: String) {
        guard !stream.terminal else { return }
        stream.terminal = true
        cleanup(stream)
        stream.finish(.success(()))
        sendBestEffort(Frame("call.cancel", ["call_id": .string(stream.callID), "reason": .string(reason)]))
    }

    /// Fail the stream locally and tell the server.
    func abort(_ stream: StreamState, _ error: Error, reason: String) {
        guard !stream.terminal else { return }
        stream.terminal = true
        cleanup(stream)
        stream.finish(.failure(error))
        sendBestEffort(Frame("call.cancel", ["call_id": .string(stream.callID), "reason": .string(reason)]))
    }

    private func fail(_ stream: StreamState, _ error: Error, trailers: [String: String] = [:]) {
        guard !stream.terminal else { return }
        stream.terminal = true
        stream.trailers = trailers
        cleanup(stream)
        stream.finish(.failure(error))
    }

    /// (Re)start the gap timer; measured between arrivals, not between reads.
    private func armIdle(_ stream: StreamState) {
        stream.idleTimer?.cancel()
        stream.idleTimer = nil
        guard let idle = stream.idleTimeout, !stream.ended, !stream.terminal else { return }
        stream.idleTimer = Task { [weak self] in
            try? await Task.sleep(for: idle)
            if Task.isCancelled { return }
            await self?.abort(stream, MeshCallError.localDeadline(idle: true), reason: "deadline_exceeded")
        }
    }

    private func cleanup(_ stream: StreamState) {
        stream.input?.credit.close(MeshCallError.callCancelled("Call is closed"))
        stream.timer?.cancel()
        stream.idleTimer?.cancel()
        stream.timer = nil
        stream.idleTimer = nil
        streams.removeValue(forKey: stream.callID)
        sealedCalls.removeValue(forKey: stream.callID)
    }

    // MARK: E2E session handling for unary calls

    /// Whether `error` means the instance lost this client's E2E session; the
    /// connection was already reset, so a unary call may be retried once.
    nonisolated static func lostSession(_ error: Error) -> Bool {
        (error as? MeshCallError).map { $0.code == .unauthenticated && $0.reason == "e2e_session_unknown" } ?? false
    }
}

let initialStreamCredit: Int64 = 16

extension Duration {
    var roundedMilliseconds: Int64 {
        let (seconds, attoseconds) = components
        return seconds * 1000 + Int64((Double(attoseconds) / 1e15).rounded())
    }
}
