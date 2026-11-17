import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// How the Router (or the network) ended a connection.
struct SocketClosed: Error {
    let code: Int
    let reason: String
    /// The HTTP status of a refused upgrade, when the socket never opened.
    let status: Int?
    let underlying: String?
}

/// A message-oriented connection: `URLSessionWebSocketTask` in the app, a
/// scripted peer in tests.
protocol WebSocketConnection: AnyObject, Sendable {
    /// Waits for the upgrade to complete.
    func open() async throws
    func send(_ text: String) async throws
    /// The next text message, or `SocketClosed` once the connection ended.
    func receive() async throws -> Data
    func close(code: Int, reason: String)
}

/// `URLSessionWebSocketTask` with the close status kept, so a 4401 from the
/// Router reaches the caller as the error it stands for.
final class URLSessionWebSocket: NSObject, WebSocketConnection, URLSessionWebSocketDelegate, @unchecked Sendable {
    private let lock = NSLock()
    private var session: URLSession!
    private var task: URLSessionWebSocketTask!
    private var opened: CheckedContinuation<Void, Error>?
    private var didOpen = false
    private var closedInfo: SocketClosed?

    init(url: URL, maxFrameSize: Int, handshakeTimeout: Duration) {
        super.init()
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = max(1, handshakeTimeout.seconds)
        configuration.waitsForConnectivity = false
        configuration.httpCookieStorage = nil
        configuration.urlCache = nil
        // The delegate queue is serial; this object guards its state with a lock.
        let queue = OperationQueue()
        queue.maxConcurrentOperationCount = 1
        session = URLSession(configuration: configuration, delegate: self, delegateQueue: queue)
        var request = URLRequest(url: url)
        request.timeoutInterval = max(1, handshakeTimeout.seconds)
        task = session.webSocketTask(with: request)
        task.maximumMessageSize = maxFrameSize
    }

    func open() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            lock.lock()
            if didOpen {
                lock.unlock()
                continuation.resume()
                return
            }
            if let closedInfo {
                lock.unlock()
                continuation.resume(throwing: closedInfo)
                return
            }
            opened = continuation
            lock.unlock()
            task.resume()
        }
    }

    func send(_ text: String) async throws {
        do {
            try await task.send(.string(text))
        } catch {
            throw await closeStatus(after: error)
        }
    }

    func receive() async throws -> Data {
        do {
            switch try await task.receive() {
            case .string(let text): return Data(text.utf8)
            case .data(let data): return data
            @unknown default: return Data()
            }
        } catch {
            throw await closeStatus(after: error)
        }
    }

    func close(code: Int, reason: String) {
        let closeCode = URLSessionWebSocketTask.CloseCode(rawValue: code) ?? .normalClosure
        task.cancel(with: closeCode, reason: Data(reason.utf8))
        // A peer that is truly gone never completes the close handshake.
        let session = session
        DispatchQueue.global().asyncAfter(deadline: .now() + 1) { [weak self] in
            self?.finish(SocketClosed(code: code, reason: reason, status: nil, underlying: nil))
            session?.invalidateAndCancel()
        }
    }

    /// The close status that explains a failed send or receive. The delegate
    /// usually reports it just before or after the failure itself.
    private func closeStatus(after error: Error) async -> SocketClosed {
        for _ in 0..<30 {
            if let closed = currentClosed() { return closed }
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
        let code = task.closeCode.rawValue
        let reason = task.closeReason.map { String(decoding: $0, as: UTF8.self) } ?? ""
        finish(SocketClosed(code: code, reason: reason, status: nil, underlying: String(describing: error)))
        return currentClosed()!
    }

    private func currentClosed() -> SocketClosed? {
        lock.lock()
        defer { lock.unlock() }
        return closedInfo
    }

    private func finish(_ closed: SocketClosed) {
        lock.lock()
        if closedInfo != nil {
            lock.unlock()
            return
        }
        closedInfo = closed
        let opening = opened
        opened = nil
        lock.unlock()
        opening?.resume(throwing: closed)
    }

    // MARK: URLSessionWebSocketDelegate

    func urlSession(
        _ session: URLSession, webSocketTask: URLSessionWebSocketTask, didOpenWithProtocol protocol: String?
    ) {
        lock.lock()
        didOpen = true
        let opening = opened
        opened = nil
        lock.unlock()
        opening?.resume()
    }

    func urlSession(
        _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
        didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?
    ) {
        finish(
            SocketClosed(
                code: closeCode.rawValue, reason: reason.map { String(decoding: $0, as: UTF8.self) } ?? "",
                status: nil, underlying: nil))
        session.finishTasksAndInvalidate()
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        let status = (task.response as? HTTPURLResponse)?.statusCode
        let code = webSocketTaskCloseCode(task)
        finish(
            SocketClosed(
                code: code, reason: "", status: status == 101 ? nil : status,
                underlying: error.map { String(describing: $0) }))
        session.finishTasksAndInvalidate()
    }

    private func webSocketTaskCloseCode(_ task: URLSessionTask) -> Int {
        (task as? URLSessionWebSocketTask)?.closeCode.rawValue ?? 0
    }
}
