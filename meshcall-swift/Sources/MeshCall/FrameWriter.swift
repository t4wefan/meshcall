import Foundation

/// Sends frames one at a time: control frames (cancels, windows, pings) first,
/// then one frame per call in turn, so a large stream cannot starve the
/// others. `enqueue` is synchronous: frames leave in the order the client
/// produced them, which keeps a call's sequence numbers in order.
final class FrameWriter: @unchecked Sendable {
    private let socket: WebSocketConnection
    private let maxFrameSize: Int
    private let lock = NSLock()
    private var controls: [Pending] = []
    private var calls: [String: [Pending]] = [:]
    private var order: [String] = []
    private var draining = false
    private var closed: MeshCallError?

    private struct Pending {
        let kind: String
        let text: String
        let ticket: Ticket
    }

    /// Settles when the frame was handed to the socket, or failed.
    final class Ticket: @unchecked Sendable {
        private let lock = NSLock()
        private var result: Result<Void, Error>?
        private var waiter: CheckedContinuation<Void, Error>?

        func settle(_ outcome: Result<Void, Error>) {
            lock.lock()
            if result != nil {
                lock.unlock()
                return
            }
            result = outcome
            let waiter = waiter
            self.waiter = nil
            lock.unlock()
            waiter?.resume(with: outcome)
        }

        func wait() async throws {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                lock.lock()
                if let result {
                    lock.unlock()
                    continuation.resume(with: result)
                    return
                }
                waiter = continuation
                lock.unlock()
            }
        }

        static func done() -> Ticket {
            let ticket = Ticket()
            ticket.settle(.success(()))
            return ticket
        }
    }

    init(socket: WebSocketConnection, maxFrameSize: Int) {
        self.socket = socket
        self.maxFrameSize = maxFrameSize
    }

    func send(_ frame: Frame) async throws {
        try await enqueue(frame).wait()
    }

    /// Queue `frame`; throws at once when it can never be sent.
    func enqueue(_ frame: Frame) throws -> Ticket {
        let text = try frame.encoded()
        let size = text.utf8.count
        lock.lock()
        defer { lock.unlock() }
        if let closed { throw closed }
        if frame.kind == "call.cancel", let callID = frame.callID, cancelQueuedCall(callID) {
            return .done()
        }
        if size > maxFrameSize {
            // The peer would close the connection on receiving it; only this
            // frame's call fails (docs/protocol.md → Frame size).
            throw MeshCallError.detailed(
                .resourceExhausted, "Not sent: \(frame.kind) is \(size) bytes, limit \(maxFrameSize)",
                source: "client", reason: "frame_too_large")
        }
        let pending = Pending(kind: frame.kind, text: text, ticket: Ticket())
        // A control frame of a call whose frames are still queued (its
        // call.open, say) must not overtake them: the peer would not know the call.
        if let callID = frame.callID, !frame.isControl || calls[callID] != nil {
            if calls[callID] == nil {
                calls[callID] = []
                order.append(callID)
            }
            calls[callID]!.append(pending)
        } else {
            controls.append(pending)
        }
        if !draining {
            draining = true
            Task { await self.drain() }
        }
        return pending.ticket
    }

    /// Drop a call's unsent frames. A cancel cannot overtake an unsent
    /// `call.open`: both are dropped; an opened call still needs the cancel.
    private func cancelQueuedCall(_ callID: String) -> Bool {
        guard let queue = calls.removeValue(forKey: callID) else { return false }
        order.removeAll { $0 == callID }
        let error = MeshCallError.callCancelled("Call was cancelled before sending")
        for pending in queue { pending.ticket.settle(.failure(error)) }
        return queue.contains { $0.kind == "call.open" }
    }

    func close(_ error: MeshCallError) {
        lock.lock()
        if closed != nil {
            lock.unlock()
            return
        }
        closed = error
        var failed = controls
        controls = []
        for callID in order { failed += calls[callID] ?? [] }
        calls = [:]
        order = []
        lock.unlock()
        for pending in failed { pending.ticket.settle(.failure(error)) }
    }

    private func next() -> Pending? {
        lock.lock()
        defer { lock.unlock() }
        if closed != nil {
            draining = false
            return nil
        }
        if !controls.isEmpty { return controls.removeFirst() }
        guard !order.isEmpty else {
            draining = false
            return nil
        }
        let callID = order.removeFirst()
        var queue = calls[callID]!
        let pending = queue.removeFirst()
        if queue.isEmpty {
            calls.removeValue(forKey: callID)
        } else {
            calls[callID] = queue
            order.append(callID)
        }
        return pending
    }

    private func drain() async {
        while let pending = next() {
            do {
                try await socket.send(pending.text)
                pending.ticket.settle(.success(()))
            } catch {
                let failure = connectionError(error)
                pending.ticket.settle(.failure(failure))
                close(failure)
                socket.close(code: 1000, reason: "send failed")
                lock.withLock { draining = false }
                return
            }
        }
    }
}

/// What a lost or refused connection means for the calls on it.
func connectionError(_ error: Error) -> MeshCallError {
    if let error = error as? MeshCallError { return error }
    if let closed = error as? SocketClosed {
        return closeError(code: closed.code, reason: closed.reason) ?? .lost()
    }
    return .lost("Connection was lost: \(error)")
}
