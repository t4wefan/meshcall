import Foundation

/// A server-to-client stream. Iterate it with `for try await`; leaving the
/// loop early (or releasing the stream) cancels the call. Credit follows
/// consumption, so a slow reader slows the server down instead of buffering.
public class ServerStream<Item: Decodable & Sendable>: AsyncSequence, @unchecked Sendable {
    public typealias Element = Item

    let core: ClientCore
    let state: StreamState
    /// Keeps the connection open while the stream is in use, even if the
    /// Mesh that opened it was released.
    let lifetime: CoreLifetime?

    init(core: ClientCore, state: StreamState, lifetime: CoreLifetime? = nil) {
        self.core = core
        self.state = state
        self.lifetime = lifetime
    }

    deinit {
        let core = core
        let state = state
        Task { await core.cancel(state, reason: "iterator_closed") }
    }

    /// A class so that leaving a `for try await` loop, which releases the
    /// iterator, cancels the call even while the stream itself is kept (as
    /// TypeScript's iterator `return()` does). After the stream ended the
    /// cancel is a no-op.
    public final class AsyncIterator: AsyncIteratorProtocol {
        let stream: ServerStream<Item>

        init(stream: ServerStream<Item>) { self.stream = stream }

        deinit {
            let core = stream.core
            let state = stream.state
            Task { await core.cancel(state, reason: "iterator_closed") }
        }

        public func next() async throws -> Item? {
            let core = stream.core
            let state = stream.state
            let value = try await withTaskCancellationHandler {
                try await core.next(state)
            } onCancel: {
                Task { await core.abort(state, MeshCallError.callCancelled(), reason: "caller_cancelled") }
            }
            guard let value else { return nil }
            do {
                return try decodeReceived(value, as: Item.self, reason: "invalid_output_item")
            } catch {
                await core.abort(state, error, reason: "invalid_output_item")
                throw error
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(stream: self) }

    /// Server trailers; populated once the stream has ended.
    public var trailers: [String: String] {
        get async { await core.trailers(state) }
    }

    /// End the call now; iteration then finishes.
    public func cancel() async {
        await core.cancel(state, reason: "client_closed")
    }
}

/// A duplex call: `send` input items while iterating output items. The output
/// ends when the call ends successfully; a failure is thrown from the
/// iteration. Read output concurrently with sending: a caller that only sends
/// stalls once the server's output credit runs out.
public final class DuplexCall<Input: Encodable & Sendable, Output: Decodable & Sendable>: ServerStream<Output>,
    @unchecked Sendable
{
    /// Send one input item; waits while the server has granted no credit.
    public func send(_ item: Input) async throws {
        let value = try JSONValue(encoding: item)
        let core = core
        let state = state
        try await withTaskCancellationHandler {
            try await core.sendInput(state, value)
        } onCancel: {
            Task { await core.abort(state, MeshCallError.callCancelled(), reason: "caller_cancelled") }
        }
    }

    /// Half-close the input; the server's iteration of it then ends.
    public func closeSend() async throws {
        try await core.closeInput(state)
    }
}

/// Decode what the server sent with this client's types: a mismatch is version
/// skew between the two, reported as `protocol_error` from the client.
func decodeReceived<T: Decodable>(_ value: JSONValue, as type: T.Type, reason: String) throws -> T {
    do {
        return try value.decode(as: T.self)
    } catch let error as DecodingError {
        var failure = MeshCallError.detailed(
            .protocolError, reason == "invalid_output_item" ? "Invalid output stream item" : "Invalid response",
            source: "client", reason: reason)
        failure = failure.with(details: ["violations": .array([violation(of: error)])])
        throw failure
    }
}

/// A violation naming the field, never the value that failed.
private func violation(of error: DecodingError) -> JSONValue {
    func path(_ codingPath: [CodingKey]) -> String {
        codingPath.map { $0.intValue.map(String.init) ?? $0.stringValue }.joined(separator: ".")
    }
    let (where_, code, message): (String, String, String)
    switch error {
    case .typeMismatch(let type, let context):
        (where_, code, message) = (path(context.codingPath), "type", "expected \(type)")
    case .valueNotFound(let type, let context):
        (where_, code, message) = (path(context.codingPath), "missing", "expected \(type), found null")
    case .keyNotFound(let key, let context):
        (where_, code, message) = (path(context.codingPath + [key]), "missing", "field required")
    case .dataCorrupted(let context):
        (where_, code, message) = (path(context.codingPath), "invalid", "invalid value")
    @unknown default:
        (where_, code, message) = ("", "invalid", "invalid value")
    }
    return ["path": .string(where_), "code": .string(code), "message": .string(message)]
}
