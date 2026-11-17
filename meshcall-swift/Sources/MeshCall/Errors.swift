import Foundation

/// A protocol error code (docs/protocol.md → Errors). Application codes are
/// plain strings: `ErrorCode("app.v1.quota_exceeded")`.
public struct ErrorCode: RawRepresentable, Hashable, Sendable, ExpressibleByStringLiteral, CustomStringConvertible {
    public let rawValue: String

    public init(rawValue: String) { self.rawValue = rawValue }
    public init(_ rawValue: String) { self.rawValue = rawValue }
    public init(stringLiteral value: String) { self.rawValue = value }

    public var description: String { rawValue }

    public static let cancelled: ErrorCode = "cancelled"
    public static let deadlineExceeded: ErrorCode = "deadline_exceeded"
    public static let invalidArgument: ErrorCode = "invalid_argument"
    public static let methodNotFound: ErrorCode = "method_not_found"
    public static let permissionDenied: ErrorCode = "permission_denied"
    public static let unauthenticated: ErrorCode = "unauthenticated"
    public static let resourceExhausted: ErrorCode = "resource_exhausted"
    public static let unavailable: ErrorCode = "unavailable"
    public static let protocolError: ErrorCode = "protocol_error"
    public static let `internal`: ErrorCode = "internal"
}

/// One field-level validation finding; never carries the input value.
public struct Violation: Sendable, Hashable {
    /// Dotted path of the offending field; `""` for the root.
    public let path: String
    /// Short validator keyword: `type`, `missing`, …
    public let code: String
    public let message: String
}

/// An RPC error: a stable `code`, a message and `details`.
///
/// Match on the code, never on the message:
///
///     catch let error as MeshCallError where error.code == .unavailable
public struct MeshCallError: Error, Sendable, Hashable, CustomStringConvertible, LocalizedError {
    public let code: ErrorCode
    public let message: String
    public let retryable: Bool
    public let details: [String: JSONValue]

    public init(_ code: ErrorCode, _ message: String, retryable: Bool = false, details: [String: JSONValue] = [:]) {
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details
    }

    /// `details.source`: `"client"`, `"router"` or `"server"`.
    public var source: String? { details["source"]?.stringValue }
    /// `details.reason` when the sender supplied one.
    public var reason: String? { details["reason"]?.stringValue }
    /// `details.instance_id`, for errors about one instance.
    public var instanceID: String? { details["instance_id"]?.stringValue }
    /// `details.attempts` when the retry policy made more than one attempt.
    public var attempts: Int? { details["attempts"]?.intValue.map { Int($0) } }

    /// Field-level findings when the server (or this client) validated a payload.
    public var violations: [Violation] {
        guard let raw = details["violations"]?.arrayValue else { return [] }
        return raw.compactMap { item in
            guard let record = item.objectValue else { return nil }
            return Violation(
                path: record["path"]?.stringValue ?? "",
                code: record["code"]?.stringValue ?? "",
                message: record["message"]?.stringValue ?? "")
        }
    }

    public var description: String {
        var text = "\(code.rawValue): \(message)"
        if let reason { text += " (\(reason))" }
        return text
    }

    public var errorDescription: String? { description }

    func with(details extra: [String: JSONValue]) -> MeshCallError {
        MeshCallError(code, message, retryable: retryable, details: details.merging(extra) { _, new in new })
    }

    static func detailed(
        _ code: ErrorCode, _ message: String, source: String, reason: String? = nil, retryable: Bool = false
    ) -> MeshCallError {
        var details: [String: JSONValue] = ["source": .string(source)]
        if let reason { details["reason"] = .string(reason) }
        return MeshCallError(code, message, retryable: retryable, details: details)
    }

    static func lost(_ message: String = "Connection was lost") -> MeshCallError {
        MeshCallError(.unavailable, message, retryable: true)
    }

    static func callCancelled(_ message: String = "Call was cancelled") -> MeshCallError {
        MeshCallError(.cancelled, message)
    }

    static func localDeadline(idle: Bool = false) -> MeshCallError {
        idle
            ? detailed(.deadlineExceeded, "Output stream idle timeout", source: "client", reason: "idle")
            : detailed(.deadlineExceeded, "Call deadline exceeded", source: "client")
    }

    static func protocolViolation(_ message: String) -> MeshCallError {
        MeshCallError(.protocolError, message)
    }

    /// Whether this is a frame `FrameWriter` did not send because it was over the size limit.
    var isFrameTooLarge: Bool { code == .resourceExhausted && reason == "frame_too_large" }
}

/// WebSocket close status for credentials that are not, or no longer, valid.
let closeUnauthenticated = 4401
/// Close reasons that name an error reason (docs/protocol.md → Tunnels).
private let closeReasons: Set<String> = ["tunnel_revoked", "tunnel_closed", "key_changed", "token_revoked"]

/// The error a Router close status stands for, or `nil` for a plain loss.
func closeError(code: Int, reason: String) -> MeshCallError? {
    if code == closeUnauthenticated {
        var details: [String: JSONValue] = ["source": "router"]
        if closeReasons.contains(reason) { details["reason"] = .string(reason) }
        return MeshCallError(
            .unauthenticated, "The Router closed the connection: " + (reason.isEmpty ? "unauthenticated" : reason),
            details: details)
    }
    if code == 1008 {
        return .detailed(
            .permissionDenied, "The Router refused the connection: " + (reason.isEmpty ? "policy violation" : reason),
            source: "router", reason: "policy_violation")
    }
    // Credentials in hello are refused like their header counterparts: 4000 +
    // the HTTP status (403, 429, 503). The last two are temporary.
    if code == 4403 || code == 4429 || code == 4503 { return handshakeError(status: code - 4000, body: reason) }
    return nil
}

/// The error an HTTP status refusing a WebSocket upgrade stands for.
func handshakeError(status: Int, body: String = "") -> MeshCallError? {
    let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
    let text = " (HTTP \(status))" + (trimmed.isEmpty ? "" : ": " + trimmed)
    let details: [String: JSONValue] = ["source": "router"]
    switch status {
    case 401: return MeshCallError(.unauthenticated, "The Router refused the credentials" + text, details: details)
    case 403: return MeshCallError(.permissionDenied, "The Router refused the connection" + text, details: details)
    case 429:
        return MeshCallError(.resourceExhausted, "The Router refused the connection" + text, retryable: true, details: details)
    case 503: return MeshCallError(.unavailable, "The Router is busy" + text, retryable: true, details: details)
    default: return nil
    }
}
