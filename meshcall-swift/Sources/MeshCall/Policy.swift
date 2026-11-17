import Foundation

let reservedMetadataPrefix = "meshcall-"

/// `meshcall-…` in any letter case: set only by the runtime and the Router.
func isReservedKey(_ key: String) -> Bool {
    key.prefix(reservedMetadataPrefix.count).lowercased() == reservedMetadataPrefix
}

func validateMetadata(_ metadata: [String: String]) throws {
    for key in metadata.keys {
        if key.isEmpty {
            throw MeshCallError.detailed(
                .invalidArgument, "metadata keys must not be empty", source: "client", reason: "invalid_metadata")
        }
        if isReservedKey(key) {
            throw MeshCallError.detailed(
                .invalidArgument, "metadata key \"\(key)\" uses the reserved prefix \"meshcall-\"", source: "client",
                reason: "reserved_metadata")
        }
    }
}

/// Exponential backoff with jitter for re-establishing a lost connection.
/// `maxAttempts` nil means keep trying until the client closes or the
/// call's timeout expires.
public struct ReconnectPolicy: Sendable, Hashable {
    public var enabled: Bool
    public var initialDelay: Duration
    public var maxDelay: Duration
    public var multiplier: Double
    public var jitter: Double
    public var maxAttempts: Int?

    public init(
        enabled: Bool = true, initialDelay: Duration = .milliseconds(100), maxDelay: Duration = .seconds(30),
        multiplier: Double = 2, jitter: Double = 0.2, maxAttempts: Int? = nil
    ) {
        self.enabled = enabled
        self.initialDelay = initialDelay
        self.maxDelay = maxDelay
        self.multiplier = multiplier
        self.jitter = jitter
        self.maxAttempts = maxAttempts
    }

    public static let `default` = ReconnectPolicy()

    func validate() throws {
        if initialDelay <= .zero || maxDelay < initialDelay || multiplier < 1 || jitter < 0 || jitter > 1
            || (maxAttempts.map { $0 < 1 } ?? false)
        {
            throw ConfigurationError("Invalid reconnect policy")
        }
    }

    func delays() -> AnyIterator<Duration> {
        var delay = initialDelay.seconds
        var attempt = 0
        return AnyIterator {
            if let max = maxAttempts, attempt >= max { return nil }
            attempt += 1
            let value = jittered(delay, jitter)
            delay = min(maxDelay.seconds, delay * multiplier)
            return .seconds(value)
        }
    }
}

/// Client-side retry for failed unary calls. Only `retryable` errors with a
/// code in `retryCodes` are retried, and a method not declared `idempotent`
/// only when the call carries an `idempotencyKey`. Attempts share the call's
/// `timeout`. Streams are never retried.
public struct RetryPolicy: Sendable, Hashable {
    public var enabled: Bool
    public var maxAttempts: Int
    public var initialDelay: Duration
    public var maxDelay: Duration
    public var multiplier: Double
    public var jitter: Double
    public var retryCodes: Set<ErrorCode>

    public init(
        enabled: Bool = true, maxAttempts: Int = 3, initialDelay: Duration = .milliseconds(100),
        maxDelay: Duration = .seconds(2), multiplier: Double = 2, jitter: Double = 0.2,
        retryCodes: Set<ErrorCode> = [.unavailable, .resourceExhausted]
    ) {
        self.enabled = enabled
        self.maxAttempts = maxAttempts
        self.initialDelay = initialDelay
        self.maxDelay = maxDelay
        self.multiplier = multiplier
        self.jitter = jitter
        self.retryCodes = retryCodes
    }

    public static let `default` = RetryPolicy()
    public static let disabled = RetryPolicy(enabled: false)

    func delay(attempt: Int) -> Duration {
        let base = min(maxDelay.seconds, initialDelay.seconds * pow(multiplier, Double(attempt - 1)))
        return .seconds(jittered(base, jitter))
    }
}

private func jittered(_ value: Double, _ jitter: Double) -> Double {
    let spread = value * jitter
    return max(0, value + Double.random(in: -1...1) * spread)
}

/// Extra time a client waits past `timeout` for the peer's own deadline error.
func clientDeadlineGrace(_ timeout: Duration) -> Duration {
    max(.seconds(1), timeout * 0.1)
}

/// A client was configured in a way that can never work; raised before any I/O.
public struct ConfigurationError: Error, Sendable, CustomStringConvertible, LocalizedError {
    public let message: String
    public init(_ message: String) { self.message = message }
    public var description: String { message }
    public var errorDescription: String? { message }
}

extension Duration {
    var seconds: Double {
        let (seconds, attoseconds) = components
        return Double(seconds) + Double(attoseconds) / 1e18
    }

    var milliseconds: Int64 {
        let (seconds, attoseconds) = components
        return seconds * 1000 + attoseconds / 1_000_000_000_000_000
    }
}
