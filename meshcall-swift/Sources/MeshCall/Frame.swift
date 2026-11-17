import Foundation

public let protocolVersion = "meshcall/1"

/// Largest encoded frame, sent or received, unless configured smaller; also the Router's ceiling.
public let defaultMaxFrameSize = 64 * 1024 * 1024

/// One wire frame: its `kind` and every field as received. Unknown fields are
/// kept (the compatibility rule: ignore, never drop what a Router forwards).
struct Frame: Sendable {
    var kind: String
    var fields: [String: JSONValue]

    init(_ kind: String, _ fields: [String: JSONValue] = [:]) {
        self.kind = kind
        self.fields = fields
    }

    subscript(key: String) -> JSONValue? {
        get { fields[key] }
        set { fields[key] = newValue }
    }

    var callID: String? { fields["call_id"]?.stringValue }
    var direction: String? { fields["direction"]?.stringValue }
    var payload: JSONValue { fields["payload"] ?? .null }

    var metadata: [String: String] {
        guard let object = fields["metadata"]?.objectValue else { return [:] }
        return object.compactMapValues(\.stringValue)
    }

    /// Frames that jump the per-call queues.
    var isControl: Bool {
        kind == "call.cancel" || kind == "stream.window" || kind == "ping" || kind == "pong"
    }

    func encoded() throws -> String {
        var object = fields
        object["kind"] = .string(kind)
        return String(decoding: try JSONValue.object(object).jsonData(), as: UTF8.self)
    }

    static func decode(_ data: Data) throws -> Frame {
        guard let value = try? JSONValue(json: data), case .object(var object) = value,
            case .string(let kind)? = object["kind"]
        else { throw FrameDecodeError(message: "Invalid MeshCall frame", callID: nil) }
        object.removeValue(forKey: "kind")
        let frame = Frame(kind, object)
        guard let rules = frameRules[kind] else {
            throw FrameDecodeError(message: "Unknown MeshCall frame kind " + kind, callID: nil)
        }
        let callID = frame.callID.flatMap { $0.isEmpty ? nil : $0 }
        let invalid = FrameDecodeError(message: "Invalid MeshCall \(kind) frame", callID: callID)
        for (field, rule) in rules {
            guard let value = object[field] else {
                if rule.required { throw invalid }
                continue
            }
            if value.isNull {
                if rule.nullable { continue }
                throw invalid
            }
            if !rule.check(value) { throw invalid }
        }
        return frame
    }
}

/// A frame failed validation. `callID` is set when the frame still named a
/// call, so the receiver can fail that call instead of the whole connection.
struct FrameDecodeError: Error {
    let message: String
    let callID: String?
}

private struct FieldRule {
    var required = false
    var nullable = true
    var check: @Sendable (JSONValue) -> Bool
}

private func string(required: Bool = false, nonEmpty: Bool = false, maxBytes: Int? = nil) -> FieldRule {
    FieldRule(required: required, nullable: !required) { value in
        guard let text = value.stringValue else { return false }
        if nonEmpty && text.isEmpty { return false }
        if let maxBytes, text.utf8.count > maxBytes { return false }
        return true
    }
}

private func integer(required: Bool = false, minimum: Int64 = 0) -> FieldRule {
    FieldRule(required: required, nullable: !required) { value in
        guard let number = value.intValue else { return false }
        return number >= minimum && number <= 9_007_199_254_740_991
    }
}

private let anyValue = FieldRule(required: true, nullable: true) { _ in true }
private let direction = FieldRule(required: true, nullable: false) { $0 == "client" || $0 == "server" }
private let metadataRule = FieldRule { value in
    guard let object = value.objectValue else { return false }
    return object.allSatisfy { !$0.key.isEmpty && $0.value.stringValue != nil }
}
private let objectRule = FieldRule { $0.objectValue != nil }
private let errorRule = FieldRule(required: true, nullable: false) { value in
    guard let object = value.objectValue, object["code"]?.stringValue != nil, object["message"]?.stringValue != nil
    else { return false }
    if let retryable = object["retryable"], retryable.boolValue == nil { return false }
    if let details = object["details"], !details.isNull, details.objectValue == nil { return false }
    return true
}

// meshcall/1 compatibility rule: unknown fields are ignored, unknown kinds rejected.
private let frameRules: [String: [String: FieldRule]] = [
    "hello": ["protocol": string(required: true), "role": string(required: true), "peer_id": string(required: true)],
    "hello.ack": ["protocol": string(required: true), "connection_id": string(required: true)],
    "server.register": ["instance_id": string(required: true)],
    "server.register.ack": ["instance_id": string(required: true)],
    "server.drain": [:],
    "server.drain.ack": [:],
    "call.open": [
        "call_id": string(required: true), "service": string(required: true), "method": string(required: true),
        "payload": anyValue, "timeout_ms": integer(minimum: 1), "idle_timeout_ms": integer(minimum: 1),
        "metadata": metadataRule, "idempotency_key": string(nonEmpty: true, maxBytes: 128),
        "instance_id": string(nonEmpty: true), "e2e_session": string(), "sealed": string(),
    ],
    "call.result": ["call_id": string(required: true), "metadata": metadataRule, "sealed": string()],
    "call.error": ["call_id": string(required: true), "error": errorRule, "metadata": metadataRule, "sealed": string()],
    "call.cancel": ["call_id": string(required: true), "reason": string()],
    "stream.item": [
        "call_id": string(required: true), "direction": direction, "sequence": integer(required: true),
        "payload": anyValue, "sealed": string(),
    ],
    "stream.end": ["call_id": string(required: true), "direction": direction],
    "stream.window": ["call_id": string(required: true), "direction": direction, "credit": integer(required: true, minimum: 1)],
    "ping": ["nonce": string(required: true)],
    "pong": ["nonce": string(required: true)],
]
