import type { RawData } from "ws";
import { Ajv2020 } from "ajv/dist/2020.js";

export const PROTOCOL_VERSION = "meshcall/1" as const;

export type StreamKind =
  | "unary"
  | "server_stream"
  | "client_stream"
  | "duplex";

export type BalanceKind =
  | "round_robin"
  | "least_inflight"
  | "random"
  | "sticky"
  | "disabled";

export interface BalancePolicy {
  kind: BalanceKind;
  key?: string | null;
}

export interface HelloFrame {
  kind: "hello";
  protocol: string;
  role: "client" | "server";
  peer_id: string;
  instance_id?: string | null;
  /** Reserved for negotiating optional features without a protocol bump. */
  capabilities?: string[] | null;
}

/** Instance → Router: leave every pool, keep serving routed calls. */
export interface ServerDrainFrame {
  kind: "server.drain";
}

export interface ServerDrainAckFrame {
  kind: "server.drain.ack";
  instance_id?: string | null;
}

export interface HelloAckFrame {
  kind: "hello.ack";
  protocol: string;
  connection_id: string;
}

export interface RegisteredMethod {
  name: string;
  stream: StreamKind;
  balance: BalancePolicy;
}

export interface RegisteredService {
  name: string;
  methods: Array<RegisteredMethod>;
}

export interface ServerRegisterFrame {
  kind: "server.register";
  instance_id: string;
  services: Array<RegisteredService>;
}

export interface ServerRegisterAckFrame {
  kind: "server.register.ack";
  instance_id: string;
}

export type Metadata = Record<string, string>;

export interface CallOpenFrame {
  kind: "call.open";
  call_id: string;
  service: string;
  method: string;
  payload: unknown;
  /** Relative budget, measured by each peer from when it processes this frame. */
  timeout_ms?: number | null;
  /** Longest allowed gap between stream items in either direction. */
  idle_timeout_ms?: number | null;
  metadata?: Metadata | null;
}

export interface CallResultFrame {
  kind: "call.result";
  call_id: string;
  payload?: unknown;
  metadata?: Metadata | null;
}

export interface ErrorPayload {
  code: string;
  message: string;
  retryable?: boolean;
  details?: Record<string, unknown> | null;
}

export interface CallErrorFrame {
  kind: "call.error";
  call_id: string;
  error: ErrorPayload;
  metadata?: Metadata | null;
}

export interface CallCancelFrame {
  kind: "call.cancel";
  call_id: string;
  reason?: string;
}

export interface StreamItemFrame {
  kind: "stream.item";
  call_id: string;
  direction: "client" | "server";
  sequence: number;
  payload: unknown;
}

export interface StreamEndFrame {
  kind: "stream.end";
  call_id: string;
  direction: "client" | "server";
}

export interface StreamWindowFrame {
  kind: "stream.window";
  call_id: string;
  direction: "client" | "server";
  credit: number;
}

export interface PingFrame {
  kind: "ping";
  nonce: string;
}

export interface PongFrame {
  kind: "pong";
  nonce: string;
}

export type Frame =
  | HelloFrame
  | HelloAckFrame
  | ServerRegisterFrame
  | ServerRegisterAckFrame
  | ServerDrainFrame
  | ServerDrainAckFrame
  | CallOpenFrame
  | CallResultFrame
  | CallErrorFrame
  | CallCancelFrame
  | StreamItemFrame
  | StreamEndFrame
  | StreamWindowFrame
  | PingFrame
  | PongFrame;

/**
 * A frame failed validation. `callId` is set when the frame still named a
 * call, so the receiver can fail that call instead of the whole connection.
 */
export class FrameDecodeError extends Error {
  public constructor(message: string, public readonly callId: string | undefined) {
    super(message);
    this.name = "FrameDecodeError";
  }
}

export function decodeFrame(data: RawData | string): Frame {
  const text = typeof data === "string" ? data
    : Array.isArray(data) ? Buffer.concat(data).toString()
    : data instanceof ArrayBuffer ? Buffer.from(data).toString() : data.toString();
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new FrameDecodeError("Invalid MeshCall frame", undefined);
  }
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new FrameDecodeError("Invalid MeshCall frame", undefined);
  }
  const callId = typeof value.call_id === "string" && value.call_id !== "" ? value.call_id : undefined;
  const validate = validators.get(value.kind);
  if (validate === undefined) {
    throw new FrameDecodeError("Unknown MeshCall frame kind " + value.kind, undefined);
  }
  if (!validate(value)) {
    throw new FrameDecodeError("Invalid MeshCall " + value.kind + " frame", callId);
  }
  return value as unknown as Frame;
}

export function encodeFrame(frame: Frame): string {
  return JSON.stringify(frame);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const string = { type: "string" };
const nullableString = { type: ["string", "null"] };
const direction = { enum: ["client", "server"] };
const integer = { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER };
const positiveMs = { type: ["integer", "null"], minimum: 1, maximum: Number.MAX_SAFE_INTEGER };
const metadata = {
  type: ["object", "null"],
  additionalProperties: { type: "string" },
  propertyNames: { minLength: 1 },
};
const balance = {
  type: "object",
  properties: {
    kind: { enum: ["round_robin", "least_inflight", "random", "sticky", "disabled"] },
    key: nullableString,
  },
  required: ["kind"],
};
const method = {
  type: "object",
  properties: {
    name: string, stream: { enum: ["unary", "server_stream", "client_stream", "duplex"] },
    balance,
  },
  required: ["name", "stream", "balance"],
};
const service = {
  type: "object",
  properties: { name: string, methods: { type: "array", items: method } },
  required: ["name", "methods"],
};
const ajv = new Ajv2020({ strict: false });
// meshcall/1 compatibility rule: unknown fields are ignored, unknown kinds rejected.
const frame = (kind: string, properties: Record<string, unknown>, required: string[]) =>
  ajv.compile({
    type: "object", properties: { kind: { const: kind }, ...properties },
    required: ["kind", ...required],
  });
const validators = new Map([
  ["hello", frame("hello", {
    protocol: string, role: { enum: ["client", "server"] }, peer_id: string,
    instance_id: nullableString, capabilities: { type: ["array", "null"], items: string },
  }, ["protocol", "role", "peer_id"])],
  ["server.drain", frame("server.drain", {}, [])],
  ["server.drain.ack", frame("server.drain.ack", { instance_id: nullableString }, [])],
  ["hello.ack", frame("hello.ack", { protocol: string, connection_id: string },
    ["protocol", "connection_id"])],
  ["server.register", frame("server.register", {
    instance_id: string, services: { type: "array", items: service },
  }, ["instance_id", "services"])],
  ["server.register.ack", frame("server.register.ack", { instance_id: string }, ["instance_id"])],
  ["call.open", frame("call.open", {
    call_id: string, service: string, method: string, payload: {},
    timeout_ms: positiveMs, idle_timeout_ms: positiveMs, metadata,
  }, ["call_id", "service", "method", "payload"])],
  ["call.result", frame("call.result", { call_id: string, payload: {}, metadata }, ["call_id"])],
  ["call.error", frame("call.error", {
    call_id: string,
    error: {
      type: "object",
      properties: {
        code: string, message: string, retryable: { type: "boolean" },
        details: { type: ["object", "null"] },
      },
      required: ["code", "message"],
    },
    metadata,
  }, ["call_id", "error"])],
  ["call.cancel", frame("call.cancel", { call_id: string, reason: string }, ["call_id"])],
  ["stream.item", frame("stream.item", {
    call_id: string, direction, sequence: integer, payload: {},
  }, ["call_id", "direction", "sequence", "payload"])],
  ["stream.end", frame("stream.end", { call_id: string, direction }, ["call_id", "direction"])],
  ["stream.window", frame("stream.window", {
    call_id: string, direction, credit: { ...integer, minimum: 1 },
  }, ["call_id", "direction", "credit"])],
  ["ping", frame("ping", { nonce: string }, ["nonce"])],
  ["pong", frame("pong", { nonce: string }, ["nonce"])],
]);
