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

export interface CallOpenFrame {
  kind: "call.open";
  call_id: string;
  service: string;
  method: string;
  payload: unknown;
  deadline_unix_ms?: number | null;
}

export interface CallResultFrame {
  kind: "call.result";
  call_id: string;
  payload?: unknown;
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
  | CallOpenFrame
  | CallResultFrame
  | CallErrorFrame
  | CallCancelFrame
  | StreamItemFrame
  | StreamEndFrame
  | StreamWindowFrame
  | PingFrame
  | PongFrame;

export function decodeFrame(data: RawData | string): Frame {
  const text = typeof data === "string" ? data
    : Array.isArray(data) ? Buffer.concat(data).toString()
    : data instanceof ArrayBuffer ? Buffer.from(data).toString() : data.toString();
  const value: unknown = JSON.parse(text);
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new Error("Invalid MeshCall frame");
  }
  const validate = validators.get(value.kind);
  if (validate === undefined || !validate(value)) {
    throw new Error("Invalid MeshCall " + value.kind + " frame");
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
const balance = {
  type: "object",
  properties: {
    kind: { enum: ["round_robin", "least_inflight", "random", "sticky", "disabled"] },
    key: nullableString,
  },
  required: ["kind"],
  additionalProperties: false,
};
const method = {
  type: "object",
  properties: {
    name: string, stream: { enum: ["unary", "server_stream", "client_stream", "duplex"] },
    balance,
  },
  required: ["name", "stream", "balance"],
  additionalProperties: false,
};
const service = {
  type: "object",
  properties: { name: string, methods: { type: "array", items: method } },
  required: ["name", "methods"],
  additionalProperties: false,
};
const ajv = new Ajv2020({ strict: false });
const frame = (kind: string, properties: Record<string, unknown>, required: string[]) =>
  ajv.compile({
    type: "object", properties: { kind: { const: kind }, ...properties },
    required: ["kind", ...required], additionalProperties: false,
  });
const validators = new Map([
  ["hello", frame("hello", {
    protocol: string, role: { enum: ["client", "server"] }, peer_id: string,
    instance_id: nullableString,
  }, ["protocol", "role", "peer_id"])],
  ["hello.ack", frame("hello.ack", { protocol: string, connection_id: string },
    ["protocol", "connection_id"])],
  ["server.register", frame("server.register", {
    instance_id: string, services: { type: "array", items: service },
  }, ["instance_id", "services"])],
  ["server.register.ack", frame("server.register.ack", { instance_id: string }, ["instance_id"])],
  ["call.open", frame("call.open", {
    call_id: string, service: string, method: string, payload: {},
    deadline_unix_ms: { type: ["integer", "null"] },
  }, ["call_id", "service", "method", "payload"])],
  ["call.result", frame("call.result", { call_id: string, payload: {} }, ["call_id"])],
  ["call.error", frame("call.error", {
    call_id: string,
    error: {
      type: "object",
      properties: {
        code: string, message: string, retryable: { type: "boolean" },
        details: { type: ["object", "null"] },
      },
      required: ["code", "message"], additionalProperties: false,
    },
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
