import type { RawData } from "ws";

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
  const value: unknown = JSON.parse(typeof data === "string" ? data : data.toString());
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new Error("Invalid MeshCall frame");
  }
  return value as unknown as Frame;
}

export function encodeFrame(frame: Frame): string {
  return JSON.stringify(frame);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
