import { randomUUID } from "node:crypto";

import WebSocket, { WebSocketServer, type RawData } from "ws";

import { MeshCallError } from "./errors.js";
import {
  type BalancePolicy,
  type CallErrorFrame,
  type CallOpenFrame,
  decodeFrame,
  encodeFrame,
  type ErrorPayload,
  type Frame,
  PROTOCOL_VERSION,
} from "./protocol.js";

export interface UnaryContext {
  readonly signal: AbortSignal;
  readonly deadlineUnixMs: number | null;
}

export interface UnaryMethod<Request = unknown, Response = unknown> {
  readonly stream: "unary";
  readonly balance?: BalancePolicy;
  readonly handler: (
    request: Request,
    context: UnaryContext,
  ) => Response | Promise<Response>;
}

export interface ServiceDefinition {
  readonly name: string;
  readonly methods: Readonly<Record<string, UnaryMethod<never, unknown>>>;
}

export interface MeshCallServerOptions {
  readonly services: ReadonlyArray<ServiceDefinition>;
  readonly host?: string;
  readonly port?: number;
}

interface ActiveCall {
  readonly controller: AbortController;
  timer: ReturnType<typeof setTimeout> | undefined;
  terminal: boolean;
}

export class MeshCallServer {
  private server: WebSocketServer | undefined;
  private readonly services = new Map<string, ServiceDefinition>();

  public constructor(private readonly options: MeshCallServerOptions) {
    for (const service of options.services) {
      if (this.services.has(service.name)) {
        throw new Error(`Duplicate service name: ${service.name}`);
      }
      this.services.set(service.name, service);
    }
  }

  public get boundPort(): number {
    const address = this.server?.address();
    if (address === undefined || address === null || typeof address === "string") {
      throw new Error("MeshCall server is not running on TCP");
    }
    return address.port;
  }

  public async start(): Promise<void> {
    if (this.server !== undefined) {
      return;
    }
    const server = new WebSocketServer({
      host: this.options.host ?? "127.0.0.1",
      port: this.options.port ?? 0,
    });
    this.server = server;
    server.on("connection", (socket) => this.handleConnection(socket));
    await new Promise<void>((resolve, reject) => {
      server.once("listening", resolve);
      server.once("error", reject);
    });
  }

  public async close(): Promise<void> {
    const server = this.server;
    this.server = undefined;
    if (server === undefined) {
      return;
    }
    for (const socket of server.clients) {
      socket.close();
    }
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error == null) {
          resolve();
        } else {
          reject(error);
        }
      });
    });
  }

  private handleConnection(socket: WebSocket): void {
    let handshaken = false;
    const calls = new Map<string, ActiveCall>();
    socket.on("message", (data) => {
      let frame: Frame;
      try {
        frame = decodeFrame(data);
      } catch {
        socket.close(1002, "Invalid MeshCall frame");
        return;
      }
      if (!handshaken) {
        if (
          frame.kind !== "hello" ||
          frame.role !== "client" ||
          frame.protocol !== PROTOCOL_VERSION
        ) {
          socket.close(1002, "Expected compatible client hello");
          return;
        }
        handshaken = true;
        this.send(socket, {
          kind: "hello.ack",
          protocol: PROTOCOL_VERSION,
          connection_id: randomUUID().replaceAll("-", ""),
        });
        return;
      }
      if (frame.kind === "ping") {
        this.send(socket, { kind: "pong", nonce: frame.nonce });
      } else if (frame.kind === "call.open") {
        this.openCall(socket, calls, frame);
      } else if (frame.kind === "call.cancel") {
        const call = calls.get(frame.call_id);
        if (call !== undefined && !call.terminal) {
          this.finishError(socket, calls, frame.call_id, call, {
            code: "cancelled",
            message: "Call was cancelled",
          });
        }
      }
    });
    socket.on("close", () => {
      for (const call of calls.values()) {
        call.controller.abort("connection_closed");
        if (call.timer !== undefined) {
          clearTimeout(call.timer);
        }
      }
      calls.clear();
    });
  }

  private openCall(
    socket: WebSocket,
    calls: Map<string, ActiveCall>,
    frame: CallOpenFrame,
  ): void {
    if (calls.has(frame.call_id)) {
      this.sendError(socket, frame.call_id, {
        code: "protocol_error",
        message: "Duplicate call_id",
      });
      return;
    }
    const service = this.services.get(frame.service);
    const method = service?.methods[frame.method];
    if (method === undefined) {
      this.sendError(socket, frame.call_id, {
        code: "method_not_found",
        message: `Unknown method ${frame.service}.${frame.method}`,
      });
      return;
    }
    if (method.stream !== "unary") {
      this.sendError(socket, frame.call_id, {
        code: "protocol_error",
        message: "TypeScript runtime currently supports unary methods only",
      });
      return;
    }
    const deadline = frame.deadline_unix_ms ?? null;
    if (deadline !== null && deadline <= Date.now()) {
      this.sendError(socket, frame.call_id, {
        code: "deadline_exceeded",
        message: "Call deadline exceeded",
      });
      return;
    }

    const call: ActiveCall = {
      controller: new AbortController(),
      timer: undefined,
      terminal: false,
    };
    calls.set(frame.call_id, call);
    if (deadline !== null) {
      call.timer = setTimeout(() => {
        this.finishError(socket, calls, frame.call_id, call, {
          code: "deadline_exceeded",
          message: "Call deadline exceeded",
        });
      }, Math.max(0, deadline - Date.now()));
    }

    Promise.resolve()
      .then(() =>
        method.handler(frame.payload as never, {
          signal: call.controller.signal,
          deadlineUnixMs: deadline,
        }),
      )
      .then((result) => {
        if (this.finish(calls, frame.call_id, call)) {
          this.send(socket, {
            kind: "call.result",
            call_id: frame.call_id,
            payload: result ?? null,
          });
        }
      })
      .catch((error: unknown) => {
        const payload =
          error instanceof MeshCallError
            ? {
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                details: error.details,
              }
            : {
                code: "internal",
                message: "Internal service error",
              };
        this.finishError(socket, calls, frame.call_id, call, payload);
      });
  }

  private finishError(
    socket: WebSocket,
    calls: Map<string, ActiveCall>,
    callId: string,
    call: ActiveCall,
    error: ErrorPayload,
  ): void {
    if (!this.finish(calls, callId, call)) {
      return;
    }
    call.controller.abort(error.code);
    this.sendError(socket, callId, error);
  }

  private finish(
    calls: Map<string, ActiveCall>,
    callId: string,
    call: ActiveCall,
  ): boolean {
    if (call.terminal) {
      return false;
    }
    call.terminal = true;
    calls.delete(callId);
    if (call.timer !== undefined) {
      clearTimeout(call.timer);
    }
    return true;
  }

  private sendError(socket: WebSocket, callId: string, error: ErrorPayload): void {
    const frame: CallErrorFrame = {
      kind: "call.error",
      call_id: callId,
      error,
    };
    this.send(socket, frame);
  }

  private send(socket: WebSocket, frame: Frame): void {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(encodeFrame(frame));
    }
  }
}
