import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { createServer, type Server as HttpServer } from "node:http";
import { dirname } from "node:path";
import WebSocket, { WebSocketServer, type RawData } from "ws";

import { closeSocket, createSocket, unixSocketPath, type WebSocketEndpoint } from "./endpoint.js";
import { MeshCallError } from "./errors.js";
import { ConsoleRpcLogger, type LogLevel, type RpcLogger } from "./logging.js";
import { decodeFrame, PROTOCOL_VERSION, type Frame, type RegisteredService } from "./protocol.js";
import { ServerSession, type RuntimeMethod, type RuntimeServices } from "./server-session.js";
import type { ServiceDefinition } from "./service.js";
import { DEFAULT_MAX_FRAME_SIZE, FrameWriter } from "./transport.js";
import { compileMethod } from "./validation.js";

export type {
  ClientStreamMethod, RpcContext, ServerStreamMethod, ServiceDefinition,
  ServiceMethod, UnaryContext, UnaryMethod,
} from "./service.js";

export interface RouterConnectionOptions {
  readonly endpoint: WebSocketEndpoint;
  readonly instanceId?: string;
}

export interface MeshCallServerOptions {
  readonly services: ReadonlyArray<ServiceDefinition>;
  readonly host?: string;
  readonly port?: number;
  readonly unixPath?: string;
  readonly router?: RouterConnectionOptions;
  readonly maxFrameSize?: number;
  readonly handshakeTimeoutMs?: number;
  readonly accessLog?: boolean;
  readonly logLevel?: LogLevel;
  readonly colorize?: boolean;
  readonly logger?: RpcLogger;
}

export class MeshCallServer {
  private listener: WebSocketServer | undefined;
  private httpServer: HttpServer | undefined;
  private routerSocket: WebSocket | undefined;
  private readonly sessions = new Map<WebSocket, ServerSession>();
  private lifecycle: Promise<void> = Promise.resolve();
  private running = false;
  private readonly services: RuntimeServices;
  private readonly registration: RegisteredService[];
  private readonly logger: RpcLogger;
  private readonly maxFrameSize: number;
  private readonly handshakeTimeout: number;
  private readonly instanceId: string;

  public constructor(private readonly options: MeshCallServerOptions) {
    if (options.router !== undefined &&
      (options.host !== undefined || options.port !== undefined || options.unixPath !== undefined)) {
      throw new TypeError("Router mode cannot be combined with a Direct listener");
    }
    if (options.unixPath !== undefined && (options.host !== undefined || options.port !== undefined)) {
      throw new TypeError("Unix socket cannot be combined with host or port");
    }
    this.maxFrameSize = options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
    this.handshakeTimeout = options.handshakeTimeoutMs ?? 10_000;
    for (const value of [this.maxFrameSize, this.handshakeTimeout]) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError("Limits must be positive integers");
    }
    this.instanceId = options.router?.instanceId ?? randomUUID().replaceAll("-", "");
    this.logger = options.logger ?? new ConsoleRpcLogger({}, options.colorize ?? false);
    const services = new Map<string, Map<string, RuntimeMethod>>();
    this.registration = [];
    for (const service of options.services) {
      if (services.has(service.name)) throw new Error("Duplicate service name: " + service.name);
      const methods = new Map<string, RuntimeMethod>();
      for (const [name, definition] of Object.entries(service.methods)) {
        if (!["unary", "server_stream", "client_stream"].includes(definition.stream)) {
          throw new TypeError("Unsupported service method shape: " + definition.stream);
        }
        methods.set(name, { definition, validators: compileMethod(definition) });
      }
      services.set(service.name, methods);
      this.registration.push({
        name: service.name,
        methods: Object.entries(service.methods).map(([name, method]) => ({
          name, stream: method.stream,
          balance: {
            kind: (method.balance ?? service.balance)?.kind ?? "round_robin",
            key: (method.balance ?? service.balance)?.key ?? null,
          },
        })),
      });
    }
    this.services = services;
  }

  public get boundPort(): number {
    const address = this.listener?.address();
    if (address === undefined || address === null || typeof address === "string") {
      throw new Error("MeshCall server is not running on TCP");
    }
    return address.port;
  }

  public get isRunning(): boolean {
    return this.running;
  }

  public start(): Promise<void> {
    const operation = this.lifecycle.then(async () => {
      if (this.running) return;
      try {
        if (this.options.router === undefined) await this.startDirect();
        else await this.startRouter(this.options.router);
        this.running = true;
      } catch (error) {
        await this.stop();
        throw error;
      }
    });
    this.lifecycle = operation.catch(() => undefined);
    return operation;
  }

  public close(): Promise<void> {
    const operation = this.lifecycle.then(() => this.stop());
    this.lifecycle = operation.catch(() => undefined);
    return operation;
  }

  private async stop(): Promise<void> {
    this.running = false;
    const listener = this.listener;
    const http = this.httpServer;
    this.listener = undefined;
    this.httpServer = undefined;
    this.routerSocket = undefined;
    const sockets = [...this.sessions.keys()];
    for (const session of this.sessions.values()) session.close();
    await Promise.all(sockets.map(closeSocket));
    this.sessions.clear();
    if (listener !== undefined) {
      await new Promise<void>((resolve) => listener.close(() => resolve()));
    }
    if (http?.listening === true) {
      await new Promise<void>((resolve) => http.close(() => resolve()));
    }
  }

  private session(socket: WebSocket): ServerSession {
    const session = new ServerSession(this.services, new FrameWriter(socket, this.maxFrameSize), {
      logger: this.logger,
      accessLog: this.options.accessLog ?? true,
      logLevel: this.options.logLevel ?? "info",
    });
    this.sessions.set(socket, session);
    socket.on("close", () => {
      session.close();
      this.sessions.delete(socket);
      if (this.routerSocket === socket) {
        this.routerSocket = undefined;
        this.running = false;
      }
    });
    socket.on("error", () => {
      session.close();
      socket.close();
    });
    return session;
  }

  private async startDirect(): Promise<void> {
    const path = this.options.unixPath;
    let listener: WebSocketServer;
    if (path === undefined) {
      listener = new WebSocketServer({
        host: this.options.host ?? "127.0.0.1", port: this.options.port ?? 0,
        maxPayload: this.maxFrameSize,
      });
    } else {
      const socketPath = unixSocketPath(path);
      mkdirSync(dirname(socketPath), { recursive: true });
      this.httpServer = createServer();
      listener = new WebSocketServer({ server: this.httpServer, maxPayload: this.maxFrameSize });
    }
    this.listener = listener;
    listener.on("connection", (socket) => this.acceptDirect(socket));
    await new Promise<void>((resolve, reject) => {
      const ready = () => { listener.off("error", failed); resolve(); };
      const failed = (error: Error) => { listener.off("listening", ready); reject(error); };
      listener.once("listening", ready);
      listener.once("error", failed);
      if (this.httpServer !== undefined) {
        this.httpServer.once("error", failed);
        this.httpServer.listen(unixSocketPath(path!));
      }
    });
  }

  private acceptDirect(socket: WebSocket): void {
    const session = this.session(socket);
    let ready = false;
    const timer = setTimeout(() => socket.close(1002, "MeshCall handshake timed out"), this.handshakeTimeout);
    socket.once("close", () => clearTimeout(timer));
    socket.on("message", (data) => {
      try {
        const frame = decodeFrame(data);
        if (!ready) {
          if (frame.kind !== "hello" || frame.role !== "client" || frame.protocol !== PROTOCOL_VERSION) {
            throw new MeshCallError("protocol_error", "Expected compatible client hello");
          }
          ready = true;
          clearTimeout(timer);
          void session.writer.send({
            kind: "hello.ack", protocol: PROTOCOL_VERSION,
            connection_id: randomUUID().replaceAll("-", ""),
          }).catch(() => socket.close());
        } else {
          session.handle(frame);
        }
      } catch {
        socket.close(1002, "Invalid MeshCall frame");
      }
    });
  }

  private async startRouter(router: RouterConnectionOptions): Promise<void> {
    const socket = createSocket(router.endpoint, this.maxFrameSize, this.handshakeTimeout);
    this.routerSocket = socket;
    const session = this.session(socket);
    await new Promise<void>((resolve, reject) => {
      let state: "hello" | "register" | "ready" = "hello";
      const fail = (error: unknown) => { clearTimeout(timer); reject(error); socket.close(); };
      const timer = setTimeout(() =>
        fail(new MeshCallError("deadline_exceeded", "Router handshake timed out")),
        this.handshakeTimeout,
      );
      socket.once("error", fail);
      socket.once("close", () => {
        clearTimeout(timer);
        reject(new MeshCallError("unavailable", "Router disconnected during registration"));
      });
      socket.once("open", () => {
        void session.writer.send({
          kind: "hello", protocol: PROTOCOL_VERSION, role: "server",
          peer_id: this.instanceId, instance_id: this.instanceId,
        }).catch(fail);
      });
      socket.on("message", (data: RawData) => {
        try {
          const frame: Frame = decodeFrame(data);
          if (state === "hello") {
            if (frame.kind !== "hello.ack" || frame.protocol !== PROTOCOL_VERSION) {
              throw new MeshCallError("protocol_error", "Expected compatible Router hello.ack");
            }
            state = "register";
            void session.writer.send({
              kind: "server.register", instance_id: this.instanceId, services: this.registration,
            }).catch(fail);
          } else if (state === "register") {
            if (frame.kind !== "server.register.ack" || frame.instance_id !== this.instanceId) {
              throw new MeshCallError("protocol_error", "Expected matching server.register.ack");
            }
            state = "ready";
            clearTimeout(timer);
            socket.off("error", fail);
            resolve();
          } else {
            session.handle(frame);
          }
        } catch (error) {
          fail(error);
        }
      });
    });
  }
}
