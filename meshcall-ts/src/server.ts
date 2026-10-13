import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { createServer, type Server as HttpServer } from "node:http";
import { dirname } from "node:path";
import WebSocket, { WebSocketServer, type RawData } from "ws";

import { closeSocket, createSocket, unixSocketPath, type WebSocketEndpoint } from "./endpoint.js";
import { tryContractEntry } from "./contract.js";
import { contractWarnings, renderWarning } from "./diagnostics.js";
import { MeshCallError } from "./errors.js";
import {
  DEFAULT_IDLE_TIMEOUT_MS, DEFAULT_PING_INTERVAL_MS, DEFAULT_PING_TIMEOUT_MS, Liveness,
  reconnectDelays, type ReconnectPolicy,
} from "./liveness.js";
import { ConsoleRpcLogger, type LogLevel, type RpcLogger } from "./logging.js";
import {
  decodeFrame, FrameDecodeError, PROTOCOL_VERSION, type Frame, type RegisteredService,
} from "./protocol.js";
import { ServerSession, type RuntimeMethod, type RuntimeServices } from "./server-session.js";
import type { ServiceDefinition, ServerInterceptor } from "./service.js";
import { DEFAULT_MAX_FRAME_SIZE, FrameWriter } from "./transport.js";
import { compileMethod } from "./validation.js";

import type { RouterCredentials } from "./router-auth.js";

export type {
  ClientStreamMethod, RpcContext, ServerCallNext, ServerInterceptor, ServerStreamMethod,
  ServiceDefinition, ServiceMethod, UnaryContext, UnaryMethod,
} from "./service.js";

export interface RouterConnectionOptions {
  readonly auth?: RouterCredentials;
  /**
   * One Router, or several: the instance keeps one registration per Router,
   * each with its own reconnect loop, so clients can fail over between them.
   * `start()` succeeds once any Router accepted the registration.
   */
  readonly endpoint: WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint>;
  readonly instanceId?: string;
}

interface RouterLink {
  readonly endpoint: WebSocketEndpoint;
  socket: WebSocket | undefined;
  timer: ReturnType<typeof setTimeout> | undefined;
}

export interface MeshCallServerOptions {
  readonly services: ReadonlyArray<ServiceDefinition>;
  readonly host?: string;
  readonly port?: number;
  readonly unixPath?: string;
  readonly router?: RouterConnectionOptions;
  readonly maxFrameSize?: number;
  readonly handshakeTimeoutMs?: number;
  /** Direct mode: drop clients that send nothing for this long; `null` disables. */
  readonly idleTimeoutMs?: number | null;
  /** Router mode: keepalive ping cadence toward the Router; `null` disables. */
  readonly pingIntervalMs?: number | null;
  readonly pingTimeoutMs?: number;
  /** Router mode: how to re-register after the Router connection drops. */
  readonly reconnect?: ReconnectPolicy;
  /** Wrap every call; the first interceptor is outermost. */
  readonly interceptors?: ReadonlyArray<ServerInterceptor>;
  /** Fail `start()` on advisory contract warnings (also `MESHCALL_STRICT=1`). */
  readonly strictContracts?: boolean;
  /**
   * Calls this instance accepts concurrently. Enforced locally and, in Router
   * mode, announced at registration so the Router routes around a full
   * instance with a retryable `resource_exhausted` (`reason: "overloaded"`).
   */
  readonly maxInflight?: number;
  readonly accessLog?: boolean;
  readonly logLevel?: LogLevel;
  readonly colorize?: boolean;
  readonly logger?: RpcLogger;
}

export class MeshCallServer {
  private listener: WebSocketServer | undefined;
  private httpServer: HttpServer | undefined;
  private readonly routerLinks: RouterLink[] = [];
  private readonly sessions = new Map<WebSocket, ServerSession>();
  private lifecycle: Promise<void> = Promise.resolve();
  private running = false;
  private stopping = false;
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
    if (options.maxInflight !== undefined && (!Number.isSafeInteger(options.maxInflight) || options.maxInflight < 1)) {
      throw new RangeError("maxInflight must be a positive integer");
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
      const contract = tryContractEntry(service);
      this.registration.push({
        name: service.name,
        ...(contract === undefined ? {} : { contract }),
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

  /** One summarised block per start; details come from `contractWarnings()`. */
  private logContractWarnings(): void {
    const findings = this.options.services.flatMap((service) => contractWarnings(service));
    if (findings.length === 0) return;
    if (this.options.strictContracts === true) {
      throw new Error("Strict mode: " + findings.length + " contract warning(s)\n" + findings.map(renderWarning).join("\n"));
    }
    const byCode = new Map<string, number>();
    for (const f of findings) byCode.set(f.code, (byCode.get(f.code) ?? 0) + 1);
    const summary = [...byCode.entries()].sort().map(([code, count]) => code + "×" + count).join(", ");
    this.logger.warning(findings.length + " contract warning(s) (" + summary
      + "); call contractWarnings(service) for details and hints, or set MESHCALL_STRICT=1 / strictContracts to fail on them");
  }

  /** Router mode: whether at least one Router holds this instance's registration. */
  public get routerConnected(): boolean {
    return this.routerLinks.some((link) => link.socket?.readyState === WebSocket.OPEN);
  }

  /** Router mode: the endpoints currently holding a registration. */
  public get routerEndpoints(): ReadonlyArray<WebSocketEndpoint> {
    return this.routerLinks.filter((link) => link.socket?.readyState === WebSocket.OPEN).map((link) => link.endpoint);
  }

  /**
   * Re-register one link after its Router connection drops. The server stays
   * running throughout; calls that arrive in between fail at that Router with
   * retryable `unavailable` (other Routers keep serving).
   */
  private scheduleRouterReconnect(link: RouterLink, initialFailure = false): void {
    const router = this.options.router;
    const policy = this.options.reconnect ?? {};
    if (router === undefined || this.stopping || !(policy.enabled ?? true)) {
      if (!this.routerConnected) this.running = false;
      return;
    }
    const where = "instance " + this.instanceId + " at " + JSON.stringify(link.endpoint);
    if (!initialFailure) this.logger.warning("Router connection lost for %s; reconnecting", where);
    const delays = reconnectDelays(policy);
    const attempt = (): void => {
      const next = delays.next();
      if (next.done === true) {
        this.logger.error("Giving up on Router reconnection for %s", where);
        if (!this.routerConnected) this.running = false;
        return;
      }
      link.timer = setTimeout(() => {
        link.timer = undefined;
        if (this.stopping) return;
        this.startRouterLink(router, link).then(
          () => this.logger.info("Router connection restored for %s", where),
          (error: unknown) => {
            this.logger.warning("Router re-registration failed for %s: %s", where, error);
            attempt();
          },
        );
      }, next.value);
    };
    attempt();
  }

  public start(): Promise<void> {
    const operation = this.lifecycle.then(async () => {
      if (this.running) return;
      this.stopping = false;
      try {
        if (this.options.router === undefined) await this.startDirect();
        else await this.startRouter(this.options.router);
        this.running = true;
        this.logContractWarnings();
      } catch (error) {
        await this.stop();
        throw error;
      }
    });
    this.lifecycle = operation.catch(() => undefined);
    return operation;
  }

  /**
   * Stop the server. With `drain: true` it first stops taking new calls
   * (leaving the Router pool or closing the listener) and waits up to
   * `drainTimeoutMs` for in-flight calls to finish, so a rolling restart fails
   * no request; calls still running after that are cancelled as usual.
   */
  public close(options: { readonly drain?: boolean; readonly drainTimeoutMs?: number } = {}): Promise<void> {
    const operation = this.lifecycle.then(() => this.stop(options));
    this.lifecycle = operation.catch(() => undefined);
    return operation;
  }

  private async drain(timeoutMs: number): Promise<void> {
    const deadline = performance.now() + timeoutMs;
    const connected = this.routerLinks.filter((link) => link.socket !== undefined);
    if (this.options.router !== undefined) {
      await Promise.all(connected.map(async (link) => {
        const session = link.socket === undefined ? undefined : this.sessions.get(link.socket);
        if (session === undefined) return;
        await session.writer.send({ kind: "server.drain" });
        await withDeadline(session.awaitDrainAck(), deadline, "Router did not acknowledge drain");
      }));
    } else {
      // Stop accepting; existing clients keep running their calls.
      this.listener?.close();
    }
    while ([...this.sessions.values()].some((session) => session.inFlight > 0)) {
      if (performance.now() >= deadline) {
        this.logger.warning("Drain timed out with calls still in flight");
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  private async stop(options: { readonly drain?: boolean; readonly drainTimeoutMs?: number } = {}): Promise<void> {
    this.stopping = true;
    if (options.drain === true && this.running) {
      try {
        await this.drain(options.drainTimeoutMs ?? 30_000);
      } catch (error) {
        this.logger.warning("Drain did not complete cleanly: %s", error);
      }
    }
    this.running = false;
    for (const link of this.routerLinks) {
      if (link.timer !== undefined) clearTimeout(link.timer);
      link.timer = undefined;
      link.socket = undefined;
    }
    this.routerLinks.length = 0;
    const listener = this.listener;
    const http = this.httpServer;
    this.listener = undefined;
    this.httpServer = undefined;
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

  private session(socket: WebSocket, role: "direct" | "router"): { session: ServerSession; liveness: Liveness } {
    const writer = new FrameWriter(socket, this.maxFrameSize);
    const liveness = new Liveness(socket, writer, role === "direct"
      ? { idleTimeoutMs: this.options.idleTimeoutMs === undefined
          ? DEFAULT_IDLE_TIMEOUT_MS : this.options.idleTimeoutMs }
      : {
          pingIntervalMs: this.options.pingIntervalMs === undefined
            ? DEFAULT_PING_INTERVAL_MS : this.options.pingIntervalMs,
          pingTimeoutMs: this.options.pingTimeoutMs ?? DEFAULT_PING_TIMEOUT_MS,
        });
    const session = new ServerSession(this.services, writer, {
      logger: this.logger,
      accessLog: this.options.accessLog ?? true,
      logLevel: this.options.logLevel ?? "info",
      interceptors: this.options.interceptors ?? [],
      inflight: () => [...this.sessions.values()].reduce((sum, s) => sum + s.inFlight, 0),
      ...(this.options.maxInflight === undefined ? {} : { maxInflight: this.options.maxInflight }),
    }, liveness);
    this.sessions.set(socket, session);
    socket.on("close", () => {
      session.close();
      this.sessions.delete(socket);
      const link = this.routerLinks.find((candidate) => candidate.socket === socket);
      if (link !== undefined) {
        link.socket = undefined;
        this.scheduleRouterReconnect(link);
      }
    });
    socket.on("error", () => {
      session.close();
      socket.close();
    });
    return { session, liveness };
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
    const { session, liveness } = this.session(socket, "direct");
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
          liveness.start();
          void session.writer.send({
            kind: "hello.ack", protocol: PROTOCOL_VERSION,
            connection_id: randomUUID().replaceAll("-", ""),
          }).catch(() => socket.close());
        } else {
          session.handle(frame);
        }
      } catch (error) {
        if (ready && error instanceof FrameDecodeError && error.callId !== undefined) {
          session.rejectFrame(error.callId, error.message);
          return;
        }
        socket.close(1002, "Invalid MeshCall frame");
      }
    });
  }

  private async startRouter(router: RouterConnectionOptions): Promise<void> {
    const endpoints = Array.isArray(router.endpoint)
      ? [...(router.endpoint as ReadonlyArray<WebSocketEndpoint>)] : [router.endpoint as WebSocketEndpoint];
    if (endpoints.length === 0) throw new RangeError("At least one Router endpoint is required");
    this.routerLinks.length = 0;
    for (const endpoint of endpoints) this.routerLinks.push({ endpoint, socket: undefined, timer: undefined });
    const results = await Promise.allSettled(this.routerLinks.map((link) => this.startRouterLink(router, link)));
    if (results.every((r) => r.status === "rejected")) {
      throw (results[0] as PromiseRejectedResult).reason;
    }
    results.forEach((result, index) => {
      if (result.status === "rejected") {
        const link = this.routerLinks[index] as RouterLink;
        this.logger.warning("Router %s refused instance %s at start (%s); retrying in the background",
          JSON.stringify(link.endpoint), this.instanceId, result.reason);
        this.scheduleRouterReconnect(link, true);
      }
    });
  }

  private async startRouterLink(router: RouterConnectionOptions, link: RouterLink): Promise<void> {
    const socket = createSocket(link.endpoint, this.maxFrameSize, this.handshakeTimeout, router.auth);
    link.socket = socket;
    const { session, liveness } = this.session(socket, "router");
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
              ...(this.options.maxInflight === undefined ? {} : { max_inflight: this.options.maxInflight }),
            }).catch(fail);
          } else if (state === "register") {
            if (frame.kind !== "server.register.ack" || frame.instance_id !== this.instanceId) {
              throw new MeshCallError("protocol_error", "Expected matching server.register.ack");
            }
            state = "ready";
            clearTimeout(timer);
            socket.off("error", fail);
            liveness.start();
            resolve();
          } else {
            session.handle(frame);
          }
        } catch (error) {
          if (state === "ready" && error instanceof FrameDecodeError && error.callId !== undefined) {
            session.rejectFrame(error.callId, error.message);
            return;
          }
          fail(error);
        }
      });
    });
  }
}

async function withDeadline<T>(promise: Promise<T>, deadline: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), Math.max(0, deadline - performance.now()));
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
