import {
  CallCancelledError, DeadlineExceededError, ErrorCode, InvalidArgumentError, MeshCallError, ProtocolError, UnavailableError,
} from "./errors.js";
import { AsyncQueue, CreditWindow, INITIAL_STREAM_CREDIT } from "./flow.js";
import { validateMetadata, type Liveness } from "./liveness.js";
import { safelyLog, type LogLevel, type RpcLogger } from "./logging.js";
import type {
  CallOpenFrame, ErrorPayload, Frame, Metadata, StreamEndFrame, StreamItemFrame, StreamWindowFrame,
} from "./protocol.js";
import type { DuplexChannel, RpcContext, ServerCallNext, ServerInterceptor, ServiceMethod } from "./service.js";
import { isFrameTooLarge, type FrameWriter } from "./transport.js";
import { InvalidOutputError, validatePayload, type MethodValidators } from "./validation.js";

export interface RuntimeMethod {
  definition: ServiceMethod;
  validators: MethodValidators;
}

export type RuntimeServices = ReadonlyMap<string, ReadonlyMap<string, RuntimeMethod>>;

export interface SessionOptions {
  logger: RpcLogger;
  accessLog: boolean;
  logLevel: LogLevel;
  interceptors: ReadonlyArray<ServerInterceptor>;
  /** This instance's id and the services whose calls must name it. */
  instanceId: string;
  targeted: ReadonlySet<string>;
  /** Server-wide in-flight count and cap, for local max_inflight enforcement. */
  inflight?: () => number;
  maxInflight?: number;
}

export class ServerSession {
  private readonly calls = new Map<string, ServerCall>();
  private closed = false;
  private drainAcked: (() => void) | undefined;
  private readonly drainAck = new Promise<void>((resolve) => { this.drainAcked = resolve; });

  public constructor(
    private readonly services: RuntimeServices,
    public readonly writer: FrameWriter,
    public readonly options: SessionOptions,
    private readonly liveness?: Liveness,
  ) {}

  public handle(frame: Frame): void {
    if (this.closed) return;
    if (this.liveness?.observe(frame) === true) return;
    if (frame.kind === "ping") {
      void this.writer.send({ kind: "pong", nonce: frame.nonce }).catch(() => this.close());
      return;
    }
    if (frame.kind === "pong") return;
    if (frame.kind === "server.drain.ack") {
      this.drainAcked?.();
      return;
    }
    if (frame.kind === "call.open") {
      const existing = this.calls.get(frame.call_id);
      if (existing !== undefined) {
        void existing.fail(new ProtocolError("Duplicate call_id"));
        return;
      }
      if (this.options.maxInflight !== undefined && (this.options.inflight?.() ?? this.calls.size) >= this.options.maxInflight) {
        const started = performance.now();
        void this.writer.send({
          kind: "call.error", call_id: frame.call_id,
          error: {
            code: "resource_exhausted", message: "Instance is at its max_inflight", retryable: true,
            details: { source: "server", reason: "overloaded" },
          },
        }).catch(() => undefined).finally(() => this.logAccess(frame, "resource_exhausted", started, this.options.logger));
        return;
      }
      const method = this.services.get(frame.service)?.get(frame.method);
      if (method === undefined) {
        const started = performance.now();
        void this.writer.send({
          kind: "call.error", call_id: frame.call_id,
          error: { code: "method_not_found", message: "Unknown method " + frame.service + "." + frame.method },
        }).catch(() => undefined).finally(() =>
          this.logAccess(frame, "method_not_found", started, this.options.logger),
        );
        return;
      }
      // Targeting, applied the same way the Router does (protocol.md →
      // Targeted calls), so direct and routed services behave alike.
      const rejection = this.openError(frame, method);
      if (rejection !== undefined) {
        const started = performance.now();
        void this.writer.send({
          kind: "call.error", call_id: frame.call_id,
          error: { code: rejection.code, message: rejection.message, retryable: rejection.retryable, details: rejection.details },
        }).catch(() => undefined).finally(() => this.logAccess(frame, rejection.code, started, this.options.logger));
        return;
      }
      const call = new ServerCall(this, frame, method);
      this.calls.set(frame.call_id, call);
      call.start();
      return;
    }
    if (!("call_id" in frame)) return;
    const call = this.calls.get(frame.call_id);
    if (call === undefined) return;
    try {
      if (frame.kind === "call.cancel") {
        void call.fail(new CallCancelledError("Call was cancelled"));
      } else if (frame.kind === "stream.window") {
        call.receiveWindow(frame);
      } else if (frame.kind === "stream.item") {
        call.receiveItem(frame);
      } else if (frame.kind === "stream.end") {
        call.receiveEnd(frame);
      } else {
        throw new ProtocolError("Unexpected client call frame");
      }
    } catch (error) {
      void call.fail(error);
    }
  }

  private openError(frame: CallOpenFrame, method: RuntimeMethod): MeshCallError | undefined {
    const instance = frame.instance_id ?? undefined;
    if (instance !== undefined && instance !== this.options.instanceId) {
      return new UnavailableError("Instance " + instance + " is not this server (" + this.options.instanceId + ")", {
        details: { source: "server", reason: "instance_unknown", instance_id: instance },
      });
    }
    if (instance === undefined && this.options.targeted.has(frame.service)) {
      return new InvalidArgumentError(frame.service + " is a targeted service: call.open needs instance_id", {
        details: { source: "server", reason: "instance_required" },
      });
    }
    if (method.definition.stream === "duplex" && frame.idempotency_key != null) {
      return new InvalidArgumentError("Duplex calls are never replayed and take no idempotency key", {
        details: { source: "server", reason: "idempotency_key_on_duplex" },
      });
    }
    return undefined;
  }

  /** Calls currently executing on this connection. */
  public get inFlight(): number {
    return this.calls.size;
  }

  /** Resolves once the Router acknowledges `server.drain` on this connection. */
  public awaitDrainAck(): Promise<void> {
    return this.drainAck;
  }

  public remove(call: ServerCall): void {
    if (this.calls.get(call.frame.call_id) === call) this.calls.delete(call.frame.call_id);
  }

  /** Answer a malformed frame for `callId` without dropping the peer. */
  public rejectFrame(callId: string, message: string): void {
    const error = new ProtocolError(message);
    const call = this.calls.get(callId);
    if (call !== undefined) {
      void call.fail(error);
      return;
    }
    void this.writer.send({
      kind: "call.error", call_id: callId, error: { code: error.code, message: error.message },
    }).catch(() => undefined);
  }

  public close(): void {
    if (this.closed) return;
    this.closed = true;
    this.liveness?.stop();
    for (const call of [...this.calls.values()]) call.disconnect();
    this.writer.close();
  }

  public logAccess(frame: CallOpenFrame, status: string, started: number, logger: RpcLogger): void {
    if (!this.options.accessLog) return;
    safelyLog(() => logger.log(accessLevel(status, this.options.logLevel),
      "RPC call " + frame.service + "." + frame.method + " status=" + status
      + " duration_ms=" + (performance.now() - started).toFixed(2) + " call_id=" + frame.call_id,
    ));
  }
}

class ServerCall {
  private readonly controller = new AbortController();
  private readonly input = new AsyncQueue<unknown>();
  private readonly credit = new CreditWindow();
  private readonly started = performance.now();
  private readonly context: RpcContext;
  private readonly trailers: Metadata = {};
  private readonly idleTimeoutMs: number | null;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private idleTimer: ReturnType<typeof setTimeout> | undefined;
  private terminal = false;
  private inputClosed = false;
  private outputClosed = false;
  /** Serialises a duplex handler's sends so sequences follow call order. */
  private sending: Promise<void> = Promise.resolve();
  private inputAllowance = 0;
  private inputSequence = 0;
  private outputSequence = 0;
  private output: AsyncIterator<unknown> | undefined;

  public constructor(
    private readonly session: ServerSession,
    public readonly frame: CallOpenFrame,
    private readonly method: RuntimeMethod,
  ) {
    // The budget is relative and measured from when this peer saw call.open.
    const deadlineMs = frame.timeout_ms === undefined || frame.timeout_ms === null
      ? null : this.started + frame.timeout_ms;
    this.idleTimeoutMs = frame.idle_timeout_ms ?? null;
    const trailers = this.trailers;
    this.context = {
      signal: this.controller.signal,
      deadlineMs,
      remainingMs: () => deadlineMs === null ? null : Math.max(0, deadlineMs - performance.now()),
      callId: frame.call_id,
      service: frame.service,
      method: frame.method,
      metadata: Object.freeze({ ...(frame.metadata ?? {}) }),
      idempotencyKey: frame.idempotency_key ?? null,
      instanceId: session.options.instanceId,
      setTrailer(key: string, value: string): void {
        validateMetadata({ [key]: value });
        trailers[key] = value;
      },
      get trailers(): Readonly<Metadata> { return { ...trailers }; },
      logger: session.options.logger.bind({
        service: frame.service, method: frame.method, call_id: frame.call_id,
      }),
    };
  }

  public start(): void {
    this.armDeadline();
    if (!this.terminal) void this.run();
  }

  private armDeadline(): void {
    const deadline = this.context.deadlineMs;
    if (deadline === null || this.terminal) return;
    const remaining = deadline - performance.now();
    if (remaining <= 0) {
      void this.fail(new DeadlineExceededError("Call deadline exceeded", {
        details: { source: "server" },
      }));
    } else {
      // setTimeout saturates above 2^31-1 ms; re-check instead of firing early.
      this.timer = setTimeout(() => this.armDeadline(), Math.min(remaining, 2 ** 31 - 1));
    }
  }

  /** (Re)start the gap timer for the client's input stream. */
  private armIdle(): void {
    if (this.idleTimer !== undefined) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
    }
    if (this.idleTimeoutMs === null || this.inputClosed || this.terminal) return;
    this.idleTimer = setTimeout(() => {
      void this.fail(new DeadlineExceededError("Input stream idle timeout", {
        details: { source: "server", reason: "idle" },
      }));
    }, this.idleTimeoutMs);
  }

  public receiveWindow(frame: StreamWindowFrame): void {
    if (!this.hasOutput() || frame.direction !== "server") {
      throw new ProtocolError("Invalid output stream window");
    }
    this.credit.grant(frame.credit);
  }

  public receiveItem(frame: StreamItemFrame): void {
    if (!this.hasInput() || frame.direction !== "client"
      || this.inputClosed || this.inputAllowance <= 0 || frame.sequence !== this.inputSequence) {
      throw new ProtocolError("Invalid input stream item, sequence, or credit");
    }
    const value = validatePayload(this.method.validators.input, frame.payload, "input_item");
    this.inputAllowance -= 1;
    this.inputSequence += 1;
    this.armIdle();
    this.input.push(value);
  }

  public receiveEnd(frame: StreamEndFrame): void {
    if (!this.hasInput() || frame.direction !== "client" || this.inputClosed) {
      throw new ProtocolError("Invalid input stream end");
    }
    this.inputClosed = true;
    this.armIdle();
    this.input.close();
  }

  public async fail(error: unknown): Promise<void> {
    if (this.terminal) return;
    const payload: ErrorPayload = error instanceof MeshCallError
      ? { code: error.code, message: error.message, retryable: error.retryable, details: error.details }
      : { code: "internal", message: "Internal service error" };
    if (error instanceof InvalidOutputError) {
      // A service bug: the detail stays in the server log (protocol.md → Validation failures).
      safelyLog(() => this.context.logger.error(
        "Handler produced an invalid " + (error.subject === "response" ? "response" : "output item") + ": %s",
        JSON.stringify(error.violations),
      ));
    } else if (!(error instanceof MeshCallError)) {
      safelyLog(() => this.context.logger.error("Unhandled service exception: %s", error));
    }
    this.controller.abort(payload.code);
    await this.finish({
      kind: "call.error", call_id: this.frame.call_id, error: payload, ...this.trailerField(),
    }, payload.code);
    this.closeIterator();
  }

  public disconnect(): void {
    if (!this.cleanup()) return;
    this.session.remove(this);
    this.controller.abort("connection_closed");
    this.closeIterator();
    this.session.logAccess(this.frame, "unavailable", this.started, this.context.logger);
  }

  private async run(): Promise<void> {
    try {
      const request = validatePayload(this.method.validators.request, this.frame.payload, "request");
      let next: ServerCallNext = () => this.invokeHandler(request);
      for (const interceptor of [...this.session.options.interceptors].reverse()) {
        const inner = next;
        next = () => interceptor(this.context, request, inner);
      }
      const result = await next();
      if (this.terminal) return;
      const payload = validatePayload(this.method.validators.response, result ?? null, "response");
      await this.finish({
        kind: "call.result", call_id: this.frame.call_id, payload, ...this.trailerField(),
      }, "ok");
    } catch (error) {
      await this.fail(error);
    }
  }

  private hasInput(): boolean {
    const stream = this.method.definition.stream;
    return stream === "client_stream" || stream === "duplex";
  }

  private hasOutput(): boolean {
    const stream = this.method.definition.stream;
    return stream === "server_stream" || stream === "duplex";
  }

  /** Run the handler to completion, streaming every output item. */
  private async invokeHandler(request: unknown): Promise<unknown> {
    const definition = this.method.definition;
    if (definition.stream === "unary") {
      return definition.handler(request, this.context);
    }
    if (definition.stream === "client_stream") {
      await this.openInput();
      if (this.terminal) return null;
      return definition.handler(request, this.inputItems(), this.context);
    }
    if (definition.stream === "duplex") {
      await this.openInput();
      if (this.terminal) return null;
      await definition.handler(request, this.duplexChannel(), this.context);
      if (!this.terminal) await this.serialized(() => this.closeOutput());
      return null;
    }
    const iterable = await definition.handler(request, this.context);
    this.output = iterable[Symbol.asyncIterator]();
    if (this.terminal) {
      this.closeIterator();
      return null;
    }
    while (!this.terminal) {
      // Pull before acquiring credit: holding one prefetched item lets the
      // stream end when the producer is exhausted exactly as credit runs out,
      // without waiting for a window that will never come.
      const next = await this.output.next();
      if (this.terminal) return null;
      if (next.done === true) break;
      if (!await this.sendOutput(next.value)) return null;
    }
    if (this.terminal) return null;
    await this.closeOutput();
    return null;
  }

  /** Grant the client its first input credit and start the input idle clock. */
  private async openInput(): Promise<void> {
    this.inputAllowance = INITIAL_STREAM_CREDIT;
    await this.session.writer.send({
      kind: "stream.window", call_id: this.frame.call_id,
      direction: "client", credit: INITIAL_STREAM_CREDIT,
    });
    this.armIdle();
  }

  /** Validate and send one output item once credit allows; false if the call ended. */
  private async sendOutput(value: unknown): Promise<boolean> {
    const payload = validatePayload(this.method.validators.output, value, "output_item");
    await this.credit.acquire();
    if (this.terminal) return false;
    try {
      await this.session.writer.send({
        kind: "stream.item", call_id: this.frame.call_id, direction: "server",
        sequence: this.outputSequence, payload,
      });
    } catch (error) {
      // The call ends with this error even if the handler goes on.
      if (isFrameTooLarge(error)) await this.fail(error);
      throw error;
    }
    this.outputSequence += 1;
    return true;
  }

  private async closeOutput(): Promise<void> {
    if (this.outputClosed || this.terminal) return;
    this.outputClosed = true;
    await this.session.writer.send({
      kind: "stream.end", call_id: this.frame.call_id, direction: "server",
    });
  }

  private serialized(operation: () => Promise<void>): Promise<void> {
    const run = this.sending.then(operation);
    this.sending = run.catch(() => undefined);
    return run;
  }

  private duplexChannel(): DuplexChannel<unknown, unknown> {
    const items = this.inputItems();
    return {
      [Symbol.asyncIterator]: () => items[Symbol.asyncIterator](),
      send: (item) => this.serialized(async () => {
        if (this.terminal) throw new CallCancelledError("Call is closed");
        if (this.outputClosed) throw new ProtocolError("Call output stream is closed");
        if (!await this.sendOutput(item)) throw new CallCancelledError("Call is closed");
      }),
      closeSend: () => this.serialized(() => this.closeOutput()),
    };
  }

  private trailerField(): { metadata?: Metadata } {
    return Object.keys(this.trailers).length === 0 ? {} : { metadata: { ...this.trailers } };
  }

  private async *inputItems(): AsyncIterable<unknown> {
    while (!this.terminal) {
      const next = await this.input.next();
      if (next.done === true || this.terminal) return;
      if (!this.inputClosed) {
        this.inputAllowance += 1;
        await this.session.writer.send({
          kind: "stream.window", call_id: this.frame.call_id, direction: "client", credit: 1,
        });
      }
      yield next.value;
    }
  }

  /** Mark the call terminal and stop its timers and streams; idempotent. */
  private cleanup(): boolean {
    if (this.terminal) return false;
    this.terminal = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    if (this.idleTimer !== undefined) clearTimeout(this.idleTimer);
    const error = new CallCancelledError("Call is closed");
    this.credit.close(error);
    this.input.fail(error);
    return true;
  }

  private async finish(frame: Frame, status: string): Promise<void> {
    if (!this.cleanup()) return;
    try {
      await this.session.writer.send(frame);
    } catch (error) {
      if (!isFrameTooLarge(error)) {
        this.session.close();
      } else {
        safelyLog(() => this.context.logger.error("Terminal frame over the size limit: %s", error.message));
        status = error.code;
        await this.session.writer.send({
          kind: "call.error", call_id: this.frame.call_id,
          error: { code: error.code, message: error.message, retryable: false, details: error.details },
        }).catch(() => this.session.close());
      }
    } finally {
      // Leave the in-flight set only once the terminal frame is written: a
      // draining server closes its connections as soon as nothing is in
      // flight, and must not drop a result that is still being sent.
      this.session.remove(this);
      this.session.logAccess(this.frame, status, this.started, this.context.logger);
    }
  }

  private closeIterator(): void {
    try {
      void this.output?.return?.().catch(() => undefined);
    } catch {
      // Iterator cleanup is best effort when a handler ignores cancellation.
    }
  }
}

/** Successful calls log at the configured level; failures stand out by severity. */
function accessLevel(status: string, configured: LogLevel): LogLevel {
  if (status === "ok") return configured;
  return status === ErrorCode.INTERNAL || status === ErrorCode.PROTOCOL_ERROR ? "error" : "warning";
}
