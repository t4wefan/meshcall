import { MeshCallError } from "./errors.js";
import { AsyncQueue, CreditWindow, INITIAL_STREAM_CREDIT } from "./flow.js";
import { safelyLog, type LogLevel, type RpcLogger } from "./logging.js";
import type {
  CallOpenFrame, ErrorPayload, Frame, StreamEndFrame, StreamItemFrame, StreamWindowFrame,
} from "./protocol.js";
import type { RpcContext, ServiceMethod } from "./service.js";
import type { FrameWriter } from "./transport.js";
import { validatePayload, type MethodValidators } from "./validation.js";

export interface RuntimeMethod {
  definition: ServiceMethod;
  validators: MethodValidators;
}

export type RuntimeServices = ReadonlyMap<string, ReadonlyMap<string, RuntimeMethod>>;

export interface SessionOptions {
  logger: RpcLogger;
  accessLog: boolean;
  logLevel: LogLevel;
}

export class ServerSession {
  private readonly calls = new Map<string, ServerCall>();
  private closed = false;

  public constructor(
    private readonly services: RuntimeServices,
    public readonly writer: FrameWriter,
    public readonly options: SessionOptions,
  ) {}

  public handle(frame: Frame): void {
    if (this.closed) return;
    if (frame.kind === "ping") {
      void this.writer.send({ kind: "pong", nonce: frame.nonce }).catch(() => this.close());
      return;
    }
    if (frame.kind === "call.open") {
      const existing = this.calls.get(frame.call_id);
      if (existing !== undefined) {
        void existing.fail(new MeshCallError("protocol_error", "Duplicate call_id"));
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
        void call.fail(new MeshCallError("cancelled", "Call was cancelled"));
      } else if (frame.kind === "stream.window") {
        call.receiveWindow(frame);
      } else if (frame.kind === "stream.item") {
        call.receiveItem(frame);
      } else if (frame.kind === "stream.end") {
        call.receiveEnd(frame);
      } else {
        throw new MeshCallError("protocol_error", "Unexpected client call frame");
      }
    } catch (error) {
      void call.fail(error);
    }
  }

  public remove(call: ServerCall): void {
    if (this.calls.get(call.frame.call_id) === call) this.calls.delete(call.frame.call_id);
  }

  public close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const call of [...this.calls.values()]) call.disconnect();
    this.writer.close();
  }

  public logAccess(frame: CallOpenFrame, status: string, started: number, logger: RpcLogger): void {
    if (!this.options.accessLog) return;
    safelyLog(() => logger.log(this.options.logLevel,
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
  private timer: ReturnType<typeof setTimeout> | undefined;
  private terminal = false;
  private inputClosed = false;
  private inputAllowance = 0;
  private inputSequence = 0;
  private outputSequence = 0;
  private output: AsyncIterator<unknown> | undefined;

  public constructor(
    private readonly session: ServerSession,
    public readonly frame: CallOpenFrame,
    private readonly method: RuntimeMethod,
  ) {
    this.context = {
      signal: this.controller.signal,
      deadlineUnixMs: frame.deadline_unix_ms ?? null,
      callId: frame.call_id,
      service: frame.service,
      method: frame.method,
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
    const deadline = this.context.deadlineUnixMs;
    if (deadline === null || this.terminal) return;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      void this.fail(new MeshCallError("deadline_exceeded", "Call deadline exceeded"));
    } else {
      this.timer = setTimeout(() => this.armDeadline(), Math.min(remaining, 2 ** 31 - 1));
    }
  }

  public receiveWindow(frame: StreamWindowFrame): void {
    if (this.method.definition.stream !== "server_stream" || frame.direction !== "server") {
      throw new MeshCallError("protocol_error", "Invalid output stream window");
    }
    this.credit.grant(frame.credit);
  }

  public receiveItem(frame: StreamItemFrame): void {
    if (this.method.definition.stream !== "client_stream" || frame.direction !== "client"
      || this.inputClosed || this.inputAllowance <= 0 || frame.sequence !== this.inputSequence) {
      throw new MeshCallError("protocol_error", "Invalid input stream item, sequence, or credit");
    }
    const value = validatePayload(this.method.validators.input, frame.payload, "input stream item");
    this.inputAllowance -= 1;
    this.inputSequence += 1;
    this.input.push(value);
  }

  public receiveEnd(frame: StreamEndFrame): void {
    if (this.method.definition.stream !== "client_stream" || frame.direction !== "client") {
      throw new MeshCallError("protocol_error", "Invalid input stream end");
    }
    this.inputClosed = true;
    this.input.close();
  }

  public async fail(error: unknown): Promise<void> {
    if (this.terminal) return;
    const payload: ErrorPayload = error instanceof MeshCallError
      ? { code: error.code, message: error.message, retryable: error.retryable, details: error.details }
      : { code: "internal", message: "Internal service error" };
    if (!(error instanceof MeshCallError)) {
      safelyLog(() => this.context.logger.error("Unhandled service exception: %s", error));
    }
    this.controller.abort(payload.code);
    await this.finish({ kind: "call.error", call_id: this.frame.call_id, error: payload }, payload.code);
    this.closeIterator();
  }

  public disconnect(): void {
    if (!this.cleanup()) return;
    this.controller.abort("connection_closed");
    this.closeIterator();
    this.session.logAccess(this.frame, "unavailable", this.started, this.context.logger);
  }

  private async run(): Promise<void> {
    try {
      const request = validatePayload(this.method.validators.request, this.frame.payload, "request");
      const definition = this.method.definition;
      let result: unknown;
      if (definition.stream === "unary") {
        result = await definition.handler(request, this.context);
      } else if (definition.stream === "client_stream") {
        this.inputAllowance = INITIAL_STREAM_CREDIT;
        await this.session.writer.send({
          kind: "stream.window", call_id: this.frame.call_id,
          direction: "client", credit: INITIAL_STREAM_CREDIT,
        });
        if (this.terminal) return;
        result = await definition.handler(request, this.inputItems(), this.context);
      } else {
        const iterable = await definition.handler(request, this.context);
        this.output = iterable[Symbol.asyncIterator]();
        if (this.terminal) {
          this.closeIterator();
          return;
        }
        while (!this.terminal) {
          await this.credit.acquire();
          const next = await this.output.next();
          if (this.terminal) return;
          if (next.done === true) break;
          const payload = validatePayload(this.method.validators.output, next.value, "output stream item");
          await this.session.writer.send({
            kind: "stream.item", call_id: this.frame.call_id, direction: "server",
            sequence: this.outputSequence, payload,
          });
          this.outputSequence += 1;
        }
        if (this.terminal) return;
        await this.session.writer.send({
          kind: "stream.end", call_id: this.frame.call_id, direction: "server",
        });
        result = null;
      }
      if (this.terminal) return;
      const payload = validatePayload(this.method.validators.response, result ?? null, "response");
      await this.finish({ kind: "call.result", call_id: this.frame.call_id, payload }, "ok");
    } catch (error) {
      await this.fail(error);
    }
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

  private cleanup(): boolean {
    if (this.terminal) return false;
    this.terminal = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    const error = new MeshCallError("cancelled", "Call is closed");
    this.credit.close(error);
    this.input.fail(error);
    this.session.remove(this);
    return true;
  }

  private async finish(frame: Frame, status: string): Promise<void> {
    if (!this.cleanup()) return;
    try {
      await this.session.writer.send(frame);
    } catch {
      this.session.close();
    } finally {
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
