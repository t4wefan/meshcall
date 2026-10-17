/**
 * Subscriptions (docs/protocol.md → Subscriptions), shared by the WebSocket
 * and HTTP clients; no Node imports.
 */
import { MeshCallError } from "./errors.js";
import { safelyLog, type RpcLogger } from "./log.js";
import { reconnectDelays, type ReconnectPolicy } from "./policy.js";
import type { Metadata } from "./protocol.js";
import type { RpcStream } from "./rpc.js";

const RESUMABLE = new Set(["unavailable", "resource_exhausted"]);
const NO_CURSOR = Symbol("no cursor");

/**
 * Successive server-stream calls presented as one iterator: after a retryable
 * failure the call is reopened with the reconnect backoff, resuming after the
 * last item's cursor. A non-retryable error, `cancel()` / `break`, or the
 * server ending the stream ends it.
 */
export class MeshCallSubscription<Item> implements RpcStream<Item> {
  private stream: RpcStream<Item> | undefined;
  private last: unknown = NO_CURSOR;
  private delays: Iterator<number> | undefined;
  private done = false;
  private lastTrailers: Readonly<Metadata> = {};

  /** @internal */
  public constructor(
    private readonly open: (request: Record<string, unknown>) => RpcStream<Item>,
    private readonly request: Record<string, unknown>,
    private readonly spec: { readonly cursor?: string; readonly resume?: string },
    private readonly reconnect: ReconnectPolicy,
    private readonly logger: RpcLogger,
    private readonly name: string,
  ) {}

  public [Symbol.asyncIterator](): AsyncIterableIterator<Item> {
    return this;
  }

  public async next(): Promise<IteratorResult<Item>> {
    for (;;) {
      if (this.done) return { done: true, value: undefined };
      if (this.stream === undefined) {
        const resume = this.spec.resume !== undefined && this.last !== NO_CURSOR
          ? { [this.spec.resume]: this.last } : {};
        this.stream = this.open({ ...this.request, ...resume });
      }
      const stream = this.stream;
      let result: IteratorResult<Item>;
      try {
        result = await stream.next();
      } catch (error) {
        this.lastTrailers = stream.trailers;
        this.stream = undefined;
        if (!(error instanceof MeshCallError) || !error.retryable || !RESUMABLE.has(error.code)) {
          this.done = true;
          throw error;
        }
        await this.backoff(error);
        continue;
      }
      if (result.done === true) {
        this.lastTrailers = stream.trailers;
        this.done = true;
        return result;
      }
      this.delays = undefined; // an item arrived: the next failure starts over
      if (this.spec.cursor !== undefined) this.last = (result.value as Record<string, unknown>)[this.spec.cursor];
      return result;
    }
  }

  private async backoff(error: MeshCallError): Promise<void> {
    this.delays ??= reconnectDelays(this.reconnect);
    const next = this.reconnect.enabled === false ? undefined : this.delays.next();
    if (next === undefined || next.done === true) {
      this.done = true;
      throw error;
    }
    safelyLog(() => this.logger.warning("Subscription %s interrupted (%s); resubscribing in %dms%s", this.name,
      error.message, Math.round(next.value), this.last === NO_CURSOR ? "" : " after " + JSON.stringify(this.last)));
    await new Promise((resolve) => setTimeout(resolve, next.value));
  }

  /** Trailers of the latest call; populated once it has terminated. */
  public get trailers(): Readonly<Metadata> {
    return this.stream?.trailers ?? this.lastTrailers;
  }

  public async cancel(reason = "client_closed"): Promise<void> {
    this.done = true;
    const stream = this.stream;
    this.stream = undefined;
    await stream?.cancel(reason);
  }

  public async return(): Promise<IteratorResult<Item>> {
    await this.cancel("iterator_closed");
    return { done: true, value: undefined };
  }
}

