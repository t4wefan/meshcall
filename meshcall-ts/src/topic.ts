/**
 * In-process fan-out for subscriptions (docs/protocol.md → Subscriptions).
 *
 *   @method.subscription({ cursor: "seq", resume: "since", … })
 *   events({ since }) { return topic.subscribe(since); }
 *
 * Fan-out across instances is not a Topic's job: route subscribers to the
 * instance that produces the events (a targeted service) or bridge an
 * external broker into each instance's Topic.
 */
import { InvalidArgumentError, MeshCallError } from "./errors.js";

export interface TopicOptions<T> {
  /** Items kept for `subscribe(after)` to replay; needs `cursor`. */
  readonly history?: number;
  /** The item's position; cursors must be ordered (numbers or strings). */
  readonly cursor?: (item: T) => number | string;
  /** Items a subscriber may fall behind by before it is cut off. Default 1024. */
  readonly queueSize?: number;
}

class Subscriber<T> implements AsyncIterableIterator<T> {
  public readonly items: T[] = [];
  public failure: MeshCallError | undefined;
  public closed = false;
  private waiter: (() => void) | undefined;

  public constructor(private readonly leave: (subscriber: Subscriber<T>) => void) {}

  public wake(): void {
    this.waiter?.();
    this.waiter = undefined;
  }

  public [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return this;
  }

  public async next(): Promise<IteratorResult<T>> {
    for (;;) {
      if (this.items.length > 0) return { done: false, value: this.items.shift() as T };
      if (this.failure !== undefined) {
        this.leave(this);
        throw this.failure;
      }
      if (this.closed) {
        this.leave(this);
        return { done: true, value: undefined };
      }
      await new Promise<void>((resolve) => { this.waiter = resolve; });
    }
  }

  public async return(): Promise<IteratorResult<T>> {
    this.closed = true;
    this.leave(this);
    return { done: true, value: undefined };
  }
}

/**
 * Publish items to every subscriber in this process. A subscriber that falls
 * `queueSize` items behind is cut off with retryable `resource_exhausted`
 * (`reason: "slow_subscriber"`), so its client resubscribes after its cursor.
 * With `history` and `cursor`, `subscribe(after)` first replays the kept items
 * newer than `after`; a cursor older than the history is refused with
 * `invalid_argument` (`reason: "cursor_expired"`) rather than skipping items.
 */
export class Topic<T> {
  private readonly subscribersSet = new Set<Subscriber<T>>();
  private readonly history: T[] = [];
  private evicted: number | string | undefined;
  private closed = false;
  private readonly keep: number;
  private readonly queueSize: number;

  public constructor(private readonly options: TopicOptions<T> = {}) {
    this.keep = options.history ?? 0;
    this.queueSize = options.queueSize ?? 1024;
    if (!Number.isSafeInteger(this.keep) || this.keep < 0 || !Number.isSafeInteger(this.queueSize) || this.queueSize < 1) {
      throw new RangeError("history must be >= 0 and queueSize positive");
    }
    if (this.keep > 0 && options.cursor === undefined) throw new TypeError("history needs cursor to know what was seen");
  }

  public get subscribers(): number {
    return this.subscribersSet.size;
  }

  /** Hand `item` to every subscriber; never blocks. */
  public publish(item: T): void {
    if (this.closed) throw new Error("Topic is closed");
    if (this.keep > 0) {
      if (this.history.length === this.keep) this.evicted = this.options.cursor!(this.history.shift() as T);
      this.history.push(item);
    }
    for (const subscriber of this.subscribersSet) {
      if (subscriber.items.length >= this.queueSize) {
        this.subscribersSet.delete(subscriber);
        subscriber.failure = new MeshCallError("resource_exhausted", "Subscriber fell too far behind", {
          retryable: true, details: { source: "server", reason: "slow_subscriber" },
        });
      } else {
        subscriber.items.push(item);
      }
      subscriber.wake();
    }
  }

  /** Items published from now on, preceded by kept ones after `after`. Registered immediately. */
  public subscribe(after?: number | string | null): AsyncIterableIterator<T> {
    if (this.closed) throw new Error("Topic is closed");
    const subscriber = new Subscriber<T>((s) => this.subscribersSet.delete(s));
    if (after !== undefined && after !== null && this.keep > 0) {
      if (this.evicted !== undefined && after < this.evicted) {
        throw new InvalidArgumentError("The cursor is older than this topic's history", {
          details: { source: "server", reason: "cursor_expired" },
        });
      }
      for (const item of this.history) {
        if (this.options.cursor!(item) > after) subscriber.items.push(item);
      }
    }
    this.subscribersSet.add(subscriber);
    return subscriber;
  }

  /** End every subscription normally; later publishes fail. */
  public close(): void {
    this.closed = true;
    for (const subscriber of this.subscribersSet) {
      subscriber.closed = true;
      subscriber.wake();
    }
    this.subscribersSet.clear();
  }
}
