import { MeshCallError } from "./errors.js";

export const INITIAL_STREAM_CREDIT = 16;

export class CreditWindow {
  private credit = 0;
  private closed = false;
  private failure: unknown;
  private readonly waiters: Array<{
    resolve: () => void;
    reject: (error: unknown) => void;
  }> = [];

  public acquire(): Promise<void> {
    if (this.closed) {
      return Promise.reject(this.failure);
    }
    if (this.credit > 0) {
      this.credit -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  public grant(credit: number): void {
    if (!Number.isSafeInteger(credit) || credit <= 0) {
      throw new MeshCallError("protocol_error", "Stream credit must be positive");
    }
    if (this.closed) {
      return;
    }
    this.credit += credit;
    while (this.credit > 0 && this.waiters.length > 0) {
      this.credit -= 1;
      this.waiters.shift()?.resolve();
    }
  }

  public close(error: unknown): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.failure = error;
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(error);
    }
  }
}

export class AsyncQueue<Item> {
  private readonly values: Item[] = [];
  private readonly waiters: Array<{
    resolve: (result: IteratorResult<Item>) => void;
    reject: (error: unknown) => void;
  }> = [];
  private ended = false;
  private failed = false;
  private failure: unknown;

  public push(value: Item): void {
    if (this.ended) {
      return;
    }
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter.resolve({ done: false, value });
    } else {
      this.values.push(value);
    }
  }

  public close(): void {
    if (this.ended) {
      return;
    }
    this.ended = true;
    this.flush();
  }

  public fail(error: unknown): void {
    if (this.failed) {
      return;
    }
    this.ended = true;
    this.failed = true;
    this.failure = error;
    this.flush();
  }

  public next(): Promise<IteratorResult<Item>> {
    if (this.values.length > 0) {
      const value = this.values.shift() as Item;
      return Promise.resolve({ done: false, value });
    }
    if (this.failed) {
      return Promise.reject(this.failure);
    }
    if (this.ended) {
      return Promise.resolve({ done: true, value: undefined as never });
    }
    return new Promise<IteratorResult<Item>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  private flush(): void {
    while (this.values.length > 0 && this.waiters.length > 0) {
      const value = this.values.shift() as Item;
      this.waiters.shift()?.resolve({ done: false, value });
    }
    if (this.values.length > 0 || this.waiters.length === 0) {
      return;
    }
    const waiters = this.waiters.splice(0);
    if (this.failed) {
      for (const waiter of waiters) {
        waiter.reject(this.failure);
      }
    } else if (this.ended) {
      for (const waiter of waiters) {
        waiter.resolve({ done: true, value: undefined as never });
      }
    }
  }
}

