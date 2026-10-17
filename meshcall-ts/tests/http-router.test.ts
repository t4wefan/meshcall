import assert from "node:assert/strict";
import test from "node:test";

import {
  MeshCallError, Router, WebSocketRouter, mutates, query, serve, service, stream, z,
  type DuplexChannel,
} from "../src/index.js";
import { HTTPRouter } from "../src/web.js";

const Sum = z.object({ total: z.int() }).meta({ id: "Sum" });
const Item = z.object({ value: z.int() }).meta({ id: "Item" });
const Tick = z.object({ seq: z.int() }).meta({ id: "Tick" });

@service("test.v1.HttpShape", { description: "Shapes over the HTTP client." })
class HttpShape {
  private subscriptionCalls = 0;

  @query({ description: "Add two numbers.", input: { a: z.int(), b: z.int() }, output: Sum })
  async add(a: number, b: number) {
    return { total: a + b };
  }

  @stream({ description: "Yield 0..n-1.", input: { n: z.int() }, output: Item })
  async *count(n: number) {
    for (let value = 0; value < n; value += 1) yield { value };
  }

  @mutates({
    description: "Add every uploaded value.", idempotent: true,
    input: {}, streamInput: Item, output: Sum,
  })
  async upload(items: AsyncIterable<{ value: number }>) {
    let total = 0;
    for await (const item of items) total += item.value;
    return { total };
  }

  @mutates({ description: "Always fails.", destructive: true, input: {}, output: Sum })
  async fail(): Promise<{ total: number }> {
    throw new MeshCallError("app.nope", "no", { details: { from: "service" } });
  }

  @mutates({
    description: "Tick, failing once to force a resume.", reason: "duplex is not part of this test",
    input: {}, streamInput: Item, streamOutput: Tick,
  })
  async talk(channel: DuplexChannel<{ value: number }, { seq: number }>) {
    for await (const item of channel) await channel.send({ seq: item.value });
  }
}

async function running() {
  const router = new WebSocketRouter();
  await router.start();
  const server = await serve(new HttpShape(), {
    router: new Router({ url: `ws://127.0.0.1:${router.boundPort}` }),
  });
  const http = await HTTPRouter.connect(`http://127.0.0.1:${router.boundPort}`);
  return {
    http,
    close: async () => {
      await http.close();
      await server.close();
      await router.close();
    },
  };
}

test("HTTPRouter carries every HTTP call shape", { timeout: 20000 }, async () => {
  const { http, close } = await running();
  try {
    assert.deepEqual(await http.unary("test.v1.HttpShape", "add", { a: 2, b: 3 }), { total: 5 });

    const items: number[] = [];
    for await (const item of http.serverStream<{ n: number }, { value: number }>("test.v1.HttpShape", "count", { n: 4 })) {
      items.push(item.value);
    }
    assert.deepEqual(items, [0, 1, 2, 3]);

    async function* upload() {
      yield { value: 1 };
      yield { value: 2 };
    }
    assert.deepEqual(await http.clientStream("test.v1.HttpShape", "upload", {}, upload()), { total: 3 });

    await assert.rejects(http.unary("test.v1.HttpShape", "fail", {}), (error: unknown) => {
      assert.ok(error instanceof MeshCallError);
      assert.equal(error.code, "app.nope");
      assert.equal((error.details as { from?: string }).from, "service");
      return true;
    });

    assert.throws(() => http.duplex("test.v1.HttpShape", "talk", {}), (error: unknown) => {
      assert.ok(error instanceof MeshCallError);
      assert.equal((error.details as { reason?: string }).reason, "duplex_over_http");
      return true;
    });

    const services = await http.listServices();
    assert.deepEqual(services.map((entry) => entry.name), ["test.v1.HttpShape"]);
  } finally {
    await close();
  }
});

test("HTTPRouter.WithOptions applies metadata and timeouts", { timeout: 20000 }, async () => {
  const { http, close } = await running();
  try {
    const scoped = http.withOptions({ metadata: { "x-trace": "t1" }, timeoutMs: 5000 });
    assert.deepEqual(await scoped.unary("test.v1.HttpShape", "add", { a: 1, b: 1 }), { total: 2 });
  } finally {
    await close();
  }
});
