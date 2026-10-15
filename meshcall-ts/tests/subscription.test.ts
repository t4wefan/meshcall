import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  contractEntry, defineService, defineType, MeshCallClient, MeshCallError, MeshCallServer, subscriptionMethod, Topic,
} from "../src/index.js";

interface Tick { seq: number }
interface Ticks { since?: number | null }

const tickType = defineType<Tick>("Tick", {
  type: "object", properties: { seq: { type: "integer" } }, required: ["seq"],
});
const ticksType = defineType<Ticks>("Ticks", {
  type: "object", properties: { since: { type: ["integer", "null"], default: null } },
});

const topic = new Topic<Tick>({ history: 1000, cursor: (tick) => tick.seq });
const opened: Array<number | null | undefined> = [];
let failEvery = 0;

const service = defineService({
  name: "test.v1.Feed", description: "Subscription fixtures.",
  sourceModule: "subscription.test", sourceQualname: "Feed",
  methods: {
    ticks: subscriptionMethod({
      description: "Every tick after since.", readOnly: true, cursor: "seq", resume: "since",
      request: ticksType, outputItem: tickType,
      handler: async function* ({ since }) {
        opened.push(since);
        let sent = 0;
        for await (const tick of topic.subscribe(since)) {
          yield tick;
          sent += 1;
          if (failEvery > 0 && sent === failEvery) throw new MeshCallError("unavailable", "flap", { retryable: true });
        }
      },
    }),
    broken: subscriptionMethod({
      description: "Fails for good after one item.", readOnly: true, request: ticksType, outputItem: tickType,
      handler: async function* () {
        yield { seq: 0 };
        throw new MeshCallError("app.gone", "no more");
      },
    }),
  },
});

async function running() {
  const server = new MeshCallServer({ services: [service], accessLog: false });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, {
    reconnect: { initialDelayMs: 10, maxDelayMs: 50 },
    logger: { bind() { return this; }, log() {}, debug() {}, info() {}, warning() {}, error() {} },
  });
  return { client, close: async () => { await client.close(); await server.close(); } };
}

async function take(iterator: AsyncIterator<Tick>, n: number): Promise<number[]> {
  const out: number[] = [];
  for (let i = 0; i < n; i += 1) {
    const next = await iterator.next();
    assert.equal(next.done, false);
    out.push((next.value as Tick).seq);
  }
  return out;
}

test("the contract declares the subscription", () => {
  const methods = contractEntry(service).methods as Array<Record<string, unknown>>;
  assert.equal(methods[0]?.stream, "server_stream");
  assert.deepEqual(methods[0]?.subscription, { cursor: "seq", resume: "since" });
  assert.deepEqual(methods[1]?.subscription, { cursor: null, resume: null });
  assert.throws(() => subscriptionMethod({
    description: "", cursor: "seq", request: ticksType, outputItem: tickType, handler: async function* () {},
  }), /together/);
  assert.throws(() => subscriptionMethod({
    description: "", cursor: "nope", resume: "since", request: ticksType, outputItem: tickType, handler: async function* () {},
  }), /not a property/);
});

test("resumes after retryable failures and dropped connections", { timeout: 10000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  failEvery = 3;
  opened.length = 0;
  const ticks = client.subscribe<Ticks, Tick>("test.v1.Feed", "ticks", {}, { cursor: "seq", resume: "since" });
  const first = ticks.next();
  while (topic.subscribers === 0) await delay(5);
  for (let seq = 0; seq < 10; seq += 1) topic.publish({ seq });
  assert.deepEqual([((await first).value as Tick).seq, ...await take(ticks, 9)], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  assert.deepEqual(opened, [null, 2, 5, 8]); // the schema default fills since

  // The connection itself drops: every call on it fails with retryable unavailable.
  failEvery = 0;
  topic.publish({ seq: 10 });
  assert.deepEqual(await take(ticks, 1), [10]);
  (client as unknown as { socket: { terminate(): void } }).socket.terminate();
  topic.publish({ seq: 11 });
  assert.deepEqual(await take(ticks, 1), [11]);
  assert.equal(opened.at(-1), 10);
  await ticks.return();
});

test("non-retryable errors end the subscription", { timeout: 5000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const broken = client.subscribe<Ticks, Tick>("test.v1.Feed", "broken", {});
  assert.deepEqual(await take(broken, 1), [0]);
  await assert.rejects(broken.next(), (error) => error instanceof MeshCallError && error.code === "app.gone");
  assert.deepEqual(await broken.next(), { done: true, value: undefined });
});

test("a topic cuts off slow subscribers and refuses expired cursors", async () => {
  const local = new Topic<Tick>({ history: 2, cursor: (tick) => tick.seq, queueSize: 2 });
  const slow = local.subscribe();
  for (let seq = 0; seq < 5; seq += 1) local.publish({ seq });
  assert.deepEqual(await take(slow, 2), [0, 1]);
  await assert.rejects(slow.next(), (error) => error instanceof MeshCallError && error.code === "resource_exhausted" && error.retryable);
  assert.equal(local.subscribers, 0);
  const replay = local.subscribe(2);
  assert.deepEqual(await take(replay, 2), [3, 4]);
  assert.throws(() => local.subscribe(1), (error) => error instanceof MeshCallError
    && JSON.stringify(error.details) === JSON.stringify({ source: "server", reason: "cursor_expired" }));
  local.close();
  assert.deepEqual(await replay.next(), { done: true, value: undefined });
});
