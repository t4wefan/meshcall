import assert from "node:assert/strict";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { WebSocketServer } from "ws";

import {
  clientStreamMethod, defineService, defineType, MeshCallClient, MeshCallError, MeshCallServer,
  serverStreamMethod, unaryMethod, WebSocketRouter, type MeshCallServerOptions,
} from "../src/index.js";
import { decodeFrame, encodeFrame, PROTOCOL_VERSION, type CallOpenFrame } from "../src/protocol.js";
import { findRouterBinary } from "../src/router.js";

const routerMissing = (() => { try { findRouterBinary(); return false; } catch { return "Go Router binary not built"; } })();

interface Value { value: number }
interface Echo { value: number; metadata: Record<string, string>; remainingKnown: boolean }
interface Trickle { count: number; gapMs: number }

const valueType = defineType<Value>("Value", {
  type: "object", properties: { value: { type: "integer" } }, required: ["value"],
});
const echoType = defineType<Echo>("Echo", {
  type: "object",
  properties: {
    value: { type: "integer" },
    metadata: { type: "object", additionalProperties: { type: "string" } },
    remainingKnown: { type: "boolean" },
  },
  required: ["value", "metadata", "remainingKnown"],
});
const trickleType = defineType<Trickle>("Trickle", {
  type: "object", properties: { count: { type: "integer" }, gapMs: { type: "integer" } },
  required: ["count", "gapMs"],
});

const service = defineService({
  name: "test.v1.TimingService",
  sourceModule: "timing.test", sourceQualname: "TimingService",
  methods: {
    echo: unaryMethod({
      request: valueType, response: echoType,
      handler: (request, ctx) => {
        ctx.setTrailer("handled-by", "typescript");
        return { value: request.value, metadata: { ...ctx.metadata }, remainingKnown: ctx.remainingMs() !== null };
      },
    }),
    trickle: serverStreamMethod({
      request: trickleType, outputItem: valueType,
      handler: async function* ({ count, gapMs }, { signal }) {
        for (let value = 0; value < count; value += 1) {
          await delay(gapMs, undefined, { signal });
          yield { value };
        }
      },
    }),
    collect: clientStreamMethod({
      request: valueType, inputItem: valueType, response: valueType,
      handler: async (_, items, ctx) => {
        let total = 0;
        for await (const item of items) total += item.value;
        ctx.setTrailer("items-seen", String(total));
        return { value: total };
      },
    }),
  },
});

const isError = (code: string, details?: Record<string, unknown>) => (error: unknown) =>
  error instanceof MeshCallError && error.code === code
  && (details === undefined || JSON.stringify(error.details) === JSON.stringify(details));

async function running(options: Partial<MeshCallServerOptions> = {}, clientOptions = {}) {
  const server = new MeshCallServer({ services: [service], accessLog: false, ...options });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, clientOptions);
  return { server, client, close: async () => { await client.close(); await server.close(); } };
}

test("metadata reaches the handler and trailers come back on streams", { timeout: 5000 }, async (t) => {
  const app = await running();
  t.after(app.close);
  const echoed = await app.client.unary<Value, Echo>(service.name, "echo", { value: 4 }, {
    timeoutMs: 5000, metadata: { traceparent: "00-abc", tenant: "t1" },
  });
  assert.deepEqual(echoed, { value: 4, metadata: { traceparent: "00-abc", tenant: "t1" }, remainingKnown: true });
  await assert.rejects(
    app.client.unary(service.name, "echo", { value: 1 }, { metadata: { "meshcall-x": "1" } }),
    RangeError,
  );
  const stream = app.client.serverStream<Trickle, Value>(service.name, "trickle", { count: 2, gapMs: 1 });
  const seen: number[] = [];
  for await (const item of stream) seen.push(item.value);
  assert.deepEqual(seen, [0, 1]);
  assert.deepEqual(stream.trailers, {});
});

test("idle timeouts are enforced by the consuming side in each direction", { timeout: 5000 }, async (t) => {
  const app = await running();
  t.after(app.close);
  // Server stream: client watches arrival gaps.
  const slow = app.client.serverStream<Trickle, Value>(service.name, "trickle", { count: 3, gapMs: 300 }, { idleTimeoutMs: 50 });
  await assert.rejects(slow.next(), isError("deadline_exceeded", { source: "client", reason: "idle" }));
  const fast = app.client.serverStream<Trickle, Value>(service.name, "trickle", { count: 3, gapMs: 1 }, { idleTimeoutMs: 1000 });
  const seen: number[] = [];
  for await (const item of fast) seen.push(item.value);
  assert.deepEqual(seen, [0, 1, 2]);
  // Client stream: server watches arrival gaps.
  await assert.rejects(app.client.clientStream(
    service.name, "collect", { value: 0 },
    (async function* () { yield { value: 1 }; await delay(2000); yield { value: 2 }; })(),
    { idleTimeoutMs: 50 },
  ), isError("deadline_exceeded", { source: "server", reason: "idle" }));
});

test("the client deadline fires locally when the peer never answers", { timeout: 5000 }, async (t) => {
  const opened: CallOpenFrame[] = [];
  const fake = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(fake, "listening");
  t.after(() => new Promise<void>((resolve) => {
    for (const socket of fake.clients) socket.terminate();
    fake.close(() => resolve());
  }));
  fake.on("connection", (socket) => {
    socket.on("message", (data) => {
      const frame = decodeFrame(data);
      if (frame.kind === "hello") {
        socket.send(encodeFrame({ kind: "hello.ack", protocol: PROTOCOL_VERSION, connection_id: "fake" }));
      } else if (frame.kind === "call.open") {
        opened.push(frame);
      }
    });
  });
  const address = fake.address();
  assert.ok(typeof address === "object" && address !== null);
  const client = new MeshCallClient("ws://127.0.0.1:" + address.port, undefined, { deadlineGraceMs: 50 });
  t.after(() => client.close());
  const started = performance.now();
  await assert.rejects(
    client.unary(service.name, "echo", { value: 1 }, { timeoutMs: 100 }),
    isError("deadline_exceeded", { source: "client" }),
  );
  const elapsed = performance.now() - started;
  assert.ok(elapsed >= 100 && elapsed < 1000, "fired after " + elapsed + "ms");
  assert.equal(opened[0]?.timeout_ms, 100);
  // Streams use the same local budget and report the same source.
  const stream = client.serverStream(service.name, "trickle", { count: 1, gapMs: 1 }, { timeoutMs: 50 });
  await assert.rejects(stream.next(), isError("deadline_exceeded", { source: "client" }));
});

test("a direct server drops silent clients while pinging clients stay connected", { timeout: 8000 }, async (t) => {
  const app = await running({ idleTimeoutMs: 1000 }, { pingIntervalMs: null });
  t.after(app.close);
  const chatty = new MeshCallClient("ws://127.0.0.1:" + app.server.boundPort, undefined, {
    pingIntervalMs: 200, pingTimeoutMs: 500,
  });
  t.after(() => chatty.close());
  await app.client.connect();
  await chatty.connect();
  await delay(1600);
  // The silent client's socket was closed by the server; a new call reconnects.
  assert.deepEqual(
    (await chatty.unary<Value, Echo>(service.name, "echo", { value: 2 })).value, 2,
  );
  // The in-flight call of a silent client fails as a retryable connection loss.
  const silent = new MeshCallClient("ws://127.0.0.1:" + app.server.boundPort, undefined, { pingIntervalMs: null });
  t.after(() => silent.close());
  const pending = silent.serverStream<Trickle, Value>(service.name, "trickle", { count: 1, gapMs: 3000 });
  await assert.rejects(pending.next(), (error: unknown) =>
    error instanceof MeshCallError && error.code === "unavailable" && error.retryable);
});

test("unknown frame fields are ignored and undefined payloads are sent as null", () => {
  const frame = decodeFrame(JSON.stringify({
    kind: "call.open", call_id: "x", service: "s", method: "m", payload: {},
    timeout_ms: 5, metadata: { k: "v" }, future_field: [1, 2, 3],
  }));
  assert.equal(frame.kind, "call.open");
  assert.equal((frame as CallOpenFrame).timeout_ms, 5);
  assert.throws(() => decodeFrame(JSON.stringify({ kind: "call.future", call_id: "x" })));
  assert.throws(() => decodeFrame(JSON.stringify({
    kind: "call.open", call_id: "x", service: "s", method: "m", payload: {}, metadata: { k: 1 },
  })));
  assert.throws(() => decodeFrame(JSON.stringify({
    kind: "call.open", call_id: "x", service: "s", method: "m", payload: {}, timeout_ms: 0,
  })));
});

test("the client reconnects after a direct server restart, bounded by the call timeout", { timeout: 10000 }, async (t) => {
  const first = new MeshCallServer({ services: [service], accessLog: false });
  await first.start();
  const port = first.boundPort;
  const client = new MeshCallClient("ws://127.0.0.1:" + port, undefined, {
    reconnect: { initialDelayMs: 50, maxDelayMs: 200 },
  });
  t.after(() => client.close());
  assert.equal((await client.unary<Value, Echo>(service.name, "echo", { value: 1 })).value, 1);
  await first.close();
  await assert.rejects(
    client.unary(service.name, "echo", { value: 2 }, { timeoutMs: 300 }),
    (error: unknown) => error instanceof MeshCallError
      && (error.code === "unavailable" || error.code === "deadline_exceeded"),
  );
  const second = new MeshCallServer({ services: [service], port, accessLog: false });
  t.after(() => second.close());
  await second.start();
  assert.equal((await client.unary<Value, Echo>(service.name, "echo", { value: 3 }, { timeoutMs: 5000 })).value, 3);
});

test("a Router-mode server re-registers after the Router restarts", { timeout: 15000, skip: routerMissing }, async (t) => {
  const router = new WebSocketRouter();
  await router.start();
  const port = router.boundPort;
  const endpoint = "ws://127.0.0.1:" + port;
  const server = new MeshCallServer({
    services: [service], router: { endpoint, instanceId: "phoenix" }, accessLog: false,
    reconnect: { initialDelayMs: 50, maxDelayMs: 200 },
  });
  t.after(() => server.close());
  await server.start();
  const client = new MeshCallClient(endpoint, undefined, { reconnect: { initialDelayMs: 50, maxDelayMs: 200 } });
  t.after(() => client.close());
  assert.equal((await client.unary<Value, Echo>(service.name, "echo", { value: 1 })).value, 1);
  await router.stop();
  for (let i = 0; i < 100 && server.routerConnected; i += 1) await delay(20);
  assert.equal(server.routerConnected, false);
  assert.equal(server.isRunning, true);
  const restarted = new WebSocketRouter({ port });
  t.after(() => restarted.stop());
  await restarted.start();
  for (let i = 0; i < 200 && !server.routerConnected; i += 1) await delay(20);
  assert.equal(server.routerConnected, true);
  assert.equal((await client.unary<Value, Echo>(service.name, "echo", { value: 2 }, { timeoutMs: 5000 })).value, 2);
});

test("a malformed frame that names a call fails only that call", { timeout: 5000 }, async (t) => {
  const app = await running();
  t.after(app.close);
  await app.client.connect();
  const stream = app.client.serverStream<Trickle, Value>(service.name, "trickle", { count: 3, gapMs: 500 });
  const first = stream.next();
  await delay(50);
  // Reach past the public API to inject a bad window frame for the open call.
  const socket = (app.client as unknown as { socket: import("ws").WebSocket }).socket;
  const streams = (app.client as unknown as { streams: Map<string, unknown> }).streams;
  const [callId] = streams.keys();
  assert.ok(callId);
  socket.send(JSON.stringify({ kind: "stream.window", call_id: callId, direction: "server", credit: -1 }));
  await assert.rejects(first, isError("protocol_error"));
  assert.equal((await app.client.unary<Value, Echo>(service.name, "echo", { value: 7 })).value, 7);
  // The same on the server side: the client stays connected and other calls work.
  const serverSocket = socket;
  serverSocket.send(JSON.stringify({ kind: "call.cancel", call_id: "nope", reason: 5 }));
  await delay(20);
  assert.equal((await app.client.unary<Value, Echo>(service.name, "echo", { value: 8 })).value, 8);
});

test("drain finishes in-flight streams, refuses new work, then stops", { timeout: 10000, skip: routerMissing }, async (t) => {
  // Direct mode: the listener closes while existing calls run to completion.
  const app = await running();
  const port = app.server.boundPort;
  const stream = app.client.serverStream<Trickle, Value>(service.name, "trickle", { count: 5, gapMs: 40 });
  assert.equal((await stream.next()).value?.value, 0);
  const closing = app.server.close({ drain: true, drainTimeoutMs: 5000 });
  await delay(50);
  const late = new MeshCallClient("ws://127.0.0.1:" + port, undefined, { reconnect: { enabled: false } });
  t.after(() => late.close());
  await assert.rejects(late.unary(service.name, "echo", { value: 1 }, { timeoutMs: 1000 }));
  const rest: number[] = [];
  for await (const item of stream) rest.push(item.value);
  assert.deepEqual(rest, [1, 2, 3, 4]);
  await closing;
  await app.client.close();

  // Router mode: the draining instance leaves the pool; its routed call finishes.
  const router = new WebSocketRouter();
  t.after(() => router.stop());
  await router.start();
  const endpoint = "ws://127.0.0.1:" + router.boundPort;
  const tagged = (tag: string) => defineService({
    ...service,
    methods: {
      ...service.methods,
      echo: unaryMethod({
        request: valueType, response: echoType, description: "Say who answered.",
        handler: async (request) => {
          await delay(request.value);
          return { value: request.value, metadata: { by: tag }, remainingKnown: false };
        },
      }),
    },
  });
  const a = new MeshCallServer({ services: [tagged("a")], router: { endpoint, instanceId: "a" }, accessLog: false });
  const b = new MeshCallServer({ services: [tagged("b")], router: { endpoint, instanceId: "b" }, accessLog: false });
  t.after(async () => { await a.close(); await b.close(); });
  await a.start();
  await b.start();
  const client = new MeshCallClient(endpoint);
  t.after(() => client.close());
  const who = async (ms = 0) => (await client.unary<Value, Echo>(service.name, "echo", { value: ms })).metadata.by;
  assert.equal(await who(), "a"); // round robin starts at a
  const slow = who(400);          // now routed to b... keep calling until one lands on a
  await delay(50);
  const pendingOnA = who(400);    // round robin: a
  await delay(50);
  const draining = a.close({ drain: true, drainTimeoutMs: 5000 });
  await delay(50);
  assert.deepEqual([await who(), await who()], ["b", "b"]);
  assert.equal(await pendingOnA, "a");
  await slow;
  await draining;
  assert.equal(a.isRunning, false);
  assert.equal(await who(), "b");
});

test("the retry policy re-sends unary calls only when it is safe", { timeout: 5000 }, async (t) => {
  const seen = new Map<string, number>();
  const flakyReq = defineType<{ tag: string; failTimes: number }>("Flaky", {
    type: "object", properties: { tag: { type: "string" }, failTimes: { type: "integer" } }, required: ["tag", "failTimes"],
  });
  const attemptsType = defineType<{ attempts: number }>("Attempts", {
    type: "object", properties: { attempts: { type: "integer" } }, required: ["attempts"],
  });
  const flaky = defineService({
    name: "test.v1.FlakyService", description: "Fails a configurable number of times.",
    sourceModule: "timing.test", sourceQualname: "Flaky",
    methods: {
      flaky: unaryMethod({
        description: "Fail failTimes times with retryable unavailable, then succeed.",
        request: flakyReq, response: attemptsType,
        handler: ({ tag, failTimes }, ctx) => {
          const count = (seen.get(tag) ?? 0) + 1;
          seen.set(tag, count);
          if (count <= failTimes) {
            throw new MeshCallError("unavailable", "try again", { retryable: true, details: { key: ctx.idempotencyKey } });
          }
          return { attempts: count };
        },
      }),
    },
  });
  const server = new MeshCallServer({ services: [flaky], accessLog: false });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, {
    retry: { maxAttempts: 3, initialDelayMs: 10, maxDelayMs: 20 },
  });
  t.after(async () => { await client.close(); await server.close(); });
  const call = (tag: string, failTimes: number, options = {}) =>
    client.unary<{ tag: string; failTimes: number }, { attempts: number }>(flaky.name, "flaky", { tag, failTimes }, options);

  await assert.rejects(call("plain", 1), (e: unknown) => e instanceof MeshCallError && e.code === "unavailable" && !("attempts" in (e.details ?? {})));
  assert.equal((await call("idem", 2, { idempotent: true })).attempts, 3);
  assert.equal((await call("keyed", 1, { idempotencyKey: "k-1" })).attempts, 2);
  await assert.rejects(call("hopeless", 10, { idempotent: true }), (e: unknown) =>
    e instanceof MeshCallError && JSON.stringify(e.details) === JSON.stringify({ key: null, attempts: 3 }));
  await assert.rejects(call("budget", 10, { idempotent: true, timeoutMs: 15 }), isError("unavailable"));
});

test("maxInflight is enforced locally in direct mode", { timeout: 5000 }, async (t) => {
  const server = new MeshCallServer({ services: [service], accessLog: false, maxInflight: 1 });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, { retry: false });
  t.after(async () => { await client.close(); await server.close(); });
  const stream = client.serverStream<Trickle, Value>(service.name, "trickle", { count: 3, gapMs: 200 });
  await stream.next();
  await assert.rejects(client.unary(service.name, "echo", { value: 1 }), (e: unknown) =>
    e instanceof MeshCallError && e.code === "resource_exhausted" && e.retryable
    && JSON.stringify(e.details) === JSON.stringify({ source: "server", reason: "overloaded" }));
  const rest: number[] = [];
  for await (const item of stream) rest.push(item.value);
  assert.deepEqual(rest, [1, 2]);
  assert.equal((await client.unary<Value, Echo>(service.name, "echo", { value: 4 })).value, 4);
});
