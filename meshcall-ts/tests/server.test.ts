import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  clientStreamMethod, ConsoleRpcLogger, defineService, defineType, MeshCallClient,
  MeshCallError, MeshCallServer, renderContract, serverStreamMethod, unaryMethod,
  type LogContext, type LogLevel, type MeshCallServerOptions, type RpcLogger,
} from "../src/index.js";
import { decodeFrame } from "../src/protocol.js";

interface Value { value: number }
const valueType = defineType<Value>("Value", {
  type: "object", properties: { value: { type: "integer", minimum: 0 } },
  required: ["value"], additionalProperties: false,
});

const service = defineService({
  name: "test.v1.StreamService",
  sourceModule: "server.test", sourceQualname: "StreamService",
  balance: { kind: "least_inflight" },
  methods: {
    echo: unaryMethod({
      request: valueType, response: valueType,
      handler: (request) => request,
    }),
    download: serverStreamMethod({
      request: valueType, outputItem: valueType,
      handler: async function* (request, { signal }) {
        for (let value = 0; value < request.value; value += 1) {
          signal.throwIfAborted();
          yield { value };
        }
      },
    }),
    upload: clientStreamMethod({
      request: valueType, inputItem: valueType, response: valueType,
      handler: async (request, items, { logger }) => {
        let total = request.value;
        for await (const item of items) total += item.value;
        logger.info("upload complete");
        return { value: total };
      },
    }),
    invalidResult: unaryMethod({
      request: valueType, response: valueType,
      handler: () => ({ value: -1 }),
    }),
    invalidStream: serverStreamMethod({
      request: valueType, outputItem: valueType,
      handler: async function* () { yield { value: -1 }; },
    }),
  },
});

const isError = (code: string) => (error: unknown) =>
  error instanceof MeshCallError && error.code === code;

async function running(options: Partial<MeshCallServerOptions> = {}) {
  const server = new MeshCallServer({ services: [service], accessLog: false, ...options });
  await server.start();
  const client = new MeshCallClient(
    options.unixPath === undefined ? "ws://127.0.0.1:" + server.boundPort
      : { unixPath: options.unixPath },
  );
  return { server, client, close: async () => { await client.close(); await server.close(); } };
}

test("service contract exports both streaming shapes and inherited balancing", () => {
  const document = JSON.parse(renderContract([service]));
  const methods = document.services[0].methods;
  assert.equal(methods[1].stream, "server_stream");
  assert.equal(methods[1].response, null);
  assert.equal(methods[1].output_item.qualname, "Value");
  assert.equal(methods[2].stream, "client_stream");
  assert.equal(methods[2].input_item.qualname, "Value");
  assert.equal(methods[2].balance.kind, "least_inflight");
});

test("real TypeScript server round-trips streams beyond the initial window", { timeout: 5000 }, async (t) => {
  const app = await running();
  t.after(app.close);
  const stream = app.client.serverStream<Value, Value>(service.name, "download", { value: 64 });
  const downloaded: number[] = [];
  for await (const item of stream) downloaded.push(item.value);
  assert.deepEqual(downloaded, Array.from({ length: 64 }, (_, i) => i));
  const result = await app.client.clientStream<Value, Value, Value>(
    service.name, "upload", { value: 7 },
    (async function* () { for (const value of downloaded) yield { value }; })(),
  );
  assert.equal(result.value, 2023);
  assert.deepEqual(await app.client.unary(service.name, "echo", { value: 9 }), { value: 9 });
});

test("request, response, input items and output items are validated at runtime", { timeout: 5000 }, async (t) => {
  const app = await running();
  t.after(app.close);
  await assert.rejects(app.client.unary(service.name, "echo", { value: "wrong" }), isError("invalid_argument"));
  await assert.rejects(app.client.unary(service.name, "invalidResult", { value: 1 }), isError("invalid_argument"));
  await assert.rejects(app.client.clientStream(
    service.name, "upload", { value: 0 },
    (async function* () { yield { value: "wrong" }; })(),
  ), isError("invalid_argument"));
  const invalid = app.client.serverStream(service.name, "invalidStream", { value: 1 });
  await assert.rejects(invalid.next(), isError("invalid_argument"));
  // Invalid calls do not poison other calls on the same connection.
  assert.deepEqual(await app.client.unary(service.name, "echo", { value: 3 }), { value: 3 });
});

test("a paused output stream is bounded and does not block concurrent unary calls", { timeout: 5000 }, async (t) => {
  let produced = 0;
  let closed = false;
  const bounded = defineService({
    ...service,
    methods: {
      ...service.methods,
      download: serverStreamMethod({
        request: valueType, outputItem: valueType,
        handler: async function* () {
          try {
            for (let value = 0; value < 100; value += 1) {
              produced += 1;
              yield { value };
            }
          } finally { closed = true; }
        },
      }),
    },
  });
  const app = await running({ services: [bounded] });
  t.after(app.close);
  await app.client.connect();
  const stream = app.client.serverStream(service.name, "download", { value: 100 });
  for (let i = 0; i < 100 && produced < 16; i += 1) await delay(5);
  assert.equal(produced, 16);
  assert.deepEqual(await app.client.unary(service.name, "echo", { value: 9 }), { value: 9 });
  assert.equal(produced, 16);
  await stream.cancel();
  for (let i = 0; i < 100 && !closed; i += 1) await delay(5);
  assert.equal(closed, true);
});

test("stream deadlines and iterator exit cancel handlers and log once", { timeout: 5000 }, async (t) => {
  const logs: Array<{ level: LogLevel; message: string; context: LogContext }> = [];
  class RecordingLogger extends ConsoleRpcLogger {
    public constructor(private readonly fields: LogContext = {}) { super(); }
    public override bind(fields: LogContext): RpcLogger {
      return new RecordingLogger({ ...this.fields, ...fields });
    }
    public override log(level: LogLevel, message: string): void {
      logs.push({ level, message, context: this.fields });
    }
  }
  let cancelled = 0;
  const cancellable = defineService({
    ...service,
    methods: {
      ...service.methods,
      download: serverStreamMethod({
        request: valueType, outputItem: valueType,
        handler: async function* (_, { signal }) {
          try {
            yield { value: 0 };
            await delay(1000, undefined, { signal });
            yield { value: 1 };
          } finally { cancelled += 1; }
        },
      }),
    },
  });
  const app = await running({
    services: [cancellable], accessLog: true, logLevel: "warning", logger: new RecordingLogger(),
  });
  t.after(app.close);
  const stream = app.client.serverStream(service.name, "download", { value: 1 }, { timeoutMs: 30 });
  await stream.next();
  await assert.rejects(stream.next(), isError("deadline_exceeded"));
  for await (const _ of app.client.serverStream(service.name, "download", { value: 1 })) break;
  for (let i = 0; i < 100 && cancelled < 2; i += 1) await delay(5);
  assert.equal(cancelled, 2);
  const access = logs.filter((entry) => entry.message.startsWith("RPC call "));
  assert.equal(access.length, 2);
  assert.equal(new Set(access.map((entry) => entry.context.call_id)).size, 2);
  assert.ok(access.every((entry) => entry.level === "warning" && entry.context.method === "download"));
});

test("failed startup rolls back and the same server can start after the port is released", { timeout: 5000 }, async (t) => {
  const first = await running();
  t.after(first.close);
  const second = new MeshCallServer({ services: [service], port: first.server.boundPort, accessLog: false });
  t.after(() => second.close());
  await assert.rejects(second.start(), { code: "EADDRINUSE" });
  assert.equal(second.isRunning, false);
  await first.close();
  await Promise.all([second.start(), second.start()]);
  assert.equal(second.isRunning, true);
  await Promise.all([second.close(), second.close()]);
  assert.equal(second.isRunning, false);
});

test("Unix sockets support the same clients and are removed on shutdown", {
  timeout: 5000, skip: process.platform === "win32",
}, async (t) => {
  const path = "/tmp/meshcall-ts-" + randomUUID() + ".sock";
  const app = await running({ unixPath: path });
  t.after(app.close);
  assert.deepEqual(await app.client.unary(service.name, "echo", { value: 5 }), { value: 5 });
  await app.close();
  assert.equal(existsSync(path), false);
});

test("malformed logical frames are rejected before dispatch", () => {
  for (const frame of [
    { kind: "unknown" },
    { kind: "call.error", call_id: "x" },
    { kind: "stream.window", call_id: "x", direction: "server", credit: 0 },
    { kind: "stream.item", call_id: "x", direction: "server", sequence: -1, payload: {} },
    { kind: "call.open", call_id: "x", service: "s", method: "m" },
  ]) assert.throws(() => decodeFrame(JSON.stringify(frame)));
});

test("cancelling before a queued call opens does not execute its handler", { timeout: 5000 }, async (t) => {
  let invoked = 0;
  const counted = defineService({
    ...service,
    methods: {
      ...service.methods,
      echo: unaryMethod({
        request: valueType, response: valueType,
        handler: (request) => { invoked += 1; return request; },
      }),
    },
  });
  const app = await running({ services: [counted] });
  t.after(app.close);
  await app.client.connect();
  const controller = new AbortController();
  const result = app.client.unary(service.name, "echo", { value: 1 }, { signal: controller.signal });
  queueMicrotask(() => controller.abort());
  await assert.rejects(result, isError("cancelled"));
  await app.client.unary(service.name, "echo", { value: 2 });
  assert.equal(invoked, 1);
});
