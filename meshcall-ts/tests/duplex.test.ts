import assert from "node:assert/strict";
import test from "node:test";

import {
  contractEntry, defineService, defineType, duplexMethod, MeshCallClient, MeshCallError, MeshCallServer,
  type ClientInterceptor,
} from "../src/index.js";

interface Turn { text: string }
interface Prefix { prefix: string }

const turnType = defineType<Turn>("Turn", {
  type: "object", properties: { text: { type: "string" } }, required: ["text"],
});
const prefixType = defineType<Prefix>("Prefix", {
  type: "object", properties: { prefix: { type: "string", default: "" } },
});

const service = defineService({
  name: "test.v1.DuplexService",
  description: "Duplex fixtures.",
  sourceModule: "duplex.test", sourceQualname: "DuplexService",
  methods: {
    echo: duplexMethod({
      description: "Answer each input as it arrives.",
      reason: "replies interleave with what the caller keeps sending",
      request: prefixType, inputItem: turnType, outputItem: turnType,
      handler: async ({ prefix }, channel) => {
        for await (const turn of channel) await channel.send({ text: prefix + turn.text });
      },
    }),
    close_early: duplexMethod({
      description: "Send one item, close the output, then keep reading input.",
      reason: "the server stops talking before the caller does",
      request: prefixType, inputItem: turnType, outputItem: turnType,
      handler: async (_, channel) => {
        await channel.send({ text: "only" });
        await channel.closeSend();
        for await (const _turn of channel) { /* drain */ }
      },
    }),
    fail_after_one: duplexMethod({
      description: "Echo one input, then fail with a trailer.",
      reason: "fails part way through",
      request: prefixType, inputItem: turnType, outputItem: turnType,
      handler: async (_, channel, ctx) => {
        for await (const turn of channel) {
          await channel.send(turn);
          ctx.setTrailer("seen", turn.text);
          throw new MeshCallError("app.failed", "gave up");
        }
      },
    }),
  },
});

async function running(interceptors: ClientInterceptor[] = []) {
  const server = new MeshCallServer({ services: [service], accessLog: false });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, { interceptors });
  return { client, close: async () => { await client.close(); await server.close(); } };
}

test("the contract carries the reason and no response", () => {
  const methods = contractEntry(service).methods as Array<Record<string, unknown>>;
  const echo = methods[0] as Record<string, unknown>;
  assert.equal(echo.stream, "duplex");
  assert.equal(echo.duplex_reason, "replies interleave with what the caller keeps sending");
  assert.equal(echo.response, null);
  assert.notEqual(echo.input_item, null);
  assert.notEqual(echo.output_item, null);
});

test("a duplex method needs a reason and cannot be idempotent", () => {
  assert.throws(() => duplexMethod({
    description: "", reason: " ", request: prefixType, inputItem: turnType, outputItem: turnType,
    handler: async () => undefined,
  }), /reason/);
  const sneaky = { ...service.methods.echo, idempotent: true };
  assert.throws(() => defineService({
    name: "test.v1.Sneaky", description: "", sourceModule: "m", sourceQualname: "q", methods: { echo: sneaky },
  }), /never replayed/);
});

test("interleaved echo", { timeout: 5000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>("test.v1.DuplexService", "echo", { prefix: "> " });
  for (const text of ["a", "b", "c"]) {
    await call.send({ text });
    assert.deepEqual(await call.next(), { done: false, value: { text: "> " + text } });
  }
  await call.closeSend();
  assert.deepEqual(await call.next(), { done: true, value: undefined });
});

test("reading concurrently avoids a credit deadlock", { timeout: 10000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>("test.v1.DuplexService", "echo", { prefix: "" });
  const count = 500;
  const sender = (async () => {
    for (let index = 0; index < count; index += 1) await call.send({ text: String(index) });
    await call.closeSend();
  })();
  const received: string[] = [];
  for await (const turn of call) received.push(turn.text);
  await sender;
  assert.deepEqual(received, Array.from({ length: count }, (_, index) => String(index)));
});

test("the server's half-close keeps the input open", { timeout: 5000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>("test.v1.DuplexService", "close_early", { prefix: "" });
  assert.deepEqual(await call.next(), { done: false, value: { text: "only" } });
  for (let index = 0; index < 3; index += 1) await call.send({ text: "still accepted" });
  await call.closeSend();
  assert.deepEqual(await call.next(), { done: true, value: undefined });
});

test("an error after items is thrown from the iteration", { timeout: 5000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>("test.v1.DuplexService", "fail_after_one", { prefix: "" });
  await call.send({ text: "x" });
  assert.deepEqual(await call.next(), { done: false, value: { text: "x" } });
  await assert.rejects(call.next(), (error) => error instanceof MeshCallError && error.code === "app.failed");
  assert.deepEqual(call.trailers, { seen: "x" });
});

test("an idempotency key smuggled in by an interceptor is refused", { timeout: 5000 }, async (t) => {
  const addKey: ClientInterceptor = (info, next) => {
    info.options = { ...info.options, idempotencyKey: "k" };
    return next(info);
  };
  const { client, close } = await running([addKey]);
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>("test.v1.DuplexService", "echo", { prefix: "" });
  await assert.rejects(call.next(), (error) =>
    error instanceof MeshCallError && error.code === "invalid_argument"
    && JSON.stringify(error.details) === JSON.stringify({ source: "server", reason: "idempotency_key_on_duplex" }));
});
