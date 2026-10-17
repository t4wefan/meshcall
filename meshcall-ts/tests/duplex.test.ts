import assert from "node:assert/strict";
import test from "node:test";

import {
  contractEntry, MeshCallClient, MeshCallError, MeshCallServer, method, service, z,
  type ClientInterceptor, type DuplexChannel, type DuplexOptions, type RpcContext,
} from "../src/index.js";

const Turn = z.object({ text: z.string() });
const Prefix = z.object({ prefix: z.string().default("") });
type Turn = z.infer<typeof Turn>;
type Prefix = z.input<typeof Prefix>;

const NAME = "test.v1.DuplexService";

@service(NAME, { description: "Duplex fixtures." })
class DuplexService {
  @method.duplex({
    description: "Answer each input as it arrives.",
    reason: "replies interleave with what the caller keeps sending",
    request: Prefix, inputItem: Turn, outputItem: Turn,
  })
  async echo({ prefix }: z.output<typeof Prefix>, channel: DuplexChannel<Turn, Turn>) {
    for await (const turn of channel) await channel.send({ text: prefix + turn.text });
  }

  @method.duplex({
    description: "Send one item, close the output, then keep reading input.",
    reason: "the server stops talking before the caller does",
    request: Prefix, inputItem: Turn, outputItem: Turn,
  })
  async close_early(_: z.output<typeof Prefix>, channel: DuplexChannel<Turn, Turn>) {
    await channel.send({ text: "only" });
    await channel.closeSend();
    for await (const _turn of channel) { /* drain */ }
  }

  @method.duplex({
    description: "Echo one input, then fail with a trailer.",
    reason: "fails part way through",
    request: Prefix, inputItem: Turn, outputItem: Turn,
  })
  async fail_after_one(_: z.output<typeof Prefix>, channel: DuplexChannel<Turn, Turn>, ctx: RpcContext) {
    for await (const turn of channel) {
      await channel.send(turn);
      ctx.setTrailer("seen", turn.text);
      throw new MeshCallError("app.failed", "gave up");
    }
  }
}

async function running(interceptors: ClientInterceptor[] = []) {
  const server = new MeshCallServer({ services: [new DuplexService()], accessLog: false });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, { interceptors });
  return { client, close: async () => { await client.close(); await server.close(); } };
}

test("the contract carries the reason and no response", () => {
  const methods = contractEntry(DuplexService).methods as Array<Record<string, unknown>>;
  const echo = methods[0] as Record<string, unknown>;
  assert.equal(echo.stream, "duplex");
  assert.equal(echo.duplex_reason, "replies interleave with what the caller keeps sending");
  assert.equal(echo.response, null);
  assert.notEqual(echo.input_item, null);
  assert.notEqual(echo.output_item, null);
});

test("a duplex method needs a reason and cannot be idempotent", () => {
  assert.throws(() => {
    @service("test.v1.Reasonless", { description: "" })
    class Reasonless {
      @method.duplex({ description: "", reason: " ", request: Prefix, inputItem: Turn, outputItem: Turn })
      async echo() { /* the decorator throws first */ }
    }
    return Reasonless;
  }, /reason/);
  const sneaky: DuplexOptions<typeof Prefix, typeof Turn, typeof Turn> = {
    description: "", reason: "r", request: Prefix, inputItem: Turn, outputItem: Turn,
    // @ts-expect-error a duplex call is never replayed, so it has no idempotent hint
    idempotent: true,
  };
  assert.ok(sneaky);
});

test("interleaved echo", { timeout: 15000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>(NAME, "echo", { prefix: "> " });
  for (const text of ["a", "b", "c"]) {
    await call.send({ text });
    assert.deepEqual(await call.next(), { done: false, value: { text: "> " + text } });
  }
  await call.closeSend();
  assert.deepEqual(await call.next(), { done: true, value: undefined });
});

test("reading concurrently avoids a credit deadlock", { timeout: 15000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>(NAME, "echo", { prefix: "" });
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

test("the server's half-close keeps the input open", { timeout: 15000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>(NAME, "close_early", { prefix: "" });
  assert.deepEqual(await call.next(), { done: false, value: { text: "only" } });
  for (let index = 0; index < 3; index += 1) await call.send({ text: "still accepted" });
  await call.closeSend();
  assert.deepEqual(await call.next(), { done: true, value: undefined });
});

test("an error after items is thrown from the iteration", { timeout: 5000 }, async (t) => {
  const { client, close } = await running();
  t.after(close);
  const call = client.duplex<Prefix, Turn, Turn>(NAME, "fail_after_one", { prefix: "" });
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
  const call = client.duplex<Prefix, Turn, Turn>(NAME, "echo", { prefix: "" });
  await assert.rejects(call.next(), (error) =>
    error instanceof MeshCallError && error.code === "invalid_argument"
    && JSON.stringify(error.details) === JSON.stringify({ source: "server", reason: "idempotency_key_on_duplex" }));
});
