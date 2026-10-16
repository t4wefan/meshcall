import assert from "node:assert/strict";
import test from "node:test";

import { MeshCallClient, MeshCallError, MeshCallServer, method, service, z, type RpcContext } from "../src/index.js";

const LIMIT = 64 * 1024;

const Size = z.object({ bytes: z.number().int() });
const Text = z.object({ text: z.string() });
type Size = z.infer<typeof Size>;
type Text = z.infer<typeof Text>;

@service("test.v1.Big", { description: "Oversized frames." })
class Big {
  @method({ description: "A result of bytes bytes.", readOnly: true, request: Size, response: Text })
  async result({ bytes }: Size, ctx: RpcContext) {
    ctx.setTrailer("t", "v");
    return { text: "x".repeat(bytes) };
  }

  @method({ description: "Return the text.", readOnly: true, request: Text, response: Text })
  async echo(request: Text) {
    return request;
  }

  @method.serverStream({ description: "One small item, then one of bytes bytes.", readOnly: true, request: Size, outputItem: Text })
  async *items({ bytes }: Size) {
    yield { text: "small" };
    yield { text: "x".repeat(bytes) };
  }

  @method.clientStream({ description: "Count the uploaded bytes.", idempotent: true, request: Size, inputItem: Text, response: Size })
  async upload(_: Size, items: AsyncIterable<Text>) {
    let bytes = 0;
    for await (const item of items) bytes += item.text.length;
    return { bytes };
  }

  @method({ description: "Sleep bytes milliseconds.", readOnly: true, request: Size, response: Size })
  async sleep(request: Size) {
    await new Promise((resolve) => setTimeout(resolve, request.bytes));
    return request;
  }
}

function tooLarge(source: string) {
  return (error: unknown): boolean => {
    assert.ok(error instanceof MeshCallError, String(error));
    assert.equal(error.code, "resource_exhausted");
    assert.equal(error.retryable, false);
    assert.equal(error.details?.reason, "frame_too_large");
    assert.equal(error.details?.source, source);
    return true;
  };
}

test("a frame over the limit fails only its call", { timeout: 10000 }, async () => {
  const server = new MeshCallServer({ services: [new Big()], accessLog: false, maxFrameSize: LIMIT });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort, undefined, { maxFrameSize: LIMIT });
  try {
    const slow = client.unary<Size, Size>("test.v1.Big", "sleep", { bytes: 300 });
    await new Promise((resolve) => setTimeout(resolve, 50));

    await assert.rejects(client.unary("test.v1.Big", "result", { bytes: 2 * LIMIT }), tooLarge("server"));

    const received: string[] = [];
    await assert.rejects((async () => {
      for await (const item of client.serverStream<Size, Text>("test.v1.Big", "items", { bytes: 2 * LIMIT })) {
        received.push(item.text);
      }
    })(), tooLarge("server"));
    assert.deepEqual(received, ["small"]);

    await assert.rejects(client.unary("test.v1.Big", "echo", { text: "x".repeat(2 * LIMIT) }), tooLarge("client"));

    async function* upload() {
      yield { text: "ok" };
      yield { text: "x".repeat(2 * LIMIT) };
    }
    await assert.rejects(client.clientStream("test.v1.Big", "upload", { bytes: 0 }, upload()), tooLarge("client"));

    assert.deepEqual(await slow, { bytes: 300 });
    assert.deepEqual(await client.unary("test.v1.Big", "result", { bytes: 10 }), { text: "x".repeat(10) });
  } finally {
    await client.close();
    await server.close();
  }
});
