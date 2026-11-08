import assert from "node:assert/strict";
import test from "node:test";

import {
  DeadlineExceededError, findRouterBinary, LocalRouter, Mesh as NodeMesh, method, PermissionDeniedError, service,
  UnauthenticatedError, z,
  type DuplexChannel, type RpcContext,
} from "../src/index.js";
import { Mesh, Router } from "../src/web.js";

const routerMissing = (() => {
  try {
    findRouterBinary();
    return false;
  } catch {
    return "Go Router binary not built";
  }
})();

const Empty = z.object({});
const Count = z.object({ n: z.number().int() });
const Value = z.object({ value: z.number().int() });
const Line = z.object({ text: z.string() });
const Caller = z.object({ principal: z.string().nullable() });

@service("test.v1.Box", { targeted: true, description: "A box reached from a browser." })
class Box {
  @method({ description: "Who is calling.", readOnly: true, request: Empty, response: Caller })
  whoami(_: unknown, ctx: RpcContext) { return { principal: ctx.tunnel?.principal ?? null }; }

  @method.serverStream({ description: "Count up.", readOnly: true, request: Count, outputItem: Value })
  async *count({ n }: { n: number }) { for (let i = 0; i < n; i++) yield { value: i }; }

  @method.clientStream({ description: "Sum the input.", request: Empty, inputItem: Value, response: Value })
  async sum(_: unknown, items: AsyncIterable<{ value: number }>) {
    let value = 0;
    for await (const item of items) value += item.value;
    return { value };
  }

  @method.duplex({ description: "A terminal.", reason: "Input and output interleave.", request: Empty, inputItem: Line, outputItem: Line })
  async terminal(_: unknown, channel: DuplexChannel<{ text: string }, { text: string }>) {
    for await (const line of channel) await channel.send({ text: "$ " + line.text });
  }

  @method({ description: "Never answers in time.", readOnly: true, request: Empty, response: Empty })
  async slow() { await new Promise((r) => setTimeout(r, 2000)); return {}; }
}

test("meshcall/web over WebSocket: every call shape through a tunnel", { timeout: 20000, skip: routerMissing }, async () => {
  await using router = await LocalRouter.start({ port: 0 });
  const host = new NodeMesh(router).mount(new Box(), { instanceId: "box-a", accessLog: false, acceptTunnels: true });
  await using _host = await host.startServing();
  await using issuer = await NodeMesh.connect(router);
  const opened = await issuer.tunnels.open({ instance: "box-a", scopes: ["test.v1.Box"], principal: "user:phone" });

  // The browser entry point: credentials in hello, never in the URL.
  const browser = await Mesh.connect(new Router(router.url), { tunnel: opened.credential });
  try {
    const at = { instance: "box-a" } as const;
    assert.deepEqual(await browser.unary("test.v1.Box", "whoami", {}, at), { principal: "user:phone" });

    const values: number[] = [];
    for await (const item of browser.serverStream<object, { value: number }>("test.v1.Box", "count", { n: 40 }, at)) values.push(item.value);
    assert.deepEqual(values, Array.from({ length: 40 }, (_, i) => i));

    async function* items() { for (let i = 1; i <= 30; i++) yield { value: i }; }
    assert.deepEqual(await browser.clientStream("test.v1.Box", "sum", {}, items(), at), { value: 465 });

    const terminal = browser.duplex<object, { text: string }, { text: string }>("test.v1.Box", "terminal", {}, at);
    for (const text of ["ls", "pwd"]) {
      await terminal.send({ text });
      assert.deepEqual(await terminal.next(), { done: false, value: { text: "$ " + text } });
    }
    await terminal.closeSend();
    assert.deepEqual(await terminal.next(), { done: true, value: undefined });

    const denied = await browser.unary("test.v1.Box", "whoami", {}, { instance: "box-b" }).then(() => undefined, (e: unknown) => e);
    assert.ok(denied instanceof PermissionDeniedError);
    assert.equal(denied.reason, "tunnel_scope");
    const late = await browser.unary("test.v1.Box", "slow", {}, { ...at, timeoutMs: 100 }).then(() => undefined, (e: unknown) => e);
    assert.ok(late instanceof DeadlineExceededError);

    // Revoking closes the connection with 4401 tunnel_revoked; later calls
    // get the same answer rather than a bare connection loss.
    const held = browser.unary("test.v1.Box", "slow", {}, at).then(() => undefined, (e: unknown) => e);
    await new Promise((r) => setTimeout(r, 100));
    await issuer.tunnels.revoke(opened.tunnel_id);
    const inFlight = await held;
    assert.ok(inFlight instanceof UnauthenticatedError && inFlight.reason === "tunnel_revoked", String(inFlight));
    const gone = await browser.unary("test.v1.Box", "whoami", {}, at).then(() => undefined, (e: unknown) => e);
    assert.ok(gone instanceof UnauthenticatedError, String(gone));
  } finally {
    await browser.close();
  }
});

test("meshcall/web keeps one way in for tunnel credentials", () => {
  const router = new Router("wss://relay.example");
  assert.throws(() => new Mesh(router, { token: "mtun_abc" }), /tunnel/u);
  assert.throws(() => new Mesh(router, { tunnel: "mk_abc" }), /mtun_/u);
  assert.throws(() => new Mesh(new Router({ unixPath: "/tmp/x.sock" })), /ws:\/\//u);
});
