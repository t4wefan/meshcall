// docs/protocol.md → Several Routers.
// conformance-scenario: routers.several
import assert from "node:assert/strict";
import test from "node:test";

import {
  findRouterBinary,
  LocalRouter,
  Mesh,
  method,
  Router,
  service,
  UnavailableError,
  z,
  type DuplexChannel,
} from "../src/index.js";
import { Mesh as WebMesh, Router as WebRouter } from "../src/web.js";

const routerMissing = (() => { try { findRouterBinary(); return false; } catch { return "Go Router binary not built"; } })();

const Empty = z.object({});
const Count = z.object({ n: z.number().int() });
const Where = z.object({ instance: z.string() });

@service("test.v1.Box", { targeted: true, description: "A box on one of several Routers." })
class Box {
  @method({ description: "Name this instance.", readOnly: true, request: Empty, response: Where })
  whoami(_: unknown, ctx: { instanceId: string }) { return { instance: ctx.instanceId }; }

  @method.serverStream({ description: "Count up to n.", readOnly: true, request: Count, outputItem: Count })
  async *count({ n }: { n: number }) { for (let i = 0; i < n; i++) yield { n: i }; }

  @method.clientStream({ description: "Add the items to n.", request: Count, inputItem: Count, response: Count })
  async sum({ n }: { n: number }, items: AsyncIterable<{ n: number }>) {
    let total = n;
    for await (const item of items) total += item.n;
    return { n: total };
  }

  @method.duplex({
    description: "Double each item.", reason: "answers as items arrive",
    request: Empty, inputItem: Count, outputItem: Count,
  })
  async double(_: unknown, channel: DuplexChannel<{ n: number }, { n: number }>) {
    for await (const item of channel) await channel.send({ n: item.n * 2 });
  }
}

@service("test.v1.Plain", { description: "A service registered on one Router only." })
class Plain {
  @method({ description: "Answer.", readOnly: true, request: Empty, response: Where })
  hello(_: unknown, ctx: { instanceId: string }) { return { instance: ctx.instanceId }; }
}

async function drain<T>(items: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const item of items) out.push(item);
  return out;
}

async function* counts(...values: number[]) { for (const n of values) yield { n }; }

for (const flavour of ["node", "web"] as const) {
  test(`${flavour}: calls reach the Router their instance or service is registered on`, { timeout: 30000, skip: routerMissing }, async () => {
    await using a = await LocalRouter.start({ port: 0 });
    await using b = await LocalRouter.start({ port: 0 });
    // Registered on b only: a answers instance_unknown / no_instances.
    const box = new Mesh(new Router(b.url)).mount(new Box(), new Plain(), { instanceId: "box-b", accessLog: false, acceptTunnels: true });
    await using _box = await box.startServing();

    const routers = [a.url, b.url];
    const fresh = () => flavour === "node" ? new Mesh(new Router(routers)) : new WebMesh(new WebRouter(routers));
    const at = { instance: "box-b" };

    const unary = fresh();
    try {
      assert.deepEqual(unary.routers, routers);
      assert.deepEqual(await unary.unary("test.v1.Box", "whoami", {}, at), { instance: "box-b" });
      assert.deepEqual(await unary.unary("test.v1.Box", "whoami", {}, at), { instance: "box-b" }, "remembered");
      assert.deepEqual(await unary.unary("test.v1.Plain", "hello", {}), { instance: "box-b" });
      await assert.rejects(unary.unary("test.v1.Box", "whoami", {}, { instance: "nowhere" }),
        (error: unknown) => error instanceof UnavailableError && error.reason === "instance_unknown");
    } finally {
      await unary.close();
    }

    // Every call shape moves on from a Router that did not route it, with
    // nothing lost from what the caller already handed over.
    const stream = fresh();
    try {
      assert.deepEqual(await drain(stream.serverStream("test.v1.Box", "count", { n: 3 }, at)), [{ n: 0 }, { n: 1 }, { n: 2 }]);
    } finally {
      await stream.close();
    }
    const upload = fresh();
    try {
      assert.deepEqual(await upload.clientStream("test.v1.Box", "sum", { n: 1 }, counts(2, 3, 4), at), { n: 10 });
    } finally {
      await upload.close();
    }
    const duplex = fresh();
    try {
      const call = duplex.duplex<object, { n: number }, { n: number }>("test.v1.Box", "double", {}, at);
      const sent = Promise.all([call.send({ n: 1 }), call.send({ n: 2 })]).then(() => call.closeSend());
      assert.deepEqual(await drain(call), [{ n: 2 }, { n: 4 }]);
      await sent;
    } finally {
      await duplex.close();
    }

    // What belongs to one Router is asked of that Router.
    const issuer = fresh();
    try {
      await assert.rejects(issuer.registry.list(), /mesh\.at\(router\)/u);
      assert.deepEqual((await issuer.at(a.url).registry.list()).map((s) => s.name), []);
      assert.ok((await issuer.at(a.url).at(b.url).registry.list()).some((s) => s.name === "test.v1.Box"));
      // A tunnel is opened where the instance is, and revoked everywhere.
      const opened = await issuer.tunnels.open({ instance: "box-b", scopes: ["test.v1.Box"], principal: "user:alice", ttlSeconds: 60 });
      assert.equal(opened.router, b.url);
      const client = flavour === "node"
        ? new Mesh(new Router(opened.router), { tunnel: opened.credential })
        : new WebMesh(new WebRouter(opened.router), { tunnel: opened.credential });
      try {
        assert.deepEqual(await client.unary("test.v1.Box", "whoami", {}), { instance: "box-b" });
      } finally {
        await client.close();
      }
      assert.equal(await issuer.tunnels.revoke(opened.tunnel_id), true);
      assert.equal(await issuer.tunnels.revoke(opened.tunnel_id), false);
    } finally {
      await issuer.close();
    }

    // A Router that cannot be reached is skipped too.
    await a.stop();
    const failover = fresh();
    try {
      assert.deepEqual(await failover.unary("test.v1.Box", "whoami", {}, at), { instance: "box-b" });
    } finally {
      await failover.close();
    }
  });
}

test("an instance registers with every Router, asking its source for each", { timeout: 20000, skip: routerMissing }, async () => {
  await using a = await LocalRouter.start({ port: 0 });
  await using b = await LocalRouter.start({ port: 0 });
  const asked = new Set<string>();
  const box = new Mesh(new Router([a.url, b.url]), { token: (router) => { asked.add(router); return "mt_" + router.length; } })
    .mount(new Box(), { instanceId: "box-ab", accessLog: false });
  await using _box = await box.startServing();
  assert.deepEqual([...asked].sort(), [a.url, b.url].sort());
  for (const router of [a, b]) {
    await using caller = await Mesh.connect(router);
    assert.deepEqual(await caller.unary("test.v1.Box", "whoami", {}, { instance: "box-ab" }), { instance: "box-ab" });
  }
});
