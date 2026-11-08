import assert from "node:assert/strict";
import test from "node:test";

import {
  findRouterBinary,
  InvalidArgumentError,
  LocalRouter,
  Mesh,
  method,
  PermissionDeniedError,
  Router,
  service,
  UnauthenticatedError,
  z,
  type DuplexChannel,
  type RpcContext,
  type TunnelClosed,
  type TunnelOffer,
} from "../src/index.js";

const routerMissing = (() => {
  try {
    findRouterBinary();
    return false;
  } catch {
    return "Go Router binary not built";
  }
})();

const Empty = z.object({});
const Caller = z.object({ instance: z.string(), tunnelId: z.string().nullable(), principal: z.string().nullable() });
const Line = z.object({ text: z.string() });

@service("test.v1.Workspace", { targeted: true, description: "One developer box." })
class Workspace {
  @method({ description: "Report the instance and the tunnel.", readOnly: true, request: Empty, response: Caller })
  whoami(_: unknown, ctx: RpcContext) {
    return { instance: ctx.instanceId, tunnelId: ctx.tunnel?.id ?? null, principal: ctx.tunnel?.principal ?? null };
  }

  @method.duplex({
    description: "Echo terminal lines.", reason: "A terminal interleaves input and output.",
    request: Empty, inputItem: Line, outputItem: Line,
  })
  async terminal(_: unknown, channel: DuplexChannel<{ text: string }, { text: string }>) {
    for await (const line of channel) await channel.send({ text: "> " + line.text });
  }
}

@service("test.v1.Admin", { description: "Never reachable through a tunnel." })
class Admin {
  @method({ description: "Report the instance.", readOnly: true, request: Empty, response: Caller })
  whoami(_: unknown, ctx: RpcContext) { return { instance: ctx.instanceId, tunnelId: null, principal: null }; }
}

const rejection = (promise: Promise<unknown>) => promise.then(() => undefined, (e: unknown) => e);

test("a tunnel scopes a Mesh to one instance", { timeout: 15000, skip: routerMissing }, async () => {
  await using router = await LocalRouter.start({ port: 0 });
  const offers: TunnelOffer[] = [];
  const closed: TunnelClosed[] = [];
  const box = new Mesh(router)
    .mount(new Workspace(), {
      instanceId: "box-a", accessLog: false,
      acceptTunnels: (offer) => { offers.push(offer); return offer.principal !== "user:nobody"; },
      onTunnelClosed: (c) => { closed.push(c); },
    })
    .mount(new Admin());
  await using _box = await box.startServing();
  const other = new Mesh(router).mount(new Workspace(), { instanceId: "box-b", accessLog: false, acceptTunnels: true });
  await using _other = await other.startServing();
  await using issuer = await Mesh.connect(router);

  const opened = await issuer.tunnels.open({ instance: "box-a", scopes: ["test.v1.Workspace"], principal: "user:alice", ttlSeconds: 60 });
  assert.match(opened.credential, /^mtun_/u);
  assert.equal(offers.at(-1)?.principal, "user:alice");
  const refused = await rejection(issuer.tunnels.open({ instance: "box-a", scopes: ["test.v1.Workspace"], principal: "user:nobody" }));
  assert.ok(refused instanceof PermissionDeniedError);
  assert.equal(refused.reason, "tunnel_rejected");

  await using client = await Mesh.connect(new Router(router.url), { tunnel: opened.credential });
  assert.deepEqual(
    await client.unary("test.v1.Workspace", "whoami", {}, { instance: "box-a" }),
    { instance: "box-a", tunnelId: opened.tunnel_id, principal: "user:alice" },
  );
  for (const [svc, instance] of [["test.v1.Workspace", "box-b"], ["test.v1.Admin", undefined]] as const) {
    const denied = await rejection(client.unary(svc, "whoami", {}, instance === undefined ? {} : { instance }));
    assert.ok(denied instanceof PermissionDeniedError);
    assert.equal(denied.reason, "tunnel_scope");
  }
  const services = await client.registry.list();
  assert.deepEqual(services.map((s) => s.name), ["test.v1.Workspace"]);
  assert.deepEqual(services[0]!.instances.map((i) => i.id), ["box-a"]);

  // Duplex through the tunnel.
  const call = client.duplex<object, { text: string }, { text: string }>("test.v1.Workspace", "terminal", {}, { instance: "box-a" });
  for (const text of ["ls", "pwd"]) {
    await call.send({ text });
    assert.deepEqual(await call.next(), { done: false, value: { text: "> " + text } });
  }
  await call.closeSend();
  assert.deepEqual(await call.next(), { done: true, value: undefined });

  // The Router checks open; reserved metadata keys are refused in any case.
  const badTtl = await rejection(issuer.tunnels.open({ instance: "box-a", scopes: ["test.v1.Workspace"], principal: "p", ttlSeconds: 0 }));
  assert.ok(badTtl instanceof InvalidArgumentError, String(badTtl));
  const reserved = await rejection(client.unary("test.v1.Workspace", "whoami", {}, { instance: "box-a", metadata: { "MeshCall-Tunnel-Principal": "x" } }));
  assert.ok(reserved instanceof InvalidArgumentError && reserved.reason === "reserved_metadata", String(reserved));

  assert.deepEqual((await issuer.tunnels.list()).map((t) => t.tunnel_id), [opened.tunnel_id]);
  const held = client.duplex<object, { text: string }, { text: string }>("test.v1.Workspace", "terminal", {}, { instance: "box-a" });
  await held.send({ text: "x" });
  await held.next();
  assert.equal(await issuer.tunnels.revoke(opened.tunnel_id), true);
  // A call in flight learns why; the next one is refused at connect (HTTP 401).
  const revoked = await rejection(held.next());
  assert.ok(revoked instanceof UnauthenticatedError && revoked.reason === "tunnel_revoked", String(revoked));
  const again = await rejection(client.unary("test.v1.Workspace", "whoami", {}, { instance: "box-a" }));
  assert.ok(again instanceof UnauthenticatedError, String(again));
  for (let i = 0; i < 100 && closed.length === 0; i++) await new Promise((r) => setTimeout(r, 20));
  assert.deepEqual(closed, [{ tunnelId: opened.tunnel_id, principal: "user:alice", reason: "revoked" }]);
});

test("tunnel credentials have one way in", () => {
  const router = new Router("ws://127.0.0.1:1");
  assert.throws(() => new Mesh(router, { token: "mtun_abc" }), /tunnel/u);
  assert.throws(() => new Mesh(router, { tunnel: "mt_abc" }), /mtun_/u);
  assert.throws(() => new Mesh(router, { token: "mt_x", tunnel: "mtun_x" }), /one of/u);
  assert.throws(() => new Mesh(router, { tunnel: "mtun_abc" }).mount(new Workspace()), /cannot serve/u);
  assert.throws(() => new Mesh(router).mount(new Workspace(), { onTunnelClosed: () => undefined }), /needs acceptTunnels/u);
});
