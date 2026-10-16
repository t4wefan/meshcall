import assert from "node:assert/strict";
import test from "node:test";

import {
  InvalidArgumentError, lintService, MeshCallClient, MeshCallServer, method, service, UnavailableError, z,
  type RpcContext, type ServiceOptions,
} from "../src/index.js";
import { serviceSpec } from "../src/contract.js";

const Empty = z.object({});
const Instance = z.object({ instance: z.string() });

function actorClass(name: string, options: ServiceOptions) {
  @service(name, options)
  class Actor {
    @method({ description: "Report the serving instance.", readOnly: true, request: Empty, response: Instance })
    whoami(_: unknown, ctx: RpcContext) { return { instance: ctx.instanceId }; }
  }
  return Actor;
}

const Actor = actorClass("test.v1.Actor", { targeted: true, description: "Holds per-instance state." });
const Plain = actorClass("test.v1.Plain", { description: "Holds per-instance state." });
const actor = { name: "test.v1.Actor" };
const plain = { name: "test.v1.Plain" };

test("direct server applies targeting rules", { timeout: 5000 }, async (t) => {
  const server = new MeshCallServer({ services: [new Actor(), new Plain()], instanceId: "node-7", accessLog: false });
  await server.start();
  const client = new MeshCallClient("ws://127.0.0.1:" + server.boundPort);
  t.after(async () => { await client.close(); await server.close(); });

  const required = await client.unary(actor.name, "whoami", {}).then(() => undefined, (e: unknown) => e);
  assert.ok(required instanceof InvalidArgumentError);
  assert.equal(required.reason, "instance_required");
  assert.deepEqual(await client.unary(actor.name, "whoami", {}, { instance: "node-7" }), { instance: "node-7" });
  const unknown = await client.unary(actor.name, "whoami", {}, { instance: "node-8" }).then(() => undefined, (e: unknown) => e);
  assert.ok(unknown instanceof UnavailableError);
  assert.equal(unknown.reason, "instance_unknown");
  assert.equal(unknown.retryable, false);
  assert.deepEqual(await client.unary(plain.name, "whoami", {}), { instance: "node-7" });
  assert.deepEqual(await client.unary(plain.name, "whoami", {}, { instance: "node-7" }), { instance: "node-7" });
});

test("sticky balancing on a targeted service warns", () => {
  const Sticky = actorClass("test.v1.Sticky", {
    targeted: true, description: "Holds per-instance state.", balance: { kind: "sticky", key: "request.key" },
  });
  assert.ok(lintService(serviceSpec(Sticky)).some((w) => w.code === "W006"));
});
