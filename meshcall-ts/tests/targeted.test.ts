import assert from "node:assert/strict";
import test from "node:test";

import {
  defineService, defineType, InvalidArgumentError, lintService, MeshCallClient, MeshCallServer, UnavailableError, unaryMethod,
} from "../src/index.js";

const emptyType = defineType<Record<string, never>>("Empty", { type: "object", properties: {} });
const instanceType = defineType<{ instance: string }>("Instance", {
  type: "object", properties: { instance: { type: "string" } }, required: ["instance"],
});

const actor = defineService({
  name: "test.v1.Actor", targeted: true, description: "Holds per-instance state.",
  sourceModule: "targeted.test", sourceQualname: "Actor",
  methods: {
    whoami: unaryMethod({ description: "Report the serving instance.", readOnly: true, request: emptyType, response: instanceType, handler: (_, ctx) => ({ instance: ctx.instanceId }) }),
  },
});
const plain = defineService({ ...actor, name: "test.v1.Plain", targeted: false, sourceQualname: "Plain" });

test("direct server applies targeting rules", { timeout: 5000 }, async (t) => {
  const server = new MeshCallServer({ services: [actor, plain], instanceId: "node-7", accessLog: false });
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
  const sticky = defineService({ ...actor, name: "test.v1.Sticky", balance: { kind: "sticky", key: "request.key" } });
  assert.ok(lintService(sticky).some((w) => w.code === "W006"));
});
