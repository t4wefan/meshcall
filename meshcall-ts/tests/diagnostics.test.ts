import assert from "node:assert/strict";
import test from "node:test";

import {
  ConsoleRpcLogger, contractWarnings, defineService, defineType, lintService, MeshCallServer,
  serverStreamMethod, unaryMethod, type LogLevel,
} from "../src/index.js";

const value = defineType<{ value: number }>("Value", {
  type: "object", properties: { value: { type: "integer" } }, required: ["value"],
});

test("missing descriptions and annotations are recorded with hints", () => {
  const shop = defineService({
    name: "shop.v1", sourceModule: "diagnostics.test", sourceQualname: "Shop",
    methods: {
      getItem: unaryMethod({ request: value, response: value, handler: (r) => r }),
      deleteItem: unaryMethod({
        request: value, response: value, description: "Remove an item for good.", handler: (r) => r,
      }),
    },
  });
  const codes = contractWarnings(shop).map((w) => w.code + " " + w.subject);
  assert.ok(codes.includes("W001 shop.v1"));
  assert.ok(codes.includes("W001 shop.v1.getItem"));
  assert.ok(codes.includes("W002 shop.v1.deleteItem"));
  assert.ok(!codes.includes("W001 shop.v1.deleteItem"));
  assert.ok(contractWarnings(shop).every((w) => w.hint.length > 0 && w.silence.length > 0));
});

test("explicit empty descriptions and annotations silence; contradictions warn", () => {
  const quiet = defineService({
    name: "quiet.v1", description: "", sourceModule: "diagnostics.test", sourceQualname: "Quiet",
    methods: {
      deleteItem: unaryMethod({ request: value, response: value, description: "", destructive: true, handler: (r) => r }),
      getItem: unaryMethod({ request: value, response: value, description: "Fetch.", readOnly: true, handler: (r) => r }),
    },
  });
  assert.deepEqual(contractWarnings(quiet), []);
  const odd = defineService({
    name: "odd.v1", description: "Odd.", sourceModule: "diagnostics.test", sourceQualname: "Odd",
    methods: {
      deleteItem: unaryMethod({ request: value, response: value, description: "Mutates.", readOnly: true, handler: (r) => r }),
      getItem: serverStreamMethod({
        request: value, outputItem: value, description: "Reads.", destructive: true,
        handler: async function* (r) { yield r; },
      }),
      old: unaryMethod({ request: value, response: value, description: "Old.", idempotent: true, deprecated: "", handler: (r) => r }),
    },
  });
  const codes = lintService(odd).map((w) => w.code);
  assert.equal(codes.filter((c) => c === "W003").length, 2);
  assert.ok(codes.includes("W005"));
  assert.throws(() => defineService({
    name: "bad.v1", sourceModule: "diagnostics.test", sourceQualname: "Bad",
    methods: { x: unaryMethod({ request: value, response: value, description: "x", readOnly: true, destructive: true, handler: (r) => r }) },
  }), /readOnly and destructive/u);
});

test("servers summarise warnings at start and strict mode fails", { timeout: 5000 }, async (t) => {
  const logs: Array<[LogLevel, string]> = [];
  class Recording extends ConsoleRpcLogger {
    public override bind(): Recording { return this; }
    public override log(level: LogLevel, message: string): void { logs.push([level, message]); }
  }
  const noisy = defineService({
    name: "noisy.v1", sourceModule: "diagnostics.test", sourceQualname: "Noisy",
    methods: { createThing: unaryMethod({ request: value, response: value, handler: (r) => r }) },
  });
  const server = new MeshCallServer({ services: [noisy], accessLog: false, logger: new Recording() });
  t.after(() => server.close());
  await server.start();
  const summary = logs.find(([level, message]) => level === "warning" && message.includes("contract warning"));
  assert.ok(summary, "no summary logged");
  assert.match(summary![1], /W001×2, W002×1/u);
  const strict = new MeshCallServer({ services: [noisy], accessLog: false, strictContracts: true, logger: new Recording() });
  t.after(() => strict.close());
  await assert.rejects(strict.start(), /Strict mode: 3 contract warning/u);
  process.env.MESHCALL_STRICT = "1";
  try {
    assert.throws(() => defineService({
      name: "strict.v1", sourceModule: "diagnostics.test", sourceQualname: "Strict",
      methods: { ping: unaryMethod({ request: value, response: value, handler: (r) => r }) },
    }), /Strict mode/u);
  } finally {
    delete process.env.MESHCALL_STRICT;
  }
});
