import assert from "node:assert/strict";
import test from "node:test";

import {
  ConsoleRpcLogger, contractWarnings, lintService, MeshCallServer, method, service, z,
  type LogLevel, type ServiceOptions, type UnaryOptions,
} from "../src/index.js";
import { serviceSpec } from "../src/contract.js";

const Value = z.object({ value: z.number().int() });
type Value = z.infer<typeof Value>;

test("a missing description is a compile error", () => {
  // @ts-expect-error a service needs a description ("" declares none on purpose)
  const serviceOptions: ServiceOptions = { targeted: true };
  // @ts-expect-error a method needs a description ("" declares none on purpose)
  const methodOptions: UnaryOptions<typeof Value, typeof Value> = { request: Value, response: Value };
  assert.ok(serviceOptions && methodOptions);
});

test("missing annotations are recorded with hints", () => {
  @service("shop.v1", { description: "Items for sale." })
  class Shop {
    @method({ description: "Fetch an item.", request: Value, response: Value })
    getItem(request: Value) { return request; }

    @method({ description: "Remove an item for good.", request: Value, response: Value })
    deleteItem(request: Value) { return request; }
  }
  const codes = contractWarnings(Shop).map((w) => w.code + " " + w.subject);
  assert.deepEqual(codes, ["W002 shop.v1.deleteItem"]);
  assert.deepEqual(contractWarnings(new Shop()), contractWarnings(Shop));
  assert.ok(contractWarnings(Shop).every((w) => w.hint.length > 0 && w.silence.length > 0));
});

test("explicit empty descriptions and annotations silence; contradictions warn", () => {
  @service("quiet.v1", { description: "" })
  class Quiet {
    @method({ description: "", destructive: true, request: Value, response: Value })
    deleteItem(request: Value) { return request; }

    @method({ description: "Fetch.", readOnly: true, request: Value, response: Value })
    getItem(request: Value) { return request; }
  }
  assert.deepEqual(contractWarnings(Quiet), []);

  @service("odd.v1", { description: "Odd." })
  class Odd {
    @method({ description: "Mutates.", readOnly: true, request: Value, response: Value })
    deleteItem(request: Value) { return request; }

    @method.serverStream({ description: "Reads.", destructive: true, request: Value, outputItem: Value })
    async *getItem(request: Value) { yield request; }

    @method({ description: "Old.", idempotent: true, deprecated: "", request: Value, response: Value })
    old(request: Value) { return request; }
  }
  const codes = lintService(serviceSpec(Odd)).map((w) => w.code);
  assert.equal(codes.filter((c) => c === "W003").length, 2);
  assert.ok(codes.includes("W005"));

  assert.throws(() => {
    @service("bad.v1", { description: "" })
    class Bad {
      @method({ description: "x", readOnly: true, destructive: true, request: Value, response: Value })
      x(request: Value) { return request; }
    }
    return Bad;
  }, /readOnly and destructive/u);
});

test("servers summarise warnings at start and strict mode fails", { timeout: 5000 }, async (t) => {
  const logs: Array<[LogLevel, string]> = [];
  class Recording extends ConsoleRpcLogger {
    public override bind(): Recording { return this; }
    public override log(level: LogLevel, message: string): void { logs.push([level, message]); }
  }

  @service("noisy.v1", { description: "Creates things." })
  class Noisy {
    @method({ description: "Create a thing.", request: Value, response: Value })
    createThing(request: Value) { return request; }

    @method({ description: "Delete a thing.", request: Value, response: Value })
    deleteThing(request: Value) { return request; }
  }
  const server = new MeshCallServer({ services: [new Noisy()], accessLog: false, logger: new Recording() });
  t.after(() => server.close());
  await server.start();
  const summary = logs.find(([level, message]) => level === "warning" && message.includes("contract warning"));
  assert.ok(summary, "no summary logged");
  assert.match(summary![1], /W002×2/u);
  const strict = new MeshCallServer({
    services: [new Noisy()], accessLog: false, strictContracts: true, logger: new Recording(),
  });
  t.after(() => strict.close());
  await assert.rejects(strict.start(), /Strict mode: 2 contract warning/u);
  process.env.MESHCALL_STRICT = "1";
  try {
    assert.throws(() => {
      @service("strict.v1", { description: "Strict." })
      class Strict {
        @method({ description: "Start pinging.", request: Value, response: Value })
        startPing(request: Value) { return request; }
      }
      return Strict;
    }, /Strict mode/u);
  } finally {
    delete process.env.MESHCALL_STRICT;
  }
});
