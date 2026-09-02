import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  MeshCallClient,
  MeshCallError,
  MeshCallServer,
  defineService,
  defineType,
  renderContract,
  type ServiceDefinition,
  unaryMethod,
} from "../src/index.js";

interface AddRequest {
  left: number;
  right: number;
}

interface AddResult {
  total: number;
}

const service = {
  name: "test.v1.MathService",
  methods: {
    add: {
      stream: "unary",
      handler: (request: AddRequest): AddResult => ({
        total: request.left + request.right,
      }),
    },
    slow: {
      stream: "unary",
      handler: async (): Promise<AddResult> => {
        await delay(100);
        return { total: 0 };
      },
    },
  },
} satisfies ServiceDefinition;

const addRequest = defineType<AddRequest>("AddRequest", {
  type: "object",
  properties: {
    left: { type: "number" },
    right: { type: "number" },
  },
  required: ["left", "right"],
  additionalProperties: false,
});

const addResult = defineType<AddResult>("AddResult", {
  type: "object",
  properties: { total: { type: "number" } },
  required: ["total"],
  additionalProperties: false,
});

const exportableService = defineService({
  name: "test.v1.ExportedMathService",
  sourceModule: "runtime.test",
  sourceQualname: "ExportedMathService",
  methods: {
    add: unaryMethod({
      request: addRequest,
      response: addResult,
      handler: (request) => ({ total: request.left + request.right }),
    }),
  },
});

test("TypeScript services export a portable contract", () => {
  assert.doesNotThrow(
    () => new MeshCallServer({ services: [exportableService] }),
  );
  const contract = JSON.parse(renderContract([exportableService])) as {
    services: Array<{
      name: string;
      methods: Array<{
        name: string;
        request: { qualname: string };
        response: { qualname: string };
      }>;
    }>;
  };

  assert.equal(contract.services[0]?.name, exportableService.name);
  assert.equal(contract.services[0]?.methods[0]?.name, "add");
  assert.equal(contract.services[0]?.methods[0]?.request.qualname, "AddRequest");
  assert.equal(contract.services[0]?.methods[0]?.response.qualname, "AddResult");
});

test("unary client and server round trip", async () => {
  const server = new MeshCallServer({ services: [service] });
  await server.start();
  const client = new MeshCallClient(`ws://127.0.0.1:${server.boundPort}`);
  try {
    const result = await client.unary<AddRequest, AddResult>(
      service.name,
      "add",
      { left: 7, right: 5 },
    );
    assert.deepEqual(result, { total: 12 });
  } finally {
    await client.close();
    await server.close();
  }
});

test("unary deadline has a stable error code", async () => {
  const server = new MeshCallServer({ services: [service] });
  await server.start();
  const client = new MeshCallClient(`ws://127.0.0.1:${server.boundPort}`);
  try {
    await assert.rejects(
      client.unary<Record<string, never>, AddResult>(service.name, "slow", {}, {
        timeoutMs: 10,
      }),
      (error: unknown) =>
        error instanceof MeshCallError && error.code === "deadline_exceeded",
    );
  } finally {
    await client.close();
    await server.close();
  }
});
