import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  MeshCallClient,
  MeshCallError,
  MeshCallServer,
  type ServiceDefinition,
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
