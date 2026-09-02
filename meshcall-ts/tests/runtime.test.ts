import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { WebSocketServer } from "ws";

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
import {
  decodeFrame,
  encodeFrame,
} from "../src/protocol.js";

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

test("client and server streaming use the shared protocol", async () => {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await new Promise<void>((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const port = (server.address() as AddressInfo).port;
  server.on("connection", (socket) => {
    let handshaken = false;
    let downloadCallId: string | undefined;
    let uploadCallId: string | undefined;
    const uploaded: number[] = [];
    socket.on("message", (data) => {
      const frame = decodeFrame(data);
      if (!handshaken) {
        assert.equal(frame.kind, "hello");
        handshaken = true;
        socket.send(
          encodeFrame({
            kind: "hello.ack",
            protocol: "meshcall/1",
            connection_id: "test-connection",
          }),
        );
        return;
      }
      if (frame.kind === "call.open" && frame.method === "download") {
        downloadCallId = frame.call_id;
      } else if (frame.kind === "call.open" && frame.method === "upload") {
        uploadCallId = frame.call_id;
        socket.send(
          encodeFrame({
            kind: "stream.window",
            call_id: frame.call_id,
            direction: "client",
            credit: 3,
          }),
        );
      } else if (
        frame.kind === "stream.window" &&
        frame.direction === "server" &&
        frame.call_id === downloadCallId
      ) {
        for (const [sequence, value] of [4, 5].entries()) {
          socket.send(
            encodeFrame({
              kind: "stream.item",
              call_id: frame.call_id,
              direction: "server",
              sequence,
              payload: { value },
            }),
          );
        }
        socket.send(
          encodeFrame({
            kind: "stream.end",
            call_id: frame.call_id,
            direction: "server",
          }),
        );
        socket.send(
          encodeFrame({
            kind: "call.result",
            call_id: frame.call_id,
            payload: null,
          }),
        );
      } else if (
        frame.kind === "stream.item" &&
        frame.direction === "client" &&
        frame.call_id === uploadCallId
      ) {
        uploaded.push((frame.payload as { value: number }).value);
      } else if (
        frame.kind === "stream.end" &&
        frame.direction === "client" &&
        frame.call_id === uploadCallId
      ) {
        socket.send(
          encodeFrame({
            kind: "call.result",
            call_id: frame.call_id,
            payload: { total: uploaded.reduce((sum, value) => sum + value, 0) },
          }),
        );
      }
    });
  });

  const client = new MeshCallClient(`ws://127.0.0.1:${port}`);
  try {
    const download = client.serverStream<
      Record<string, never>,
      { value: number }
    >("test.v1.StreamService", "download", {});
    const downloaded: Array<{ value: number }> = [];
    for await (const item of download) {
      downloaded.push(item);
    }
    assert.deepEqual(downloaded, [{ value: 4 }, { value: 5 }]);

    const result = await client.clientStream<
      Record<string, never>,
      { value: number },
      { total: number }
    >(
      "test.v1.StreamService",
      "upload",
      {},
      (async function* () {
        yield { value: 2 };
        yield { value: 3 };
        yield { value: 5 };
      })(),
    );
    assert.deepEqual(result, { total: 10 });
  } finally {
    await client.close();
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error == null ? resolve() : reject(error)));
    });
  }
});
