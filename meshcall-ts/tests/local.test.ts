import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import {
  defineService, defineType, MeshCallClient, MeshCallServer, unaryMethod, WebSocketRouter, type WebSocketEndpoint,
} from "../src/index.js";
import { findRouterBinary } from "../src/router.js";

// The local transport: Unix sockets, or named pipes on Windows (only there).
const windows = process.platform === "win32";
const routerMissing = (() => { try { findRouterBinary(); return false; } catch { return "Go Router binary not built"; } })();

/** A fresh local endpoint for this platform. */
function localEndpoint(): { pipe: string } | { unixPath: string } {
  return windows ? { pipe: "meshcall-test-" + randomUUID() } : { unixPath: "/tmp/meshcall-" + randomUUID() + ".sock" };
}

const value = defineType<{ value: number }>("Value", {
  type: "object", properties: { value: { type: "integer" } }, required: ["value"],
});
const service = defineService({
  name: "test.v1.Local", description: "", sourceModule: "local.test", sourceQualname: "Local",
  methods: { echo: unaryMethod({ description: "", request: value, response: value, handler: (r) => r }) },
});

test("a direct server over the local transport", { timeout: 10000 }, async () => {
  const endpoint = localEndpoint();
  const server = new MeshCallServer({ services: [service], accessLog: false, ...endpoint });
  await server.start();
  const client = new MeshCallClient(endpoint as WebSocketEndpoint);
  try {
    assert.deepEqual(await client.unary("test.v1.Local", "echo", { value: 7 }), { value: 7 });
  } finally {
    await client.close();
    await server.close();
  }
});

test("a Router over the local transport", { timeout: 15000, skip: routerMissing }, async () => {
  const endpoint = localEndpoint();
  const router = new WebSocketRouter(endpoint);
  await router.start();
  const server = new MeshCallServer({ services: [service], accessLog: false, router: { endpoint } });
  const client = new MeshCallClient(endpoint);
  try {
    await server.start();
    assert.deepEqual(await client.unary("test.v1.Local", "echo", { value: 1 }), { value: 1 });
    assert.throws(() => router.boundPort, /TCP port/);
  } finally {
    await client.close();
    await server.close();
    await router.stop();
  }
});

test("the other platform's transport is refused", () => {
  if (windows) {
    assert.throws(() => new MeshCallServer({ services: [service], unixPath: "/tmp/x.sock" }), /use pipe/);
    assert.throws(() => new WebSocketRouter({ unixPath: "/tmp/x.sock" }), /use pipe/);
  } else {
    assert.throws(() => new MeshCallServer({ services: [service], pipe: "meshcall" }), /Windows-only/);
    assert.throws(() => new WebSocketRouter({ pipe: "meshcall" }), /Windows-only/);
  }
  assert.throws(() => new WebSocketRouter({ port: 1, pipe: "meshcall" }), /Choose one/);
});
