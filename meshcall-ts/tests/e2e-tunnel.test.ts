import assert from "node:assert/strict";
import test from "node:test";

import {
  e2ePublicKey,
  findRouterBinary,
  generateE2EKey,
  LocalRouter,
  Mesh,
  MeshCallError,
  method,
  PermissionDeniedError,
  Router,
  service,
  UnauthenticatedError,
  z,
  type DuplexChannel,
  type RpcContext,
} from "../src/index.js";
import { Mesh as WebMesh, Router as WebRouter } from "../src/web.js";

const routerMissing = (() => {
  try {
    findRouterBinary();
    return false;
  } catch {
    return "Go Router binary not built";
  }
})();

const Num = z.object({ value: z.number().int() });
const Empty = z.object({});
const Sealed = z.object({ sealed: z.boolean() });
const Line = z.object({ text: z.string() });

@service("test.v1.Box", { targeted: true, description: "A box reached end to end." })
class Box {
  @method({ description: "Double a number.", readOnly: true, request: Num, response: Num })
  double(request: { value: number }) { return { value: request.value * 2 }; }

  @method.serverStream({ description: "Yield 0..value-1.", readOnly: true, request: Num, outputItem: Num })
  async *count(request: { value: number }) {
    for (let i = 0; i < request.value; i++) yield { value: i };
  }

  @method.clientStream({ description: "Add the items to value.", idempotent: true, request: Num, inputItem: Num, response: Num })
  async total(request: { value: number }, items: AsyncIterable<{ value: number }>) {
    let total = request.value;
    for await (const item of items) total += item.value;
    return { value: total };
  }

  @method.duplex({
    description: "Echo lines.", reason: "A terminal interleaves input and output.",
    request: Empty, inputItem: Line, outputItem: Line,
  })
  async echo(_: unknown, channel: DuplexChannel<{ text: string }, { text: string }>) {
    for await (const line of channel) await channel.send({ text: "> " + line.text });
  }

  @method({ description: "Whether the call came through a sealed tunnel.", readOnly: true, request: Empty, response: Sealed })
  seen(_: unknown, ctx: RpcContext) { return { sealed: ctx.tunnel?.sealed === true }; }

  @method({ description: "Fail with an application error.", readOnly: true, request: Num, response: Num })
  fail(request: { value: number }): { value: number } {
    throw new MeshCallError("app.nope", "secret 42", { details: { value: request.value } });
  }
}

const rejection = (promise: Promise<unknown>) => promise.then(() => undefined, (e: unknown) => e);
const at = { instance: "box-a" };

test("sealed calls through a Router", { timeout: 20000, skip: routerMissing }, async () => {
  const keys = await generateE2EKey();
  const other = await generateE2EKey();
  assert.equal(await e2ePublicKey(keys.privateKey), keys.publicKey);
  await using router = await LocalRouter.start({ port: 0 });
  const serve = () => new Mesh(router)
    .mount(new Box(), { instanceId: "box-a", accessLog: false, acceptTunnels: true, e2ePrivateKey: keys.privateKey, requireE2E: true })
    .startServing();
  let running = await serve();
  try {
    await using issuer = await Mesh.connect(router);
    const opened = await issuer.tunnels.open({
      instance: "box-a", scopes: ["test.v1.Box"], principal: "user:alice", e2eKey: keys.publicKey,
    });
    assert.equal(opened.e2e_key, keys.publicKey);
    assert.ok(opened.credential.startsWith("mtun_e2e."));
    // The instance requires end-to-end tunnels: a plain offer is refused.
    const plainOffer = await rejection(issuer.tunnels.open({ instance: "box-a", scopes: ["test.v1.Box"], principal: "p" }));
    assert.ok(plainOffer instanceof PermissionDeniedError && plainOffer.reason === "tunnel_rejected"
      && plainOffer.message.includes("end-to-end"), String(plainOffer));
    const refused = await rejection(issuer.tunnels.open({
      instance: "box-a", scopes: ["test.v1.Box"], principal: "p", e2eKey: other.publicKey,
    }));
    assert.ok(refused instanceof PermissionDeniedError && refused.reason === "tunnel_rejected", String(refused));

    const url = new Router(router.url);
    await using mesh = await Mesh.connect(url, { tunnel: opened.credential, e2eKey: keys.publicKey });
    const box = mesh.withOptions(at);
    assert.deepEqual(await box.unary("test.v1.Box", "double", { value: 21 }), { value: 42 });
    const items: number[] = [];
    for await (const item of box.serverStream<object, { value: number }>("test.v1.Box", "count", { value: 4 })) items.push(item.value);
    assert.deepEqual(items, [0, 1, 2, 3]);
    async function* inputs() { for (const value of [1, 2, 3]) yield { value }; }
    assert.deepEqual(await box.clientStream("test.v1.Box", "total", { value: 1 }, inputs()), { value: 7 });
    const chat = box.duplex<object, { text: string }, { text: string }>("test.v1.Box", "echo", {});
    for (const text of ["ls", "pwd"]) {
      await chat.send({ text });
      assert.deepEqual(await chat.next(), { done: false, value: { text: "> " + text } });
    }
    await chat.closeSend();
    assert.deepEqual(await chat.next(), { done: true, value: undefined });
    assert.deepEqual(await box.unary("test.v1.Box", "seen", {}), { sealed: true });
    const app = await rejection(box.unary("test.v1.Box", "fail", { value: 3 }));
    assert.ok(app instanceof MeshCallError && app.code === "app.nope" && app.message === "secret 42", String(app));
    assert.deepEqual(app.details, { value: 3 });

    // The Router's own services answer on an E2E connection: never sealed.
    const who = await mesh.auth.whoami();
    assert.equal(who.tunnel?.e2e_key, keys.publicKey);
    assert.deepEqual((await mesh.registry.list()).map((s) => s.name), ["test.v1.Box"]);
    // Without the key the credential is refused before anything is sent.
    assert.throws(() => new Mesh(url, { tunnel: opened.credential }), /end-to-end tunnel credential/u);
    // A client pinning the wrong key cannot complete the handshake.
    const wrong = new Mesh(url, { tunnel: opened.credential, e2eKey: other.publicKey });
    const failed = await rejection(wrong.unary("test.v1.Box", "double", { value: 1 }, at));
    await wrong.close();
    assert.ok(failed instanceof UnauthenticatedError && failed.reason === "e2e_handshake_failed", String(failed));

    // The instance restarts with the same key and loses its sessions: a
    // stream is told once, and the next call handshakes again.
    await running.close();
    running = await serve();
    let unknown = 0;
    for (let i = 0; i < 100; i++) {
      try {
        const got: number[] = [];
        for await (const item of box.serverStream<object, { value: number }>("test.v1.Box", "count", { value: 2 })) got.push(item.value);
        assert.deepEqual(got, [0, 1]);
        break;
      } catch (error) {
        if (error instanceof MeshCallError && error.reason === "e2e_session_unknown") unknown += 1;
        assert.ok(unknown <= 1, String(error));
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    let result: unknown;
    for (let i = 0; i < 100; i++) {
      try {
        result = await box.unary("test.v1.Box", "double", { value: 2 });
        break;
      } catch (error) {
        assert.ok(!(error instanceof MeshCallError && error.reason === "e2e_session_unknown"), String(error));
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    assert.deepEqual(result, { value: 4 });
  } finally {
    await running.close();
  }
});

test("meshcall/web seals every call shape end to end", { timeout: 20000, skip: routerMissing }, async () => {
  const keys = await generateE2EKey();
  const other = await generateE2EKey();
  await using router = await LocalRouter.start({ port: 0 });
  const serve = () => new Mesh(router)
    .mount(new Box(), { instanceId: "box-a", accessLog: false, acceptTunnels: true, e2ePrivateKey: keys.privateKey, requireE2E: true })
    .startServing();
  let running = await serve();
  try {
    await using issuer = await Mesh.connect(router);
    const opened = await issuer.tunnels.open({
      instance: "box-a", scopes: ["test.v1.Box"], principal: "user:phone", e2eKey: keys.publicKey,
    });
    const url = new WebRouter(router.url);
    const browser = await WebMesh.connect(url, { tunnel: opened.credential, e2eKey: keys.publicKey });
    try {
      assert.deepEqual(await browser.unary("test.v1.Box", "double", { value: 21 }, at), { value: 42 });
      const items: number[] = [];
      for await (const item of browser.serverStream<object, { value: number }>("test.v1.Box", "count", { value: 4 }, at)) {
        items.push(item.value);
      }
      assert.deepEqual(items, [0, 1, 2, 3]);
      async function* inputs() { for (const value of [1, 2, 3]) yield { value }; }
      assert.deepEqual(await browser.clientStream("test.v1.Box", "total", { value: 1 }, inputs(), at), { value: 7 });
      const chat = browser.duplex<object, { text: string }, { text: string }>("test.v1.Box", "echo", {}, at);
      for (const text of ["ls", "pwd"]) {
        await chat.send({ text });
        assert.deepEqual(await chat.next(), { done: false, value: { text: "> " + text } });
      }
      await chat.closeSend();
      assert.deepEqual(await chat.next(), { done: true, value: undefined });
      assert.deepEqual(await browser.unary("test.v1.Box", "seen", {}, at), { sealed: true });
      const app = await rejection(browser.unary("test.v1.Box", "fail", { value: 3 }, at));
      assert.ok(app instanceof MeshCallError && app.code === "app.nope" && app.message === "secret 42", String(app));

      const wrong = new WebMesh(url, { tunnel: opened.credential, e2eKey: other.publicKey });
      const failed = await rejection(wrong.unary("test.v1.Box", "double", { value: 1 }, at));
      await wrong.close();
      assert.ok(failed instanceof MeshCallError && failed.reason === "e2e_handshake_failed", String(failed));

      // A stream after the restart is told once; the next call handshakes again.
      assert.deepEqual((await browser.registry.list()).map((s) => s.name), ["test.v1.Box"]);
      await running.close();
      running = await serve();
      let unknown = 0;
      for (let i = 0; i < 100; i++) {
        try {
          const got: number[] = [];
          for await (const item of browser.serverStream<object, { value: number }>("test.v1.Box", "count", { value: 2 }, at)) {
            got.push(item.value);
          }
          assert.deepEqual(got, [0, 1]);
          break;
        } catch (error) {
          if (error instanceof MeshCallError && error.reason === "e2e_session_unknown") unknown += 1;
          assert.ok(unknown <= 1, String(error));
          await new Promise((r) => setTimeout(r, 50));
        }
      }
      let result: unknown;
      for (let i = 0; i < 100; i++) {
        try {
          result = await browser.unary("test.v1.Box", "double", { value: 2 }, at);
          break;
        } catch (error) {
          assert.ok(!(error instanceof MeshCallError && error.reason === "e2e_session_unknown"), String(error));
          await new Promise((r) => setTimeout(r, 50));
        }
      }
      assert.deepEqual(result, { value: 4 });
    } finally {
      await browser.close();
    }
  } finally {
    await running.close();
  }
});

test("e2eKey needs a tunnel and a WebSocket Router", async () => {
  const { publicKey } = await generateE2EKey();
  assert.throws(() => new Mesh(new Router("ws://127.0.0.1:1"), { e2eKey: publicKey }), /tunnel/u);
  assert.throws(() => new Mesh(new Router("http://127.0.0.1:1"), { tunnel: "mtun_e2e.x", e2eKey: publicKey }), /HTTP/u);
  assert.throws(() => new Mesh(new Router("ws://127.0.0.1:1"), { tunnel: "mtun_e2e.x", e2eKey: "short" }), /32 bytes/u);
  assert.throws(() => new Mesh(new Router("ws://127.0.0.1:1"), { tunnel: "mtun_x", e2eKey: publicKey }), /mtun_e2e/u);
  assert.throws(() => new Mesh(new Router("http://127.0.0.1:1"), { tunnel: "mtun_e2e.x" }), /end-to-end tunnel credential/u);
  assert.throws(() => new WebMesh(new WebRouter("ws://127.0.0.1:1"), { e2eKey: publicKey }), /tunnel/u);
  assert.throws(() => new WebMesh(new WebRouter("https://r.example"), { tunnel: "mtun_e2e.x", e2eKey: publicKey }), /HTTP/u);
  assert.throws(() => new WebMesh(new WebRouter("https://r.example"), { tunnel: "mtun_e2e.x" }), /end-to-end tunnel credential/u);
  assert.throws(() => new Mesh(new Router("ws://127.0.0.1:1")).mount(new Box(), { requireE2E: true }), /requireE2E needs acceptTunnels/u);
});
