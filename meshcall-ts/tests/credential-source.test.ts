// docs/protocol.md → Credential sources.
// conformance-scenario: auth.credential_source
import assert from "node:assert/strict";
import { pbkdf2Sync } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  CredentialSourceError,
  findRouterBinary,
  generateE2EKey,
  LocalRouter,
  Mesh,
  MeshCallError,
  method,
  Router,
  service,
  UnauthenticatedError,
  WebSocketRouter,
  z,
  type RpcContext,
} from "../src/index.js";
import { Mesh as WebMesh, Router as WebRouter } from "../src/web.js";

const routerMissing = (() => { try { findRouterBinary(); return false; } catch { return "Go Router binary not built"; } })();

const Empty = z.object({});
const Seen = z.object({ tunnel: z.string().nullable(), sealed: z.boolean() });

@service("test.v1.Box", { targeted: true, description: "A box reached through tunnels." })
class Box {
  @method({ description: "Report the tunnel of this call.", readOnly: true, request: Empty, response: Seen })
  whoami(_: unknown, ctx: RpcContext) { return { tunnel: ctx.tunnel?.id ?? null, sealed: ctx.tunnel?.sealed ?? false }; }
}

type Caller = { unary<Req, Res>(service: string, method: string, request: Req, options?: { instance?: string }): Promise<Res> };
type Seen = { tunnel: string | null; sealed: boolean };

const whoami = (mesh: Caller) => mesh.unary<object, Seen>("test.v1.Box", "whoami", {}, { instance: "box-a" });

/** Until the Router has closed the revoked tunnel's connection. */
async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 200));
}

for (const flavour of ["node", "web"] as const) {
  for (const sealed of [false, true]) {
    const name = `${flavour}: a tunnel source replaces a revoked tunnel${sealed ? " (end to end)" : ""}`;
    test(name, { timeout: 20000, skip: routerMissing }, async () => {
      await using router = await LocalRouter.start({ port: 0 });
      const keys = await generateE2EKey();
      const box = new Mesh(router).mount(new Box(), {
        instanceId: "box-a", accessLog: false, acceptTunnels: true,
        ...(sealed ? { e2ePrivateKey: keys.privateKey, requireE2E: true } : {}),
      });
      await using _box = await box.startServing();
      await using issuer = await Mesh.connect(router);
      const open = () => issuer.tunnels.open({
        instance: "box-a", scopes: ["test.v1.Box"], principal: "user:alice", ttlSeconds: 60,
        ...(sealed ? { e2eKey: keys.publicKey } : {}),
      });

      let current = await open();
      let asked = 0;
      const source = () => { asked += 1; return current.credential; };
      const e2e = sealed ? { e2eKey: keys.publicKey } : {};
      const client: Caller & { close(): Promise<void> } = flavour === "node"
        ? new Mesh(new Router(router.url!), { tunnel: source, ...e2e })
        : new WebMesh(new WebRouter(router.url!), { tunnel: source, ...e2e });
      try {
        assert.deepEqual(await whoami(client), { tunnel: current.tunnel_id, sealed });
        assert.equal(asked, 1);

        // Revoked: the next call connects with what the source returns now.
        const first = current;
        current = await open();
        await issuer.tunnels.revoke(first.tunnel_id);
        await settle();
        assert.deepEqual(await whoami(client), { tunnel: current.tunnel_id, sealed });

        // A source whose new credential is refused too: the call fails…
        await issuer.tunnels.revoke(current.tunnel_id);
        await settle();
        asked = 0;
        await assert.rejects(whoami(client), (error: unknown) => error instanceof UnauthenticatedError);
        assert.equal(asked, 2, "asked once, and once more after the refusal");

        // …and the refusal is not kept: the next call asks the source again.
        current = await open();
        assert.deepEqual(await whoami(client), { tunnel: current.tunnel_id, sealed });
      } finally {
        await client.close();
      }
    });
  }
}

test("a failing or wrong source fails the connection attempt, not the Mesh", { timeout: 15000, skip: routerMissing }, async () => {
  await using router = await LocalRouter.start({ port: 0 });
  let fail = true;
  const mesh = new Mesh(new Router(router.url!), { token: () => {
    if (fail) throw new Error("vault unreachable");
    return "mt_any";
  } });
  try {
    await assert.rejects(mesh.registry.list(), (error: unknown) =>
      error instanceof MeshCallError && error.code === "unavailable" && error.retryable
      && error.details?.["reason"] === "credential_source");
    // Not kept: once the source answers, the next call connects (a development
    // Router takes any token).
    fail = false;
    assert.ok(Array.isArray(await mesh.registry.list()));
  } finally {
    await mesh.close();
  }
  const wrongKind = new Mesh(new Router(router.url!), { token: () => "mtun_abc" });
  try {
    await assert.rejects(wrongKind.registry.list(), (error: unknown) => error instanceof CredentialSourceError);
  } finally {
    await wrongKind.close();
  }
});

const password = "test-only-password";
const salt = Buffer.from("meshcall-test-01");
const hash = "pbkdf2-sha256$600000$" + salt.toString("hex") + "$" +
  pbkdf2Sync(password, salt, 600000, 32, "sha256").toString("hex");

test("an instance with a token source registers again after its token is revoked", { timeout: 20000, skip: routerMissing }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "meshcall-ts-source-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = join(directory, "auth.json");
  await writeFile(file, JSON.stringify({ users: [{
    username: "issuer", password_hash: hash, roles: ["client", "server"],
    register: ["test.v1.Box"], call: ["test.v1.Box/*", "meshcall.router.v1.AuthService/*"],
  }] }));
  const router = new WebSocketRouter({ authFile: file });
  t.after(() => router.stop());
  await router.start();
  const url = "ws://127.0.0.1:" + router.boundPort;
  await using issuer = await Mesh.connect(new Router(url), { credentials: { username: "issuer", password } });
  const issue = () => issuer.auth.issueToken([{ service: "test.v1.Box", register: true, instance_ids: ["box-a"] }], 60);

  let current = await issue();
  let asked = 0;
  const box = new Mesh(new Router(url), { token: () => { asked += 1; return current.token; } })
    .mount(new Box(), { instanceId: "box-a", accessLog: false });
  await using _box = await box.startServing();
  assert.deepEqual(await whoami(issuer), { tunnel: null, sealed: false });

  const first = current;
  current = await issue();
  const before = asked;
  await issuer.auth.revokeToken(first.token_id);
  // The instance reconnects with backoff, asking the source again.
  const deadline = Date.now() + 10_000;
  for (;;) {
    try {
      assert.deepEqual(await whoami(issuer), { tunnel: null, sealed: false });
      break;
    } catch (error) {
      if (Date.now() > deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  assert.ok(asked > before, "the source was asked again");

  // Over HTTP: a request answered 401 is sent once more with a new token.
  const callerToken = () => issuer.auth.issueToken([{ service: "test.v1.Box", methods: ["*"] }], 60);
  let caller = await callerToken();
  const http = new Mesh(new Router("http://127.0.0.1:" + router.boundPort), { token: () => caller.token });
  assert.deepEqual(await whoami(http), { tunnel: null, sealed: false });
  const old = caller;
  caller = await callerToken();
  await issuer.auth.revokeToken(old.token_id);
  assert.deepEqual(await whoami(http), { tunnel: null, sealed: false });
});
