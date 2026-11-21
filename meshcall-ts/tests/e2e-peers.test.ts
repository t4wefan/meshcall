// Clients that authenticate themselves on end-to-end tunnels (docs/protocol.md
// → Client authentication): IK for known clients, IKpsk2 to pair a new one.
import assert from "node:assert/strict";
import test from "node:test";

import {
  findRouterBinary,
  generateE2EKey,
  generateE2EPsk,
  LocalRouter,
  Mesh,
  MeshCallError,
  method,
  PermissionDeniedError,
  Router,
  service,
  UnauthenticatedError,
  z,
  type E2EKeyPair,
  type E2EPeer,
  type RpcContext,
} from "../src/index.js";
import { b64encode, E2EClient, IKPSK2 } from "../src/e2e.js";
import { TunnelHost, TUNNEL_ID_KEY, TUNNEL_PRINCIPAL_KEY } from "../src/tunnel.js";
import { Mesh as WebMesh, Router as WebRouter } from "../src/web.js";

const routerMissing = (() => {
  try {
    findRouterBinary();
    return false;
  } catch {
    return "Go Router binary not built";
  }
})();

const Empty = z.object({});
const Seen = z.object({ protocol: z.string().nullable(), peer_key: z.string().nullable(), pairing_id: z.string().nullable() });
type Seen = z.infer<typeof Seen>;

const seen = (ctx: RpcContext): Seen => ({
  protocol: ctx.e2e?.protocol ?? null, peer_key: ctx.e2e?.peerKey ?? null, pairing_id: ctx.e2e?.pairingId ?? null,
});

@service("test.v1.Device", { description: "Served to paired clients only." })
class Device {
  @method({ description: "The end-to-end session of this call.", readOnly: true, request: Empty, response: Seen })
  whoami(_: unknown, ctx: RpcContext) { return seen(ctx); }
}

@service("test.v1.Open", { description: "Served to any end-to-end client." })
class Open {
  @method({ description: "The end-to-end session of this call.", readOnly: true, request: Empty, response: Seen })
  whoami(_: unknown, ctx: RpcContext) { return seen(ctx); }
}

/** What the device keeps: paired client keys and one-time pairings. */
class Owner {
  public readonly paired = new Set<string>();
  public readonly pairings = new Map<string, string>();
  public readonly asked: E2EPeer[] = [];

  public readonly psk = (pairingId: string) => this.pairings.get(pairingId);

  public readonly authorize = async (peer: E2EPeer): Promise<boolean> => {
    this.asked.push(peer);
    assert.ok(peer.peerKey !== null);
    if (peer.pairingId !== null) {
      // Spent here, once the client proved it holds the PSK.
      if (!this.pairings.delete(peer.pairingId)) return false;
      this.paired.add(peer.peerKey);
      return true;
    }
    return this.paired.has(peer.peerKey);
  };
}

const device = (router: LocalRouter, keys: E2EKeyPair, owner: Owner) => new Mesh(router)
  .mount(new Device(), {
    instanceId: "dev-a", accessLog: false, acceptTunnels: true, e2ePrivateKey: keys.privateKey,
    requireE2EPeer: true, authorizeE2EPeer: owner.authorize, e2ePairingPsk: owner.psk,
  })
  .mount(new Open(), { acceptTunnels: true })
  .startServing();

const rejection = (promise: Promise<unknown>) => promise.then(() => undefined, (e: unknown) => e);
const whoami = (mesh: Mesh | WebMesh, name = "test.v1.Device") => mesh.unary<object, Seen>(name, "whoami", {});
const reason = (error: unknown) => error instanceof MeshCallError ? error.reason : String(error);

test("pairing, then a known client, then refusals", { timeout: 30000, skip: routerMissing }, async () => {
  const keys = await generateE2EKey();
  const owner = new Owner();
  const alice = await generateE2EKey();
  const mallory = await generateE2EKey();
  await using router = await LocalRouter.start({ port: 0 });
  let running = await device(router, keys, owner);
  try {
    await using issuer = await Mesh.connect(router);
    const tunnel = async () => (await issuer.tunnels.open({
      instance: "dev-a", scopes: ["test.v1.Device", "test.v1.Open"], principal: "user:alice", e2eKey: keys.publicKey,
    })).credential;
    const url = new Router(router.url);
    /** A call through a fresh Mesh that is expected to fail. */
    const refused = async (options: ConstructorParameters<typeof Mesh>[1]) => {
      const mesh = new Mesh(url, options);
      try {
        return await rejection(whoami(mesh));
      } finally {
        await mesh.close();
      }
    };

    // requireE2EPeer implies requireE2E: a plain tunnel is refused.
    const plain = await rejection(issuer.tunnels.open({ instance: "dev-a", scopes: ["test.v1.Device"], principal: "p" }));
    assert.ok(plain instanceof PermissionDeniedError && plain.reason === "tunnel_rejected", String(plain));
    // Not through a tunnel at all: refused by the instance itself.
    const direct = await rejection(whoami(issuer.withOptions({ instance: "dev-a" })));
    assert.ok(direct instanceof PermissionDeniedError && direct.reason === "e2e_peer_required", String(direct));

    // An unknown client key: the handshake fails, without saying why.
    const unknown = await refused({
      tunnel: await tunnel(), e2eKey: keys.publicKey, e2eIdentity: { privateKey: mallory.privateKey, instanceId: "dev-a" },
    });
    assert.ok(unknown instanceof UnauthenticatedError && unknown.reason === "e2e_handshake_failed", String(unknown));
    assert.equal(owner.asked.at(-1)?.peerKey, mallory.publicKey);
    assert.equal(owner.asked.at(-1)?.pairingId, null);

    // An NK client reaches services that do not require a peer, not others.
    {
      await using nk = await Mesh.connect(url, { tunnel: await tunnel(), e2eKey: keys.publicKey });
      assert.deepEqual(await whoami(nk, "test.v1.Open"), { protocol: "Noise_NK_25519_AESGCM_SHA256", peer_key: null, pairing_id: null });
      const weak = await rejection(whoami(nk));
      assert.ok(weak instanceof PermissionDeniedError && weak.reason === "e2e_peer_required", String(weak));
    }

    // Pairing: the owner shows a pairing id and PSK on the device.
    owner.pairings.set("pair-1", generateE2EPsk());
    // A wrong PSK fails on the client and does not spend the pairing.
    const badPsk = await refused({
      tunnel: await tunnel(), e2eKey: keys.publicKey,
      e2eIdentity: { privateKey: mallory.privateKey, instanceId: "dev-a", pairing: { id: "pair-1", psk: generateE2EPsk() } },
    });
    assert.ok(badPsk instanceof UnauthenticatedError && badPsk.reason === "e2e_handshake_failed", String(badPsk));
    assert.ok(owner.pairings.has("pair-1") && !owner.paired.has(mallory.publicKey));

    const pairing = { id: "pair-1", psk: owner.pairings.get("pair-1")! };
    // The wrong instance id is bound in the prologue: no session.
    const elsewhere = await refused({
      tunnel: await tunnel(), e2eKey: keys.publicKey,
      e2eIdentity: { privateKey: alice.privateKey, instanceId: "dev-b", pairing },
    });
    assert.ok(elsewhere instanceof UnauthenticatedError, String(elsewhere));
    assert.ok(owner.pairings.has("pair-1"));

    await using mesh = await Mesh.connect(url, {
      tunnel: await tunnel(), e2eKey: keys.publicKey, e2eIdentity: { privateKey: alice.privateKey, instanceId: "dev-a", pairing },
    });
    assert.deepEqual(await whoami(mesh), { protocol: IKPSK2, peer_key: alice.publicKey, pairing_id: "pair-1" });
    assert.deepEqual([...owner.paired], [alice.publicKey]);
    assert.equal(owner.pairings.size, 0);

    // The device restarts and loses its sessions: the same client handshakes
    // again with IK (the pairing is spent) and goes on.
    await running.close();
    running = await device(router, keys, owner);
    let result: Seen | undefined;
    for (let i = 0; i < 100 && result === undefined; i++) {
      try {
        result = await whoami(mesh);
      } catch (error) {
        assert.notEqual(reason(error), "e2e_handshake_failed", String(error));
        await new Promise((r) => setTimeout(r, 50));
      }
    }
    assert.deepEqual(result, { protocol: "Noise_IK_25519_AESGCM_SHA256", peer_key: alice.publicKey, pairing_id: null });

    // Another client cannot reuse the spent pairing.
    const spent = await refused({
      tunnel: await tunnel(), e2eKey: keys.publicKey,
      e2eIdentity: { privateKey: mallory.privateKey, instanceId: "dev-a", pairing },
    });
    assert.ok(spent instanceof UnauthenticatedError && spent.reason === "e2e_handshake_failed", String(spent));
    assert.deepEqual([...owner.paired], [alice.publicKey]);
  } finally {
    await running.close();
  }
});

test("meshcall/web pairs, then uses IK", { timeout: 30000, skip: routerMissing }, async () => {
  const keys = await generateE2EKey();
  const owner = new Owner();
  const phone = await generateE2EKey();
  await using router = await LocalRouter.start({ port: 0 });
  const running = await device(router, keys, owner);
  try {
    await using issuer = await Mesh.connect(router);
    const opened = await issuer.tunnels.open({
      instance: "dev-a", scopes: ["test.v1.Device"], principal: "user:phone", e2eKey: keys.publicKey,
    });
    owner.pairings.set("pair-web", generateE2EPsk());
    const browser = await WebMesh.connect(new WebRouter(router.url), {
      tunnel: opened.credential, e2eKey: keys.publicKey,
      e2eIdentity: { privateKey: phone.privateKey, instanceId: "dev-a", pairing: { id: "pair-web", psk: owner.pairings.get("pair-web")! } },
    });
    try {
      assert.deepEqual(await whoami(browser), { protocol: IKPSK2, peer_key: phone.publicKey, pairing_id: "pair-web" });
      // The instance drops every session of the tunnel only when it closes;
      // a fresh connection of the same client must handshake with IK.
      await browser.close();
      assert.deepEqual(await whoami(browser), { protocol: "Noise_IK_25519_AESGCM_SHA256", peer_key: phone.publicKey, pairing_id: null });
    } finally {
      await browser.close();
    }
  } finally {
    await running.close();
  }
});

test("pairing sessions wait for their confirmation", async () => {
  const keys = await generateE2EKey();
  const client = await generateE2EKey();
  const psk = generateE2EPsk();
  let asked = 0;
  let refuse = false;
  const host = new TunnelHost(new Map([["s.v1.S", { accept: true }]]), {
    e2ePrivateKey: keys.privateKey, instanceId: "dev-a",
    e2ePairingPsk: (id) => id === "p1" ? psk : undefined,
    authorizeE2EPeer: (peer) => {
      asked += 1;
      if (refuse) throw new Error("hook failed");
      return peer.pairingId === "p1";
    },
  });
  const credential = "mtun_e2e.unit";
  const digest = Buffer.from(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(credential))).toString("hex");
  assert.deepEqual(await host.handle("offer", {
    tunnel_id: "t1", instance_id: "dev-a", principal: "p", scopes: [{ service: "s.v1.S", methods: ["*"] }],
    expires_at_unix_ms: 0, resumed: false, e2e_key: keys.publicKey, credential_digest: digest,
  }), { accept: true });
  const metadata = { [TUNNEL_ID_KEY]: "t1", [TUNNEL_PRINCIPAL_KEY]: "p" };
  const pair = async (): Promise<{ session: string; confirm: string }> => {
    const e2e = new E2EClient(keys.publicKey, credential, { privateKey: client.privateKey, instanceId: "dev-a", pairing: { id: "p1", psk } });
    const handshake = await e2e.start("c1");
    const answer = await host.handle("handshake", handshake.frame.payload, metadata);
    assert.equal(await handshake.receive({ kind: "call.result", call_id: "c1", payload: answer }, "c2"), undefined);
    return handshake.frame.payload as { session: string; confirm: string };
  };
  const failed = (promise: Promise<unknown>) => rejection(promise).then(reason);

  // Pending: not a session calls may name, and the hook was not asked yet.
  const first = await pair();
  assert.equal(host.sessions.find("t1", first.session), undefined);
  assert.equal(asked, 0);
  // A confirmation that does not open drops the pending session.
  const forged = b64encode(new Uint8Array(26));
  assert.equal(await failed(host.handle("handshake", { session: first.session, confirm: forged }, metadata)), "e2e_handshake_failed");
  assert.equal(await failed(host.handle("handshake", { session: first.session, confirm: first.confirm }, metadata)), "e2e_handshake_failed");
  assert.equal(asked, 0);
  // A hook that throws refuses.
  const second = await pair();
  refuse = true;
  assert.equal(await failed(host.handle("handshake", { session: second.session, confirm: second.confirm }, metadata)), "e2e_handshake_failed");
  assert.equal(asked, 1);
  refuse = false;
  const third = await pair();
  assert.deepEqual(await host.handle("handshake", { session: third.session, confirm: third.confirm }, metadata), {});
  assert.equal(host.sessions.find("t1", third.session)?.pairingId, "p1");
  // Unknown protocols and pairing ids read like every other failure.
  for (const payload of [
    { message: "", protocol: "Noise_XX_25519_AESGCM_SHA256" },
    { message: "", protocol: IKPSK2, pairing_id: "unknown" },
    { message: "", protocol: IKPSK2, pairing_id: "x".repeat(129) },
  ]) {
    assert.equal(await failed(host.handle("handshake", payload, metadata)), "e2e_handshake_failed");
  }
  // Pending sessions go with their tunnel.
  const fourth = await pair();
  await host.handle("closed", { tunnel_id: "t1", reason: "revoked" });
  assert.equal(await failed(host.handle("handshake", { session: fourth.session, confirm: fourth.confirm }, metadata)), "e2e_handshake_failed");
});

test("e2eIdentity is checked before anything is sent", async () => {
  const keys = await generateE2EKey();
  const me = await generateE2EKey();
  const ws = new Router("ws://127.0.0.1:1");
  assert.throws(() => new Mesh(ws, { tunnel: "mtun_e2e.x", e2eIdentity: { privateKey: me.privateKey, instanceId: "dev-a" } }), /e2eKey/u);
  assert.throws(() => new Mesh(ws, { tunnel: "mtun_e2e.x", e2eKey: keys.publicKey, e2eIdentity: { privateKey: me.privateKey, instanceId: "" } }),
    /instanceId/u);
  assert.throws(() => new Mesh(ws, {
    tunnel: "mtun_e2e.x", e2eKey: keys.publicKey, e2eIdentity: { privateKey: me.privateKey, instanceId: "a", pairing: { id: "p", psk: "short" } },
  }), /PSK/u);
  assert.throws(() => new WebMesh(new WebRouter("ws://127.0.0.1:1"), {
    tunnel: "mtun_e2e.x", e2eKey: keys.publicKey, e2eIdentity: { privateKey: "short", instanceId: "a" },
  }), /private key/u);
  assert.throws(() => new Mesh(ws).mount(new Device(), { requireE2EPeer: true }), /requireE2EPeer needs acceptTunnels/u);
});

test("requireE2EPeer needs a key and an authorization hook", { timeout: 20000, skip: routerMissing }, async () => {
  const keys = await generateE2EKey();
  await using router = await LocalRouter.start({ port: 0 });
  const noHook = new Mesh(router).mount(new Device(), { acceptTunnels: true, e2ePrivateKey: keys.privateKey, requireE2EPeer: true });
  await assert.rejects(noHook.startServing(), /requireE2EPeer needs authorizeE2EPeer/u);
  const noKey = new Mesh(router).mount(new Device(), { acceptTunnels: true, requireE2EPeer: true, authorizeE2EPeer: () => true });
  await assert.rejects(noKey.startServing(), /requireE2EPeer needs e2ePrivateKey/u);
});
