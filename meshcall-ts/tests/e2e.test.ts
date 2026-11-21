import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  aeadEncrypt, b64decode, b64encode, checkE2EIdentity, confirmAd, credentialDigest, E2E_CONFIRM_TIMEOUT_MS,
  E2E_HANDSHAKE_TIMEOUT_MS, E2E_PROLOGUE, E2E_WINDOW, E2EClient, e2ePublicKey, generateE2EKey, generateE2EPsk, IK,
  IKPSK2, NK, nkInitiate, nkRespond, NoiseHandshake, parseHandshakeRequest, peerPrologue, respondHandshake, SealedCall,
  tunnelPrologue,
} from "../src/e2e.js";
import type { CallErrorFrame, CallOpenFrame, Frame, StreamItemFrame } from "../src/protocol.js";

const TESTDATA = new URL("../../../meshcall-router/conformance/testdata/", import.meta.url);
const testdata = <T>(name: string): T => JSON.parse(readFileSync(new URL(name, TESTDATA), "utf8")) as T;
const bytes = (h: string) => Uint8Array.from(Buffer.from(h, "hex"));
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

interface NoiseVector {
  protocol_name: string; init_prologue: string; resp_prologue: string; init_psks?: string[];
  init_static?: string; init_ephemeral: string; init_remote_static: string;
  resp_static: string; resp_ephemeral: string; handshake_hash: string;
  messages: Array<{ payload: string; ciphertext: string }>;
}

for (const name of ["nk", "ik", "ikpsk2"]) {
  test(`Noise ${name} matches the cacophony test vector`, async () => {
    const v = testdata<NoiseVector>(`noise_${name}_25519_aesgcm_sha256.json`);
    const psk = v.init_psks?.[0] === undefined ? {} : { psk: bytes(v.init_psks[0]) };
    const initiator = await NoiseHandshake.create(v.protocol_name, {
      initiator: true, prologue: bytes(v.init_prologue), rs: bytes(v.init_remote_static), ephemeral: bytes(v.init_ephemeral),
      ...(v.init_static === undefined ? {} : { s: bytes(v.init_static) }), ...psk,
    });
    const responder = await NoiseHandshake.create(v.protocol_name, {
      initiator: false, prologue: bytes(v.resp_prologue), s: bytes(v.resp_static), ephemeral: bytes(v.resp_ephemeral), ...psk,
    });
    const m = v.messages;
    const message1 = await initiator.writeMessage(bytes(m[0]!.payload));
    assert.equal(hex(message1), m[0]!.ciphertext);
    assert.equal(hex(await responder.readMessage(message1)), m[0]!.payload);
    if (v.init_static !== undefined) {
      assert.equal(b64encode(responder.remoteStatic!), await e2ePublicKey(b64encode(bytes(v.init_static))));
    }
    const message2 = await responder.writeMessage(bytes(m[1]!.payload));
    assert.equal(hex(message2), m[1]!.ciphertext);
    assert.equal(hex(await initiator.readMessage(message2)), m[1]!.payload);
    const client = await initiator.split();
    const instance = await responder.split();
    assert.equal(hex(client.hash), v.handshake_hash);
    assert.equal(client.id, instance.id);
    assert.equal(client.protocol, v.protocol_name);
    const counters = [0n, 0n];
    for (const [i, message] of m.slice(2).entries()) {
      const sender = [client, instance][i % 2]!;
      const sealed = await aeadEncrypt(sender.sendKey, counters[i % 2]!, bytes(message.payload), new Uint8Array());
      counters[i % 2]! += 1n;
      assert.equal(hex(sealed), message.ciphertext);
    }
  });
}

test("the NK helpers still match the cacophony test vector", async () => {
  const v = testdata<NoiseVector>("noise_nk_25519_aesgcm_sha256.json");
  const prologue = bytes(v.init_prologue);
  const m = v.messages;
  const initiator = await nkInitiate(bytes(v.init_remote_static), {
    prologue, ephemeral: bytes(v.init_ephemeral), payload: bytes(m[0]!.payload),
  });
  assert.equal(hex(initiator.message), m[0]!.ciphertext);
  const responder = await nkRespond(bytes(v.resp_static), initiator.message, {
    prologue, ephemeral: bytes(v.resp_ephemeral), payload: bytes(m[1]!.payload),
  });
  assert.equal(hex(responder.reply), m[1]!.ciphertext);
  const client = await initiator.finish(responder.reply);
  assert.equal(hex(client.session.hash), v.handshake_hash);
});

test("IKpsk2 with another PSK does not complete", async () => {
  const instance = await generateE2EKey();
  const client = await generateE2EKey();
  const prologue = new TextEncoder().encode(E2E_PROLOGUE);
  const initiator = await NoiseHandshake.create(IKPSK2, {
    initiator: true, prologue, s: b64decode(client.privateKey), rs: b64decode(instance.publicKey), psk: new Uint8Array(32).fill(1),
  });
  const responder = await NoiseHandshake.create(IKPSK2, {
    initiator: false, prologue, s: b64decode(instance.privateKey), psk: new Uint8Array(32).fill(2),
  });
  // Message 1 opens without the PSK: it proves nothing about it ...
  await responder.readMessage(await initiator.writeMessage());
  assert.equal(b64encode(responder.remoteStatic!), client.publicKey);
  // ... message 2 does not open for an initiator holding another one.
  await assert.rejects(initiator.readMessage(await responder.writeMessage()));
});

interface HandshakeVectors {
  credential: string; credential_digest: string; instance_id: string;
  instance_private_key: string; instance_public_key: string; client_private_key: string; client_public_key: string;
  instance_ephemeral: string; client_ephemeral: string; pairing: { id: string; psk: string };
  cases: Array<{
    protocol: string; prologue: string; request: Record<string, string>; answer: { message: string };
    handshake_hash: string; session: string; confirm_request?: { session: string; confirm: string };
  }>;
}

test("MeshCall handshakes match e2e_handshakes.json byte for byte", async () => {
  const v = testdata<HandshakeVectors>("e2e_handshakes.json");
  assert.equal(await credentialDigest(v.credential), v.credential_digest);
  assert.equal(await e2ePublicKey(v.instance_private_key), v.instance_public_key);
  assert.equal(await e2ePublicKey(v.client_private_key), v.client_public_key);
  const decoder = new TextDecoder();
  for (const c of v.cases) {
    const identity = c.protocol === NK ? undefined : {
      privateKey: v.client_private_key, instanceId: v.instance_id,
      ...(c.protocol === IKPSK2 ? { pairing: v.pairing } : {}),
    };
    const prologue = c.protocol === NK
      ? tunnelPrologue(v.credential_digest) : peerPrologue(v.credential_digest, v.instance_id);
    assert.equal(decoder.decode(prologue), c.prologue);
    // The client's request ...
    const client = new E2EClient(v.instance_public_key, v.credential, identity);
    const handshake = await client.start("c1", b64decode(v.client_ephemeral));
    assert.deepEqual(handshake.frame.payload, c.request, c.protocol);
    assert.equal(handshake.callId, "c1");
    assert.equal(handshake.frame.timeout_ms, E2E_HANDSHAKE_TIMEOUT_MS);
    // ... the instance's answer ...
    const request = parseHandshakeRequest(c.request);
    const { reply, session } = await respondHandshake(request, {
      privateKey: b64decode(v.instance_private_key), digest: v.credential_digest, instanceId: v.instance_id,
      ephemeral: b64decode(v.instance_ephemeral),
      ...(c.protocol === IKPSK2 ? { psk: b64decode(v.pairing.psk) } : {}),
    });
    assert.equal(b64encode(reply), c.answer.message, c.protocol);
    assert.equal(hex(session.hash), c.handshake_hash);
    assert.equal(session.id, c.session);
    assert.equal(session.protocol, c.protocol);
    assert.equal(session.peerKey, c.protocol === NK ? null : v.client_public_key);
    assert.equal(session.pairingId, c.protocol === IKPSK2 ? v.pairing.id : null);
    // ... and the client's session, or its pairing confirmation.
    const answered = await handshake.receive({ kind: "call.result", call_id: "c1", payload: c.answer }, "c2");
    if (c.confirm_request === undefined) {
      assert.equal(answered?.id, c.session);
      continue;
    }
    assert.equal(answered, undefined);
    assert.equal(handshake.callId, "c2");
    assert.equal(handshake.timeoutMs, E2E_CONFIRM_TIMEOUT_MS);
    assert.deepEqual(handshake.frame.payload, c.confirm_request);
    assert.equal(decoder.decode(await session.open(c.confirm_request.confirm, confirmAd(session.id))), "{}");
    const confirmed = await handshake.receive({ kind: "call.result", call_id: "c2", payload: {} }, "c3");
    assert.equal(confirmed?.id, c.session);
    // Once the pairing is confirmed, the client uses IK.
    const next = await client.start("c4");
    assert.equal((next.frame.payload as { protocol?: string }).protocol, IK);
    assert.equal((next.frame.payload as { pairing_id?: string }).pairing_id, undefined);
  }
});

test("a refused pairing is retried with IKpsk2, never NK", async () => {
  const v = testdata<HandshakeVectors>("e2e_handshakes.json");
  const client = new E2EClient(v.instance_public_key, v.credential, {
    privateKey: v.client_private_key, instanceId: v.instance_id, pairing: v.pairing,
  });
  const first = await client.start("c1");
  const refusal = {
    kind: "call.error" as const, call_id: "c1",
    error: { code: "unauthenticated", message: "End-to-end handshake failed", details: { reason: "e2e_handshake_failed" } },
  };
  await assert.rejects(first.receive(refusal, "c2"), /handshake failed/u);
  // A refused confirmation does not mark the client paired either.
  const pairing = v.cases.find((c) => c.protocol === IKPSK2)!;
  const again = await client.start("c3", b64decode(v.client_ephemeral));
  assert.equal(await again.receive({ kind: "call.result", call_id: "c3", payload: pairing.answer }, "c4"), undefined);
  await assert.rejects(again.receive({ ...refusal, call_id: "c4" }, "c5"), /handshake failed/u);
  const third = await client.start("c6");
  assert.equal((third.frame.payload as { protocol?: string }).protocol, IKPSK2);
  assert.throws(() => checkE2EIdentity({ privateKey: v.client_private_key, instanceId: "" }), /instanceId/u);
  assert.throws(() => checkE2EIdentity({ privateKey: v.client_private_key, instanceId: "i", pairing: { id: "", psk: v.pairing.psk } }), /pairing id/u);
  assert.throws(() => checkE2EIdentity({ privateKey: v.client_private_key, instanceId: "i", pairing: { id: "x".repeat(129), psk: v.pairing.psk } }), /pairing id/u);
  assert.throws(() => checkE2EIdentity({ privateKey: v.client_private_key, instanceId: "i", pairing: { id: "p", psk: "short" } }), /PSK/u);
  assert.throws(() => checkE2EIdentity({ privateKey: "short", instanceId: "i" }), /private key/u);
  assert.equal(b64decode(generateE2EPsk()).length, 32);
});

async function pair(): Promise<[SealedCall, SealedCall]> {
  const keys = await generateE2EKey();
  assert.equal(await e2ePublicKey(keys.privateKey), keys.publicKey);
  const initiator = await nkInitiate(b64decode(keys.publicKey), { prologue: new TextEncoder().encode(E2E_PROLOGUE) });
  const { reply, session } = await nkRespond(b64decode(keys.privateKey), initiator.message);
  const client = await initiator.finish(reply);
  return [new SealedCall(client.session, "s.v1.S", "m"), new SealedCall(session, "s.v1.S", "m")];
}

const item = (sequence: number, payload: unknown): StreamItemFrame =>
  ({ kind: "stream.item", call_id: "c1", direction: "client", sequence, payload });

test("sealed frames bind their call and refuse replays", async () => {
  const [out, into] = await pair();
  const opened = await out.seal({
    kind: "call.open", call_id: "c1", service: "s.v1.S", method: "m", payload: { secret: 1 }, metadata: { trace: "t" },
  }) as CallOpenFrame & { e2e_session?: string; sealed?: string };
  assert.equal(opened.payload, null);
  assert.equal(opened.metadata, undefined);
  assert.ok(opened.e2e_session !== undefined && opened.sealed !== undefined);
  assert.ok(!JSON.stringify(opened).includes("secret"));
  const got = await into.open(opened) as CallOpenFrame;
  assert.deepEqual(got.payload, { secret: 1 });
  assert.deepEqual(got.metadata, { trace: "t" });
  await assert.rejects(into.open(opened), /replayed/u);
  const moved = await out.seal({ kind: "call.open", call_id: "c2", service: "s.v1.S", method: "m", payload: 1 });
  await assert.rejects(into.open({ ...moved, call_id: "c3" } as Frame), /does not open/u);
  await assert.rejects(into.open({ kind: "call.result", call_id: "c1", payload: 1 }), /in clear/u);
  const first = await out.seal(item(0, 1));
  const second = await out.seal(item(1, 2));
  assert.equal((await into.open(second) as StreamItemFrame).payload, 2);
  assert.equal((await into.open(first) as StreamItemFrame).payload, 1);
  const old = await out.seal(item(2, 0));
  for (let i = 0; i <= E2E_WINDOW; i += 1) await into.open(await out.seal(item(3 + i, i)));
  await assert.rejects(into.open(old), /too old/u);
  const error = await into.seal({
    kind: "call.error", call_id: "c1",
    error: { code: "invalid_argument", message: "bad secret 42", details: { field: "secret" } },
  }) as CallErrorFrame;
  assert.equal(error.error.message, "sealed");
  assert.equal(error.error.details, undefined);
  const restored = await out.open(error) as CallErrorFrame;
  assert.equal(restored.error.message, "bad secret 42");
  assert.deepEqual(restored.error.details, { field: "secret" });
  // A clear call.error (the Router's own) is kept as it is.
  const routerError: CallErrorFrame = { kind: "call.error", call_id: "c1", error: { code: "unavailable", message: "gone" } };
  assert.deepEqual(await out.open(routerError), routerError);
});
