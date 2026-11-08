import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  aeadEncrypt, b64decode, E2E_PROLOGUE, E2E_WINDOW, e2ePublicKey, generateE2EKey, nkInitiate, nkRespond, SealedCall,
} from "../src/e2e.js";
import type { CallErrorFrame, CallOpenFrame, Frame, StreamItemFrame } from "../src/protocol.js";

const VECTOR = JSON.parse(readFileSync(
  new URL("../../../meshcall-router/conformance/testdata/noise_nk_25519_aesgcm_sha256.json", import.meta.url), "utf8",
)) as {
  init_prologue: string; init_ephemeral: string; init_remote_static: string;
  resp_static: string; resp_ephemeral: string; handshake_hash: string;
  messages: Array<{ payload: string; ciphertext: string }>;
};
const bytes = (h: string) => Uint8Array.from(Buffer.from(h, "hex"));
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");

test("Noise NK matches the cacophony test vector", async () => {
  const v = VECTOR;
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
  assert.equal(hex(responder.received), m[0]!.payload);
  const client = await initiator.finish(responder.reply);
  assert.equal(hex(client.payload), m[1]!.payload);
  assert.equal(hex(client.session.hash), v.handshake_hash);
  assert.equal(client.session.id, responder.session.id);
  const counters = [0n, 0n];
  for (const [i, message] of m.slice(2).entries()) {
    const sender = [client.session, responder.session][i % 2]!;
    const sealed = await aeadEncrypt(sender.sendKey, counters[i % 2]!, bytes(message.payload), new Uint8Array());
    counters[i % 2]! += 1n;
    assert.equal(hex(sealed), message.ciphertext);
  }
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
