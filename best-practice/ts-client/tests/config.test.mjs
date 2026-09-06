import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { readCredentials, routerUrl } from "../dist/config.js";

test("credential files accept account or token and reject malformed input without leaking it", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "meshcall-cli-config-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = join(directory, "credentials.json");
  for (const credentials of [{ username: "client", password: "example" }, { token: "example" }]) {
    await writeFile(file, JSON.stringify(credentials));
    assert.deepEqual(await readCredentials(file), credentials);
  }
  for (const content of [
    '{"password":"do-not-disclose"}',
    '{"token":"do-not-disclose","username":"client","password":"x"}',
    '{"token":"do-not-disclose","unknown":true}',
    'not-json-do-not-disclose',
  ]) {
    await writeFile(file, content);
    await assert.rejects(readCredentials(file), (error) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /Cannot load credentials/u);
      assert.ok(!error.message.includes("do-not-disclose"));
      return true;
    });
  }
  await assert.rejects(readCredentials(join(directory, "missing")), /Cannot load credentials/u);
});

test("Router URLs require TLS on a network and keep secrets out of URLs", (t) => {
  const previous = process.env.MESHCALL_ROUTER_URL;
  t.after(() => {
    if (previous === undefined) delete process.env.MESHCALL_ROUTER_URL;
    else process.env.MESHCALL_ROUTER_URL = previous;
  });
  for (const value of ["ws://127.0.0.1:8765", "ws://[::1]:8765", "wss://router.example/rpc"]) {
    process.env.MESHCALL_ROUTER_URL = value;
    assert.equal(routerUrl(), value);
  }
  for (const value of [
    "ws://router.example:8765", "ws://0.0.0.0:8765",
    "wss://client:do-not-disclose@router.example", "wss://router.example/?token=do-not-disclose",
    "wss://router.example/#do-not-disclose", "https://router.example", "ws://127.0.0.1:0", "ws://[broken",
  ]) {
    process.env.MESHCALL_ROUTER_URL = value;
    assert.throws(routerUrl, (error) => {
      assert.ok(!error.message.includes("do-not-disclose"));
      return true;
    });
  }
});
