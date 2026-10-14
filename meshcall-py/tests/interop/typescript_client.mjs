import { pathToFileURL } from "node:url";
import assert from "node:assert/strict";

const [url, generatedIndex, runtimeIndex] = process.argv.slice(2);
if (url === undefined || generatedIndex === undefined || runtimeIndex === undefined) {
  throw new Error("Expected URL, generated client index, and runtime index");
}

const { MeshCallClient } = await import(pathToFileURL(runtimeIndex).href);
const { PythonInteropServiceClient } = await import(
  pathToFileURL(generatedIndex).href
);

const rpc = new MeshCallClient(url);
const client = new PythonInteropServiceClient(rpc);
try {
  const result = await client.greet("TypeScript", 2);
  const values = [];
  for await (const item of client.download(64)) {
    assert.equal(item.status, "ready");
    values.push(item.itemValue);
  }
  assert.deepEqual(values, Array.from({ length: 64 }, (_, i) => i));
  const upload = await client.upload((async function* () {
    for (const itemValue of values) yield { itemValue, status: "ready" };
  })(), 7);
  assert.equal(upload.total, 2023);
  const exchange = client.exchange(100);
  const echoed = [];
  for (const itemValue of [1, 2, 3]) {
    await exchange.send({ itemValue, status: "busy" });
    const next = await exchange.next();
    assert.equal(next.value.status, "busy");
    echoed.push(next.value.itemValue);
  }
  await exchange.closeSend();
  assert.equal((await exchange.next()).done, true);
  assert.deepEqual(echoed, [101, 102, 103]);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await rpc.close();
}
