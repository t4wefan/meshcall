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
  const upload = await client.upload(7, (async function* () {
    for (const itemValue of values) yield { itemValue, status: "ready" };
  })());
  assert.equal(upload.total, 2023);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await rpc.close();
}
