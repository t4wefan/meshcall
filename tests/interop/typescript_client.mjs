import { pathToFileURL } from "node:url";

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
  const result = await client.greet({ name: "TypeScript", repeat: 2 });
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await rpc.close();
}
