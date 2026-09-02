import { MeshCallClient } from "@meshcall/runtime";

import { PythonGreetingServiceClient } from "./dist/index.js";

const url = process.argv[2];
if (url === undefined) {
  throw new Error("Expected the Python service WebSocket URL");
}

const rpc = new MeshCallClient(url);
const client = new PythonGreetingServiceClient(rpc);
try {
  const result = await client.greet("TypeScript", 2);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await rpc.close();
}
