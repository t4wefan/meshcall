import { Mesh, Router } from "meshcall";

import { PythonGreetingServiceClient } from "./dist/index.js";

const url = process.argv[2];
if (url === undefined) {
  throw new Error("Expected the Router URL");
}

const mesh = await Mesh.connect(new Router(url));
const client = new PythonGreetingServiceClient(mesh);
try {
  const result = await client.greet("TypeScript", 2);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await mesh.close();
}
