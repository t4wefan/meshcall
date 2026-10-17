import { Router } from "meshcall";

import { PythonGreetingServiceClient } from "./dist/index.js";

const url = process.argv[2];
if (url === undefined) {
  throw new Error("Expected the Router URL");
}

const router = await Router.connect(url);
const client = await PythonGreetingServiceClient.connect(router);
try {
  const result = await client.greet("TypeScript", 2);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} finally {
  await router.close();
}
