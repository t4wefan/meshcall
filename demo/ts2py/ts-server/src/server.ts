import { LocalRouter, Mesh } from "meshcall";

import { TypeScriptGreetingService } from "./service.js";

// Start a local Router subprocess on an ephemeral port and serve the service.
const router = await LocalRouter.start({ port: 0 });
const mesh = new Mesh(router).mount(new TypeScriptGreetingService());
const server = await mesh.startServing({ accessLog: false });
process.stdout.write(`${JSON.stringify({ url: router.url })}\n`);

process.stdin.resume();
await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
process.stdin.pause();
await server.close();
await router.stop();
