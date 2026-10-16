import { MeshCallServer } from "meshcall";

import { TypeScriptGreetingService } from "./service.js";

const server = new MeshCallServer({ services: [new TypeScriptGreetingService()] });
await server.start();
process.stdout.write(`${JSON.stringify({ port: server.boundPort })}\n`);

process.stdin.resume();
await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
process.stdin.pause();
await server.close();
