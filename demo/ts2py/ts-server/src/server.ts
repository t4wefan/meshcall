import { MeshCallServer } from "meshcall";

import { service } from "./service.js";

const server = new MeshCallServer({ services: [service] });
await server.start();
process.stdout.write(`${JSON.stringify({ port: server.boundPort })}\n`);

process.stdin.resume();
await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
process.stdin.pause();
await server.close();
