import { serve } from "meshcall";

import { TypeScriptGreetingService } from "./service.js";

// serve() starts a bundled Router for the local default address and registers
// this instance with it; the printed URL is what clients connect to.
const server = await serve(new TypeScriptGreetingService(), { accessLog: false });
process.stdout.write(`${JSON.stringify({ url: server.url })}\n`);

process.stdin.resume();
await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
process.stdin.pause();
await server.close();
