import { writeContract } from "meshcall";

import { TypeScriptGreetingService } from "../dist/service.js";

const output = process.argv[2];
if (output === undefined) {
  throw new Error("Expected a contract output path");
}

await writeContract([TypeScriptGreetingService], output);
