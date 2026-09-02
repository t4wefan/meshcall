import { writeContract } from "@meshcall/runtime";

import { service } from "../dist/service.js";

const output = process.argv[2];
if (output === undefined) {
  throw new Error("Expected a contract output path");
}

await writeContract([service], output);
