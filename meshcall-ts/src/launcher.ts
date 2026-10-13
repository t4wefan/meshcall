#!/usr/bin/env node
/**
 * The `meshcall` bin: run the Go CLI from a Node project.
 *
 * `npx meshcall …` must behave exactly like the `meshcall` binary that ships
 * with the Router release, so this script only locates (or, on first use,
 * downloads) that binary and runs it with the same arguments and exit code.
 * Only the bootstrap commands `router install` and `router path` are answered
 * here, because they may run before any binary exists.
 */
import { spawnSync } from "node:child_process";

import { findCliBinary, findRouterBinary, installRouter } from "./router.js";

const args = process.argv.slice(2);
try {
  if (args[0] === "router" && args[1] === "install") {
    const option = (name: string): string | undefined => {
      const index = args.indexOf(name);
      return index >= 0 ? args[index + 1] : undefined;
    };
    process.stdout.write(await installRouter({
      ...(option("--version") === undefined ? {} : { version: option("--version") as string }),
      ...(option("--destination") === undefined ? {} : { destination: option("--destination") as string }),
      force: args.includes("--force"),
    }) + "\n");
  } else if (args[0] === "router" && args[1] === "path") {
    process.stdout.write(findRouterBinary() + "\n");
  } else {
    let binary: string;
    try {
      binary = findCliBinary();
    } catch {
      await installRouter(); // first use in a fresh environment
      binary = findCliBinary();
    }
    const result = spawnSync(binary, args, { stdio: "inherit" });
    process.exit(result.status ?? 1);
  }
} catch (error) {
  process.stderr.write(String(error instanceof Error ? error.message : error) + "\n");
  process.exit(1);
}
