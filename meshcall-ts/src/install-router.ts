#!/usr/bin/env node
/**
 * Download the MeshCall Router release that matches this package into the
 * per-user cache, verifying it against the release's checksums.txt.
 *
 *   npx meshcall-router-install [--version 0.1.0] [--destination DIR] [--force]
 */
import { installRouter } from "./router.js";

const args = process.argv.slice(2);
const option = (name: string): string | undefined => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : undefined;
};
try {
  const path = await installRouter({
    ...(option("--version") === undefined ? {} : { version: option("--version") as string }),
    ...(option("--destination") === undefined ? {} : { destination: option("--destination") as string }),
    force: args.includes("--force"),
  });
  process.stdout.write(path + "\n");
} catch (error) {
  process.stderr.write(String(error instanceof Error ? error.message : error) + "\n");
  process.exit(1);
}
