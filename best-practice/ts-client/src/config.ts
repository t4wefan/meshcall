import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import type { RouterCredentials } from "@meshcall/runtime";

export function routerUrl(): string {
  const value = process.env.MESHCALL_ROUTER_URL ?? "ws://127.0.0.1:8765";
  try {
    const url = new URL(value);
    if (!["ws:", "wss:"].includes(url.protocol) || !url.hostname || url.port === "0"
      || url.username || url.password || url.search || url.hash
      || (url.protocol === "ws:" && !["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))) {
      throw new Error();
    }
  } catch {
    throw new Error("MESHCALL_ROUTER_URL must be wss:// for network hosts or ws:// for loopback, without URL credentials, query, or fragment");
  }
  return value;
}

export async function readCredentials(
  file: string = process.env.MESHCALL_CLIENT_CREDENTIALS
    ?? fileURLToPath(new URL("../../.local/client.json", import.meta.url)),
): Promise<RouterCredentials> {
  try {
    const value: unknown = JSON.parse(await readFile(file, "utf8"));
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error();
    const data = value as Record<string, unknown>;
    const keys = Object.keys(data);
    if (keys.length === 1 && typeof data.token === "string" && data.token && !/[\r\n]/u.test(data.token)) {
      return { token: data.token };
    }
    if (keys.length === 2 && typeof data.username === "string" && data.username
      && !/[:\r\n]/u.test(data.username) && typeof data.password === "string" && data.password) {
      return { username: data.username, password: data.password };
    }
    throw new Error();
  } catch {
    // Never expose malformed JSON or validation input containing secrets.
    throw new Error(`Cannot load credentials from ${file}; run init-router or provide a username/password or token JSON file`);
  }
}
