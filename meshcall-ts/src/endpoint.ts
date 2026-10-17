import { connect } from "node:net";
import { resolve } from "node:path";
import WebSocket from "ws";

import { authorizationHeader, type RouterCredentials } from "./router-auth.js";

/**
 * Where to connect: a ws:// URL, a Unix socket (not on Windows), or a named
 * pipe `\\.\pipe\NAME` (Windows only, its local transport).
 */
export type WebSocketEndpoint = string | { readonly unixPath: string } | { readonly pipe: string };

const WINDOWS = process.platform === "win32";
const PIPE_PREFIX = "\\\\.\\pipe\\";

/** `NAME` or `\\.\pipe\NAME` → the pipe's full path, which `net` understands. */
export function namedPipePath(name: string): string {
  if (!WINDOWS) throw new Error("Named pipes are Windows-only; use unixPath here");
  const bare = name.startsWith(PIPE_PREFIX) ? name.slice(PIPE_PREFIX.length) : name;
  if (bare === "" || bare.length > 256 || bare.includes("\\")) {
    throw new RangeError("Invalid pipe name " + JSON.stringify(bare) + ": 1-256 characters, no backslash");
  }
  return PIPE_PREFIX + bare;
}

export function unixSocketPath(path: string): string {
  if (WINDOWS) throw new Error("Unix sockets are not supported on Windows; use pipe instead");
  const absolute = resolve(path);
  if (Buffer.byteLength(absolute) > 103) {
    throw new RangeError("Unix socket path exceeds the portable 103-byte limit");
  }
  return absolute;
}

export function createSocket(
  endpoint: WebSocketEndpoint,
  maxPayload: number,
  handshakeTimeout: number,
  auth?: RouterCredentials,
): WebSocket {
  const headers = auth === undefined ? {} : { Authorization: authorizationHeader(auth) };
  if (typeof endpoint === "string") {
    if (endpoint.startsWith("unix://")) {
      const path = unixSocketPath(endpoint.slice("unix://".length));
      return new WebSocket("ws://localhost/", {
        maxPayload, handshakeTimeout, headers, createConnection: () => connect(path),
      });
    }
    if (endpoint.startsWith("pipe://")) {
      const path = namedPipePath(endpoint.slice("pipe://".length));
      return new WebSocket("ws://localhost/", {
        maxPayload, handshakeTimeout, headers, createConnection: () => connect(path),
      });
    }
    return new WebSocket(endpoint, { maxPayload, handshakeTimeout, headers });
  }
  const path = "pipe" in endpoint ? namedPipePath(endpoint.pipe) : unixSocketPath(endpoint.unixPath);
  return new WebSocket("ws://localhost/", {
    maxPayload, handshakeTimeout, headers, createConnection: () => connect(path),
  });
}

export async function closeSocket(socket: WebSocket): Promise<void> {
  if (socket.readyState === WebSocket.CLOSED) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => socket.terminate(), 1000);
    socket.once("close", () => {
      clearTimeout(timer);
      resolve();
    });
    socket.once("error", () => undefined);
    if (socket.readyState === WebSocket.CONNECTING) socket.terminate();
    else socket.close();
  });
}
