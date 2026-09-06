import { connect } from "node:net";
import { resolve } from "node:path";
import WebSocket from "ws";

import { authorizationHeader, type RouterCredentials } from "./router-auth.js";

export type WebSocketEndpoint = string | { readonly unixPath: string };

export function unixSocketPath(path: string): string {
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
    return new WebSocket(endpoint, { maxPayload, handshakeTimeout, headers });
  }
  const path = unixSocketPath(endpoint.unixPath);
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
