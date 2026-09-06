import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { accessSync, constants, statSync } from "node:fs";
import { delimiter, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { unixSocketPath } from "./endpoint.js";
import { PROTOCOL_VERSION } from "./protocol.js";

export interface WebSocketRouterOptions {
  readonly host?: string;
  readonly port?: number;
  readonly unixPath?: string;
  readonly maxFrameSize?: number;
  readonly binaryPath?: string;
  readonly authFile?: string;
  readonly startupTimeoutMs?: number;
  readonly shutdownTimeoutMs?: number;
}

function executable(path: string): boolean {
  try {
    accessSync(path, constants.X_OK);
    return statSync(path).isFile();
  } catch { return false; }
}

function routerBinary(configured?: string): string {
  const selected = configured ?? process.env.MESHCALL_ROUTER_BINARY;
  if (selected !== undefined) {
    const path = resolve(selected);
    if (!executable(path)) throw new Error("Router binary is not executable: " + path);
    return path;
  }
  const filename = process.platform === "win32" ? "meshcall-router.exe" : "meshcall-router";
  const directory = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    resolve(directory, "../../bin", filename),
    resolve(directory, "../../../meshcall-router/bin", filename),
    ...(process.env.PATH ?? "").split(delimiter).filter(Boolean).map((path) => resolve(path, filename)),
  ];
  const found = candidates.find(executable);
  if (found !== undefined) return found;
  throw new Error("Go Router binary not found. Build meshcall-router or set MESHCALL_ROUTER_BINARY / binaryPath.");
}

async function beforeTimeout(promise: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise.then(() => true),
      new Promise<false>((resolve) => { timer = setTimeout(() => resolve(false), ms); }),
    ]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}

/** Owns a Go child process. No registration, authentication or routing runs here. */
export class WebSocketRouter {
  private child: ChildProcessWithoutNullStreams | undefined;
  private exited: Promise<number | null> | undefined;
  private lifecycle: Promise<void> = Promise.resolve();
  private running = false;
  private port: number | undefined;
  private diagnostic = "";
  private readonly unixPath: string | undefined;
  private readonly startupTimeout: number;
  private readonly shutdownTimeout: number;

  public constructor(private readonly options: WebSocketRouterOptions = {}) {
    if (options.unixPath !== undefined && (options.host !== undefined || options.port !== undefined)) {
      throw new TypeError("Unix socket cannot be combined with host or port");
    }
    if (options.port !== undefined && (!Number.isSafeInteger(options.port) || options.port < 0 || options.port > 65535)) {
      throw new RangeError("Port must be between 0 and 65535");
    }
    const maxFrameSize = options.maxFrameSize ?? 1024 * 1024;
    if (!Number.isSafeInteger(maxFrameSize) || maxFrameSize < 1 || maxFrameSize > 64 * 1024 * 1024) {
      throw new RangeError("maxFrameSize must be between 1 and 67108864");
    }
    this.startupTimeout = options.startupTimeoutMs ?? 10000;
    this.shutdownTimeout = options.shutdownTimeoutMs ?? 5000;
    for (const timeout of [this.startupTimeout, this.shutdownTimeout]) {
      if (!Number.isSafeInteger(timeout) || timeout <= 0) throw new RangeError("Process timeouts must be positive integers");
    }
    this.unixPath = options.unixPath === undefined ? undefined : unixSocketPath(options.unixPath);
  }

  public get isRunning(): boolean {
    return this.running && this.child !== undefined && this.child.exitCode === null && this.child.signalCode === null;
  }
  public get pid(): number | undefined { return this.isRunning ? this.child?.pid : undefined; }
  public get boundPort(): number {
    if (this.unixPath !== undefined) throw new Error("Unix socket listener does not have a TCP port");
    if (!this.isRunning || this.port === undefined) throw new Error("Go Router is not running");
    return this.port;
  }

  public start(): Promise<void> {
    return this.serialize(async () => {
      if (this.isRunning) return;
      await this.stopProcess();
      this.diagnostic = "";
      const args = ["--shutdown-on-stdin-close", "--max-frame-size", String(this.options.maxFrameSize ?? 1024 * 1024)];
      if (this.unixPath === undefined) args.push("--host", this.options.host ?? "127.0.0.1", "--port", String(this.options.port ?? 0));
      else args.push("--unix-socket", this.unixPath);
      if (this.options.authFile !== undefined) args.push("--auth-file", resolve(this.options.authFile));
      const child = spawn(routerBinary(this.options.binaryPath), args, { stdio: "pipe", windowsHide: true });
      this.child = child;
      child.stdin.on("error", () => undefined);
      this.exited = new Promise((resolve) => {
        child.once("close", (code) => {
          if (this.child === child) this.running = false;
          resolve(code);
        });
      });
      child.stderr.on("data", (chunk: Buffer) => {
        this.diagnostic = (this.diagnostic + chunk.toString("utf8")).slice(-8192);
      });
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await new Promise<void>((resolve, reject) => {
          let buffer = Buffer.alloc(0);
          let received = false;
          timer = setTimeout(() => reject(new Error("Go Router readiness timed out")), this.startupTimeout);
          child.on("error", reject);
          child.once("close", () => reject(new Error("Go Router exited before readiness")));
          child.stdout.on("data", (chunk: Buffer) => {
            if (received) return; // Continue draining stdout without retaining it.
            buffer = Buffer.concat([buffer, chunk]);
            if (buffer.length > 16384) { reject(new Error("Invalid Go Router readiness message")); return; }
            const newline = buffer.indexOf(10);
            if (newline < 0) return;
            received = true;
            try {
              const ready = JSON.parse(buffer.subarray(0, newline).toString("utf8")) as Record<string, unknown>;
              if (ready.kind !== "router.ready" || ready.protocol !== PROTOCOL_VERSION || ready.pid !== child.pid) {
                throw new Error("Invalid Go Router readiness message");
              }
              if (this.unixPath === undefined) {
                if (typeof ready.port !== "number" || !Number.isSafeInteger(ready.port) || ready.port < 1 || ready.port > 65535) {
                  throw new Error("Invalid Go Router bound port");
                }
                this.port = ready.port;
              } else if (ready.unix_path !== this.unixPath) throw new Error("Invalid Go Router Unix socket path");
              resolve();
            } catch (error) { reject(error); }
          });
        });
        if (child.exitCode !== null || child.signalCode !== null) throw new Error("Go Router exited during startup");
        this.running = true;
      } catch (error) {
        await this.stopProcess();
        throw new Error("Could not start Go Router: " + String(error) +
          (this.diagnostic ? "\n" + this.diagnostic.trim() : ""), { cause: error });
      } finally { if (timer !== undefined) clearTimeout(timer); }
    });
  }

  public stop(): Promise<void> { return this.serialize(() => this.stopProcess()); }
  public close(): Promise<void> { return this.stop(); }
  public waitClosed(): Promise<number | null> {
    if (this.exited === undefined) return Promise.reject(new Error("Go Router is not running"));
    return this.exited;
  }

  private serialize(operation: () => Promise<void>): Promise<void> {
    const result = this.lifecycle.then(operation);
    this.lifecycle = result.catch(() => undefined);
    return result;
  }

  private async stopProcess(): Promise<void> {
    const child = this.child;
    const exited = this.exited;
    this.running = false;
    this.port = undefined;
    if (child !== undefined && exited !== undefined) {
      child.stdin.end();
      if (!await beforeTimeout(exited, this.shutdownTimeout)) {
        child.kill("SIGTERM");
        if (!await beforeTimeout(exited, this.shutdownTimeout)) {
          child.kill("SIGKILL");
          await exited;
        }
      }
    }
    this.child = undefined;
    this.exited = undefined;
  }
}
