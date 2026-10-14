import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import {
  accessSync, constants, existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { delimiter, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

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

const RELEASE_BASE_URL = "https://github.com/t4wefan/meshcall/releases/download";
const EXECUTABLE = process.platform === "win32" ? "meshcall-router.exe" : "meshcall-router";
/** The CLI ships in the same release archive and is installed next to the Router. */
const CLI_EXECUTABLE = process.platform === "win32" ? "meshcall.exe" : "meshcall";
const PACKAGE_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

/** The Router release that matches this package. */
export function routerVersion(): string {
  const override = process.env.MESHCALL_ROUTER_VERSION;
  if (override !== undefined && override !== "") return override.replace(/^v/u, "");
  const manifest = JSON.parse(readFileSync(resolve(PACKAGE_DIR, "package.json"), "utf8")) as { version: string };
  return manifest.version;
}

export function routerCacheDir(version = routerVersion()): string {
  const base = process.env.XDG_CACHE_HOME ?? resolve(homedir(), ".cache");
  return resolve(base, "meshcall", "router", version);
}

/** process.platform / process.arch → the release asset's segments. */
const RELEASE_OS: Record<string, string> = { linux: "linux", darwin: "darwin", win32: "windows", freebsd: "freebsd" };
const RELEASE_ARCH: Record<string, string> = {
  x64: "amd64", arm64: "arm64", ia32: "386", arm: "armv7", riscv64: "riscv64", ppc64: "ppc64le", s390x: "s390x",
};

function platformAsset(version: string): string {
  const os = RELEASE_OS[process.platform];
  const arch = RELEASE_ARCH[process.arch];
  if (os === undefined || arch === undefined) {
    throw new Error("No Router build for " + process.platform + "/" + process.arch);
  }
  return "meshcall-router_" + version + "_" + os + "_" + arch + ".tar.gz";
}

/**
 * Locate the Router binary: explicit path or MESHCALL_ROUTER_BINARY, then the
 * package-local bin, the per-user cache, the enclosing source checkout of this
 * package, then PATH. The working directory is deliberately not searched.
 */
export function findRouterBinary(configured?: string): string {
  const selected = configured ?? process.env.MESHCALL_ROUTER_BINARY;
  if (selected !== undefined) {
    const path = resolve(selected);
    if (!executable(path)) throw new Error("Router binary is not executable: " + path);
    return path;
  }
  return find(EXECUTABLE, "Go Router binary");
}

/** The `meshcall` CLI: next to a configured Router binary, else searched like the Router. */
export function findCliBinary(): string {
  const configured = process.env.MESHCALL_ROUTER_BINARY;
  if (configured !== undefined) {
    const sibling = resolve(dirname(resolve(configured)), CLI_EXECUTABLE);
    if (executable(sibling)) return sibling;
  }
  return find(CLI_EXECUTABLE, "meshcall CLI");
}

/** The npm package that carries this platform's binaries (an optional dependency of `meshcall`). */
export function platformPackageName(): string {
  return "meshcall-router-" + process.platform + "-" + process.arch;
}

function platformPackageBinary(name: string): string | undefined {
  try {
    const manifest = createRequire(import.meta.url).resolve(platformPackageName() + "/package.json");
    return resolve(dirname(manifest), "bin", name);
  } catch {
    return undefined;
  }
}

function find(name: string, what: string): string {
  const candidates = [resolve(PACKAGE_DIR, "bin", name), resolve(routerCacheDir(), name)];
  const bundled = platformPackageBinary(name);
  if (bundled !== undefined) candidates.unshift(bundled);
  let root = PACKAGE_DIR;
  while (true) {
    if (existsSync(resolve(root, "meshcall-router/go.mod"))) {
      candidates.push(resolve(root, "meshcall-router/bin", name));
    }
    const parent = dirname(root);
    if (parent === root) break;
    root = parent;
  }
  candidates.push(...(process.env.PATH ?? "").split(delimiter).filter(Boolean).map((path) => resolve(path, name)));
  const found = candidates.find(executable);
  if (found !== undefined) return found;
  throw new Error(what + " not found. Run `npx meshcall router install`, build meshcall-router from source, or set MESHCALL_ROUTER_BINARY.");
}

export interface InstallRouterOptions {
  readonly version?: string;
  readonly destination?: string;
  readonly force?: boolean;
}

/** Download and verify the Router for this platform; resolves with its path. */
export async function installRouter(options: InstallRouterOptions = {}): Promise<string> {
  const version = (options.version ?? routerVersion()).replace(/^v/u, "");
  const directory = options.destination === undefined ? routerCacheDir(version) : resolve(options.destination);
  const target = resolve(directory, EXECUTABLE);
  if (!options.force && executable(target) && executable(resolve(directory, CLI_EXECUTABLE))) return target;
  const asset = platformAsset(version);
  const base = RELEASE_BASE_URL + "/v" + version;
  const checksums = Buffer.from(await fetchBytes(base + "/checksums.txt")).toString("utf8");
  const expected = checksums.split("\n").map((line) => line.trim().split(/\s+/u))
    .find((parts) => parts[1] === asset)?.[0];
  if (expected === undefined) throw new Error("Release v" + version + " has no checksum for " + asset);
  const archive = Buffer.from(await fetchBytes(base + "/" + asset));
  const actual = createHash("sha256").update(archive).digest("hex");
  if (actual !== expected) throw new Error("Checksum mismatch for " + asset + ": expected " + expected + ", got " + actual);
  // The archive carries the Router and the `meshcall` CLI; both land side by
  // side so the launcher can find the CLI next to the Router.
  const tar = gunzipSync(archive);
  mkdirSync(directory, { recursive: true });
  for (const name of [EXECUTABLE, CLI_EXECUTABLE]) {
    const binary = extractTarMember(tar, name);
    if (binary === undefined) throw new Error(asset + " does not contain " + name);
    const destination = resolve(directory, name);
    const staged = destination + "." + process.pid + ".tmp";
    writeFileSync(staged, binary, { mode: 0o755 });
    renameSync(staged, destination);
  }
  return target;
}

async function fetchBytes(url: string): Promise<ArrayBuffer> {
  const response = await fetch(url, { headers: { "user-agent": "meshcall-installer" }, redirect: "follow" });
  if (!response.ok) throw new Error("Could not download " + url + ": HTTP " + response.status);
  return response.arrayBuffer();
}

/** Minimal ustar reader: enough for GoReleaser archives. */
function extractTarMember(tar: Buffer, filename: string): Buffer | undefined {
  let offset = 0;
  while (offset + 512 <= tar.length) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) break;
    const name = header.subarray(0, 100).toString("utf8").replace(/\0.*$/su, "");
    const size = parseInt(header.subarray(124, 136).toString("utf8").replace(/\0.*$/su, "").trim() || "0", 8);
    const type = String.fromCharCode(header[156] ?? 48);
    offset += 512;
    if ((type === "0" || type === "\0") && name.split("/").pop() === filename) {
      return Buffer.from(tar.subarray(offset, offset + size));
    }
    offset += Math.ceil(size / 512) * 512;
  }
  return undefined;
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
      const child = spawn(findRouterBinary(this.options.binaryPath), args, { stdio: "pipe", windowsHide: true });
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
