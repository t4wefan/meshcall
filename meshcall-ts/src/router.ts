import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import {
  accessSync, constants, existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { createConnection } from "node:net";
import { homedir } from "node:os";
import { delimiter, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

import {
  MeshCallClient,
  type CallOptions,
  type ClientInterceptor,
  type DuplexCallOptions,
  type MeshCallClientOptions,
  type MeshCallDuplexCall,
  type MeshCallSubscription,
  type SubscriptionOptions,
} from "./client.js";
import { isService } from "./contract.js";
import { namedPipePath, unixSocketPath, type WebSocketEndpoint } from "./endpoint.js";
import type { ReconnectPolicy, RetryPolicy } from "./liveness.js";
import type { LogLevel, RpcLogger } from "./logging.js";
import { PROTOCOL_VERSION } from "./protocol.js";
import {
  REGISTRY_SERVICE,
  ROUTER_AUTH_SERVICE,
  normalizeScopeService,
  type IssuedToken,
  type RegistryService,
  type RouterCredentials,
  type RouterIdentity,
  type RouterScope,
} from "./router-auth.js";
import { mergeCallOptions, type MeshCallRpc, type RpcStream } from "./rpc.js";
import { MeshCallServer, type MeshCallServerOptions, type ServerInterceptor } from "./server.js";
import { DEFAULT_MAX_FRAME_SIZE } from "./transport.js";

export const DEFAULT_ROUTER_URL = "ws://127.0.0.1:8765";

export interface WebSocketRouterOptions {
  readonly host?: string;
  readonly port?: number;
  /** Unix socket to listen on (not on Windows: use `pipe`). */
  readonly unixPath?: string;
  /** Named pipe `\\.\pipe\NAME` to listen on (Windows only). */
  readonly pipe?: string;
  readonly maxFrameSize?: number;
  /** Bytes queued for a peer that is not reading (at least `maxFrameSize`); omitted = Router default. */
  readonly outboxSize?: number;
  readonly binaryPath?: string;
  readonly authFile?: string;
  /** Open WebSocket connections the Router accepts; omitted = Router default. */
  readonly maxConnections?: number;
  readonly metricsListen?: string;
  readonly httpCorsOrigins?: ReadonlyArray<string>;
  readonly httpDefaultTimeoutMs?: number;
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
  /** The bare pipe name, as the Router reports it. */
  private readonly pipe: string | undefined;
  private readonly startupTimeout: number;
  private readonly shutdownTimeout: number;

  public constructor(private readonly options: WebSocketRouterOptions = {}) {
    if ([options.unixPath, options.pipe, options.host ?? options.port].filter((v) => v !== undefined).length > 1) {
      throw new TypeError("Choose one of host/port, unixPath (not on Windows) or pipe (Windows)");
    }
    if (options.port !== undefined && (!Number.isSafeInteger(options.port) || options.port < 0 || options.port > 65535)) {
      throw new RangeError("Port must be between 0 and 65535");
    }
    const maxFrameSize = options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
    if (!Number.isSafeInteger(maxFrameSize) || maxFrameSize < 1 || maxFrameSize > DEFAULT_MAX_FRAME_SIZE) {
      throw new RangeError("maxFrameSize must be between 1 and 67108864");
    }
    if (options.outboxSize !== undefined && (!Number.isSafeInteger(options.outboxSize) || options.outboxSize < maxFrameSize)) {
      throw new RangeError("outboxSize must be at least maxFrameSize");
    }
    this.startupTimeout = options.startupTimeoutMs ?? 10000;
    this.shutdownTimeout = options.shutdownTimeoutMs ?? 5000;
    for (const timeout of [this.startupTimeout, this.shutdownTimeout]) {
      if (!Number.isSafeInteger(timeout) || timeout <= 0) throw new RangeError("Process timeouts must be positive integers");
    }
    if (options.maxConnections !== undefined && (!Number.isSafeInteger(options.maxConnections) || options.maxConnections < 1)) {
      throw new RangeError("maxConnections must be a positive integer");
    }
    this.unixPath = options.unixPath === undefined ? undefined : unixSocketPath(options.unixPath);
    this.pipe = options.pipe === undefined ? undefined : namedPipePath(options.pipe).slice("\\\\.\\pipe\\".length);
  }

  public get isRunning(): boolean {
    return this.running && this.child !== undefined && this.child.exitCode === null && this.child.signalCode === null;
  }
  public get pid(): number | undefined { return this.isRunning ? this.child?.pid : undefined; }
  public get boundPort(): number {
    if (this.unixPath !== undefined || this.pipe !== undefined) throw new Error("A local listener does not have a TCP port");
    if (!this.isRunning || this.port === undefined) throw new Error("Go Router is not running");
    return this.port;
  }

  public start(): Promise<void> {
    return this.serialize(async () => {
      if (this.isRunning) return;
      await this.stopProcess();
      this.diagnostic = "";
      const args = ["--shutdown-on-stdin-close", "--max-frame-size", String(this.options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE)];
      if (this.options.outboxSize !== undefined) args.push("--outbox-size", String(this.options.outboxSize));
      if (this.pipe !== undefined) args.push("--pipe", this.pipe);
      else if (this.unixPath !== undefined) args.push("--unix-socket", this.unixPath);
      else args.push("--host", this.options.host ?? "127.0.0.1", "--port", String(this.options.port ?? 0));
      if (this.options.authFile !== undefined) args.push("--auth-file", resolve(this.options.authFile));
      if (this.options.maxConnections !== undefined) args.push("--max-connections", String(this.options.maxConnections));
      if (this.options.metricsListen !== undefined) args.push("--metrics-listen", this.options.metricsListen);
      if (this.options.httpCorsOrigins !== undefined && this.options.httpCorsOrigins.length > 0) {
        args.push("--http-cors-origin", this.options.httpCorsOrigins.join(","));
      }
      if (this.options.httpDefaultTimeoutMs !== undefined) {
        args.push("--http-default-timeout", `${Math.max(1, Math.round(this.options.httpDefaultTimeoutMs))}ms`);
      }
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
              if (this.pipe !== undefined) {
                if (ready.pipe !== this.pipe) throw new Error("Invalid Go Router pipe name");
              } else if (this.unixPath !== undefined) {
                if (ready.unix_path !== this.unixPath) throw new Error("Invalid Go Router Unix socket path");
              } else {
                if (typeof ready.port !== "number" || !Number.isSafeInteger(ready.port) || ready.port < 1 || ready.port > 65535) {
                  throw new Error("Invalid Go Router bound port");
                }
                this.port = ready.port;
              }
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

export interface RouterOptions {
  readonly url?: WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint>;
  readonly unixPath?: string;
  readonly pipe?: string;
  readonly auth?: RouterCredentials;
  readonly peerId?: string;
  readonly pingIntervalMs?: number | null;
  readonly pingTimeoutMs?: number;
  readonly reconnect?: ReconnectPolicy;
  readonly retry?: RetryPolicy | false;
  readonly deadlineGraceMs?: number;
  readonly handshakeTimeoutMs?: number;
  readonly maxFrameSize?: number;
  readonly interceptors?: ReadonlyArray<ClientInterceptor>;
  readonly logger?: RpcLogger;
  readonly spawnIfLocal?: boolean;
  readonly metricsListen?: string;
  readonly httpCorsOrigins?: ReadonlyArray<string>;
  readonly httpDefaultTimeoutMs?: number;
  readonly authFile?: string;
  readonly outboxSize?: number;
  readonly maxConnections?: number;
  readonly binaryPath?: string;
  readonly startupTimeoutMs?: number;
  readonly shutdownTimeoutMs?: number;
}

function defaultRouterEndpoint(): string {
  const envUrl = (process.env.MESHCALL_ROUTER_URL ?? "").trim();
  return envUrl !== "" ? envUrl : DEFAULT_ROUTER_URL;
}

function resolveRouterEndpoints(options: RouterOptions): ReadonlyArray<WebSocketEndpoint> {
  const count = [options.url, options.unixPath, options.pipe].filter((v) => v !== undefined).length;
  if (count > 1) {
    throw new TypeError("Choose one of url, unixPath (not on Windows) or pipe (Windows)");
  }
  if (options.unixPath !== undefined) return [{ unixPath: options.unixPath }];
  if (options.pipe !== undefined) return [{ pipe: options.pipe }];
  if (options.url !== undefined) {
    const list = Array.isArray(options.url) ? [...(options.url as ReadonlyArray<WebSocketEndpoint>)] : [options.url as WebSocketEndpoint];
    if (list.length === 0) throw new RangeError("At least one Router endpoint is required");
    return list;
  }
  return [defaultRouterEndpoint()];
}

function normalizeRouterInput(
  input: Router | RouterOptions | WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint> | undefined,
): Router {
  if (input instanceof Router) return input;
  if (input === undefined) return new Router();
  return new Router(input);
}

function parseLocalTcpEndpoint(endpoint: WebSocketEndpoint): { host: string; port: number } | undefined {
  if (typeof endpoint !== "string") return undefined;
  try {
    const parsed = new URL(endpoint);
    if (parsed.protocol !== "ws:") return undefined;
    const host = parsed.hostname;
    if (host !== "127.0.0.1" && host !== "localhost" && host !== "::1") return undefined;
    const port = parsed.port === "" ? 80 : Number(parsed.port);
    if (!Number.isSafeInteger(port) || port < 0 || port > 65535) return undefined;
    return { host, port };
  } catch {
    return undefined;
  }
}

function canConnectTcp(host: string, port: number, timeoutMs = 250): Promise<boolean> {
  if (port === 0) return Promise.resolve(false);
  return new Promise((resolve) => {
    const socket = createConnection({ host, port });
    let settled = false;
    const finish = (ok: boolean) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(ok);
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish(true));
    socket.once("timeout", () => finish(false));
    socket.once("error", () => finish(false));
  });
}

async function ensureLocalRouter(router: Router): Promise<WebSocketRouter | undefined> {
  if (router.options.spawnIfLocal === false) return undefined;
  if (router.endpoints.length !== 1) return undefined;
  const endpoint = router.endpoints[0]!;
  const localTcp = parseLocalTcpEndpoint(endpoint);
  if (localTcp === undefined) return undefined;
  if (await canConnectTcp(localTcp.host, localTcp.port)) return undefined;

  const proc = new WebSocketRouter({
    host: localTcp.host,
    port: localTcp.port,
    ...(router.options.maxFrameSize === undefined ? {} : { maxFrameSize: router.options.maxFrameSize }),
    ...(router.options.outboxSize === undefined ? {} : { outboxSize: router.options.outboxSize }),
    ...(router.options.binaryPath === undefined ? {} : { binaryPath: router.options.binaryPath }),
    ...(router.options.authFile === undefined ? {} : { authFile: router.options.authFile }),
    ...(router.options.maxConnections === undefined ? {} : { maxConnections: router.options.maxConnections }),
    ...(router.options.metricsListen === undefined ? {} : { metricsListen: router.options.metricsListen }),
    ...(router.options.httpCorsOrigins === undefined ? {} : { httpCorsOrigins: router.options.httpCorsOrigins }),
    ...(router.options.httpDefaultTimeoutMs === undefined ? {} : { httpDefaultTimeoutMs: router.options.httpDefaultTimeoutMs }),
    ...(router.options.startupTimeoutMs === undefined ? {} : { startupTimeoutMs: router.options.startupTimeoutMs }),
    ...(router.options.shutdownTimeoutMs === undefined ? {} : { shutdownTimeoutMs: router.options.shutdownTimeoutMs }),
  });
  try {
    await proc.start();
    return proc;
  } catch (error) {
    if (localTcp.port !== 0 && await canConnectTcp(localTcp.host, localTcp.port)) {
      return undefined;
    }
    throw error;
  }
}

function formatEndpointUrl(endpoint: WebSocketEndpoint): string {
  if (typeof endpoint === "string") return endpoint;
  if ("unixPath" in endpoint) return "unix://" + unixSocketPath(endpoint.unixPath);
  return "pipe://" + namedPipePath(endpoint.pipe).slice("\\\\.\\pipe\\".length);
}

/**
 * Unified Router configuration and multiplexed client connection.
 *
 * 1. Pass `new Router({ ... })` to `serve` / `run` to configure how services register with the Router.
 * 2. Connect with `await Router.connect({ ... })` to share one underlying connection across multiple
 *    generated service clients (`new WorkspaceClient(router)`, `new BillingClient(router)`).
 * 3. Call built-in Router services directly (`listServices`, `describeService`, `whoami`, `issueToken`, `revokeToken`).
 */
export class Router implements MeshCallRpc {
  public readonly options: RouterOptions;
  private _endpoints: ReadonlyArray<WebSocketEndpoint>;
  private root: Router;
  private ownsRoot = true;
  private defaultCallOptions: CallOptions = {};
  private rpcClient: MeshCallClient | undefined;

  public constructor(
    options: RouterOptions | WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint> = {},
  ) {
    if (typeof options === "string" || Array.isArray(options)) {
      this.options = { url: options as WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint> };
    } else {
      this.options = options as RouterOptions;
    }
    this._endpoints = resolveRouterEndpoints(this.options);
    this.root = this;
  }

  public static async connect(
    options: RouterOptions | WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint> = {},
  ): Promise<Router> {
    const router = new Router(options);
    await router.open();
    return router;
  }

  public get endpoints(): ReadonlyArray<WebSocketEndpoint> {
    return this.root._endpoints;
  }

  /** @internal */
  public _setBoundEndpoint(endpoint: WebSocketEndpoint): void {
    this.root._endpoints = [endpoint];
  }

  public get url(): string {
    return formatEndpointUrl(this.endpoints[0]!);
  }

  public get client(): MeshCallClient {
    const active = this.root.rpcClient;
    if (active === undefined) {
      throw new Error("Router is not connected; use `await Router.connect(...)` or `await router.open()`");
    }
    return Object.keys(this.defaultCallOptions).length > 0
      ? active.withOptions(this.defaultCallOptions)
      : active;
  }

  public withOptions(options: CallOptions): Router {
    const clone = Object.create(Router.prototype) as Router;
    Object.assign(clone, {
      options: this.root.options,
      _endpoints: this.root._endpoints,
      root: this.root,
      ownsRoot: false,
      defaultCallOptions: mergeCallOptions(this.defaultCallOptions, options),
    });
    return clone;
  }

  public async open(): Promise<this> {
    if (this.root !== this) {
      await this.root.open();
      return this;
    }
    if (this.rpcClient === undefined) {
      const clientOptions: MeshCallClientOptions = {
        ...(this.options.auth === undefined ? {} : { auth: this.options.auth }),
        ...(this.options.maxFrameSize === undefined ? {} : { maxFrameSize: this.options.maxFrameSize }),
        ...(this.options.handshakeTimeoutMs === undefined ? {} : { handshakeTimeoutMs: this.options.handshakeTimeoutMs }),
        ...(this.options.deadlineGraceMs === undefined ? {} : { deadlineGraceMs: this.options.deadlineGraceMs }),
        ...(this.options.pingIntervalMs === undefined ? {} : { pingIntervalMs: this.options.pingIntervalMs }),
        ...(this.options.pingTimeoutMs === undefined ? {} : { pingTimeoutMs: this.options.pingTimeoutMs }),
        ...(this.options.reconnect === undefined ? {} : { reconnect: this.options.reconnect }),
        ...(this.options.interceptors === undefined ? {} : { interceptors: this.options.interceptors }),
        ...(this.options.retry === undefined ? {} : { retry: this.options.retry }),
        ...(this.options.logger === undefined ? {} : { logger: this.options.logger }),
      };
      this.rpcClient = new MeshCallClient(this.endpoints, this.options.peerId, clientOptions);
    }
    await this.rpcClient.connect();
    return this;
  }

  public async close(): Promise<void> {
    if (!this.ownsRoot) return;
    if (this.rpcClient !== undefined) {
      const c = this.rpcClient;
      this.rpcClient = undefined;
      await c.close();
    }
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  // ---------------------------------------------------------------------------
  // Built-in Router services: Registry & AuthService
  // ---------------------------------------------------------------------------

  public async listServices(options?: CallOptions): Promise<RegistryService[]> {
    const res = await this.unary<Record<string, never>, { services: RegistryService[] }>(
      REGISTRY_SERVICE, "list", {}, { idempotent: true, ...options },
    );
    return res.services;
  }

  public async describeService(service: string, options?: CallOptions): Promise<RegistryService> {
    return this.unary<{ service: string }, RegistryService>(
      REGISTRY_SERVICE, "describe", { service: normalizeScopeService(service) }, { idempotent: true, ...options },
    );
  }

  public async whoami(options?: CallOptions): Promise<RouterIdentity> {
    return this.unary<Record<string, never>, RouterIdentity>(
      ROUTER_AUTH_SERVICE, "whoami", {}, { idempotent: true, ...options },
    );
  }

  public async issueToken(
    scopes: ReadonlyArray<RouterScope>,
    ttlSeconds = 900,
    options?: CallOptions,
  ): Promise<IssuedToken> {
    if (scopes.length === 0) throw new RangeError("issueToken needs at least one scope");
    if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds <= 0) throw new RangeError("ttlSeconds must be a positive integer");
    const normalized = scopes.map((s) => ({
      ...s,
      service: normalizeScopeService(s.service),
    }));
    return this.unary<Record<string, unknown>, IssuedToken>(
      ROUTER_AUTH_SERVICE, "issue_token", { scopes: normalized, ttl_seconds: ttlSeconds }, options,
    );
  }

  public async revokeToken(tokenId: string, options?: CallOptions): Promise<boolean> {
    if (tokenId === "") throw new RangeError("tokenId must not be empty");
    const res = await this.unary<{ token_id: string }, { revoked: boolean }>(
      ROUTER_AUTH_SERVICE, "revoke_token", { token_id: tokenId }, { idempotent: true, ...options },
    );
    return res.revoked;
  }

  // ---------------------------------------------------------------------------
  // MeshCallRpc implementation (for passing `router` directly to generated clients)
  // ---------------------------------------------------------------------------

  public unary<Request, Response>(
    service: string, method: string, request: Request, options?: CallOptions,
  ): Promise<Response> {
    return this.client.unary(service, method, request, options);
  }

  public serverStream<Request, Item>(
    service: string, method: string, request: Request, options?: CallOptions,
  ): RpcStream<Item> {
    return this.client.serverStream(service, method, request, options);
  }

  public clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options?: CallOptions,
  ): Promise<Response> {
    return this.client.clientStream(service, method, request, items, options);
  }

  public subscribe<Request extends object, Item>(
    service: string, method: string, request: Request, options?: SubscriptionOptions,
  ): MeshCallSubscription<Item> {
    return this.client.subscribe(service, method, request, options);
  }

  public duplex<Request, InputItem, OutputItem>(
    service: string, method: string, request: Request, options?: DuplexCallOptions,
  ): MeshCallDuplexCall<InputItem, OutputItem> {
    return this.client.duplex(service, method, request, options);
  }
}

export interface ServeOptions {
  readonly router?: Router | RouterOptions | WebSocketEndpoint | ReadonlyArray<WebSocketEndpoint> | null;
  readonly listen?: WebSocketEndpoint;
  readonly instanceId?: string;
  readonly maxInflight?: number;
  readonly drainTimeoutMs?: number;
  readonly strict?: boolean;
  readonly accessLog?: boolean;
  readonly logLevel?: LogLevel;
  readonly colorize?: boolean;
  readonly logger?: RpcLogger;
  readonly interceptors?: ReadonlyArray<ServerInterceptor>;
  readonly handshakeTimeoutMs?: number;
  readonly idleTimeoutMs?: number | null;
}

export class RunningServer {
  public constructor(
    public readonly server: MeshCallServer,
    public readonly routerUrl: string,
    public readonly endpoint: WebSocketEndpoint,
    public readonly instanceId: string,
    private readonly drainTimeoutMs: number | undefined,
    private readonly spawnedRouter: WebSocketRouter | undefined,
  ) {}

  public async close(options: { readonly drain?: boolean; readonly drainTimeoutMs?: number } = {}): Promise<void> {
    try {
      await this.server.close({
        drain: options.drain ?? true,
        ...(options.drainTimeoutMs !== undefined
          ? { drainTimeoutMs: options.drainTimeoutMs }
          : this.drainTimeoutMs !== undefined
            ? { drainTimeoutMs: this.drainTimeoutMs }
            : {}),
      });
    } finally {
      if (this.spawnedRouter !== undefined) {
        await this.spawnedRouter.stop();
      }
    }
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

function splitServicesAndOptions(
  args: ReadonlyArray<object | ServeOptions>,
): { services: ReadonlyArray<object>; options: ServeOptions } {
  if (args.length === 0) {
    throw new RangeError("At least one @service instance is required");
  }
  const last = args[args.length - 1]!;
  if (
    args.length > 1 &&
    typeof last === "object" &&
    last !== null &&
    !isService(last) &&
    Object.getPrototypeOf(last) === Object.prototype
  ) {
    return {
      services: args.slice(0, -1),
      options: last as ServeOptions,
    };
  }
  return { services: args as ReadonlyArray<object>, options: {} };
}

function parseDirectListen(
  listen: WebSocketEndpoint,
): { host?: string; port?: number; unixPath?: string; pipe?: string } {
  if (typeof listen === "object") {
    if ("unixPath" in listen) return { unixPath: listen.unixPath };
    return { pipe: listen.pipe };
  }
  if (listen.startsWith("unix://")) return { unixPath: listen };
  if (listen.startsWith("pipe://")) return { pipe: listen };
  const target = listen.includes("://") ? listen : `ws://${listen}`;
  const parsed = new URL(target);
  if (parsed.protocol !== "ws:") {
    throw new TypeError(`Direct listen URL must use ws://, unix:// or pipe://, got ${JSON.stringify(listen)}`);
  }
  const host = parsed.hostname || "127.0.0.1";
  const port = parsed.port === "" ? 0 : Number(parsed.port);
  return { host, port };
}

/**
 * Start a MeshCall server in the background and return a `RunningServer` handle
 * (`await using srv = await serve(new Workspace(), { router })`).
 */
export async function serve(...args: [...services: object[], options: ServeOptions]): Promise<RunningServer>;
export async function serve(...services: object[]): Promise<RunningServer>;
export async function serve(...args: Array<object | ServeOptions>): Promise<RunningServer> {
  const { services, options } = splitServicesAndOptions(args);
  if (options.listen !== undefined && options.router !== undefined && options.router !== null) {
    throw new TypeError("Specify either `router` (Router mode) or `listen` (Direct mode), not both");
  }

  let spawnedRouter: WebSocketRouter | undefined;
  let serverOptions: MeshCallServerOptions;
  let resolvedEndpoint: WebSocketEndpoint;
  let directHost: string | undefined;

  if (options.listen !== undefined || options.router === null) {
    const direct = parseDirectListen(options.listen ?? "ws://127.0.0.1:0");
    directHost = direct.host;
    resolvedEndpoint = direct.unixPath !== undefined
      ? { unixPath: direct.unixPath }
      : direct.pipe !== undefined
        ? { pipe: direct.pipe }
        : `ws://${direct.host ?? "127.0.0.1"}:${direct.port ?? 0}`;
    serverOptions = {
      services,
      ...(direct.host === undefined ? {} : { host: direct.host }),
      ...(direct.port === undefined ? {} : { port: direct.port }),
      ...(direct.unixPath === undefined ? {} : { unixPath: direct.unixPath }),
      ...(direct.pipe === undefined ? {} : { pipe: direct.pipe }),
      ...(options.instanceId === undefined ? {} : { instanceId: options.instanceId }),
      ...(options.maxInflight === undefined ? {} : { maxInflight: options.maxInflight }),
      ...(options.strict === undefined ? {} : { strictContracts: options.strict }),
      ...(options.accessLog === undefined ? {} : { accessLog: options.accessLog }),
      ...(options.logLevel === undefined ? {} : { logLevel: options.logLevel }),
      ...(options.colorize === undefined ? {} : { colorize: options.colorize }),
      ...(options.logger === undefined ? {} : { logger: options.logger }),
      ...(options.interceptors === undefined ? {} : { interceptors: options.interceptors }),
      ...(options.handshakeTimeoutMs === undefined ? {} : { handshakeTimeoutMs: options.handshakeTimeoutMs }),
      ...(options.idleTimeoutMs === undefined ? {} : { idleTimeoutMs: options.idleTimeoutMs }),
    };
  } else {
    const routerConfig = normalizeRouterInput(options.router);
    spawnedRouter = await ensureLocalRouter(routerConfig);
    let endpoints = routerConfig.endpoints;
    if (spawnedRouter !== undefined && endpoints.length === 1) {
      const localTcp = parseLocalTcpEndpoint(endpoints[0]!);
      if (localTcp !== undefined && localTcp.port === 0) {
        endpoints = [`ws://${localTcp.host}:${spawnedRouter.boundPort}`];
        routerConfig._setBoundEndpoint(endpoints[0]!);
      }
    }
    resolvedEndpoint = endpoints[0]!;
    serverOptions = {
      services,
      router: {
        endpoint: endpoints,
        ...(routerConfig.options.auth === undefined ? {} : { auth: routerConfig.options.auth }),
        ...(options.instanceId === undefined ? {} : { instanceId: options.instanceId }),
      },
      ...(options.instanceId === undefined ? {} : { instanceId: options.instanceId }),
      ...(routerConfig.options.maxFrameSize === undefined ? {} : { maxFrameSize: routerConfig.options.maxFrameSize }),
      ...(options.handshakeTimeoutMs !== undefined
        ? { handshakeTimeoutMs: options.handshakeTimeoutMs }
        : routerConfig.options.handshakeTimeoutMs !== undefined
          ? { handshakeTimeoutMs: routerConfig.options.handshakeTimeoutMs }
          : {}),
      ...(routerConfig.options.pingIntervalMs === undefined ? {} : { pingIntervalMs: routerConfig.options.pingIntervalMs }),
      ...(routerConfig.options.pingTimeoutMs === undefined ? {} : { pingTimeoutMs: routerConfig.options.pingTimeoutMs }),
      ...(routerConfig.options.reconnect === undefined ? {} : { reconnect: routerConfig.options.reconnect }),
      ...(options.maxInflight === undefined ? {} : { maxInflight: options.maxInflight }),
      ...(options.strict === undefined ? {} : { strictContracts: options.strict }),
      ...(options.accessLog === undefined ? {} : { accessLog: options.accessLog }),
      ...(options.logLevel === undefined ? {} : { logLevel: options.logLevel }),
      ...(options.colorize === undefined ? {} : { colorize: options.colorize }),
      ...(options.logger !== undefined
        ? { logger: options.logger }
        : routerConfig.options.logger !== undefined
          ? { logger: routerConfig.options.logger }
          : {}),
      ...(options.interceptors === undefined ? {} : { interceptors: options.interceptors }),
    };
  }

  const server = new MeshCallServer(serverOptions);
  try {
    await server.start();
  } catch (error) {
    if (spawnedRouter !== undefined) {
      await spawnedRouter.stop();
    }
    throw error;
  }

  if (directHost !== undefined) {
    resolvedEndpoint = `ws://${directHost}:${server.boundPort}`;
  }
  const actualInstanceId = options.instanceId ?? "";
  return new RunningServer(
    server,
    formatEndpointUrl(resolvedEndpoint),
    resolvedEndpoint,
    actualInstanceId,
    options.drainTimeoutMs,
    spawnedRouter,
  );
}

/**
 * Run one or more `@service` instances until SIGINT or SIGTERM, then drain in-flight calls and exit.
 */
export async function run(...args: [...services: object[], options: ServeOptions]): Promise<void>;
export async function run(...services: object[]): Promise<void>;
export async function run(...args: Array<object | ServeOptions>): Promise<void> {
  const running = await (serve as (...a: Array<object | ServeOptions>) => Promise<RunningServer>)(...args);
  try {
    await new Promise<void>((resolve) => {
      const onSignal = () => {
        process.off("SIGINT", onSignal);
        process.off("SIGTERM", onSignal);
        resolve();
      };
      process.once("SIGINT", onSignal);
      process.once("SIGTERM", onSignal);
    });
  } finally {
    await running.close({ drain: true });
  }
}

