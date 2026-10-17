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

import {
  MeshCallClient,
  type CallOptions,
  type ClientInterceptor,
  type DuplexCallOptions,
  type MeshCallClientOptions,
  type MeshCallDuplexCall,
} from "./client.js";
import { isService } from "./contract.js";
import { namedPipePath, unixSocketPath, type WebSocketEndpoint } from "./endpoint.js";
import type { ReconnectPolicy, RetryPolicy } from "./liveness.js";
import type { LogLevel, RpcLogger } from "./logging.js";
import { PROTOCOL_VERSION } from "./protocol.js";
import {
  authorizationHeader,
  RouterCredentials,
} from "./router-auth.js";
import { mergeCallOptions, type MeshCallRpc, type RpcStream } from "./rpc.js";
import { MeshCallServer, type MeshCallServerOptions, type ServerInterceptor } from "./server.js";
import { DEFAULT_MAX_FRAME_SIZE } from "./transport.js";
import {
  AuthClient,
  DEFAULT_ROUTER_URL,
  formatRouterEndpointUrl,
  HttpTransport,
  RegistryClient,
  Router,
  type RouterEndpoint,
  type RouterOptions,
} from "./web.js";

export {
  AuthClient,
  DEFAULT_ROUTER_URL,
  RegistryClient,
  Router,
  type RouterEndpoint,
  type RouterOptions,
};

export interface LocalRouterOptions {
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

export type WebSocketRouterOptions = LocalRouterOptions;

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

/**
 * Owns a local `meshcall-router` child process.
 * No registration, authentication or routing runs here; pass `router` to `new Mesh(router)`.
 */
export class LocalRouter {
  public readonly isHttp = false;
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

  public constructor(private readonly options: LocalRouterOptions = {}) {
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

  /** Start a `LocalRouter` subprocess and return it (`await using router = await LocalRouter.start()`). */
  public static async start(options: LocalRouterOptions = {}): Promise<LocalRouter> {
    const router = new LocalRouter(options);
    await router.start();
    return router;
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

  public get url(): string {
    if (!this.isRunning) throw new Error("Go Router is not running");
    if (this.unixPath !== undefined) return "unix://" + this.unixPath;
    if (this.pipe !== undefined) return "pipe://" + this.pipe;
    return `ws://${this.options.host ?? "127.0.0.1"}:${this.boundPort}`;
  }

  public get httpUrl(): string {
    if (this.unixPath !== undefined || this.pipe !== undefined) {
      throw new Error("A local listener does not have a TCP HTTP URL");
    }
    return `http://${this.options.host ?? "127.0.0.1"}:${this.boundPort}`;
  }

  public get endpoints(): ReadonlyArray<WebSocketEndpoint> {
    if (!this.isRunning) throw new Error("Go Router is not running");
    if (this.unixPath !== undefined) return [{ unixPath: this.unixPath }];
    if (this.pipe !== undefined) return [{ pipe: this.pipe }];
    return [this.url];
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
  public async [Symbol.asyncDispose](): Promise<void> { await this.stop(); }
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

export const WebSocketRouter = LocalRouter;
export type WebSocketRouter = LocalRouter;

export interface MeshOptions {
  /** Bearer token (`mk_…` or `mt_…`); mutually exclusive with `credentials`. */
  readonly token?: string;
  /** Router credentials (Bearer or Basic); mutually exclusive with `token`. */
  readonly credentials?: RouterCredentials;
  /** Extra HTTP headers when connecting to an HTTP(S) Router. */
  readonly headers?: Readonly<Record<string, string>>;
  /** Custom `fetch` implementation when connecting to an HTTP(S) Router. */
  readonly fetch?: typeof fetch;
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
}

export interface ServeOptions {
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

export type MountOptions = ServeOptions;

export class RunningServer {
  public constructor(
    public readonly server: MeshCallServer,
    public readonly routerUrl: string,
    public readonly endpoint: WebSocketEndpoint,
    public readonly instanceId: string,
    private readonly drainTimeoutMs: number | undefined,
    private readonly onClosed?: () => void,
  ) {}

  public get url(): string {
    return this.routerUrl;
  }

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
      this.onClosed?.();
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

/**
 * Layer 3 `Mesh` participant: connects to a `Router` or `LocalRouter` to call
 * services (`new ChatClient(mesh)`), inspect or authenticate with the Router
 * (`mesh.registry`, `mesh.auth`), and/or mount and serve local `@service`
 * instances (`mesh.mount(...)`, `await mesh.serve()` / `await mesh.startServing()`).
 */
export class Mesh implements MeshCallRpc {
  public readonly router: Router | LocalRouter;
  public readonly options: MeshOptions;
  public readonly registry: RegistryClient;
  public readonly auth: AuthClient;
  private readonly credentials: RouterCredentials | undefined;
  private root: Mesh;
  private ownsRoot = true;
  private ownsLocalRouter = false;
  private defaultCallOptions: CallOptions = {};
  private transport: (MeshCallClient | HttpTransport) | undefined;
  private readonly mountedServices: object[] = [];
  private mountOptions: ServeOptions = {};
  private activeServer: RunningServer | undefined;

  public constructor(router: Router | LocalRouter, options: MeshOptions = {}) {
    if (!(router instanceof Router) && !(router instanceof LocalRouter)) {
      throw new TypeError(
        "Mesh requires a Router or LocalRouter instance; pass new Router(url) or new LocalRouter(...) instead of a raw URL or options object",
      );
    }
    if (options.token !== undefined && options.credentials !== undefined) {
      throw new TypeError("Specify either token or credentials on Mesh, not both");
    }
    this.router = router;
    this.options = options;
    this.credentials = options.credentials ?? (options.token !== undefined ? RouterCredentials.bearer(options.token) : undefined);
    this.root = this;
    this.registry = new RegistryClient(this);
    this.auth = new AuthClient(this);
  }

  private async ensureRouterStarted(): Promise<void> {
    if (this.root !== this) {
      await this.root.ensureRouterStarted();
      return;
    }
    if (this.router instanceof LocalRouter && !this.router.isRunning) {
      await this.router.start();
      this.ownsLocalRouter = true;
    }
  }

  /** Eagerly connect a `Mesh` participant (`await using mesh = await Mesh.connect(router, options)`). */
  public static async connect(router: Router | LocalRouter, options: MeshOptions = {}): Promise<Mesh> {
    const mesh = new Mesh(router, options);
    await mesh.connect();
    return mesh;
  }

  public withOptions(options: CallOptions): this {
    const clone = Object.create(Mesh.prototype) as this;
    Object.assign(clone, {
      router: this.root.router,
      options: this.root.options,
      credentials: this.root.credentials,
      root: this.root,
      ownsRoot: false,
      ownsLocalRouter: false,
      defaultCallOptions: mergeCallOptions(this.defaultCallOptions, options),
      registry: new RegistryClient(clone),
      auth: new AuthClient(clone),
    });
    return clone;
  }

  private ensureTransport(): MeshCallRpc {
    if (this.root !== this) {
      return this.root.ensureTransport().withOptions(this.defaultCallOptions);
    }
    if (this.transport === undefined) {
      if (this.router.isHttp) {
        this.transport = new HttpTransport(this.router.url, {
          ...(this.credentials === undefined ? {} : { authorization: authorizationHeader(this.credentials) }),
          ...(this.options.headers === undefined ? {} : { headers: this.options.headers }),
          ...(this.options.fetch === undefined ? {} : { fetch: this.options.fetch }),
          ...(this.options.retry === undefined ? {} : { retry: this.options.retry }),
          ...(this.options.reconnect === undefined ? {} : { reconnect: this.options.reconnect }),
          ...(this.options.deadlineGraceMs === undefined ? {} : { deadlineGraceMs: this.options.deadlineGraceMs }),
          ...(this.options.logger === undefined ? {} : { logger: this.options.logger }),
        });
      } else {
        const clientOptions: MeshCallClientOptions = {
          ...(this.credentials === undefined ? {} : { auth: this.credentials }),
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
        this.transport = new MeshCallClient(this.router.endpoints, this.options.peerId, clientOptions);
      }
    }
    return Object.keys(this.defaultCallOptions).length > 0
      ? this.transport.withOptions(this.defaultCallOptions)
      : this.transport;
  }

  public get client(): MeshCallClient {
    const active = this.ensureTransport();
    if (!(active instanceof MeshCallClient)) {
      throw new Error("HTTP Mesh does not use a WebSocket MeshCallClient");
    }
    return active;
  }

  public async connect(): Promise<this> {
    if (this.root !== this) {
      await this.root.connect();
      return this;
    }
    await this.ensureRouterStarted();
    const transport = this.ensureTransport();
    if (transport instanceof MeshCallClient) {
      await transport.connect();
    }
    return this;
  }

  /**
   * Register one or more `@service` instances locally on this `Mesh` (zero I/O).
   * Call `await mesh.serve()` or `await using srv = await mesh.startServing()` to publish them to the Router.
   */
  public mount(...args: [...services: object[], options: ServeOptions]): this;
  public mount(...services: object[]): this;
  public mount(...args: Array<object | ServeOptions>): this {
    if (this.root !== this) {
      (this.root.mount as (...a: Array<object | ServeOptions>) => Mesh)(...args);
      return this;
    }
    if (this.router.isHttp) {
      throw new TypeError(
        "Cannot mount services on an HTTP(S) Router; server registration requires WebSocket (ws://, wss://, unix://, or pipe://)",
      );
    }
    if (this.activeServer !== undefined) {
      throw new Error("Cannot mount services while Mesh is already serving");
    }
    const { services, options } = splitServicesAndOptions(args);
    for (const svc of services) {
      if (!isService(svc)) {
        throw new TypeError("mount() requires @service instances");
      }
    }
    this.mountedServices.push(...services);
    this.mountOptions = { ...this.mountOptions, ...options };
    return this;
  }

  /**
   * Connect to the Router and register all mounted `@service` instances in the background,
   * returning a `RunningServer` handle (`await using srv = await mesh.startServing()`).
   */
  public async startServing(options: ServeOptions = {}): Promise<RunningServer> {
    if (this.root !== this) {
      return this.root.startServing(options);
    }
    if (this.router.isHttp) {
      throw new TypeError(
        "Cannot serve services on an HTTP(S) Router; server registration requires WebSocket (ws://, wss://, unix://, or pipe://)",
      );
    }
    if (this.mountedServices.length === 0) {
      throw new RangeError("No services mounted on Mesh; call mesh.mount(...) before serving");
    }
    if (this.activeServer !== undefined) {
      throw new Error("Mesh is already serving");
    }

    await this.ensureRouterStarted();
    const merged: ServeOptions = { ...this.mountOptions, ...options };
    const endpoints = this.router.endpoints;
    const resolvedEndpoint = endpoints[0]!;
    const serverOptions: MeshCallServerOptions = {
      services: [...this.mountedServices],
      router: {
        endpoint: endpoints,
        ...(this.credentials === undefined ? {} : { auth: this.credentials }),
        ...(merged.instanceId === undefined ? {} : { instanceId: merged.instanceId }),
      },
      ...(merged.instanceId === undefined ? {} : { instanceId: merged.instanceId }),
      ...(this.options.maxFrameSize === undefined ? {} : { maxFrameSize: this.options.maxFrameSize }),
      ...(merged.handshakeTimeoutMs !== undefined
        ? { handshakeTimeoutMs: merged.handshakeTimeoutMs }
        : this.options.handshakeTimeoutMs !== undefined
          ? { handshakeTimeoutMs: this.options.handshakeTimeoutMs }
          : {}),
      ...(this.options.pingIntervalMs === undefined ? {} : { pingIntervalMs: this.options.pingIntervalMs }),
      ...(this.options.pingTimeoutMs === undefined ? {} : { pingTimeoutMs: this.options.pingTimeoutMs }),
      ...(this.options.reconnect === undefined ? {} : { reconnect: this.options.reconnect }),
      ...(merged.maxInflight === undefined ? {} : { maxInflight: merged.maxInflight }),
      ...(merged.strict === undefined ? {} : { strictContracts: merged.strict }),
      ...(merged.accessLog === undefined ? {} : { accessLog: merged.accessLog }),
      ...(merged.logLevel === undefined ? {} : { logLevel: merged.logLevel }),
      ...(merged.colorize === undefined ? {} : { colorize: merged.colorize }),
      ...(merged.logger !== undefined
        ? { logger: merged.logger }
        : this.options.logger !== undefined
          ? { logger: this.options.logger }
          : {}),
      ...(merged.interceptors === undefined ? {} : { interceptors: merged.interceptors }),
    };

    const server = new MeshCallServer(serverOptions);
    try {
      await server.start();
    } catch (error) {
      if (this.ownsLocalRouter && this.transport === undefined && this.router instanceof LocalRouter) {
        this.ownsLocalRouter = false;
        await this.router.stop();
      }
      throw error;
    }
    const running = new RunningServer(
      server,
      formatRouterEndpointUrl(resolvedEndpoint),
      resolvedEndpoint,
      merged.instanceId ?? "",
      merged.drainTimeoutMs,
      () => {
        if (this.activeServer === running) {
          this.activeServer = undefined;
        }
      },
    );
    this.activeServer = running;
    return running;
  }

  /**
   * Serve mounted `@service` instances until `SIGINT` or `SIGTERM`, then drain in-flight calls and close.
   */
  public async serve(options: ServeOptions = {}): Promise<void> {
    const running = await this.startServing(options);
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
      await this.close();
    }
  }

  /** Drain in-flight server calls and unregister mounted services from the Router. */
  public async drain(timeoutMs?: number): Promise<void> {
    if (this.root !== this) {
      await this.root.drain(timeoutMs);
      return;
    }
    const active = this.activeServer;
    if (active !== undefined) {
      this.activeServer = undefined;
      await active.close({
        drain: true,
        ...(timeoutMs === undefined ? {} : { drainTimeoutMs: timeoutMs }),
      });
    }
  }

  public async close(): Promise<void> {
    if (!this.ownsRoot) return;
    try {
      await this.drain();
    } finally {
      try {
        if (this.transport !== undefined) {
          const t = this.transport;
          this.transport = undefined;
          if (t instanceof MeshCallClient) {
            await t.close();
          }
        }
      } finally {
        if (this.ownsLocalRouter && this.router instanceof LocalRouter) {
          this.ownsLocalRouter = false;
          await this.router.stop();
        }
      }
    }
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  // ---------------------------------------------------------------------------
  // MeshCallRpc implementation (for passing `mesh` to generated clients)
  // ---------------------------------------------------------------------------

  public unary<Request, Response>(
    service: string, method: string, request: Request, options?: CallOptions,
  ): Promise<Response> {
    return this.ensureTransport().unary(service, method, request, options);
  }

  public serverStream<Request, Item>(
    service: string, method: string, request: Request, options?: CallOptions,
  ): RpcStream<Item> {
    return this.ensureTransport().serverStream(service, method, request, options);
  }

  public clientStream<Request, Item, Response>(
    service: string, method: string, request: Request, items: AsyncIterable<Item>, options?: CallOptions,
  ): Promise<Response> {
    return this.ensureTransport().clientStream(service, method, request, items, options);
  }

  public duplex<Request, InputItem, OutputItem>(
    service: string, method: string, request: Request, options?: DuplexCallOptions,
  ): MeshCallDuplexCall<InputItem, OutputItem> {
    const transport = this.ensureTransport();
    if (transport.duplex === undefined) {
      throw new Error("Duplex is not supported by this transport");
    }
    return transport.duplex(service, method, request, options);
  }
}
