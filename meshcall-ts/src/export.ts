#!/usr/bin/env node
/**
 * CLI helper: export `@service` contracts from TypeScript or JavaScript files.
 *
 * Invoked by the Go `meshcall` binary when `meshcall export` or
 * `meshcall generate` is given `.ts`, `.mts`, `.js` or `.mjs` sources.
 */
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, extname, join, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import WebSocket from "ws";

import { contractEntry, isService, serviceSpec, writeContract } from "./contract.js";
import { DEFAULT_MAX_FRAME_SIZE } from "./transport.js";
import {
  PROTOCOL_VERSION,
  decodeFrame,
  encodeFrame,
  type BalancePolicy,
  type RegisteredService,
} from "./protocol.js";

const THIS_DIR = dirname(fileURLToPath(import.meta.url));
// dist/src/export.js -> meshcall-ts package root is two levels up
const MESHCALL_TS_ROOT = resolve(THIS_DIR, "../..");

export function splitSpec(spec: string): { filePart: string; className?: string } {
  const idx = spec.lastIndexOf(":");
  if (idx < 0) return { filePart: spec };
  if (idx === 1 && /^[A-Za-z]$/u.test(spec[0] ?? "") && (spec[2] === "\\" || spec[2] === "/")) {
    return { filePart: spec };
  }
  const filePart = spec.slice(0, idx);
  const className = spec.slice(idx + 1);
  if (filePart === "") {
    throw new Error(`Expected path.ts[:Class], got ${JSON.stringify(spec)}`);
  }
  return className === "" ? { filePart } : { filePart, className };
}

function findAncestorWith(startDir: string, targetName: string): string | undefined {
  let current = resolve(startDir);
  while (true) {
    if (existsSync(join(current, targetName))) {
      return current;
    }
    const parent = dirname(current);
    if (parent === current) return undefined;
    current = parent;
  }
}

function findTscBinary(searchDirs: ReadonlyArray<string>): string {
  const binName = process.platform === "win32" ? "tsc.cmd" : "tsc";
  for (const dir of searchDirs) {
    const candidate = join(dir, "node_modules", ".bin", binName);
    if (existsSync(candidate)) return candidate;
  }
  return binName;
}

function linkNodeModules(tmpDir: string, sourceNodeModulesDirs: ReadonlyArray<string>): void {
  const nmDir = join(tmpDir, "node_modules");
  mkdirSync(nmDir, { recursive: true });
  const symlinkType = process.platform === "win32" ? "junction" : "dir";
  const meshcallLink = join(nmDir, "meshcall");
  if (existsSync(join(MESHCALL_TS_ROOT, "package.json"))) {
    try {
      symlinkSync(MESHCALL_TS_ROOT, meshcallLink, symlinkType);
    } catch {
      // ignore if already present
    }
  }
  for (const srcNm of sourceNodeModulesDirs) {
    if (!existsSync(srcNm)) continue;
    for (const entry of readdirSync(srcNm)) {
      if (entry === ".bin" || entry === "meshcall") continue;
      const targetPath = join(nmDir, entry);
      if (!existsSync(targetPath)) {
        try {
          symlinkSync(join(srcNm, entry), targetPath, symlinkType);
        } catch {
          // ignore duplicate or broken symlink
        }
      }
    }
  }
}

async function importSourceFile(filePart: string, cleanupDirs: string[]): Promise<Record<string, unknown>> {
  const absPath = resolve(filePart);
  if (!existsSync(absPath)) {
    throw new Error(`TypeScript/JavaScript source file does not exist: ${filePart}`);
  }
  const ext = extname(absPath).toLowerCase();
  if (ext === ".js" || ext === ".mjs" || ext === ".cjs") {
    return (await import(pathToFileURL(absPath).href)) as Record<string, unknown>;
  }

  const tmpDir = mkdtempSync(join(tmpdir(), "meshcall-ts-export-"));
  cleanupDirs.push(tmpDir);
  writeFileSync(join(tmpDir, "package.json"), JSON.stringify({ type: "module" }));

  const ancestorWithNm = findAncestorWith(dirname(absPath), "node_modules");
  const cwdWithNm = findAncestorWith(process.cwd(), "node_modules");
  const nmCandidates = [
    ...(ancestorWithNm ? [join(ancestorWithNm, "node_modules")] : []),
    ...(cwdWithNm ? [join(cwdWithNm, "node_modules")] : []),
    join(MESHCALL_TS_ROOT, "node_modules"),
  ];
  linkNodeModules(tmpDir, nmCandidates);

  let compileTarget = absPath;
  let rootDir: string;
  if (ancestorWithNm !== undefined && existsSync(join(ancestorWithNm, "node_modules", "meshcall"))) {
    const ancestorWithTsconfig = findAncestorWith(dirname(absPath), "tsconfig.json");
    rootDir = ancestorWithTsconfig !== undefined && ancestorWithTsconfig.startsWith(ancestorWithNm)
      ? ancestorWithTsconfig
      : dirname(absPath);
  } else {
    // Standalone file (e.g. in a temp dir without node_modules): copy its directory
    // inside tmpDir/src so TypeScript module resolution finds tmpDir/node_modules.
    const workSrc = join(tmpDir, "src");
    cpSync(dirname(absPath), workSrc, { recursive: true });
    compileTarget = join(workSrc, basename(absPath));
    rootDir = workSrc;
  }

  const outDir = join(tmpDir, "out");
  const tsconfigPath = join(tmpDir, "tsconfig.export.json");
  writeFileSync(
    tsconfigPath,
    JSON.stringify({
      compilerOptions: {
        target: "ES2022",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        rootDir,
        outDir,
        noEmit: false,
        declaration: false,
        declarationMap: false,
        sourceMap: false,
        incremental: false,
        skipLibCheck: true,
        esModuleInterop: true,
      },
      include: [compileTarget],
    }),
  );

  const tsc = findTscBinary([
    ...(ancestorWithNm ? [ancestorWithNm] : []),
    ...(cwdWithNm ? [cwdWithNm] : []),
    MESHCALL_TS_ROOT,
  ]);
  const res = spawnSync(tsc, ["-p", tsconfigPath], {
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (res.status !== 0) {
    const details = (res.stdout || "") + (res.stderr || "");
    throw new Error(`Failed to compile ${filePart} with tsc:\n${details.trim()}`);
  }

  const relPath = relative(rootDir, compileTarget);
  const compiledRel = relPath.replace(/\.mts$/iu, ".mjs").replace(/\.ts$/iu, ".js");
  const compiledPath = join(outDir, compiledRel);
  if (!existsSync(compiledPath)) {
    throw new Error(`Compiled output not found at ${compiledPath}`);
  }
  return (await import(pathToFileURL(compiledPath).href)) as Record<string, unknown>;
}

export async function loadServices(specs: ReadonlyArray<string>, cleanupDirs: string[]): Promise<object[]> {
  const services: object[] = [];
  const seenNames = new Set<string>();

  for (const spec of specs) {
    const { filePart, className } = splitSpec(spec);
    const mod = await importSourceFile(filePart, cleanupDirs);
    if (className !== undefined) {
      if (!(className in mod)) {
        throw new Error(`Module ${JSON.stringify(filePart)} has no export ${JSON.stringify(className)}`);
      }
      const candidate = mod[className];
      if (!isService(candidate)) {
        throw new Error(`${className} in ${filePart} is not a @service class`);
      }
      const sSpec = serviceSpec(candidate);
      if (!seenNames.has(sSpec.name)) {
        seenNames.add(sSpec.name);
        services.push(candidate);
      }
      continue;
    }

    const discovered: object[] = [];
    for (const value of Object.values(mod)) {
      if (isService(value)) {
        discovered.push(value);
      }
    }
    if (discovered.length === 0) {
      throw new Error(`No @service classes found in ${JSON.stringify(filePart)}`);
    }
    for (const svc of discovered) {
      const sSpec = serviceSpec(svc);
      if (!seenNames.has(sSpec.name)) {
        seenNames.add(sSpec.name);
        services.push(svc);
      }
    }
  }
  return services;
}

function normalizeBalance(balance?: BalancePolicy): BalancePolicy {
  return {
    kind: balance?.kind ?? "round_robin",
    ...(balance?.key ? { key: balance.key } : {}),
  };
}

export async function registerServicesWithRouter(
  services: ReadonlyArray<object>,
  routerUrl: string,
): Promise<void> {
  const instanceId = `export-${randomUUID().slice(0, 12)}`;
  const registered: RegisteredService[] = services.map((svc) => {
    const spec = serviceSpec(svc);
    const entry = contractEntry(svc);
    return {
      name: spec.name,
      targeted: spec.targeted,
      methods: spec.methods.map((m) => ({
        name: m.name,
        stream: m.stream,
        balance: normalizeBalance(m.balance ?? spec.balance),
      })),
      contract: entry,
    };
  });

  await new Promise<void>((resolvePromise, rejectPromise) => {
    const ws = new WebSocket(routerUrl, { maxPayload: DEFAULT_MAX_FRAME_SIZE });
    let stage: "hello" | "register" | "done" = "hello";

    ws.on("error", (err) => {
      if (stage !== "done") rejectPromise(err);
    });

    ws.on("open", () => {
      ws.send(
        encodeFrame({
          kind: "hello",
          protocol: PROTOCOL_VERSION,
          role: "server",
          peer_id: instanceId,
          instance_id: instanceId,
        }),
      );
    });

    ws.on("message", (data) => {
      try {
        const frame = decodeFrame(data);
        if (stage === "hello") {
          if (frame.kind !== "hello.ack") {
            throw new Error(`Expected hello.ack from Router, got ${frame.kind}`);
          }
          stage = "register";
          ws.send(
            encodeFrame({
              kind: "server.register",
              instance_id: instanceId,
              services: registered,
            }),
          );
          return;
        }
        if (stage === "register") {
          if (frame.kind !== "server.register.ack" || frame.instance_id !== instanceId) {
            throw new Error(`Expected matching server.register.ack, got ${frame.kind}`);
          }
          stage = "done";
          ws.close();
          resolvePromise();
        }
      } catch (err) {
        stage = "done";
        ws.close();
        rejectPromise(err);
      }
    });

    ws.on("close", () => {
      if (stage !== "done") {
        rejectPromise(new Error("Router connection closed before registration completed"));
      }
    });
  });
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const specs: string[] = [];
  let output: string | undefined;
  let routerUrl: string | undefined;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    if (arg === "--output") {
      output = argv[++i];
    } else if (arg.startsWith("--output=")) {
      output = arg.slice("--output=".length);
    } else if (arg === "--router") {
      routerUrl = argv[++i];
    } else if (arg.startsWith("--router=")) {
      routerUrl = arg.slice("--router=".length);
    } else if (arg.startsWith("-")) {
      throw new Error(`Unknown option: ${arg}`);
    } else {
      specs.push(arg);
    }
  }

  if (specs.length === 0) {
    throw new Error("Usage: node export.js <file.ts[:Class]>... [--router ws://...] [--output contract.json]");
  }
  if (output === undefined && routerUrl === undefined) {
    throw new Error("At least one of --output or --router is required");
  }

  const cleanupDirs: string[] = [];
  try {
    const services = await loadServices(specs, cleanupDirs);
    if (routerUrl !== undefined) {
      await registerServicesWithRouter(services, routerUrl);
    }
    if (output !== undefined) {
      await writeContract(services, output);
    }
  } finally {
    for (const dir of cleanupDirs) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
}

const isMain = process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((err: unknown) => {
    process.stderr.write(`error: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exit(1);
  });
}
