import type { z } from "zod";

import { InvalidArgumentError, type Violation } from "./errors.js";
import type { RpcContext } from "./service.js";

const DEPENDENCY_BRAND = Symbol.for("meshcall.dependency");
const META_BRAND = Symbol.for("meshcall.meta");

export type DependencyResult<T> =
  | T
  | Promise<T>
  | Generator<T, void, unknown>
  | AsyncGenerator<T, void, unknown>;

export interface Dependency<T = unknown> {
  readonly [DEPENDENCY_BRAND]: true;
  readonly provider: (...args: any[]) => DependencyResult<T>;
  readonly inject: ReadonlyArray<Injectable<any>>;
}

export interface MetaOptions<T = unknown> {
  readonly default?: T;
  readonly required?: boolean;
  readonly schema?: z.ZodType<T>;
}

export interface MetaDependency<T = unknown> {
  readonly [META_BRAND]: true;
  readonly key: string;
  readonly hasDefault: boolean;
  readonly defaultValue: T | undefined;
  readonly required: boolean;
  readonly schema: z.ZodType<T> | undefined;
  resolve(context: RpcContext): T;
}

export type Injectable<T = unknown> = Dependency<T> | MetaDependency<T>;

export type ResolvedInjectables<T extends ReadonlyArray<Injectable<any>>> = {
  -readonly [K in keyof T]: T[K] extends Injectable<infer U> ? U : never;
};

export function isDependency(value: unknown): value is Dependency<unknown> {
  return typeof value === "object" && value !== null && DEPENDENCY_BRAND in value;
}

export function isMetaDependency(value: unknown): value is MetaDependency<unknown> {
  return typeof value === "object" && value !== null && META_BRAND in value;
}

export function isInjectable(value: unknown): value is Injectable<unknown> {
  return isDependency(value) || isMetaDependency(value);
}

/**
 * Declare a per-call dependency provider.
 *
 * The provider is evaluated at most once per RPC call and its result is cached
 * within that call's `DependencyScope`. If the provider is a generator or
 * async generator (`function*` / `async function*`), the yielded value is
 * injected and the generator's `finally` block runs when the call finishes.
 */
export function dependency<T>(
  provider: (context: RpcContext) => DependencyResult<T>,
): Dependency<T>;
export function dependency<T, const Deps extends ReadonlyArray<Injectable<any>>>(
  provider: (...args: [...ResolvedInjectables<Deps>, RpcContext]) => DependencyResult<T>,
  options: { readonly inject: Deps },
): Dependency<T>;
export function dependency<T>(
  provider: (...args: any[]) => DependencyResult<T>,
  options?: { readonly inject?: ReadonlyArray<Injectable<any>> },
): Dependency<T>;
export function dependency<T>(
  provider: (...args: any[]) => DependencyResult<T>,
  options?: { readonly inject?: ReadonlyArray<Injectable<any>> },
): Dependency<T> {
  const inject = options?.inject ?? [];
  const dep: Dependency<T> = {
    [DEPENDENCY_BRAND]: true,
    provider,
    inject,
  };
  validateDependencyGraph(dep, provider.name || "dependency");
  return dep;
}

/**
 * Declare a metadata header injection (`context.metadata[key]`).
 *
 * By default the header is optional (`string | undefined`). Pass
 * `{ required: true }`, `{ default: ... }`, or `{ schema: z.string() }` to
 * enforce presence or parse types.
 */
export function meta(key: string): MetaDependency<string | undefined>;
export function meta(key: string, options: { readonly required: true }): MetaDependency<string>;
export function meta<T extends string>(key: string, options: { readonly default: T }): MetaDependency< string >;
export function meta<S extends z.ZodType>(
  key: string,
  options: { readonly schema: S; readonly required?: boolean; readonly default?: z.output<S> },
): MetaDependency<z.output<S>>;
export function meta<T = string | undefined>(
  key: string,
  options?: MetaOptions<T>,
): MetaDependency<T> {
  const normalizedKey = key.trim();
  if (normalizedKey === "") {
    throw new Error("meta() key must be a non-empty string");
  }
  const hasDefault = options !== undefined && "default" in options && options.default !== undefined;
  const defaultValue = options?.default;
  const required = options?.required === true;
  const schema = options?.schema;

  return {
    [META_BRAND]: true,
    key: normalizedKey,
    hasDefault,
    defaultValue,
    required,
    schema,
    resolve(context: RpcContext): T {
      const raw = context.metadata[normalizedKey];
      if (raw === undefined) {
        if (hasDefault) {
          if (schema !== undefined) {
            return parseMetaSchema(normalizedKey, schema, defaultValue);
          }
          return defaultValue as T;
        }
        if (schema !== undefined && !required) {
          const parsed = schema.safeParse(undefined);
          if (parsed.success) {
            return parsed.data;
          }
        }
        if (required || schema !== undefined) {
          const violations: Violation[] = [
            { path: `metadata.${normalizedKey}`, code: "missing", message: "Field is required" },
          ];
          throw new InvalidArgumentError(`Missing required metadata header: ${normalizedKey}`, {
            details: { source: "server", reason: "missing_metadata", violations },
          });
        }
        return undefined as T;
      }
      if (schema !== undefined) {
        return parseMetaSchema(normalizedKey, schema, raw);
      }
      return raw as unknown as T;
    },
  };
}

function parseMetaSchema<T>(key: string, schema: z.ZodType<T>, raw: unknown): T {
  const result = schema.safeParse(raw);
  if (result.success) {
    return result.data;
  }
  const violations: Violation[] = result.error.issues.map((issue) => ({
    path: [`metadata.${key}`, ...issue.path.map(String)].join("."),
    code: issue.code,
    message: issue.message,
  }));
  throw new InvalidArgumentError(`Invalid metadata header: ${key}`, {
    details: { source: "server", reason: "invalid_metadata", violations },
  });
}

/**
 * Validate a list of injectables for circular dependencies at registration time.
 */
export function validateInjectables(
  injectables: ReadonlyArray<Injectable<any>>,
  ownerName: string,
): void {
  for (const item of injectables) {
    if (isDependency(item)) {
      validateDependencyGraph(item, ownerName, new Set());
    }
  }
}

function validateDependencyGraph(
  dep: Dependency<any>,
  ownerName: string,
  visiting: Set<Dependency<any>> = new Set(),
): void {
  if (visiting.has(dep)) {
    const depName = dep.provider.name || "<anonymous>";
    throw new Error(`Circular dependency detected while resolving ${depName} for ${ownerName}`);
  }
  const next = new Set(visiting);
  next.add(dep);
  for (const child of dep.inject) {
    if (isDependency(child)) {
      validateDependencyGraph(child, ownerName, next);
    }
  }
}

function isGeneratorLike(value: unknown): value is Iterator<unknown> | AsyncIterator<unknown> {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as { next?: unknown; return?: unknown };
  return typeof candidate.next === "function" && typeof candidate.return === "function"
    && (Symbol.iterator in candidate || Symbol.asyncIterator in candidate);
}

/**
 * Per-call dependency resolution scope with caching and LIFO generator teardown.
 */
export class DependencyScope {
  private readonly cache = new Map<Dependency<any>, unknown>();
  private readonly cleanups: Array<() => Promise<void> | void> = [];

  public constructor(public readonly context: RpcContext) {}

  public async resolveAll(injectables: ReadonlyArray<Injectable<any>>): Promise<unknown[]> {
    if (injectables.length === 0) return [];
    const results = new Array<unknown>(injectables.length);
    // Resolve all meta() headers first so missing/invalid metadata fails fast
    // before opening expensive dependency resources.
    for (let i = 0; i < injectables.length; i += 1) {
      const item = injectables[i]!;
      if (isMetaDependency(item)) {
        results[i] = item.resolve(this.context);
      }
    }
    for (let i = 0; i < injectables.length; i += 1) {
      const item = injectables[i]!;
      if (!isMetaDependency(item)) {
        results[i] = await this.resolve(item);
      }
    }
    return results;
  }

  public async resolve<T>(item: Injectable<T>): Promise<T> {
    if (isMetaDependency(item)) {
      return item.resolve(this.context);
    }
    if (this.cache.has(item)) {
      return this.cache.get(item) as T;
    }

    const nestedArgs = await this.resolveAll(item.inject);
    const rawResult = item.provider(...nestedArgs, this.context);
    const resolvedOrIter = rawResult instanceof Promise ? await rawResult : rawResult;

    let value: T;
    if (isGeneratorLike(resolvedOrIter)) {
      const step = await resolvedOrIter.next();
      value = step.value as T;
      this.cleanups.push(async () => {
        await resolvedOrIter.return?.(undefined);
      });
    } else {
      value = resolvedOrIter as T;
    }

    this.cache.set(item, value);
    return value;
  }

  public async close(): Promise<void> {
    const callbacks = this.cleanups.splice(0, this.cleanups.length).reverse();
    let firstError: unknown;
    for (const cleanup of callbacks) {
      try {
        await cleanup();
      } catch (error) {
        if (firstError === undefined) firstError = error;
      }
    }
    if (firstError !== undefined) {
      throw firstError;
    }
  }

  public async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
