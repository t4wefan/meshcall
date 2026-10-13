/**
 * Advisory contract warnings: quality, not correctness.
 *
 * Hard rules (service name shape, readOnly+destructive) throw. Everything here
 * is a warning: `defineService` records them, `MeshCallServer` logs a summary
 * at start, and strict mode (`MESHCALL_STRICT=1` or `strictContracts: true`)
 * turns them into errors for CI and agents. The builders also carry a
 * compile-time signal: the overload without `description` is `@deprecated`,
 * so editors strike it through and lint rules can fail on it.
 */
import type { ExportableServiceDefinition, MethodSemantics } from "./contract.js";

export interface ContractWarning {
  readonly code: string;
  readonly subject: string;
  readonly message: string;
  /** A direct instruction an agent (or a human) can act on. */
  readonly hint: string;
  /** How to make this warning go away on purpose. */
  readonly silence: string;
}

const MUTATING = /^(create|add|insert|update|set|put|patch|delete|remove|drop|clear|reset|send|post|write|save|upload|cancel|revoke|issue|register|unregister|start|stop|restart|apply|assign|move|rename|archive|restore|purge)(?=[A-Z_]|$)/u;
const READING = /^(get|list|read|fetch|find|search|query|count|describe|show|check|is|has|exists|lookup|peek|preview|validate|estimate|stream|watch)(?=[A-Z_]|$)/u;

export function renderWarning(warning: ContractWarning): string {
  return warning.code + " " + warning.subject + ": " + warning.message
    + "\n  hint: " + warning.hint + "\n  silence: " + warning.silence;
}

export function strictMode(): boolean {
  return ["1", "true", "yes", "on"].includes((process.env.MESHCALL_STRICT ?? "").trim().toLowerCase());
}

export function ignoredCodes(): ReadonlySet<string> {
  return new Set((process.env.MESHCALL_IGNORE_WARNINGS ?? "").split(",").map((c) => c.trim().toUpperCase()).filter(Boolean));
}

/** Compute every advisory warning for a service definition; pure. */
export function lintService(service: ExportableServiceDefinition): ContractWarning[] {
  const findings: ContractWarning[] = [];
  if (service.description === undefined) {
    findings.push({
      code: "W001", subject: service.name, message: "service has no description",
      hint: "Add `description` to defineService saying what the service is for. It becomes the contract's description and the text LLM callers and generated clients show.",
      silence: 'description: "" to declare none on purpose',
    });
  }
  for (const [name, method] of Object.entries(service.methods)) {
    const subject = service.name + "." + name;
    const m = method as MethodSemantics;
    if (service.targeted === true && (method.balance ?? service.balance)?.kind === "sticky") {
      findings.push({
        code: "W006", subject, message: "sticky balancing on a targeted service is never used",
        hint: "Targeted services route by the instance id the caller passes; the Router ignores the sticky key. Drop the sticky balance or drop targeted.",
        silence: "remove the sticky policy",
      });
    }
    const annotated = m.idempotent === true || m.readOnly === true || m.destructive === true;
    const mutating = MUTATING.test(name);
    const reading = READING.test(name);
    if (m.description === undefined) {
      findings.push({
        code: "W001", subject, message: "method has no description",
        hint: "Add `description` stating what the call does and when to use it, e.g. \"Create a session and return it.\" Mention side effects. This becomes the tool description LLM callers see.",
        silence: 'description: "" to declare none on purpose',
      });
    }
    if (mutating && !annotated) {
      findings.push({
        code: "W002", subject, message: "name suggests side effects but the method has no annotation",
        hint: "Declare its semantics so retry policies and tools can reason about it: idempotent: true if repeating it is safe, destructive: true if it cannot be undone, readOnly: true if the name is misleading.",
        silence: "any of idempotent, readOnly, destructive on the method",
      });
    }
    if (m.readOnly === true && mutating) {
      findings.push({
        code: "W003", subject, message: "readOnly on a method whose name suggests mutation",
        hint: "Either the name or the annotation is wrong. Rename the method (get/list/…) or drop readOnly and declare idempotent / destructive instead.",
        silence: "fix the name or the annotation",
      });
    }
    if (m.destructive === true && reading) {
      findings.push({
        code: "W003", subject, message: "destructive on a method whose name suggests a read",
        hint: "Either the name or the annotation is wrong. Rename the method to say what it changes, or drop destructive.",
        silence: "fix the name or the annotation",
      });
    }
    if (m.deprecated === "") {
      findings.push({
        code: "W005", subject, message: "deprecated without naming a replacement",
        hint: 'Give callers somewhere to go: deprecated: "newName". Use a short sentence if there is no replacement.',
        silence: 'deprecated: "no replacement"',
      });
    }
  }
  return findings;
}

const recorded = new WeakMap<object, ReadonlyArray<ContractWarning>>();

/** Lint, drop ignored codes, record on the definition, and throw in strict mode. */
export function recordWarnings<T extends ExportableServiceDefinition>(service: T): T {
  const ignored = ignoredCodes();
  const findings = lintService(service).filter((w) => !ignored.has(w.code));
  if (findings.length > 0 && strictMode()) {
    throw new Error("Strict mode: " + findings.length + " contract warning(s) for " + service.name + "\n"
      + findings.map(renderWarning).join("\n"));
  }
  recorded.set(service, findings);
  return service;
}

/** Warnings recorded for a definition returned by `defineService`. */
export function contractWarnings(service: object): ReadonlyArray<ContractWarning> {
  return recorded.get(service) ?? [];
}
