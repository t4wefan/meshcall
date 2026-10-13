/**
 * Install the first-party MeshCall skill into a project for coding agents.
 *
 * Mirrors `meshcall-router/internal/skill` and `meshcall-py/src/meshcall/skill.py`;
 * the three embedded SKILL.md copies are kept identical to
 * `skills/meshcall/SKILL.md` by `scripts/regenerate.sh`.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const SKILL_PATH = ".claude/skills/meshcall/SKILL.md";

const BEGIN = "<!-- meshcall:begin -->";
const END = "<!-- meshcall:end -->";

export const SECTION = `${BEGIN}
## MeshCall

This project uses MeshCall (typed RPC through a Router). Read
\`${SKILL_PATH}\` before touching services or clients.
Explore a running mesh with \`meshcall router inspect\`, call with
\`meshcall call svc.method '{json}'\`, and set \`MESHCALL_STRICT=1\` while
editing a service so contract warnings fail with a hint.
${END}
`;

export function skillContent(): string {
  // dist/src/skill.js → <package root>/skills/meshcall/SKILL.md
  return readFileSync(fileURLToPath(new URL("../../skills/meshcall/SKILL.md", import.meta.url)), "utf8");
}

/** `kept` means an edited or older file was left alone; rerun with force. */
export type Outcome = "created" | "updated" | "unchanged" | "kept";

export interface Report {
  skill: Outcome;
  agents: Outcome;
  nextSteps: string[];
}

/** Write the skill and the AGENTS.md section; keep local edits unless forced. */
export function installSkill(directory: string, options: { force?: boolean } = {}): Report {
  const skill = writeSkill(join(directory, SKILL_PATH), options.force ?? false);
  const agents = writeSection(join(directory, "AGENTS.md"));
  return { skill, agents, nextSteps: nextSteps(directory) };
}

function writeSkill(path: string, force: boolean): Outcome {
  const content = skillContent();
  if (!existsSync(path)) {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, content);
    return "created";
  }
  if (readFileSync(path, "utf8") === content) return "unchanged";
  if (!force) return "kept";
  writeFileSync(path, content);
  return "updated";
}

function writeSection(path: string): Outcome {
  if (!existsSync(path)) {
    writeFileSync(path, "# Agent instructions\n\n" + SECTION);
    return "created";
  }
  const text = readFileSync(path, "utf8");
  const begin = text.indexOf(BEGIN);
  const end = text.indexOf(END);
  const updated = begin >= 0 && end > begin
    ? text.slice(0, begin) + SECTION.replace(/\n$/, "") + text.slice(end + END.length)
    : text.replace(/\n+$/, "") + "\n\n" + SECTION;
  if (updated === text) return "unchanged";
  writeFileSync(path, updated);
  return "updated";
}

function nextSteps(directory: string): string[] {
  const steps: string[] = [];
  if (existsSync(join(directory, "pyproject.toml"))) {
    steps.push("Python: `uv add meshcall`, then `uv run meshcall router install` for the Router binary");
  }
  if (existsSync(join(directory, "package.json"))) {
    steps.push("TypeScript: `npm install meshcall`, then `npx meshcall-router-install` for the Router binary");
  }
  if (steps.length === 0) {
    steps.push("Add the SDK: `uv add meshcall` (Python) or `npm install meshcall` (TypeScript), then install the Router binary");
  }
  steps.push(`Agents read ${SKILL_PATH} automatically; AGENTS.md now points at it`);
  return steps;
}
