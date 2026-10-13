#!/usr/bin/env node
/**
 * Install the MeshCall skill for coding agents into a project.
 *
 *   npx meshcall-init [dir] [--force] [--json]
 */
import { installSkill, SKILL_PATH } from "./skill.js";

const args = process.argv.slice(2);
const dir = args.find((a) => !a.startsWith("-")) ?? ".";
try {
  const report = installSkill(dir, { force: args.includes("--force") });
  if (args.includes("--json")) {
    process.stdout.write(JSON.stringify({ skill: report.skill, agents_md: report.agents, next_steps: report.nextSteps }) + "\n");
  } else {
    process.stdout.write(`${report.skill.padEnd(9)} ${SKILL_PATH}\n${report.agents.padEnd(9)} AGENTS.md\n`);
    if (report.skill === "kept") {
      process.stdout.write("the existing skill differs (edited, or from another version); rerun with --force to replace it\n");
    }
    for (const step of report.nextSteps) process.stdout.write("- " + step + "\n");
  }
} catch (error) {
  process.stderr.write(String(error instanceof Error ? error.message : error) + "\n");
  process.exit(1);
}
