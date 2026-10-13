import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { installSkill, SKILL_PATH, skillContent } from "../src/index.js";

test("installSkill is idempotent and keeps local edits", () => {
  const dir = mkdtempSync(join(tmpdir(), "meshcall-skill-"));
  writeFileSync(join(dir, "package.json"), "{}");
  let report = installSkill(dir);
  assert.deepEqual([report.skill, report.agents], ["created", "created"]);
  assert.ok(report.nextSteps.some((s) => s.includes("npm install meshcall")));
  const skill = join(dir, SKILL_PATH);
  assert.ok(readFileSync(skill, "utf8").startsWith("---\nname: meshcall\n"));
  assert.equal(readFileSync(skill, "utf8"), skillContent());

  report = installSkill(dir);
  assert.deepEqual([report.skill, report.agents], ["unchanged", "unchanged"]);

  writeFileSync(skill, "edited");
  assert.equal(installSkill(dir).skill, "kept");
  assert.equal(installSkill(dir, { force: true }).skill, "updated");

  const agents = join(dir, "AGENTS.md");
  writeFileSync(agents, "# Mine\n\nkeep this\n\n<!-- meshcall:begin -->\nstale\n<!-- meshcall:end -->\n\nand this\n");
  assert.equal(installSkill(dir).agents, "updated");
  const text = readFileSync(agents, "utf8");
  assert.ok(text.includes("keep this") && text.includes("and this") && !text.includes("stale"));
  assert.equal(text.split("<!-- meshcall:begin -->").length, 2);
});
