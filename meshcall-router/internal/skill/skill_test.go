package skill

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallIsIdempotentAndKeepsEdits(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte("[project]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := Install(dir, false)
	if err != nil || r.Skill != Created || r.Agents != Created {
		t.Fatalf("first install: %+v %v", r, err)
	}
	if !strings.Contains(strings.Join(r.NextSteps, "\n"), "uv add meshcall") {
		t.Fatalf("expected Python next steps, got %v", r.NextSteps)
	}
	skill, _ := os.ReadFile(filepath.Join(dir, filepath.FromSlash(SkillPath)))
	if !strings.HasPrefix(string(skill), "---\nname: meshcall\n") {
		t.Fatalf("skill frontmatter missing: %q", string(skill[:40]))
	}

	r, _ = Install(dir, false)
	if r.Skill != Unchanged || r.Agents != Unchanged {
		t.Fatalf("second install should change nothing: %+v", r)
	}

	// A local edit is kept without --force, replaced with it.
	path := filepath.Join(dir, filepath.FromSlash(SkillPath))
	_ = os.WriteFile(path, []byte("edited"), 0o644)
	if r, _ = Install(dir, false); r.Skill != Kept || !r.HasKept() {
		t.Fatalf("expected Kept, got %+v", r)
	}
	if r, _ = Install(dir, true); r.Skill != Updated {
		t.Fatalf("expected Updated, got %+v", r)
	}

	// The AGENTS.md section is replaced in place, surrounding text untouched.
	agents := filepath.Join(dir, "AGENTS.md")
	_ = os.WriteFile(agents, []byte("# Mine\n\nkeep this\n\n"+beginMarker+"\nstale\n"+endMarker+"\n\nand this\n"), 0o644)
	if r, _ = Install(dir, false); r.Agents != Updated {
		t.Fatalf("expected section update, got %+v", r)
	}
	text, _ := os.ReadFile(agents)
	if !strings.Contains(string(text), "keep this") || !strings.Contains(string(text), "and this") ||
		strings.Contains(string(text), "stale") || strings.Count(string(text), beginMarker) != 1 ||
		!strings.Contains(string(text), ".agents/skills/meshcall/SKILL.md") {
		t.Fatalf("section not replaced in place:\n%s", text)
	}
}

func TestInstallForTargetedAgents(t *testing.T) {
	// Claude target -> .claude/skills/meshcall/SKILL.md + CLAUDE.md
	claudeDir := t.TempDir()
	r, err := InstallForAgents(claudeDir, []string{"claude"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Files) != 2 || r.Files[0].Path != ".claude/skills/meshcall/SKILL.md" || r.Files[1].Path != "CLAUDE.md" {
		t.Fatalf("unexpected claude files: %+v", r.Files)
	}
	claudeMD, _ := os.ReadFile(filepath.Join(claudeDir, "CLAUDE.md"))
	if !strings.Contains(string(claudeMD), ".claude/skills/meshcall/SKILL.md") {
		t.Fatalf("CLAUDE.md missing claude skill pointer:\n%s", claudeMD)
	}

	// Cursor target -> .cursor/rules/meshcall.mdc (with MDC frontmatter) + AGENTS.md
	cursorDir := t.TempDir()
	r, err = InstallForAgents(cursorDir, []string{"cursor"}, false)
	if err != nil {
		t.Fatal(err)
	}
	mdc, err := os.ReadFile(filepath.Join(cursorDir, ".cursor/rules/meshcall.mdc"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(mdc), "---\ndescription: ") || !strings.Contains(string(mdc), "\nalwaysApply: false\n---\n") {
		t.Fatalf("invalid Cursor MDC frontmatter:\n%s", string(mdc[:160]))
	}
	cursorAgents, _ := os.ReadFile(filepath.Join(cursorDir, "AGENTS.md"))
	if !strings.Contains(string(cursorAgents), ".cursor/rules/meshcall.mdc") {
		t.Fatalf("AGENTS.md should point at .cursor/rules/meshcall.mdc:\n%s", cursorAgents)
	}

	// All target -> 5 skill/rule files + 3 instruction files (AGENTS.md, CLAUDE.md, .github/copilot-instructions.md)
	allDir := t.TempDir()
	r, err = InstallForAgents(allDir, []string{"all"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Files) != 8 {
		t.Fatalf("expected 8 files for --agent all, got %d: %+v", len(r.Files), r.Files)
	}
	allAgents, _ := os.ReadFile(filepath.Join(allDir, "AGENTS.md"))
	if !strings.Contains(string(allAgents), ".agents/skills/meshcall/SKILL.md") {
		t.Fatalf("AGENTS.md in --agent all should point at universal skill:\n%s", allAgents)
	}
	copilotInstr, _ := os.ReadFile(filepath.Join(allDir, ".github/copilot-instructions.md"))
	if !strings.Contains(string(copilotInstr), ".github/skills/meshcall/SKILL.md") {
		t.Fatalf("copilot-instructions.md missing copilot skill pointer:\n%s", copilotInstr)
	}

	// Unknown agent error
	if _, err := InstallForAgents(t.TempDir(), []string{"unknown-agent"}, false); err == nil {
		t.Fatal("expected error for unknown agent")
	}
}
