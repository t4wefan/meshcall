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
	if r, _ = Install(dir, false); r.Skill != Kept {
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
		strings.Contains(string(text), "stale") || strings.Count(string(text), beginMarker) != 1 {
		t.Fatalf("section not replaced in place:\n%s", text)
	}
}
