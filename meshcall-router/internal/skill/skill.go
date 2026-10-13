// Package skill installs the first-party MeshCall skill into a project so
// coding agents learn the CLI workflow instead of guessing at frames.
//
// SKILL.md is copied from skills/meshcall/SKILL.md by scripts/regenerate.sh;
// the drift check keeps the three embedded copies (Go, Python, TypeScript)
// identical to the source.
package skill

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//go:embed SKILL.md
var Content string

// SkillPath is where agents (Claude Code and compatible tools) discover
// project skills.
const SkillPath = ".claude/skills/meshcall/SKILL.md"

const (
	beginMarker = "<!-- meshcall:begin -->"
	endMarker   = "<!-- meshcall:end -->"
)

// Section is the AGENTS.md block that points agents at the skill; it is
// replaced in place on re-runs so projects can keep their own text around it.
const Section = beginMarker + `
## MeshCall

This project uses MeshCall (typed RPC through a Router). Read
` + "`" + SkillPath + "`" + ` before touching services or clients.
Explore a running mesh with ` + "`meshcall router inspect`" + `, call with
` + "`meshcall call svc.method '{json}'`" + `, and set ` + "`MESHCALL_STRICT=1`" + ` while
editing a service so contract warnings fail with a hint.
` + endMarker + "\n"

// Outcome of one written file.
type Outcome string

const (
	Created   Outcome = "created"
	Updated   Outcome = "updated"
	Unchanged Outcome = "unchanged"
	// Kept means an edited or older file was left alone; rerun with force.
	Kept Outcome = "kept"
)

type Report struct {
	Skill     Outcome
	Agents    Outcome
	NextSteps []string
}

// Install writes the skill and the AGENTS.md section under dir. An existing
// skill file with different content is kept unless force is set, so local
// edits survive an upgrade until the user decides.
func Install(dir string, force bool) (Report, error) {
	var r Report
	skillFile := filepath.Join(dir, filepath.FromSlash(SkillPath))
	var err error
	if r.Skill, err = writeSkill(skillFile, force); err != nil {
		return r, err
	}
	if r.Agents, err = writeSection(filepath.Join(dir, "AGENTS.md")); err != nil {
		return r, err
	}
	r.NextSteps = nextSteps(dir)
	return r, nil
}

func writeSkill(path string, force bool) (Outcome, error) {
	existing, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return "", err
		}
		return Created, os.WriteFile(path, []byte(Content), 0o644)
	case err != nil:
		return "", err
	case string(existing) == Content:
		return Unchanged, nil
	case !force:
		return Kept, nil
	default:
		return Updated, os.WriteFile(path, []byte(Content), 0o644)
	}
}

func writeSection(path string) (Outcome, error) {
	existing, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Created, os.WriteFile(path, []byte("# Agent instructions\n\n"+Section), 0o644)
	}
	if err != nil {
		return "", err
	}
	text := string(existing)
	begin, end := strings.Index(text, beginMarker), strings.Index(text, endMarker)
	var updated string
	if begin >= 0 && end > begin {
		updated = text[:begin] + strings.TrimSuffix(Section, "\n") + text[end+len(endMarker):]
	} else {
		updated = strings.TrimRight(text, "\n") + "\n\n" + Section
	}
	if updated == text {
		return Unchanged, nil
	}
	return Updated, os.WriteFile(path, []byte(updated), 0o644)
}

// nextSteps tells the user what the skill assumes is installed, per ecosystem.
func nextSteps(dir string) []string {
	var steps []string
	if exists(filepath.Join(dir, "pyproject.toml")) {
		steps = append(steps, "Python: `uv add meshcall`, then `uv run meshcall router install` for the Router binary")
	}
	if exists(filepath.Join(dir, "package.json")) {
		steps = append(steps, "TypeScript: `npm install meshcall`, then `npx meshcall-router-install` for the Router binary")
	}
	if len(steps) == 0 {
		steps = append(steps, "Add the SDK: `uv add meshcall` (Python) or `npm install meshcall` (TypeScript), then install the Router binary")
	}
	return append(steps, fmt.Sprintf("Agents read %s automatically; AGENTS.md now points at it", SkillPath))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
