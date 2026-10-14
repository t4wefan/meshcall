// Package skill installs the first-party MeshCall skill into a project so
// coding agents learn the CLI workflow instead of guessing at frames.
//
// SKILL.md is copied from skills/meshcall/SKILL.md by scripts/regenerate.sh;
// the drift check keeps the embedded copy identical to the source.
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

// SkillPath is the default universal path where coding agents (OpenAI Codex,
// Google Antigravity, OpenCode, Cursor, Amp, etc.) discover project skills.
const SkillPath = ".agents/skills/meshcall/SKILL.md"

const (
	beginMarker = "<!-- meshcall:begin -->"
	endMarker   = "<!-- meshcall:end -->"
)

// Section is the default AGENTS.md block that points agents at SkillPath; it
// is replaced in place on re-runs so projects can keep their own text around it.
var Section = sectionFor(SkillPath)

func sectionFor(skillPath string) string {
	return beginMarker + `
## MeshCall

This project uses MeshCall (typed RPC through a Router). Read
` + "`" + skillPath + "`" + ` before touching services or clients.
Explore a running mesh with ` + "`meshcall router inspect`" + `, call with
` + "`meshcall call svc.method '{json}'`" + `, and set ` + "`MESHCALL_STRICT=1`" + ` while
editing a service so contract warnings fail with a hint.
` + endMarker + "\n"
}

// Target describes where a specific coding agent discovers skills and project
// instructions.
type Target struct {
	Name            string
	SkillPath       string
	InstructionPath string
	Transform       func(raw string) string
}

var targetsByName = map[string]Target{
	"universal": {
		Name:            "universal",
		SkillPath:       SkillPath,
		InstructionPath: "AGENTS.md",
	},
	"claude": {
		Name:            "claude",
		SkillPath:       ".claude/skills/meshcall/SKILL.md",
		InstructionPath: "CLAUDE.md",
	},
	"cursor": {
		Name:            "cursor",
		SkillPath:       ".cursor/rules/meshcall.mdc",
		InstructionPath: "AGENTS.md",
		Transform:       cursorMDC,
	},
	"copilot": {
		Name:            "copilot",
		SkillPath:       ".github/skills/meshcall/SKILL.md",
		InstructionPath: ".github/copilot-instructions.md",
	},
	"windsurf": {
		Name:            "windsurf",
		SkillPath:       ".windsurf/rules/meshcall.md",
		InstructionPath: "AGENTS.md",
	},
}

var allTargetNames = []string{"universal", "claude", "cursor", "copilot", "windsurf"}

// ResolveTargets normalizes user-supplied agent names (supporting comma-separated
// values, aliases like "agents"/"codex"/"antigravity"/"gemini", and "all").
// An empty input defaults to ["universal"].
func ResolveTargets(agents []string) ([]Target, error) {
	var tokens []string
	for _, raw := range agents {
		for _, part := range strings.Split(raw, ",") {
			if p := strings.ToLower(strings.TrimSpace(part)); p != "" {
				tokens = append(tokens, p)
			}
		}
	}
	if len(tokens) == 0 {
		tokens = []string{"universal"}
	}
	seen := map[string]bool{}
	var out []Target
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			out = append(out, targetsByName[name])
		}
	}
	for _, tok := range tokens {
		switch tok {
		case "all":
			for _, name := range allTargetNames {
				add(name)
			}
		case "universal", "agents", "agent", "codex", "antigravity", "gemini", "opencode", "default":
			add("universal")
		case "claude", "claude-code":
			add("claude")
		case "cursor":
			add("cursor")
		case "copilot", "github-copilot":
			add("copilot")
		case "windsurf":
			add("windsurf")
		default:
			return nil, fmt.Errorf("unknown --agent %q (supported: universal, claude, cursor, copilot, windsurf, all)", tok)
		}
	}
	return out, nil
}

// cursorMDC converts the YAML frontmatter of SKILL.md into Cursor's .mdc rule
// frontmatter while keeping the markdown body intact.
func cursorMDC(raw string) string {
	body := raw
	description := "Explore, call and build MeshCall services (typed streaming RPC between Python, TypeScript and the Go Router)."
	if strings.HasPrefix(raw, "---\n") {
		if end := strings.Index(raw[4:], "\n---\n"); end >= 0 {
			front := raw[4 : 4+end]
			body = strings.TrimLeft(raw[4+end+5:], "\n")
			for _, line := range strings.Split(front, "\n") {
				if after, ok := strings.CutPrefix(line, "description:"); ok {
					description = strings.TrimSpace(after)
				}
			}
		}
	}
	return fmt.Sprintf("---\ndescription: %s\nglobs:\nalwaysApply: false\n---\n\n%s", description, body)
}

// Outcome of one written file.
type Outcome string

const (
	Created   Outcome = "created"
	Updated   Outcome = "updated"
	Unchanged Outcome = "unchanged"
	// Kept means an edited or older file was left alone; rerun with force.
	Kept Outcome = "kept"
)

// FileStatus records the outcome for one written path relative to project root.
type FileStatus struct {
	Path    string  `json:"path"`
	Outcome Outcome `json:"outcome"`
}

type Report struct {
	AgentsList []string     `json:"agents"`
	Files      []FileStatus `json:"files"`
	Skill      Outcome      `json:"skill"`
	Agents     Outcome      `json:"agents_md"`
	NextSteps  []string     `json:"next_steps"`
}

// HasKept reports whether any skill file was left untouched because it differed
// and force was false.
func (r Report) HasKept() bool {
	for _, f := range r.Files {
		if f.Outcome == Kept {
			return true
		}
	}
	return false
}

// Install writes the default universal skill (.agents/skills/meshcall/SKILL.md)
// and the AGENTS.md section under dir.
func Install(dir string, force bool) (Report, error) {
	return InstallForAgents(dir, nil, force)
}

// InstallForAgents writes the skill files and instruction sections for the
// selected coding agents under dir. An existing skill file with different
// content is kept unless force is set, so local edits survive an upgrade.
func InstallForAgents(dir string, agents []string, force bool) (Report, error) {
	var r Report
	targets, err := ResolveTargets(agents)
	if err != nil {
		return r, err
	}
	for _, t := range targets {
		r.AgentsList = append(r.AgentsList, t.Name)
	}

	// Write each target's skill/rule file.
	var skillPaths []string
	for _, t := range targets {
		content := Content
		if t.Transform != nil {
			content = t.Transform(Content)
		}
		fullPath := filepath.Join(dir, filepath.FromSlash(t.SkillPath))
		outcome, err := writeSkillContent(fullPath, content, force)
		if err != nil {
			return r, err
		}
		if r.Skill == "" {
			r.Skill = outcome
		}
		r.Files = append(r.Files, FileStatus{Path: t.SkillPath, Outcome: outcome})
		skillPaths = append(skillPaths, t.SkillPath)
	}

	// Deduplicate instruction files (e.g. AGENTS.md shared by universal, cursor, windsurf).
	// If universal is among the targets for an instruction file, point at its skill path first.
	instrSkill := map[string]string{}
	var instrOrder []string
	for _, t := range targets {
		if _, exists := instrSkill[t.InstructionPath]; !exists {
			instrOrder = append(instrOrder, t.InstructionPath)
			instrSkill[t.InstructionPath] = t.SkillPath
		}
	}
	for _, instrRel := range instrOrder {
		fullPath := filepath.Join(dir, filepath.FromSlash(instrRel))
		outcome, err := writeSectionFor(fullPath, sectionFor(instrSkill[instrRel]))
		if err != nil {
			return r, err
		}
		if r.Agents == "" || instrRel == "AGENTS.md" {
			r.Agents = outcome
		}
		r.Files = append(r.Files, FileStatus{Path: instrRel, Outcome: outcome})
	}

	r.NextSteps = nextSteps(dir, skillPaths, instrOrder)
	return r, nil
}

func writeSkillContent(path string, content string, force bool) (Outcome, error) {
	existing, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return "", err
		}
		return Created, os.WriteFile(path, []byte(content), 0o644)
	case err != nil:
		return "", err
	case string(existing) == content:
		return Unchanged, nil
	case !force:
		return Kept, nil
	default:
		return Updated, os.WriteFile(path, []byte(content), 0o644)
	}
}

func writeSectionFor(path string, section string) (Outcome, error) {
	existing, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return "", err
		}
		return Created, os.WriteFile(path, []byte("# Agent instructions\n\n"+section), 0o644)
	}
	if err != nil {
		return "", err
	}
	text := string(existing)
	begin, end := strings.Index(text, beginMarker), strings.Index(text, endMarker)
	var updated string
	if begin >= 0 && end > begin {
		updated = text[:begin] + strings.TrimSuffix(section, "\n") + text[end+len(endMarker):]
	} else {
		updated = strings.TrimRight(text, "\n") + "\n\n" + section
	}
	if updated == text {
		return Unchanged, nil
	}
	return Updated, os.WriteFile(path, []byte(updated), 0o644)
}

// nextSteps tells the user what the skill assumes is installed, per ecosystem.
func nextSteps(dir string, skillPaths []string, instrPaths []string) []string {
	var steps []string
	if exists(filepath.Join(dir, "pyproject.toml")) {
		steps = append(steps, "Python: `uv add meshcall` (bundles the Router & CLI; `uv run meshcall router install` on fallback platforms)")
	}
	if exists(filepath.Join(dir, "package.json")) {
		steps = append(steps, "TypeScript: `yarn add meshcall` or `npm install meshcall` (bundles the Router & CLI)")
	}
	if len(steps) == 0 {
		steps = append(steps, "Add the SDK: `uv add meshcall` (Python) or `yarn add meshcall` / `npm install meshcall` (TypeScript)")
	}
	return append(steps, fmt.Sprintf("Agents discover %s automatically; %s now points at it",
		strings.Join(skillPaths, ", "), strings.Join(instrPaths, ", ")))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
