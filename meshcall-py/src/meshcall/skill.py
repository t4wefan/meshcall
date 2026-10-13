"""Install the first-party MeshCall skill into a project for coding agents.

Mirrors ``meshcall-router/internal/skill`` and ``meshcall-ts/src/skill.ts``;
the three embedded ``SKILL.md`` copies are kept identical to
``skills/meshcall/SKILL.md`` by ``scripts/regenerate.sh``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from importlib import resources
from pathlib import Path

SKILL_PATH = Path(".claude/skills/meshcall/SKILL.md")

_BEGIN = "<!-- meshcall:begin -->"
_END = "<!-- meshcall:end -->"

SECTION = f"""{_BEGIN}
## MeshCall

This project uses MeshCall (typed RPC through a Router). Read
`{SKILL_PATH.as_posix()}` before touching services or clients.
Explore a running mesh with `meshcall router inspect`, call with
`meshcall call svc.method '{{json}}'`, and set `MESHCALL_STRICT=1` while
editing a service so contract warnings fail with a hint.
{_END}
"""


def skill_content() -> str:
    return (
        resources.files("meshcall")
        .joinpath("skills/meshcall/SKILL.md")
        .read_text(encoding="utf-8")
    )


class Outcome(StrEnum):
    CREATED = "created"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    #: An edited or older file was left alone; rerun with ``force``.
    KEPT = "kept"


@dataclass
class Report:
    skill: Outcome
    agents: Outcome
    next_steps: list[str] = field(default_factory=list)


def install(directory: Path, *, force: bool = False) -> Report:
    """Write the skill and the AGENTS.md section; keep local edits unless forced."""
    skill = _write_skill(directory / SKILL_PATH, force)
    agents = _write_section(directory / "AGENTS.md")
    return Report(skill=skill, agents=agents, next_steps=_next_steps(directory))


def _write_skill(path: Path, force: bool) -> Outcome:
    content = skill_content()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return Outcome.CREATED
    if path.read_text(encoding="utf-8") == content:
        return Outcome.UNCHANGED
    if not force:
        return Outcome.KEPT
    path.write_text(content, encoding="utf-8")
    return Outcome.UPDATED


def _write_section(path: Path) -> Outcome:
    if not path.exists():
        path.write_text("# Agent instructions\n\n" + SECTION, encoding="utf-8")
        return Outcome.CREATED
    text = path.read_text(encoding="utf-8")
    begin, end = text.find(_BEGIN), text.find(_END)
    if begin >= 0 and end > begin:
        updated = text[:begin] + SECTION.rstrip("\n") + text[end + len(_END) :]
    else:
        updated = text.rstrip("\n") + "\n\n" + SECTION
    if updated == text:
        return Outcome.UNCHANGED
    path.write_text(updated, encoding="utf-8")
    return Outcome.UPDATED


def _next_steps(directory: Path) -> list[str]:
    steps: list[str] = []
    if (directory / "pyproject.toml").exists():
        steps.append(
            "Python: `uv add meshcall`, then `uv run meshcall router install` "
            "for the Router binary"
        )
    if (directory / "package.json").exists():
        steps.append(
            "TypeScript: `npm install meshcall`, then `npx meshcall-router-install` "
            "for the Router binary"
        )
    if not steps:
        steps.append(
            "Add the SDK: `uv add meshcall` (Python) or `npm install meshcall` "
            "(TypeScript), then install the Router binary"
        )
    steps.append(
        f"Agents read {SKILL_PATH.as_posix()} automatically; AGENTS.md now points at it"
    )
    return steps
