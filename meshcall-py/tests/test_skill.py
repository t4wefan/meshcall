"""`meshcall init`: installs the agent skill idempotently and keeps local edits."""

from __future__ import annotations

from pathlib import Path

from meshcall.skill import SKILL_PATH, Outcome, install, skill_content


def test_install_is_idempotent_and_keeps_edits(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n")
    report = install(tmp_path)
    assert (report.skill, report.agents) == (Outcome.CREATED, Outcome.CREATED)
    assert any("uv add meshcall" in step for step in report.next_steps)
    skill = tmp_path / SKILL_PATH
    assert skill.read_text().startswith("---\nname: meshcall\n")
    assert skill.read_text() == skill_content()

    again = install(tmp_path)
    assert (again.skill, again.agents) == (Outcome.UNCHANGED, Outcome.UNCHANGED)

    skill.write_text("edited")
    assert install(tmp_path).skill is Outcome.KEPT
    assert install(tmp_path, force=True).skill is Outcome.UPDATED

    agents = tmp_path / "AGENTS.md"
    agents.write_text(
        "# Mine\n\nkeep this\n\n<!-- meshcall:begin -->\nstale\n<!-- meshcall:end -->\n\nand this\n"
    )
    assert install(tmp_path).agents is Outcome.UPDATED
    text = agents.read_text()
    assert "keep this" in text and "and this" in text and "stale" not in text
    assert text.count("<!-- meshcall:begin -->") == 1
