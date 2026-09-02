from __future__ import annotations

import pytest
from demo.showcase import run_demo


@pytest.mark.asyncio
async def test_complete_showcase_runs(capsys: pytest.CaptureFixture[str]) -> None:
    await run_demo()

    output = capsys.readouterr().out
    assert "default unary with expanded parameters" in output
    assert "unary with a request object" in output
    assert "received=[3, 4, 5, 6]" in output
    assert "total=20" in output
    assert "generated client models and signatures" in output
    assert "duplex" not in output
