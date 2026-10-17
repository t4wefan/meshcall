from __future__ import annotations

import pytest
from showcase_demo.run import run_demo


@pytest.mark.asyncio
async def test_complete_showcase_runs(capsys: pytest.CaptureFixture[str]) -> None:
    await run_demo()

    output = capsys.readouterr().out
    assert "query with expanded parameters" in output
    assert "query with a request object" in output
    assert "mutating call with metadata injection" in output
    assert "caller=showcase" in output
    assert "received=[3, 4, 5, 6]" in output
    assert "total=20" in output
    assert "services=['demo.v1.ShowcaseService']" in output
    assert "duplex" not in output
