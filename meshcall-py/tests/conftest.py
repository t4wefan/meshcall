from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEMO_PROJECTS = ("quickstart", "showcase", "router")
for import_root in (
    PROJECT_ROOT,
    *(PROJECT_ROOT / "demo" / name / "src" for name in DEMO_PROJECTS),
):
    import_root_string = str(import_root)
    if import_root_string not in sys.path:
        sys.path.insert(0, import_root_string)
