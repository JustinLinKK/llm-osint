from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
AGENT_SRC = REPO_ROOT / "services" / "agent-langgraph" / "src"
TOOLS_SRC = REPO_ROOT / "apps" / "mcp-server" / "src" / "tools" / "tools_python"

for path in (AGENT_SRC, TOOLS_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
