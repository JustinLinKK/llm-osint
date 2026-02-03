from __future__ import annotations

import json
import os
import subprocess
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class McpCallResult:
    ok: bool
    content: Dict[str, Any]
    raw: Dict[str, Any]


class StdioMcpClient:
    def __init__(self, command: Optional[list[str]] = None, cwd: Optional[str] = None) -> None:
        self._command = command or self._default_command()
        self._cwd = cwd or self._default_cwd()
        self._process: Optional[subprocess.Popen[str]] = None
        self._lock = threading.Lock()

    def _default_cwd(self) -> str:
        return str(Path(__file__).resolve().parents[3] / "apps" / "mcp-server")

    def _default_command(self) -> list[str]:
        env_cmd = os.getenv("MCP_SERVER_CMD")
        if env_cmd:
            return env_cmd.split(" ")
        return ["node", "dist/index.js"]

    def start(self) -> None:
        if self._process is not None:
            return
        self._process = subprocess.Popen(
            self._command,
            cwd=self._cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def close(self) -> None:
        if self._process is None:
            return
        try:
            self._process.terminate()
        finally:
            self._process = None

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> McpCallResult:
        if self._process is None or self._process.stdin is None or self._process.stdout is None:
            raise RuntimeError("MCP client not started")

        request_id = str(uuid.uuid4())
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }

        with self._lock:
            self._process.stdin.write(json.dumps(request) + "\n")
            self._process.stdin.flush()
            response_line = self._process.stdout.readline().strip()

        if not response_line:
            return McpCallResult(ok=False, content={"error": "empty response"}, raw={})

        response = json.loads(response_line)
        if "error" in response:
            return McpCallResult(ok=False, content=response["error"], raw=response)

        result = response.get("result", {})
        content = _parse_mcp_content(result)
        is_error = bool(result.get("isError"))
        return McpCallResult(ok=not is_error, content=content, raw=response)


def _parse_mcp_content(result: Dict[str, Any]) -> Dict[str, Any]:
    content_items = result.get("content", [])
    if not content_items:
        return {}

    first = content_items[0]
    if first.get("type") == "text":
        text = first.get("text", "")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"text": text}

    return {"content": content_items}
