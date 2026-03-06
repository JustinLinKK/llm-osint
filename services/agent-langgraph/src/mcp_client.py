from __future__ import annotations

import json
import os
import subprocess
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol

import requests

from logger import get_logger
from run_monitor import notify_progress

logger = get_logger(__name__)


@dataclass
class McpCallResult:
    ok: bool
    content: Dict[str, Any]
    raw: Dict[str, Any]


class McpClientProtocol(Protocol):
    def start(self) -> None:
        ...

    def close(self) -> None:
        ...

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> McpCallResult:
        ...


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

        container = os.getenv("MCP_SERVER_CONTAINER")
        if container:
            node_cmd = os.getenv("MCP_SERVER_CONTAINER_NODE", "node")
            entrypoint = os.getenv("MCP_SERVER_CONTAINER_ENTRYPOINT", "apps/mcp-server/dist/index.js")
            return ["docker", "exec", "-i", container, node_cmd, entrypoint]

        return ["node", "dist/index.js"]

    def start(self) -> None:
        if self._process is not None:
            return
        env = dict(os.environ)
        env.setdefault("DOTENV_CONFIG_QUIET", "true")
        self._process = subprocess.Popen(
            self._command,
            cwd=self._cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        logger.info("MCP client started", extra={"tool": "mcp_client", "cwd": self._cwd})

    def close(self) -> None:
        if self._process is None:
            return
        try:
            self._process.terminate()
        finally:
            self._process = None
            logger.info("MCP client closed", extra={"tool": "mcp_client"})

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
        logger.info("MCP tool call", extra={"tool": name, "request_id": request_id})
        notify_progress(f"MCP_TOOL_CALL:{name}")

        with self._lock:
            self._process.stdin.write(json.dumps(request) + "\n")
            self._process.stdin.flush()

            response = None
            last_line = ""
            for _ in range(8):
                line = self._process.stdout.readline()
                if not line:
                    break
                last_line = line.strip()
                if not last_line:
                    continue
                try:
                    response = json.loads(last_line)
                    break
                except json.JSONDecodeError:
                    continue

        if response is None:
            logger.error("MCP empty response", extra={"tool": name, "request_id": request_id})
            notify_progress(f"MCP_TOOL_RETURNED:{name}")
            return McpCallResult(ok=False, content={"error": "empty or invalid response", "raw": last_line}, raw={})
        if "error" in response:
            logger.error("MCP tool error", extra={"tool": name, "request_id": request_id, "error": response.get("error")})
            notify_progress(f"MCP_TOOL_RETURNED:{name}")
            return McpCallResult(ok=False, content=response["error"], raw=response)

        result = response.get("result", {})
        content = _parse_mcp_content(result)
        is_error = bool(result.get("isError"))
        if is_error:
            logger.error("MCP tool returned error", extra={"tool": name, "request_id": request_id})
        else:
            logger.info("MCP tool returned", extra={"tool": name, "request_id": request_id})
        notify_progress(f"MCP_TOOL_RETURNED:{name}")
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


class StreamableHttpMcpClient:
    def __init__(self, server_url: Optional[str] = None) -> None:
        self._server_url = server_url or os.getenv("MCP_SERVER_URL", "http://mcp-server:3001/mcp")
        self._protocol_version = os.getenv("MCP_PROTOCOL_VERSION", "2025-11-25")
        self._init_timeout_seconds = float(os.getenv("MCP_HTTP_INIT_TIMEOUT_SECONDS", "30"))
        self._request_timeout_seconds = float(os.getenv("MCP_HTTP_TIMEOUT_SECONDS", "300"))
        self._session_id: Optional[str] = None
        self._started = False

    def start(self) -> None:
        if self._started:
            return

        init_request = {
            "jsonrpc": "2.0",
            "id": str(uuid.uuid4()),
            "method": "initialize",
            "params": {
                "protocolVersion": self._protocol_version,
                "capabilities": {},
                "clientInfo": {"name": "agent-langgraph", "version": "1.0.0"},
            },
        }

        response = self._post(
            init_request,
            include_session=False,
            include_protocol=False,
            timeout_seconds=self._init_timeout_seconds,
        )
        if response is None:
            raise RuntimeError("MCP initialize returned no response")

        self._session_id = response.headers.get("mcp-session-id")
        body = response.json()

        if "error" in body:
            raise RuntimeError(f"MCP initialize failed: {body['error']}")

        self._started = True
        self._send_initialized_notification()
        logger.info(
            "MCP HTTP client started",
            extra={"tool": "mcp_client", "url": self._server_url, "session_id": self._session_id},
        )

    def close(self) -> None:
        if not self._started:
            return
        try:
            if self._session_id:
                headers = self._headers(include_session=True, include_protocol=True)
                requests.delete(self._server_url, headers=headers, timeout=10)
        finally:
            logger.info("MCP HTTP client closed", extra={"tool": "mcp_client"})
            self._started = False

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> McpCallResult:
        if not self._started:
            raise RuntimeError("MCP client not started")

        request_id = str(uuid.uuid4())
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        logger.info("MCP tool call", extra={"tool": name, "request_id": request_id})
        notify_progress(f"MCP_TOOL_CALL:{name}")

        response = self._post(
            request,
            include_session=True,
            include_protocol=True,
            timeout_seconds=self._request_timeout_seconds,
        )
        if response is None:
            logger.error("MCP empty response", extra={"tool": name, "request_id": request_id})
            notify_progress(f"MCP_TOOL_RETURNED:{name}")
            return McpCallResult(ok=False, content={"error": "empty response"}, raw={})

        try:
            payload = response.json()
        except ValueError:
            logger.error("MCP invalid JSON response", extra={"tool": name, "request_id": request_id})
            notify_progress(f"MCP_TOOL_RETURNED:{name}")
            return McpCallResult(ok=False, content={"error": "invalid JSON response"}, raw={})

        if "error" in payload:
            logger.error("MCP tool error", extra={"tool": name, "request_id": request_id, "error": payload.get("error")})
            notify_progress(f"MCP_TOOL_RETURNED:{name}")
            return McpCallResult(ok=False, content=payload.get("error", {}), raw=payload)

        result = payload.get("result", {})
        content = _parse_mcp_content(result)
        is_error = bool(result.get("isError"))
        if is_error:
            logger.error("MCP tool returned error", extra={"tool": name, "request_id": request_id})
        else:
            logger.info("MCP tool returned", extra={"tool": name, "request_id": request_id})
        notify_progress(f"MCP_TOOL_RETURNED:{name}")

        return McpCallResult(ok=not is_error, content=content, raw=payload)

    def _headers(self, include_session: bool, include_protocol: bool) -> Dict[str, str]:
        headers = {
            "content-type": "application/json",
            "accept": "application/json, text/event-stream",
        }
        if include_session and self._session_id:
            headers["mcp-session-id"] = self._session_id
        if include_protocol:
            headers["mcp-protocol-version"] = self._protocol_version
        return headers

    def _post(
        self,
        payload: Dict[str, Any],
        include_session: bool,
        include_protocol: bool,
        timeout_seconds: Optional[float] = None,
    ) -> Optional[requests.Response]:
        headers = self._headers(include_session=include_session, include_protocol=include_protocol)
        timeout = timeout_seconds if timeout_seconds is not None else self._request_timeout_seconds
        try:
            response = requests.post(self._server_url, headers=headers, json=payload, timeout=timeout)
        except requests.RequestException as exc:
            logger.error(
                "MCP HTTP request failed",
                extra={"tool": "mcp_client", "url": self._server_url, "error": str(exc)},
            )
            return None
        if response.status_code == 202:
            return None
        return response

    def _send_initialized_notification(self) -> None:
        notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }
        self._post(
            notification,
            include_session=True,
            include_protocol=True,
            timeout_seconds=self._init_timeout_seconds,
        )


def _load_tool_server_map() -> Dict[str, str]:
    default_map = {
        "fetch_url": "normal",
        "ingest_text": "normal",
        "ingest_graph_entity": "normal",
        "ingest_graph_entities": "normal",
        "ingest_graph_relations": "normal",
        "extract_webpage": "normal",
        "crawl_webpage": "normal",
        "map_webpage": "normal",
        "tavily_research": "normal",
        "tavily_person_search": "normal",
        "person_search": "normal",
        "x_get_user_posts_api": "normal",
        "linkedin_download_html_ocr": "normal",
        "google_serp_person_search": "normal",
        "arxiv_search_and_download": "normal",

        # Kali OSINT core baseline (low-noise defaults for automation).
        "osint_maigret_username": "kali",
        "osint_amass_domain": "kali",
        "osint_whatweb_target": "kali",
        "osint_exiftool_extract": "kali",

        # Kali enrichment tools (use with strong post-filtering).
        "osint_holehe_email": "kali",
        "osint_theharvester_email_domain": "kali",
        "osint_reconng_domain": "kali",
        "osint_spiderfoot_scan": "kali",
        "osint_sublist3r_domain": "kali",

        # Kali manual workflows.
        "osint_maltego_manual": "kali",
        "osint_foca_manual": "kali",

        # Kali deprioritized/legacy (avoid in automated baseline).
        "osint_sherlock_username": "kali",
        "osint_whatsmyname_username": "kali",
        "osint_phoneinfoga_number": "kali",
        "osint_dnsdumpster_domain": "kali",

        # Disabled by default (requires API key).
        # HIBP key: https://haveibeenpwned.com/API/Key
        # "osint_hibp_email": "kali",
        # Disabled by default (requires API key).
        # Shodan key: https://account.shodan.io/
        # "osint_shodan_host": "kali",
    }
    raw = os.getenv("MCP_TOOL_SERVER_MAP")
    if not raw:
        return default_map
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.error("Invalid MCP_TOOL_SERVER_MAP JSON; using defaults")
        return default_map
    if not isinstance(parsed, dict):
        logger.error("MCP_TOOL_SERVER_MAP must be a JSON object; using defaults")
        return default_map
    merged = dict(default_map)
    for key, value in parsed.items():
        if isinstance(key, str) and isinstance(value, str) and value in {"normal", "kali"}:
            merged[key] = value
    return merged


class RoutedMcpClient:
    def __init__(self) -> None:
        normal_url = os.getenv("MCP_SERVER_URL", "http://mcp-server:3001/mcp")
        kali_url = os.getenv("MCP_SERVER_KALI_URL", "http://mcp-server-kali:3002/mcp")
        self._clients: Dict[str, StreamableHttpMcpClient] = {
            "normal": StreamableHttpMcpClient(server_url=normal_url),
            "kali": StreamableHttpMcpClient(server_url=kali_url),
        }
        self._tool_server_map = _load_tool_server_map()
        self._started_servers: set[str] = set()

    def start(self) -> None:
        # Keep startup lightweight; connect lazily per route.
        return

    def close(self) -> None:
        for server_name in list(self._started_servers):
            client = self._clients.get(server_name)
            if client is not None:
                client.close()
        self._started_servers.clear()

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> McpCallResult:
        server_name = self._resolve_server_for_tool(name)
        client = self._clients.get(server_name)
        if client is None:
            return McpCallResult(
                ok=False,
                content={"error": f"No MCP client configured for server '{server_name}'"},
                raw={},
            )
        if server_name not in self._started_servers:
            client.start()
            self._started_servers.add(server_name)
            logger.info("MCP route started", extra={"tool": name, "server": server_name})

        logger.info("MCP tool routed", extra={"tool": name, "server": server_name})
        return client.call_tool(name, arguments)

    def _resolve_server_for_tool(self, tool_name: str) -> str:
        # Keep ingest/fetch on normal to centralize data-plane writes.
        if tool_name == "fetch_url" or tool_name.startswith("ingest_"):
            return "normal"
        # Route all OSINT wrappers to kali by convention.
        if tool_name.startswith("osint_"):
            return "kali"
        return self._tool_server_map.get(tool_name, "normal")
