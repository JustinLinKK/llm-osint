from __future__ import annotations

import threading
import time

from mcp_client import McpCallResult, RoutedMcpClient, StreamableHttpMcpClient


class _FakeResponse:
    status_code = 200

    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.headers = {}

    def json(self) -> dict:
        return self._payload


def test_streamable_http_mcp_client_reports_progress_for_tool_calls(monkeypatch) -> None:
    stages: list[str] = []

    client = StreamableHttpMcpClient(server_url="http://example.test/mcp")
    client._started = True

    monkeypatch.setattr("mcp_client.notify_progress", stages.append)
    monkeypatch.setattr(
        client,
        "_post",
        lambda *_, **__: _FakeResponse(
            {
                "jsonrpc": "2.0",
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": "{\"results\": [{\"document_id\": \"doc-1\", \"snippet\": \"evidence\"}]}",
                        }
                    ]
                },
            }
        ),
    )

    result = client.call_tool("vector_search", {"runId": "run-1", "query": "ada", "k": 3})

    assert result.ok is True
    assert stages == ["MCP_TOOL_CALL:vector_search", "MCP_TOOL_RETURNED:vector_search"]


def test_streamable_http_mcp_client_reports_progress_while_http_waits(monkeypatch) -> None:
    client = StreamableHttpMcpClient(server_url="http://example.test/mcp")
    stages: list[str] = []

    monkeypatch.setattr("mcp_client.progress_keepalive_seconds", lambda: 0.01)
    monkeypatch.setattr("mcp_client.notify_progress", stages.append)

    def _slow_post(*_, **__) -> _FakeResponse:
        time.sleep(0.05)
        return _FakeResponse({"jsonrpc": "2.0", "result": {}})

    monkeypatch.setattr("mcp_client.requests.post", _slow_post)

    response = client._post(
        {"method": "tools/call", "params": {"name": "vector_search"}},
        include_session=False,
        include_protocol=False,
        timeout_seconds=0.2,
    )

    assert response is not None
    assert "MCP_TOOL_WAIT:vector_search" in stages


def test_streamable_http_mcp_client_handles_non_requests_exceptions(monkeypatch) -> None:
    client = StreamableHttpMcpClient(server_url="http://example.test/mcp")

    monkeypatch.setattr(
        "mcp_client.requests.post",
        lambda *_, **__: (_ for _ in ()).throw(RuntimeError("socket exploded")),
    )

    response = client._post(
        {"method": "tools/call", "params": {"name": "vector_search"}},
        include_session=False,
        include_protocol=False,
        timeout_seconds=0.1,
    )

    assert response is None


def test_routed_mcp_client_starts_each_server_once_under_parallel_calls(monkeypatch) -> None:
    client = RoutedMcpClient()
    started = []

    class _FakeServer:
        def start(self) -> None:
            started.append("start")
            time.sleep(0.02)

        def close(self) -> None:
            return None

        def call_tool(self, name: str, arguments: dict) -> McpCallResult:
            return McpCallResult(ok=True, content={"tool": name, "arguments": arguments}, raw={})

    fake_server = _FakeServer()
    client._clients = {"normal": fake_server}
    client._tool_server_map = {"vector_search": "normal"}
    client._started_servers = set()
    client._startup_locks = {"normal": threading.Lock()}

    results: list[dict] = []

    def _invoke() -> None:
        result = client.call_tool("vector_search", {"query": "ada"})
        results.append(result.content)

    threads = [threading.Thread(target=_invoke), threading.Thread(target=_invoke)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert started == ["start"]
    assert len(results) == 2
