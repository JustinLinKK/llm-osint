from __future__ import annotations

from mcp_client import StreamableHttpMcpClient


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
