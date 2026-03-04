from __future__ import annotations

import unified_research_mcp


def test_crawl_webpage_uses_plural_instructions(monkeypatch, tmp_path) -> None:
    captured: dict = {}

    def _fake_request(api_url: str, payload: dict | None, *, timeout_seconds: int, method: str = "POST") -> dict:
        captured["api_url"] = api_url
        captured["payload"] = payload
        captured["timeout_seconds"] = timeout_seconds
        captured["method"] = method
        return {"results": []}

    monkeypatch.setattr(unified_research_mcp, "_tavily_json_request", _fake_request)

    result = unified_research_mcp._tool_crawl_webpage(
        {
            "url": "https://docs.tavily.com",
            "instructions": "Find all pages on the Python SDK",
            "output_dir": str(tmp_path),
        }
    )

    assert result["results_found"] == 0
    assert captured["method"] == "POST"
    assert captured["payload"]["instructions"] == "Find all pages on the Python SDK"
    assert "instruction" not in captured["payload"]


def test_map_webpage_accepts_legacy_instruction_alias(monkeypatch, tmp_path) -> None:
    captured: dict = {}

    def _fake_request(api_url: str, payload: dict | None, *, timeout_seconds: int, method: str = "POST") -> dict:
        captured["payload"] = payload
        return {"results": []}

    monkeypatch.setattr(unified_research_mcp, "_tavily_json_request", _fake_request)

    result = unified_research_mcp._tool_map_webpage(
        {
            "url": "https://docs.tavily.com",
            "instruction": "Find all pages on the Python SDK",
            "output_dir": str(tmp_path),
        }
    )

    assert result["results_found"] == 0
    assert captured["payload"]["instructions"] == "Find all pages on the Python SDK"
    assert "instruction" not in captured["payload"]
