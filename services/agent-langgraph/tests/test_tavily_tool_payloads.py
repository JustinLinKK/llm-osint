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


def test_tavily_person_search_honors_query_override(monkeypatch, tmp_path) -> None:
    captured: dict = {}

    def _fake_request(api_url: str, payload: dict | None, *, timeout_seconds: int, method: str = "POST") -> dict:
        captured["payload"] = payload
        return {"query": payload.get("query"), "results": []}

    monkeypatch.setattr(unified_research_mcp, "_tavily_json_request", _fake_request)

    result = unified_research_mcp._tool_tavily_person_search(
        {
            "target_name": "Xinyu Frederick Pi",
            "query": "Find the public GitHub profile, account, or repositories associated with Xinyu Frederick Pi.",
            "output_dir": str(tmp_path),
        }
    )

    assert captured["payload"]["query"] == (
        "Find the public GitHub profile, account, or repositories associated with Xinyu Frederick Pi."
    )
    assert result["target_name"] == "Xinyu Frederick Pi"
    assert result["requested_query"] == captured["payload"]["query"]


def test_tavily_person_search_uses_advanced_defaults_and_normalizes_scores(monkeypatch, tmp_path) -> None:
    captured: dict = {}

    def _fake_request(api_url: str, payload: dict | None, *, timeout_seconds: int, method: str = "POST") -> dict:
        captured["payload"] = payload
        return {
            "query": payload.get("query"),
            "results": [
                {
                    "title": "Example profile",
                    "url": "https://example.com/profile",
                    "content": "Example snippet",
                    "raw_content": "Example raw content",
                    "score": 0.42,
                }
            ],
        }

    monkeypatch.setattr(unified_research_mcp, "_tavily_json_request", _fake_request)

    result = unified_research_mcp._tool_tavily_person_search(
        {
            "target_name": "Xinyu Pi",
            "output_dir": str(tmp_path),
        }
    )

    assert captured["payload"]["include_answer"] == "advanced"
    assert captured["payload"]["search_depth"] == "advanced"
    assert captured["payload"]["include_raw_content"] == "text"
    assert captured["payload"]["chunks_per_source"] == 3
    assert result["results"][0]["score"] == 4.2
    assert result["results"][0]["score_raw"] == 0.42
    assert result["summary"]["score_scale"] == "0-10 normalized from Tavily 0-1 scores"
    assert result["summary"]["chunks_per_source_requested"] == 5
    assert result["summary"]["chunks_per_source"] == 3


def test_extract_webpage_defaults_to_text_advanced_and_five_chunks(monkeypatch, tmp_path) -> None:
    captured: dict = {}

    def _fake_request(api_url: str, payload: dict | None, *, timeout_seconds: int, method: str = "POST") -> dict:
        captured["payload"] = payload
        return {"results": []}

    monkeypatch.setattr(unified_research_mcp, "_tavily_json_request", _fake_request)

    result = unified_research_mcp._tool_extract_webpage(
        {
            "url": "https://docs.tavily.com",
            "output_dir": str(tmp_path),
        }
    )

    assert result["results_found"] == 0
    assert captured["payload"]["extract_depth"] == "advanced"
    assert captured["payload"]["format"] == "text"
    assert captured["payload"]["chunks_per_source"] == 5
