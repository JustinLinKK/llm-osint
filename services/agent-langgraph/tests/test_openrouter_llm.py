from __future__ import annotations

import time

import pytest
import requests

from openrouter_llm import OpenRouterLLM


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.text = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def test_complete_json_returns_parsed_payload(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setattr(
        "openrouter_llm.requests.post",
        lambda *_, **__: _FakeResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": "{\"title\": \"Frederick Pi profile\"}"
                        }
                    }
                ]
            }
        ),
    )

    llm = OpenRouterLLM()
    parsed = llm.complete_json("system", {"prompt": "test"}, timeout=1)

    assert parsed == {"title": "Frederick Pi profile"}


def test_complete_json_enforces_wall_clock_timeout(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    events: list[tuple[str, dict]] = []

    def _slow_post(*_, **__) -> _FakeResponse:
        time.sleep(0.2)
        return _FakeResponse({"choices": [{"message": {"content": "{}"}}]})

    monkeypatch.setattr("openrouter_llm.requests.post", _slow_post)
    monkeypatch.setattr("openrouter_llm.emit_run_event", lambda run_id, event_type, payload: events.append((event_type, payload)))

    llm = OpenRouterLLM()

    with pytest.raises(requests.exceptions.Timeout):
        llm.complete_json("system", {"prompt": "test"}, timeout=0.01, run_id="run-1", operation="test.timeout")

    assert [event_type for event_type, _ in events] == ["LLM_CALL_STARTED", "LLM_CALL_FAILED"]
    assert "wall-clock timeout" in events[-1][1]["error"]


def test_plan_tools_uses_env_configured_timeout(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_TIMEOUT_SECONDS", "90")
    monkeypatch.setenv("OPENROUTER_PLANNER_TIMEOUT_SECONDS", "150")
    captured: dict = {}

    def _fake_post(*_, **kwargs) -> _FakeResponse:
        captured["timeout"] = kwargs.get("timeout")
        return _FakeResponse({"choices": [{"message": {"content": "{\"plan\": []}"}}]})

    monkeypatch.setattr("openrouter_llm.requests.post", _fake_post)

    llm = OpenRouterLLM()
    llm.plan_tools("prompt", ["Ada"], [])

    assert captured["timeout"] == (10.0, 150.0)


def test_complete_json_reports_progress_while_waiting(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    stages: list[str] = []

    def _slow_post(*_, **__) -> _FakeResponse:
        time.sleep(0.05)
        return _FakeResponse({"choices": [{"message": {"content": "{\"ok\": true}"}}]})

    monkeypatch.setattr("openrouter_llm.requests.post", _slow_post)
    monkeypatch.setattr("openrouter_llm.progress_keepalive_seconds", lambda: 0.01)
    monkeypatch.setattr("openrouter_llm.notify_progress", stages.append)

    llm = OpenRouterLLM()
    parsed = llm.complete_json("system", {"prompt": "test"}, timeout=0.2, operation="test.keepalive")

    assert parsed == {"ok": True}
    assert "LLM_CALL_WAIT:test.keepalive" in stages


def test_complete_json_surfaces_http_response_body(monkeypatch) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")

    class _BadResponse(_FakeResponse):
        def __init__(self) -> None:
            super().__init__({})
            self.text = "{\"error\":\"provider rejected payload\"}"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("400 Client Error: Bad Request for url")

    monkeypatch.setattr("openrouter_llm.requests.post", lambda *_, **__: _BadResponse())

    llm = OpenRouterLLM()
    with pytest.raises(requests.HTTPError) as excinfo:
        llm.complete_json("system", {"prompt": "test"}, timeout=1)

    assert "provider rejected payload" in str(excinfo.value)
