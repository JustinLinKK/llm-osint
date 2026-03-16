from __future__ import annotations

import run_monitor


def test_default_stall_timeout_uses_largest_external_timeout_plus_buffer(monkeypatch) -> None:
    monkeypatch.delenv("LANGGRAPH_STALL_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("OPENROUTER_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("OPENROUTER_PLANNER_TIMEOUT_SECONDS", "150")
    monkeypatch.setenv("OPENROUTER_WORKER_TIMEOUT_SECONDS", "200")
    monkeypatch.setenv("OPENROUTER_REPORT_TIMEOUT_SECONDS", "400")
    monkeypatch.setenv("OPENROUTER_REPORT_WORKER_TIMEOUT_SECONDS", "390")
    monkeypatch.setenv("OPENROUTER_TITLE_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("MCP_HTTP_INIT_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("MCP_HTTP_TIMEOUT_SECONDS", "300")

    assert run_monitor.default_stall_timeout_seconds() == 490.0


def test_wait_with_progress_touches_monitor_on_each_keepalive(monkeypatch) -> None:
    sleep_calls: list[float] = []
    stages: list[str] = []

    monkeypatch.setattr(run_monitor, "progress_keepalive_seconds", lambda: 5.0)
    monkeypatch.setattr(run_monitor.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr(run_monitor, "notify_progress", stages.append)

    run_monitor.wait_with_progress(12.0, "LONG_WAIT")

    assert sleep_calls == [5.0, 5.0, 2.0]
    assert stages == ["LONG_WAIT", "LONG_WAIT", "LONG_WAIT"]
