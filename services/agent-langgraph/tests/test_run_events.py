from __future__ import annotations

import run_events


def test_emit_run_event_is_best_effort_and_still_notifies_progress(monkeypatch) -> None:
    stages: list[str] = []

    monkeypatch.setattr(
        run_events.psycopg,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("database unavailable")),
    )
    monkeypatch.setattr(run_events, "notify_progress", stages.append)

    run_events.emit_run_event("run-1", "PLANNER_STARTED", {"component": "planner"})

    assert stages == ["PLANNER_STARTED"]
