from __future__ import annotations

import os
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from logger import get_logger

logger = get_logger(__name__)

HeartbeatEmitter = Callable[[str, Dict[str, Any]], None]
STALL_TIMEOUT_BUFFER_SECONDS = 90.0

_ACTIVE_MONITOR: Optional["RunMonitor"] = None
_ACTIVE_MONITOR_LOCK = threading.Lock()


def _env_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return float(default)
    try:
        return float(raw_value)
    except ValueError:
        return float(default)


def progress_keepalive_seconds() -> float:
    return max(1.0, _env_float("LANGGRAPH_PROGRESS_KEEPALIVE_SECONDS", 10.0))


def default_stall_timeout_seconds() -> float:
    explicit = os.getenv("LANGGRAPH_STALL_TIMEOUT_SECONDS")
    if explicit is not None and explicit.strip():
        return max(1.0, _env_float("LANGGRAPH_STALL_TIMEOUT_SECONDS", 360.0))

    openrouter_timeout = max(0.1, _env_float("OPENROUTER_TIMEOUT_SECONDS", 120.0))
    planner_timeout = max(0.1, _env_float("OPENROUTER_PLANNER_TIMEOUT_SECONDS", openrouter_timeout))
    worker_timeout = max(0.1, _env_float("OPENROUTER_WORKER_TIMEOUT_SECONDS", openrouter_timeout))
    report_timeout = max(
        0.1,
        _env_float(
            "OPENROUTER_REPORT_TIMEOUT_SECONDS",
            _env_float(
                "OPENROUTER_PLANNER_TIMEOUT_SECONDS",
                _env_float("OPENROUTER_TIMEOUT_SECONDS", 400.0),
            ),
        ),
    )
    report_worker_timeout = max(
        0.1,
        _env_float(
            "OPENROUTER_REPORT_WORKER_TIMEOUT_SECONDS",
            _env_float(
                "OPENROUTER_WORKER_TIMEOUT_SECONDS",
                _env_float("OPENROUTER_TIMEOUT_SECONDS", 400.0),
            ),
        ),
    )
    title_timeout = max(
        0.1,
        _env_float("OPENROUTER_TITLE_TIMEOUT_SECONDS", min(openrouter_timeout, 45.0)),
    )
    mcp_init_timeout = max(0.1, _env_float("MCP_HTTP_INIT_TIMEOUT_SECONDS", 30.0))
    mcp_request_timeout = max(0.1, _env_float("MCP_HTTP_TIMEOUT_SECONDS", 300.0))

    largest_timeout = max(
        planner_timeout,
        worker_timeout,
        report_timeout,
        report_worker_timeout,
        title_timeout,
        mcp_init_timeout,
        mcp_request_timeout,
    )
    return largest_timeout + STALL_TIMEOUT_BUFFER_SECONDS


def wait_with_progress(duration_seconds: float, stage: Optional[str] = None) -> None:
    remaining = max(0.0, float(duration_seconds))
    if remaining <= 0:
        return

    cadence = progress_keepalive_seconds()
    while remaining > 0:
        sleep_for = min(cadence, remaining)
        time.sleep(sleep_for)
        remaining -= sleep_for
        if stage:
            notify_progress(stage)


@dataclass
class RunMonitor:
    run_id: str
    emit_event: HeartbeatEmitter
    heartbeat_interval_seconds: float = field(
        default_factory=lambda: float(os.getenv("LANGGRAPH_HEARTBEAT_INTERVAL_SECONDS", "30"))
    )
    stall_timeout_seconds: float = field(
        default_factory=default_stall_timeout_seconds
    )

    def __post_init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.monotonic()
        self._last_progress_at = self._started_at
        self._last_stage = "run_started"
        self._stalled_emitted = False
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=f"run-monitor-{self.run_id[:8]}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=1)

    def touch(self, stage: Optional[str] = None) -> None:
        with self._lock:
            self._last_progress_at = time.monotonic()
            if stage:
                self._last_stage = stage
            self._stalled_emitted = False

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            return {
                "runId": self.run_id,
                "pid": os.getpid(),
                "stage": self._last_stage,
                "uptimeSeconds": round(now - self._started_at, 3),
                "secondsSinceProgress": round(now - self._last_progress_at, 3),
                "stallTimeoutSeconds": self.stall_timeout_seconds,
            }

    def _loop(self) -> None:
        interval = max(5.0, self.heartbeat_interval_seconds)
        while not self._stop_event.wait(interval):
            snapshot = self.snapshot()
            try:
                self.emit_event("RUN_HEARTBEAT", snapshot)
            except Exception:
                logger.exception("Run heartbeat emission failed", extra={"run_id": self.run_id})

            if snapshot["secondsSinceProgress"] < self.stall_timeout_seconds:
                continue

            should_signal = False
            with self._lock:
                if not self._stalled_emitted:
                    self._stalled_emitted = True
                    should_signal = True

            if not should_signal:
                continue

            try:
                self.emit_event("RUN_STALLED", snapshot)
            except Exception:
                logger.exception("Run stalled emission failed", extra={"run_id": self.run_id})

            logger.error(
                "Run stalled watchdog triggered",
                extra={
                    "run_id": self.run_id,
                    "stage": snapshot["stage"],
                    "seconds_since_progress": snapshot["secondsSinceProgress"],
                },
            )
            os.kill(os.getpid(), signal.SIGTERM)


def set_active_monitor(monitor: Optional[RunMonitor]) -> None:
    global _ACTIVE_MONITOR
    with _ACTIVE_MONITOR_LOCK:
        _ACTIVE_MONITOR = monitor


def notify_progress(stage: Optional[str] = None) -> None:
    with _ACTIVE_MONITOR_LOCK:
        monitor = _ACTIVE_MONITOR
    if monitor is not None:
        monitor.touch(stage)
