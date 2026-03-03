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

_ACTIVE_MONITOR: Optional["RunMonitor"] = None
_ACTIVE_MONITOR_LOCK = threading.Lock()


@dataclass
class RunMonitor:
    run_id: str
    emit_event: HeartbeatEmitter
    heartbeat_interval_seconds: float = field(
        default_factory=lambda: float(os.getenv("LANGGRAPH_HEARTBEAT_INTERVAL_SECONDS", "30"))
    )
    stall_timeout_seconds: float = field(
        default_factory=lambda: float(os.getenv("LANGGRAPH_STALL_TIMEOUT_SECONDS", "360"))
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
