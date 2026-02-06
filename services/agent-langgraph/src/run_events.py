from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv
from logger import get_logger

logger = get_logger(__name__)


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def emit_run_event(run_id: str, event_type: str, payload: Dict[str, Any]) -> None:
    _load_env()
    dsn = os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO run_events(run_id, type, ts, payload) VALUES (%s, %s, now(), %s::jsonb)",
                (run_id, event_type, Jsonb(payload)),
            )
    logger.info("Run event emitted", extra={"run_id": run_id, "type": event_type})
