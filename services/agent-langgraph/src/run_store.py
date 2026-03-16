from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

from report_models import PrimaryTargetContractModel


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_dsn() -> str:
    _load_env()
    return os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")


def load_run_constraints(run_id: str) -> Dict[str, Any]:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT constraints FROM runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
    if not row or not isinstance(row[0], dict):
        return {}
    return dict(row[0])


def load_primary_target_contract(run_id: str) -> PrimaryTargetContractModel:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT primary_target_contract FROM runs WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
    payload = row[0] if row and isinstance(row[0], dict) else {}
    return PrimaryTargetContractModel.model_validate(payload or {})


def ensure_run_exists(run_id: str, prompt: str, status: str = "created") -> None:
    dsn = _get_dsn()
    normalized_prompt = str(prompt or "").strip() or "Direct run bootstrap"
    normalized_status = str(status or "").strip() or "created"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runs(run_id, prompt, seeds, constraints, status)
                VALUES (%s, %s, '[]'::jsonb, '{}'::jsonb, %s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (run_id, normalized_prompt, normalized_status),
            )
            conn.commit()


def persist_primary_target_contract(run_id: str, contract: PrimaryTargetContractModel) -> None:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE runs
                SET primary_target_contract = %s::jsonb
                WHERE run_id = %s
                """,
                (Jsonb(contract.model_dump()), run_id),
            )
            conn.commit()
