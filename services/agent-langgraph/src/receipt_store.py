from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


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


def _get_dsn() -> str:
    _load_env()
    return os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")


@dataclass
class ArtifactRecord:
    artifact_id: str
    summary_id: Optional[str]


def insert_artifact(
    run_id: str,
    tool_name: str,
    kind: str,
    document_id: Optional[str] = None,
    bucket: Optional[str] = None,
    object_key: Optional[str] = None,
    version_id: Optional[str] = None,
    etag: Optional[str] = None,
    size_bytes: Optional[int] = None,
    content_type: Optional[str] = None,
    sha256: Optional[str] = None,
) -> str:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifacts(
                    run_id, document_id, tool_name, kind, bucket, object_key,
                    version_id, etag, size_bytes, content_type, sha256
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING artifact_id
                """,
                (
                    run_id,
                    document_id,
                    tool_name,
                    kind,
                    bucket,
                    object_key,
                    version_id,
                    etag,
                    size_bytes,
                    content_type,
                    sha256,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert artifact")
            logger.info("Artifact stored", extra={"tool": tool_name, "artifact_id": str(row[0])})
            return str(row[0])


def insert_artifact_summary(
    artifact_id: str,
    summary: str,
    key_facts: List[Dict[str, Any]],
    confidence: Optional[float] = None,
) -> str:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO artifact_summaries(artifact_id, summary, key_facts, confidence)
                VALUES (%s, %s, %s::jsonb, %s)
                RETURNING summary_id
                """,
                (
                    artifact_id,
                    summary,
                    Jsonb(key_facts),
                    confidence,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert artifact summary")
            logger.info("Artifact summary stored", extra={"summary_id": str(row[0])})
            return str(row[0])


def insert_tool_receipt(
    run_id: str,
    tool_name: str,
    ok: bool,
    arguments: Dict[str, Any],
    summary_id: Optional[str],
    artifact_ids: List[str],
    vector_upserts: Dict[str, Any],
    graph_upserts: Dict[str, Any],
    next_hints: List[str],
) -> str:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tool_call_receipts(
                    run_id, tool_name, ok, arguments, summary_id, artifact_ids,
                    vector_upserts, graph_upserts, next_hints
                ) VALUES (%s, %s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                RETURNING receipt_id
                """,
                (
                    run_id,
                    tool_name,
                    ok,
                    Jsonb(arguments),
                    summary_id,
                    Jsonb(artifact_ids),
                    Jsonb(vector_upserts),
                    Jsonb(graph_upserts),
                    Jsonb(next_hints),
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert tool receipt")
            logger.info("Tool receipt stored", extra={"tool": tool_name, "receipt_id": str(row[0])})
            return str(row[0])


def insert_run_note(run_id: str, note: str, citations: List[Dict[str, Any]]) -> str:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO run_notes(run_id, note, citations)
                VALUES (%s, %s, %s::jsonb)
                RETURNING note_id
                """,
                (
                    run_id,
                    note,
                    Jsonb(citations),
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to insert run note")
            logger.info("Run note stored", extra={"run_id": run_id, "note_id": str(row[0])})
            return str(row[0])
