from __future__ import annotations

import os
from pathlib import Path
from typing import Any, List

import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_dsn() -> str:
    _load_env()
    return os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")


def persist_report_snapshot(
    run_id: str,
    report_type: str,
    status: str,
    refine_round: int,
    quality_ok: bool,
    final_report: str,
    evidence_appendix: str,
    section_drafts: List[Any],
    claim_ledger: List[Any],
    evidence_refs: List[Any],
) -> None:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO report_runs(run_id, report_type, status, refine_round, quality_ok, final_report, evidence_appendix, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (run_id) DO UPDATE SET
                        report_type = EXCLUDED.report_type,
                        status = EXCLUDED.status,
                        refine_round = EXCLUDED.refine_round,
                        quality_ok = EXCLUDED.quality_ok,
                        final_report = EXCLUDED.final_report,
                        evidence_appendix = EXCLUDED.evidence_appendix,
                        updated_at = now()
                    """,
                    (run_id, report_type, status, refine_round, quality_ok, final_report, evidence_appendix),
                )

                cur.execute("DELETE FROM section_drafts WHERE run_id = %s", (run_id,))
                cur.execute("DELETE FROM claim_ledger WHERE run_id = %s", (run_id,))
                cur.execute("DELETE FROM evidence_refs WHERE run_id = %s", (run_id,))

                section_rows = []
                for idx, section in enumerate(section_drafts):
                    citation_keys = getattr(section, "citation_keys", [])
                    section_rows.append(
                        (
                            run_id,
                            getattr(section, "section_id", None),
                            idx,
                            getattr(section, "title", None),
                            getattr(section, "content", ""),
                            Jsonb(citation_keys if isinstance(citation_keys, list) else []),
                        )
                    )
                if section_rows:
                    cur.executemany(
                        """
                        INSERT INTO section_drafts(
                            run_id, section_id, section_order, title, content, citation_keys
                        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        section_rows,
                    )

                claim_rows = []
                for claim in claim_ledger:
                    evidence_keys = getattr(claim, "evidence_keys", [])
                    conflict_flags = getattr(claim, "conflict_flags", [])
                    claim_rows.append(
                        (
                            run_id,
                            getattr(claim, "claim_id", None),
                            getattr(claim, "section_id", None),
                            getattr(claim, "text", ""),
                            getattr(claim, "confidence", 0.0),
                            getattr(claim, "impact", "medium"),
                            Jsonb(evidence_keys if isinstance(evidence_keys, list) else []),
                            Jsonb(conflict_flags if isinstance(conflict_flags, list) else []),
                        )
                    )
                if claim_rows:
                    cur.executemany(
                        """
                        INSERT INTO claim_ledger(
                            run_id, claim_id, section_id, claim_text, confidence, impact, evidence_keys, conflict_flags
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                        """,
                        claim_rows,
                    )

                evidence_rows = []
                for ref in evidence_refs:
                    object_ref = getattr(ref, "object_ref", {})
                    evidence_rows.append(
                        (
                            run_id,
                            getattr(ref, "citation_key", None),
                            getattr(ref, "section_id", None),
                            getattr(ref, "document_id", None),
                            getattr(ref, "snippet", ""),
                            getattr(ref, "source_url", None),
                            getattr(ref, "score", None),
                            Jsonb(object_ref if isinstance(object_ref, dict) else {}),
                        )
                    )
                if evidence_rows:
                    cur.executemany(
                        """
                        INSERT INTO evidence_refs(
                            run_id, citation_key, section_id, document_id, snippet, source_url, score, object_ref
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        evidence_rows,
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
