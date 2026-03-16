from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

import psycopg
from dotenv import load_dotenv


INLINE_CITATION_REGEX = re.compile(r"\[[A-Z0-9_]+\]")
TEMPLATE_PREFIXES = (
    "Section:",
    "Objective:",
    "Section group:",
    "Graph chain:",
    "Next step:",
    "Revision focus:",
)


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_dsn() -> str:
    _load_env()
    return os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", str(text or "")))


def _citation_count(text: str) -> int:
    return len(INLINE_CITATION_REGEX.findall(str(text or "")))


def _is_template_fallback(text: str) -> bool:
    normalized = str(text or "").strip()
    return bool(normalized) and any(normalized.startswith(prefix) for prefix in TEMPLATE_PREFIXES)


def _load_run_snapshot(cur: psycopg.Cursor[Any], run_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT report_type, status, refine_round, quality_ok, updated_at
        FROM report_runs
        WHERE run_id = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    snapshot: Dict[str, Any] = {
        "report_type": None,
        "status": None,
        "refine_round": None,
        "quality_ok": None,
        "updated_at": None,
    }
    if row:
        snapshot.update(
            {
                "report_type": row[0],
                "status": row[1],
                "refine_round": row[2],
                "quality_ok": row[3],
                "updated_at": row[4].isoformat() if row[4] is not None else None,
            }
        )
    return snapshot


def _load_sections(cur: psycopg.Cursor[Any], run_id: str) -> list[Dict[str, Any]]:
    cur.execute(
        """
        SELECT section_id, title, content, citation_keys
        FROM section_drafts
        WHERE run_id = %s
        ORDER BY section_order ASC, section_id ASC
        """,
        (run_id,),
    )
    sections: list[Dict[str, Any]] = []
    for section_id, title, content, citation_keys in cur.fetchall():
        word_count = _word_count(content or "")
        inline_citations = _citation_count(content or "")
        sections.append(
            {
                "section_id": section_id,
                "title": title,
                "word_count": word_count,
                "char_count": len(content or ""),
                "inline_citation_count": inline_citations,
                "citation_density_per_100_words": round((inline_citations / max(1, word_count)) * 100.0, 3),
                "stored_citation_keys": len(citation_keys or []),
                "template_fallback": _is_template_fallback(content or ""),
            }
        )
    return sections


def _load_evidence_summary(cur: psycopg.Cursor[Any], run_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT section_id, COUNT(*), COUNT(source_url)
        FROM evidence_refs
        WHERE run_id = %s
        GROUP BY section_id
        ORDER BY section_id ASC
        """,
        (run_id,),
    )
    per_section = [
        {
            "section_id": section_id,
            "evidence_count": evidence_count,
            "source_url_count": source_url_count,
        }
        for section_id, evidence_count, source_url_count in cur.fetchall()
    ]
    total_evidence = sum(item["evidence_count"] for item in per_section)
    return {
        "total_evidence_refs": total_evidence,
        "sections": per_section,
    }


def _load_llm_event_summary(cur: psycopg.Cursor[Any], run_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT type, payload
        FROM run_events
        WHERE run_id = %s
          AND type IN ('LLM_CALL_STARTED', 'LLM_CALL_COMPLETED', 'LLM_CALL_FAILED', 'STAGE2_NODE_COMPLETED')
        ORDER BY ts ASC
        """,
        (run_id,),
    )
    by_model_operation: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "started": 0,
            "completed": 0,
            "failed": 0,
            "timeout_failures": 0,
            "last_section_id": None,
        }
    )
    reflection_events: list[Dict[str, Any]] = []
    for event_type, payload in cur.fetchall():
        if not isinstance(payload, dict):
            continue
        if event_type == "STAGE2_NODE_COMPLETED" and payload.get("stage") == "final_reflection_node":
            reflection_events.append(
                {
                    "quality_ok": bool(payload.get("quality_ok", False)),
                    "targeted_count": int(payload.get("targeted_count", 0) or 0),
                }
            )
            continue
        model = str(payload.get("model") or "unknown")
        operation = str(payload.get("operation") or "unknown")
        bucket = by_model_operation[(model, operation)]
        bucket["last_section_id"] = payload.get("sectionId") or bucket["last_section_id"]
        if event_type == "LLM_CALL_STARTED":
            bucket["started"] += 1
        elif event_type == "LLM_CALL_COMPLETED":
            bucket["completed"] += 1
        elif event_type == "LLM_CALL_FAILED":
            bucket["failed"] += 1
            error = str(payload.get("error") or "")
            if "timeout" in error.casefold():
                bucket["timeout_failures"] += 1

    operations = []
    for (model, operation), stats in sorted(by_model_operation.items()):
        started = int(stats["started"])
        completed = int(stats["completed"])
        failed = int(stats["failed"])
        operations.append(
            {
                "model": model,
                "operation": operation,
                "started": started,
                "completed": completed,
                "failed": failed,
                "timeout_failures": int(stats["timeout_failures"]),
                "completion_rate": round(completed / max(1, started), 4),
                "failure_rate": round(failed / max(1, started), 4),
                "timeout_rate": round(int(stats["timeout_failures"]) / max(1, started), 4),
                "last_section_id": stats["last_section_id"],
            }
        )
    return {"operations": operations, "final_reflection_events": reflection_events}


def evaluate_run(run_id: str) -> Dict[str, Any]:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            snapshot = _load_run_snapshot(cur, run_id)
            sections = _load_sections(cur, run_id)
            evidence = _load_evidence_summary(cur, run_id)
            llm = _load_llm_event_summary(cur, run_id)

    total_words = sum(item["word_count"] for item in sections)
    total_citations = sum(item["inline_citation_count"] for item in sections)
    template_sections = [item["section_id"] for item in sections if item["template_fallback"]]
    return {
        "run_id": run_id,
        "report": snapshot,
        "llm": llm,
        "sections": sections,
        "evidence": evidence,
        "summary": {
            "section_count": len(sections),
            "total_words": total_words,
            "total_inline_citations": total_citations,
            "citation_density_per_100_words": round((total_citations / max(1, total_words)) * 100.0, 3),
            "template_fallback_sections": template_sections,
            "template_fallback_rate": round(len(template_sections) / max(1, len(sections)), 4),
            "parse_fallback_rate_estimate": round(len(template_sections) / max(1, len(sections)), 4),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Stage 2 model behavior for persisted runs.")
    parser.add_argument("--run-id", action="append", required=True, dest="run_ids")
    args = parser.parse_args()

    payload = {"runs": [evaluate_run(run_id) for run_id in args.run_ids]}
    print(json.dumps(payload, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
