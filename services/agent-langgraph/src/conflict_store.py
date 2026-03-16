from __future__ import annotations

import os
from pathlib import Path
from typing import List

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from conflict_models import ConflictEvidenceRefModel, GraphConflictCaseModel


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_dsn() -> str:
    _load_env()
    return os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")


def persist_conflict_snapshot(run_id: str, conflict_cases: List[GraphConflictCaseModel]) -> None:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("DELETE FROM graph_conflict_cases WHERE run_id = %s", (run_id,))
                case_rows = []
                evidence_rows = []
                for case in conflict_cases:
                    case_rows.append(
                        (
                            case.case_id,
                            run_id,
                            case.target_type,
                            case.target_id or None,
                            case.field_name,
                            case.relation_type or None,
                            case.scope,
                            case.status,
                            bool(case.blocking),
                            case.chosen_value,
                            case.deterministic_winner,
                            float(case.confidence or 0.0),
                            case.rationale,
                            Jsonb([item.model_dump() for item in case.candidate_values]),
                            Jsonb(list(case.source_tools)),
                            Jsonb(list(case.source_domains)),
                            Jsonb(list(case.graph_entity_ids)),
                            Jsonb(list(case.graph_relation_ids)),
                            Jsonb(list(case.notes)),
                        )
                    )
                    for index, evidence in enumerate(case.evidence):
                        evidence_rows.append(
                            (
                                evidence.evidence_id or f"{case.case_id}:evidence:{index}",
                                run_id,
                                case.case_id,
                                evidence.candidate_value or None,
                                evidence.polarity,
                                evidence.document_id,
                                evidence.chunk_id,
                                Jsonb(evidence.object_ref if isinstance(evidence.object_ref, dict) else {}),
                                evidence.source_url,
                                evidence.source_domain,
                                evidence.snippet,
                                evidence.source_tool or None,
                                evidence.retrieved_at,
                                float(evidence.score or 0.0),
                            )
                        )

                if case_rows:
                    cur.executemany(
                        """
                        INSERT INTO graph_conflict_cases(
                            case_id, run_id, target_type, target_id, field_name, relation_type, scope, status,
                            blocking, chosen_value, deterministic_winner, confidence, rationale,
                            candidate_values, source_tools, source_domains, graph_entity_ids,
                            graph_relation_ids, notes
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                            %s::jsonb, %s::jsonb
                        )
                        """,
                        case_rows,
                    )

                if evidence_rows:
                    cur.executemany(
                        """
                        INSERT INTO graph_conflict_evidence(
                            evidence_id, run_id, case_id, candidate_value, polarity, document_id, chunk_id,
                            object_ref, source_url, source_domain, snippet, source_tool, retrieved_at, score
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s::jsonb, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        evidence_rows,
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise


def load_conflict_cases(run_id: str) -> List[GraphConflictCaseModel]:
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    case_id, target_type, target_id, field_name, relation_type, scope, status, blocking,
                    chosen_value, deterministic_winner, confidence, rationale, candidate_values,
                    source_tools, source_domains, graph_entity_ids, graph_relation_ids, notes
                FROM graph_conflict_cases
                WHERE run_id = %s
                ORDER BY blocking DESC, confidence DESC, case_id ASC
                """,
                (run_id,),
            )
            case_rows = cur.fetchall()
            cur.execute(
                """
                SELECT
                    evidence_id, case_id, candidate_value, polarity, document_id, chunk_id, object_ref,
                    source_url, source_domain, snippet, source_tool, retrieved_at, score
                FROM graph_conflict_evidence
                WHERE run_id = %s
                ORDER BY case_id ASC, evidence_id ASC
                """,
                (run_id,),
            )
            evidence_rows = cur.fetchall()

    evidence_by_case: dict[str, List[ConflictEvidenceRefModel]] = {}
    for row in evidence_rows:
        evidence = ConflictEvidenceRefModel(
            evidence_id=str(row[0] or ""),
            case_id=str(row[1] or ""),
            candidate_value=str(row[2] or ""),
            polarity=str(row[3] or "supporting"),
            document_id=str(row[4]) if row[4] else None,
            chunk_id=str(row[5]) if row[5] else None,
            object_ref=dict(row[6] or {}),
            source_url=str(row[7]) if row[7] else None,
            source_domain=str(row[8]) if row[8] else None,
            snippet=str(row[9] or ""),
            source_tool=str(row[10] or ""),
            retrieved_at=row[11].isoformat() if row[11] else None,
            score=float(row[12] or 0.0),
        )
        evidence_by_case.setdefault(evidence.case_id, []).append(evidence)

    cases: List[GraphConflictCaseModel] = []
    for row in case_rows:
        case_id = str(row[0] or "")
        candidate_values = row[12] if isinstance(row[12], list) else []
        cases.append(
            GraphConflictCaseModel(
                case_id=case_id,
                run_id=run_id,
                target_type=str(row[1] or "entity"),
                target_id=str(row[2] or ""),
                field_name=str(row[3] or ""),
                relation_type=str(row[4] or ""),
                scope=str(row[5] or "primary"),
                status=str(row[6] or "detected"),
                blocking=bool(row[7]),
                chosen_value=str(row[8]) if row[8] is not None else None,
                deterministic_winner=str(row[9]) if row[9] is not None else None,
                confidence=float(row[10] or 0.0),
                rationale=str(row[11] or ""),
                candidate_values=candidate_values,
                evidence=evidence_by_case.get(case_id, []),
                source_tools=list(row[13] or []),
                source_domains=list(row[14] or []),
                graph_entity_ids=list(row[15] or []),
                graph_relation_ids=list(row[16] or []),
                notes=list(row[17] or []),
            )
        )
    return cases

