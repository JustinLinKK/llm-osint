from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, List

from langgraph.graph import END, StateGraph

from env import load_env
from logger import get_logger
from mcp_client import McpClientProtocol, RoutedMcpClient
from openrouter_llm import OpenRouterLLM, get_openrouter_timeout, invoke_complete_json
from report_helpers import (
    assemble_evidence_appendix,
    assemble_final_report,
    build_coverage_ledger,
    build_depth_quality_issues,
    build_report_memory,
    build_section_queries,
    contradiction_query_hints,
    coverage_is_complete,
    detect_section_anchor_drift,
    dedupe_claims,
    dedupe_evidence,
    dedupe_str_list,
    default_outline,
    decide_report_type,
    draft_section_content,
    fallback_claims,
    graph_context_signals,
    graph_multi_entity_query,
    is_conflict_or_methodology_section,
    latest_draft_per_section,
    needs_conflict_resolution,
    needs_timeline_normalization,
    pack_evidence,
    pick_primary_entities,
    run_consistency_validator,
    section_allows_related_subjects,
    vector_multi_query,
)
from report_models import (
    ClaimModel,
    ConsistencyIssueModel,
    EvidenceRefModel,
    PrimaryTargetContractModel,
    ReportResult,
    ReportState,
    SectionDraftModel,
    SectionReflectionModel,
    SectionTaskModel,
    Stage2ModelConfig,
    make_initial_report_state,
)
from report_store import persist_report_snapshot
from run_store import ensure_run_exists, load_primary_target_contract
from run_events import emit_run_event
from system_prompts import (
    REPORT_OUTLINE_SYSTEM_PROMPT,
    REPORT_SECTION_CLAIMS_SYSTEM_PROMPT,
    REPORT_SECTION_REFLECTION_SYSTEM_PROMPT,
)
from target_normalization import extract_person_targets
from tool_worker_graph import ToolReceipt

logger = get_logger(__name__)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _stage2_final_timeout() -> float:
    return get_openrouter_timeout(
        "OPENROUTER_REPORT_TIMEOUT_SECONDS",
        get_openrouter_timeout(
            "OPENROUTER_PLANNER_TIMEOUT_SECONDS",
            get_openrouter_timeout("OPENROUTER_TIMEOUT_SECONDS", 400.0),
        ),
    )


def _stage2_worker_timeout() -> float:
    return get_openrouter_timeout(
        "OPENROUTER_REPORT_WORKER_TIMEOUT_SECONDS",
        get_openrouter_timeout(
            "OPENROUTER_WORKER_TIMEOUT_SECONDS",
            get_openrouter_timeout("OPENROUTER_TIMEOUT_SECONDS", 400.0),
        ),
    )


def _env_text(name: str) -> str:
    value = os.getenv(name)
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _resolve_stage2_model_config(
    stage2_model_config: Stage2ModelConfig | None,
) -> Stage2ModelConfig:
    explicit = stage2_model_config or Stage2ModelConfig()
    report_model = (
        explicit.final_report_model
        or _env_text("OPENROUTER_REPORT_MODEL")
        or _env_text("OPENROUTER_PLANNER_MODEL")
        or _env_text("OPENROUTER_MODEL")
    )
    worker_model = (
        explicit.section_draft_model
        or _env_text("OPENROUTER_REPORT_WORKER_MODEL")
        or _env_text("OPENROUTER_WORKER_MODEL")
        or report_model
    )
    return Stage2ModelConfig(
        outline_model=explicit.outline_model or _env_text("OPENROUTER_REPORT_OUTLINE_MODEL") or report_model,
        section_query_model=explicit.section_query_model or _env_text("OPENROUTER_REPORT_SECTION_QUERY_MODEL") or worker_model,
        section_claim_model=explicit.section_claim_model or _env_text("OPENROUTER_REPORT_SECTION_CLAIM_MODEL") or worker_model,
        section_draft_model=explicit.section_draft_model or _env_text("OPENROUTER_REPORT_SECTION_DRAFT_MODEL") or worker_model,
        final_reflection_model=explicit.final_reflection_model or _env_text("OPENROUTER_REPORT_FINAL_REFLECTION_MODEL") or report_model,
        final_report_model=explicit.final_report_model or _env_text("OPENROUTER_REPORT_FINAL_REPORT_MODEL") or report_model,
    )


def _looks_like_template_section(content: str) -> bool:
    text = str(content or "").strip()
    if not text:
        return True
    template_prefixes = (
        "Section:",
        "Objective:",
        "Section group:",
        "Graph chain:",
        "Next step:",
        "Revision focus:",
    )
    return any(text.startswith(prefix) for prefix in template_prefixes)


def _section_inline_citation_count(content: str) -> int:
    return len(re.findall(r"\[[A-Z0-9_]+\]", str(content or "")))


def _subject_signature(value: str) -> str:
    tokens = [token.casefold() for token in re.findall(r"[A-Za-z][A-Za-z'-]*", str(value or ""))]
    return " ".join(tokens)


def _subject_matches_primary(subject_name: str, primary_entities: List[str]) -> bool:
    subject_signature = _subject_signature(subject_name)
    if not subject_signature:
        return False
    for entity in primary_entities:
        aliases = [str(entity or "").strip(), *extract_person_targets(str(entity or "").strip())]
        for alias in aliases:
            if subject_signature == _subject_signature(alias):
                return True
    return False


def _claim_or_evidence_mentions_primary(
    claim: ClaimModel,
    evidence: List[EvidenceRefModel],
    primary_entities: List[str],
) -> bool:
    blobs = [claim.text]
    for item in evidence:
        blobs.extend([item.snippet, item.title or "", item.source_url or ""])
    for entity in primary_entities:
        aliases = [str(entity or "").strip(), *extract_person_targets(str(entity or "").strip())]
        for alias in aliases:
            alias_text = str(alias or "").strip().casefold()
            if alias_text and any(alias_text in str(blob or "").casefold() for blob in blobs):
                return True
    return False


def verify_claims_for_task(
    task: SectionTaskModel,
    evidence: List[EvidenceRefModel],
    claims: List[ClaimModel],
    *,
    primary_entities: List[str] | None = None,
) -> tuple[List[ClaimModel], List[str]]:
    valid_keys = {item.citation_key for item in evidence}
    issues: List[str] = []
    verified: List[ClaimModel] = []
    primary_entities = list(primary_entities or task.entity_ids[:1])
    allow_related_subjects = section_allows_related_subjects(task)
    allow_unanchored_related = is_conflict_or_methodology_section(task)

    for claim in claims:
        matched = [key for key in claim.evidence_keys if key in valid_keys]
        if not matched:
            issues.append(f"{task.section_id}: dropped unsupported claim: {claim.claim_id}")
            if claim.impact == "high":
                issues.append(f"{task.section_id}: high-impact claim without evidence: {claim.claim_id}")
            continue
        matched_evidence = [item for item in evidence if item.citation_key in matched]
        primary_evidence = next(
            (
                item
                for item in matched_evidence
                if item.source_url or item.document_id or item.object_ref or item.graph_ref
            ),
            None,
        )
        if primary_evidence is None:
            issues.append(f"{task.section_id}: dropped claim without stable evidence reference: {claim.claim_id}")
            continue
        has_run_evidence_link = bool(
            primary_evidence.evidence_object_key
            or (isinstance(primary_evidence.object_ref, dict) and (primary_evidence.object_ref.get("objectKey") or primary_evidence.object_ref.get("object_key")))
            or primary_evidence.document_id
        )
        if not has_run_evidence_link:
            issues.append(f"{task.section_id}: dropped claim without run-linked evidence object: {claim.claim_id}")
            continue

        subject_name = str(claim.subject_name or claim.subject_entity_id or "").strip() or None
        about_primary_subject = claim.about_primary_subject
        if about_primary_subject is None:
            if subject_name and primary_entities:
                about_primary_subject = _subject_matches_primary(subject_name, primary_entities)
            elif subject_name:
                about_primary_subject = False
            else:
                about_primary_subject = True

        if subject_name and primary_entities and _subject_matches_primary(subject_name, primary_entities):
            about_primary_subject = True

        if not subject_name and about_primary_subject and primary_entities:
            subject_name = primary_entities[0]

        if subject_name and not about_primary_subject:
            anchored_to_primary = _claim_or_evidence_mentions_primary(
                claim,
                matched_evidence,
                primary_entities,
            )
            if not allow_related_subjects:
                issues.append(f"{task.section_id}: dropped off-target claim: {claim.claim_id}")
                continue
            if not allow_unanchored_related and not anchored_to_primary:
                issues.append(f"{task.section_id}: dropped unanchored related-subject claim: {claim.claim_id}")
                continue

        normalized = claim.model_copy(
            update={
                "evidence_keys": matched,
                "subject_entity_id": claim.subject_entity_id or subject_name or (primary_entities[0] if about_primary_subject and primary_entities else None),
                "subject_name": subject_name,
                "about_primary_subject": about_primary_subject,
                "object": claim.object or claim.text,
                "source_url": primary_evidence.source_url,
                "source_type": primary_evidence.source_type,
                "retrieved_at": primary_evidence.retrieved_at,
                "quote_span": primary_evidence.snippet[:280],
            }
        )
        verified.append(normalized)

    return (verified, issues)


# Keep graph assembly in one place so node ordering and route transitions are easy to audit.
def build_report_graph(
    mcp_client: McpClientProtocol,
    llm3: OpenRouterLLM | None = None,
    *,
    section_llm: OpenRouterLLM | None = None,
    final_llm: OpenRouterLLM | None = None,
    role_llms: Dict[str, OpenRouterLLM] | None = None,
) -> StateGraph:
    graph = StateGraph(ReportState)
    role_llms = dict(role_llms or {})
    if section_llm is None and final_llm is None:
        section_llm = llm3
        final_llm = llm3
    elif section_llm is None:
        section_llm = final_llm
    elif final_llm is None:
        final_llm = section_llm
    outline_llm = role_llms.get("outline") or final_llm
    section_query_llm = role_llms.get("section_query") or section_llm
    section_claim_llm = role_llms.get("section_claim") or section_llm
    section_draft_llm = role_llms.get("section_draft") or section_llm
    final_reflection_llm = role_llms.get("final_reflection") or final_llm
    final_report_llm = role_llms.get("final_report") or final_llm
    # Run-local caches prevent repeated DB round-trips across sections/refinement rounds.
    entity_signal_cache: Dict[str, tuple[List[str], List[str], List[str]]] = {}
    vector_query_cache: Dict[str, List[Dict[str, Any]]] = {}
    cache_lock = Lock()
    max_outline_sections = max(1, int(os.getenv("STAGE2_MAX_OUTLINE_SECTIONS", "12")))
    max_graph_entities = max(1, int(os.getenv("STAGE2_MAX_GRAPH_CONTEXT_ENTITIES", "4")))
    max_vector_queries = max(1, int(os.getenv("STAGE2_MAX_VECTOR_QUERIES_PER_SECTION", "3")))
    max_section_workers = max(1, int(os.getenv("STAGE2_MAX_SECTION_WORKERS", "4")))
    vector_k = max(1, int(os.getenv("STAGE2_VECTOR_K", "8")))
    persist_draft_snapshots = _env_flag("STAGE2_PERSIST_DRAFT_SNAPSHOTS", False)

    def emit_stage(state: ReportState, stage: str, status: str, **payload: Any) -> None:
        emit_run_event(
            state["run_id"],
            f"STAGE2_NODE_{status.upper()}",
            {"component": "report_subgraph", "stage": stage, **payload},
        )

    def _stage1_conflict_issue_models(conflicts: List[Any]) -> List[ConsistencyIssueModel]:
        return [
            ConsistencyIssueModel(
                issue_id=f"stage1_conflict_{index}",
                severity="high" if bool(getattr(item, "blocking", False)) else "medium",
                description=str(
                    getattr(item, "rationale", "")
                    or f"Stage 1 conflict on {getattr(item, 'relation_type', '') or getattr(item, 'field_name', 'field')}."
                ),
                conflicting_sections=["conflict_resolution"],
                targeted_queries=[
                    str(getattr(item, "relation_type", "") or getattr(item, "field_name", "")).replace("_", " ").strip()
                ],
            )
            for index, item in enumerate(conflicts[:6], start=1)
        ]

    def _stage1_conflict_query_hints(conflicts: List[Any]) -> List[str]:
        return dedupe_str_list(
            [
                str(getattr(item, "relation_type", "") or getattr(item, "field_name", "")).replace("_", " ").strip()
                for item in conflicts
                if str(getattr(item, "relation_type", "") or getattr(item, "field_name", "")).strip()
            ]
        )

    def _stage1_conflict_limit_notes(conflicts: List[Any]) -> List[str]:
        if not conflicts:
            return []
        return [f"Stage 1 left {len(conflicts)} graph conflict(s) unresolved."]

    def _merge_consistency_issues(*groups: List[ConsistencyIssueModel]) -> List[ConsistencyIssueModel]:
        deduped: Dict[str, ConsistencyIssueModel] = {}
        for group in groups:
            for item in group:
                issue_id = str(getattr(item, "issue_id", "") or "").strip()
                description = str(getattr(item, "description", "") or "").strip()
                key = issue_id or description.casefold()
                if key and key not in deduped:
                    deduped[key] = item
        return list(deduped.values())

    def report_init_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "report_init_node", "started")
        report_type = decide_report_type(state.get("prompt", ""), state.get("noteboard", []))
        primary_target_contract = state.get("primary_target_contract") or PrimaryTargetContractModel()
        stage1_conflict_cases = list(state.get("stage1_conflict_cases", []))
        stage1_resolved_conflicts = list(state.get("stage1_resolved_conflicts", []))
        stage1_unresolved_conflicts = list(state.get("stage1_unresolved_conflicts", []))
        primary_entities = pick_primary_entities(
            mcp_client=mcp_client,
            run_id=state["run_id"],
            prompt=state.get("prompt", ""),
            noteboard=state.get("noteboard", []),
            receipts=state.get("stage1_receipts", []),
            primary_target_contract=primary_target_contract,
        )
        stage1_conflict_issues = _stage1_conflict_issue_models(stage1_unresolved_conflicts)
        output = {
            "report_type": report_type,
            "primary_entities": primary_entities,
            "primary_target_contract": primary_target_contract,
            "section_drafts": [],
            "claim_ledger": [],
            "evidence_refs": [],
            "section_issues": [],
            "section_reflections": [],
            "missing_section_ids": [],
            "quality_ok": False,
            "done": False,
            "section_hits": [],
            "section_evidence_buffer": [],
            "section_claims_buffer": [],
            "section_issues_buffer": [],
            "report_memory": state.get("report_memory").model_copy(
                update={
                    "question": state.get("prompt", ""),
                    "entities": [],
                    "claims": [],
                    "evidence": [],
                    "open_questions": [],
                    "limits": _stage1_conflict_limit_notes(stage1_unresolved_conflicts),
                    "consistency_issues": stage1_conflict_issues,
                    "step_count": 0,
                    "primary_target_contract": primary_target_contract,
                    "stage1_conflict_cases": stage1_conflict_cases,
                    "stage1_resolved_conflicts": stage1_resolved_conflicts,
                    "stage1_unresolved_conflicts": stage1_unresolved_conflicts,
                }
            ),
            "consistency_issues": stage1_conflict_issues,
            "contradiction_query_hints": _stage1_conflict_query_hints(stage1_conflict_cases),
        }
        emit_stage(state, "report_init_node", "completed", report_type=report_type, primary_entity_count=len(primary_entities))
        return output

    def build_outline_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "build_outline_node", "started")
        fallback_outline = default_outline(state.get("report_type", "person"), state.get("primary_entities", []))
        fallback_outline = _normalize_outline(fallback_outline, max_outline_sections)
        if outline_llm is None:
            emit_stage(state, "build_outline_node", "completed", outline_count=len(fallback_outline), reason="llm_unavailable")
            return {"outline": fallback_outline}

        payload = {
            "prompt": state.get("prompt", ""),
            "report_type": state.get("report_type", "person"),
            "primary_entities": state.get("primary_entities", []),
            "primary_target_contract": state.get("primary_target_contract").model_dump() if state.get("primary_target_contract") else {},
            "noteboard": state.get("noteboard", [])[-12:],
            "output_schema": {
                "outline": [
                    {
                        "section_id": "string",
                        "title": "string",
                        "objective": "string",
                        "required": "boolean",
                        "section_group": "string",
                        "graph_chain": ["string"],
                        "entity_ids": ["string"],
                        "query_hints": ["string"],
                    }
                ]
            },
        }
        try:
            parsed = invoke_complete_json(
                outline_llm,
                REPORT_OUTLINE_SYSTEM_PROMPT,
                payload,
                temperature=0.2,
                timeout=_stage2_final_timeout(),
                run_id=state["run_id"],
                operation="stage2.outline",
            )
            outline_raw = parsed.get("outline")
            if isinstance(outline_raw, list):
                outline = [SectionTaskModel.model_validate(item) for item in outline_raw]
                if outline:
                    normalized = _normalize_outline(outline, max_outline_sections)
                    emit_stage(state, "build_outline_node", "completed", outline_count=len(normalized), source="llm")
                    return {"outline": normalized}
        except Exception:
            logger.exception("Stage 2 outline generation failed")
        emit_stage(state, "build_outline_node", "completed", outline_count=len(fallback_outline), source="fallback")
        return {"outline": fallback_outline}

    def _cached_graph_context_signals(run_id: str, entity_id: str) -> tuple[List[str], List[str], List[str]]:
        with cache_lock:
            cached = entity_signal_cache.get(entity_id)
        if cached is not None:
            return cached
        resolved = graph_context_signals(mcp_client, run_id, [entity_id])
        with cache_lock:
            return entity_signal_cache.setdefault(entity_id, resolved)

    def _cached_vector_rows(run_id: str, query: str) -> List[Dict[str, Any]]:
        cache_key = f"{run_id}::{query.strip().lower()}"
        with cache_lock:
            cached = vector_query_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        resolved = vector_multi_query(mcp_client, run_id, [query], k=vector_k)
        with cache_lock:
            cached = vector_query_cache.setdefault(cache_key, resolved)
        return list(cached)

    def _extract_claims_for_task(
        state: ReportState,
        task: SectionTaskModel,
        evidence: List[EvidenceRefModel],
    ) -> List[ClaimModel]:
        emit_stage(state, "extract_claims_node", "started", section_id=task.section_id)
        if section_claim_llm is None or not evidence:
            claims = fallback_claims(task, evidence)
            emit_stage(
                state,
                "extract_claims_node",
                "completed",
                section_id=task.section_id,
                claim_count=len(claims),
                reason=("llm_unavailable" if section_claim_llm is None else "no_evidence"),
            )
            return claims

        payload = {
            "section": task.model_dump(),
            "evidence": [item.model_dump() for item in evidence],
            "output_schema": {
                "claims": [
                    {
                        "claim_id": "string",
                        "text": "string",
                        "subject_name": "string",
                        "about_primary_subject": "boolean",
                        "confidence": "number",
                        "impact": "low|medium|high",
                        "evidence_keys": ["string"],
                        "conflict_flags": ["string"],
                    }
                ]
            },
        }
        try:
            parsed = invoke_complete_json(
                section_claim_llm,
                REPORT_SECTION_CLAIMS_SYSTEM_PROMPT,
                payload,
                temperature=0.1,
                timeout=_stage2_worker_timeout(),
                run_id=state["run_id"],
                operation="stage2.claim_extract",
                metadata={"sectionId": task.section_id},
            )
            raw_claims = parsed.get("claims")
            if isinstance(raw_claims, list):
                claims = [ClaimModel.model_validate({**item, "section_id": task.section_id}) for item in raw_claims]
                emit_stage(
                    state,
                    "extract_claims_node",
                    "completed",
                    section_id=task.section_id,
                    claim_count=len(claims),
                    source="llm",
                )
                return claims
        except Exception:
            logger.exception("Stage 2 claim extraction failed", extra={"section_id": task.section_id})

        claims = fallback_claims(task, evidence)
        emit_stage(state, "extract_claims_node", "completed", section_id=task.section_id, claim_count=len(claims), source="fallback")
        return claims

    def _verify_claims_for_task(
        state: ReportState,
        task: SectionTaskModel,
        evidence: List[EvidenceRefModel],
        claims: List[ClaimModel],
    ) -> tuple[List[ClaimModel], List[str]]:
        emit_stage(state, "verify_claims_node", "started", section_id=task.section_id)
        verified, issues = verify_claims_for_task(
            task,
            evidence,
            claims,
            primary_entities=list(state.get("primary_entities", [])),
        )

        emit_stage(
            state,
            "verify_claims_node",
            "completed",
            section_id=task.section_id,
            claim_count=len(verified),
            issue_count=len(issues),
        )
        return (verified, issues)

    def _process_section_task(state: ReportState, task: SectionTaskModel) -> Dict[str, Any]:
        emit_stage(state, "graph_context_node", "started", section_id=task.section_id)
        entity_ids = dedupe_str_list(list(state.get("primary_entities", [])) + list(task.entity_ids))[:max_graph_entities]
        aliases: List[str] = []
        handles: List[str] = []
        domains: List[str] = []
        for entity_id in entity_ids:
            item_aliases, item_handles, item_domains = _cached_graph_context_signals(state["run_id"], entity_id)
            aliases.extend(item_aliases)
            handles.extend(item_handles)
            domains.extend(item_domains)
        hydrated_task = task.model_copy(update={"entity_ids": entity_ids, "query_hints": dedupe_str_list(list(task.query_hints) + aliases + handles + domains)})
        emit_stage(
            state,
            "graph_context_node",
            "completed",
            section_id=task.section_id,
            entity_count=len(entity_ids),
            query_hint_count=len(hydrated_task.query_hints),
        )

        emit_stage(state, "vector_retrieve_node", "started", section_id=task.section_id)
        queries = dedupe_str_list(build_section_queries(hydrated_task, section_query_llm, run_id=state["run_id"]))[:max_vector_queries]
        hits: List[Dict[str, Any]] = []
        graph_hits = graph_multi_entity_query(mcp_client, state["run_id"], hydrated_task.entity_ids)
        for query in queries:
            hits.extend(_cached_vector_rows(state["run_id"], query))
        hits.extend(graph_hits)
        hits = _dedupe_hit_rows(hits)
        emit_stage(
            state,
            "vector_retrieve_node",
            "completed",
            section_id=task.section_id,
            query_count=len(queries),
            hit_count=len(hits),
            graph_hit_count=len(graph_hits),
        )

        emit_stage(state, "evidence_pack_node", "started", section_id=task.section_id)
        evidence = pack_evidence(hydrated_task.section_id, hits, k=vector_k, section_context=hydrated_task)
        emit_stage(state, "evidence_pack_node", "completed", section_id=task.section_id, evidence_count=len(evidence))

        claims = _extract_claims_for_task(state, hydrated_task, evidence)
        verified_claims, issues = _verify_claims_for_task(state, hydrated_task, evidence, claims)

        emit_stage(state, "draft_section_node", "started", section_id=task.section_id)
        section_draft = SectionDraftModel(
            section_id=hydrated_task.section_id,
            title=hydrated_task.title,
            content=draft_section_content(state["run_id"], hydrated_task, verified_claims, evidence, section_draft_llm),
            citation_keys=[item.citation_key for item in evidence],
        )
        emit_stage(
            state,
            "draft_section_node",
            "completed",
            section_id=task.section_id,
            citation_count=len(section_draft.citation_keys),
            next_section=None,
        )
        return {
            "task": hydrated_task,
            "section_draft": section_draft,
            "claims": verified_claims,
            "evidence": evidence,
            "issues": issues,
        }

    def section_router_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "section_router_node", "started")
        outline = list(state.get("outline", []))
        missing_ids = set(state.get("missing_section_ids", []))
        draft_lookup = {draft.section_id: draft for draft in state.get("section_drafts", [])}
        outline_lookup = {item.section_id: item for item in outline}
        revision_targets = [item for item in state.get("section_reflections", []) if item.status != "ok"]

        if revision_targets:
            section_tasks: List[SectionTaskModel] = []
            for reflection in revision_targets:
                base_task = outline_lookup.get(reflection.section_id)
                if base_task is None:
                    draft = draft_lookup.get(reflection.section_id)
                    if draft is None:
                        continue
                    base_task = SectionTaskModel(
                        section_id=reflection.section_id,
                        title=draft.title,
                        objective=reflection.critique or f"Improve section {draft.title}",
                        required=True,
                    )
                draft = draft_lookup.get(reflection.section_id)
                current_content = reflection.current_content or (draft.content if draft is not None else "")
                section_tasks.append(
                    base_task.model_copy(
                        update={
                            "query_hints": dedupe_str_list(
                                list(base_task.query_hints) + list(reflection.query_hints) + state.get("query_hints", [])
                            ),
                            "revision_query_hints": dedupe_str_list(
                                list(getattr(base_task, "revision_query_hints", []))
                                + list(reflection.query_hints)
                                + state.get("query_hints", [])
                            ),
                            "current_content": current_content,
                            "reflection_source": reflection.reflection_source or "final_reflection_node",
                            "revision_focus": reflection.critique,
                            "next_step_suggestion": reflection.next_step_suggestion,
                        }
                    )
                )
        else:
            section_tasks = [item for item in outline if item.section_id in missing_ids] if missing_ids else outline

        if state.get("refine_round", 0) > 0:
            section_tasks = [
                task.model_copy(
                    update={
                        "query_hints": dedupe_str_list(list(task.query_hints) + state.get("query_hints", [])),
                        "revision_query_hints": dedupe_str_list(list(task.revision_query_hints) + state.get("query_hints", [])),
                    }
                )
                for task in section_tasks
            ]

        if not revision_targets and needs_timeline_normalization(state.get("noteboard", [])):
            if not any(item.section_id == "timeline_normalization" for item in section_tasks):
                section_tasks.append(
                    SectionTaskModel(
                        section_id="timeline_normalization",
                        title="Timeline normalization",
                        objective="Normalize dates/events into one consistent timeline with unknowns marked.",
                        required=False,
                        section_group="Timeline",
                        graph_chain=["Person", "TimelineEvent", "Experience/Affiliation/Credential", "Organization/Publication"],
                        entity_ids=state.get("primary_entities", []),
                        query_hints=["timeline", "date normalization"],
                    )
                )

        if not revision_targets and needs_conflict_resolution(state.get("noteboard", []), state.get("claim_ledger", [])):
            if not any(item.section_id == "conflict_resolution" for item in section_tasks):
                section_tasks.append(
                    SectionTaskModel(
                        section_id="conflict_resolution",
                        title="Conflict resolution",
                        objective="List conflicting claims and unresolved uncertainties with citations.",
                        required=False,
                        section_group="Risk",
                        graph_chain=["Primary Subject", "Conflicting Branch", "Evidence", "Uncertainty"],
                        entity_ids=state.get("primary_entities", []),
                        query_hints=["conflict", "contradiction", "disagreement"],
                    )
                )

        output = {
            "section_tasks": section_tasks,
            "pending_section_tasks": [],
            "active_task": None,
        }
        emit_stage(
            state,
            "section_router_node",
            "completed",
            active_section=None,
            pending_count=len(section_tasks),
        )
        return output

    def route_after_section_router(state: ReportState) -> str:
        return "process_sections_node" if state.get("section_tasks") else "reduce_sections_node"

    def process_sections_node(state: ReportState) -> Dict[str, Any]:
        tasks = list(state.get("section_tasks", []))
        if not tasks:
            return {
                "pending_section_tasks": [],
                "active_task": None,
                "section_hits": [],
                "section_evidence_buffer": [],
                "section_claims_buffer": [],
                "section_issues_buffer": [],
            }

        emit_stage(
            state,
            "process_sections_node",
            "started",
            section_count=len(tasks),
            worker_count=min(max_section_workers, len(tasks)),
        )
        results_by_section: Dict[str, Dict[str, Any]] = {}
        worker_limit = min(max_section_workers, len(tasks))
        if worker_limit == 1:
            for task in tasks:
                results_by_section[task.section_id] = _process_section_task(state, task)
        else:
            with ThreadPoolExecutor(max_workers=worker_limit) as executor:
                futures = {executor.submit(_process_section_task, state, task): task for task in tasks}
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        results_by_section[task.section_id] = future.result()
                    except Exception:
                        logger.exception("Stage 2 section worker failed", extra={"section_id": task.section_id})
                        results_by_section[task.section_id] = {
                            "task": task,
                            "section_draft": None,
                            "claims": [],
                            "evidence": [],
                            "issues": [f"{task.section_id}: section worker failed before draft generation."],
                        }

        ordered_results = [results_by_section[task.section_id] for task in tasks if task.section_id in results_by_section]
        new_drafts = [item["section_draft"] for item in ordered_results if item.get("section_draft") is not None]
        new_claims = [claim for item in ordered_results for claim in item.get("claims", [])]
        new_evidence = [evidence_item for item in ordered_results for evidence_item in item.get("evidence", [])]
        new_issues = [issue for item in ordered_results for issue in item.get("issues", [])]
        latest_observation = ""
        if new_evidence:
            latest_observation = new_evidence[0].snippet
        elif new_drafts:
            latest_observation = new_drafts[0].content[:240]
        output = {
            "section_drafts": list(state.get("section_drafts", [])) + new_drafts,
            "claim_ledger": list(state.get("claim_ledger", [])) + new_claims,
            "evidence_refs": list(state.get("evidence_refs", [])) + new_evidence,
            "section_issues": list(state.get("section_issues", [])) + new_issues,
            "pending_section_tasks": [],
            "active_task": None,
            "section_hits": [],
            "section_evidence_buffer": [],
            "section_claims_buffer": [],
            "section_issues_buffer": [],
            "report_memory": build_report_memory(
                question=state.get("prompt", ""),
                report_type=state.get("report_type", "person"),
                primary_entities=state.get("primary_entities", []),
                primary_target_contract=state.get("primary_target_contract"),
                noteboard=state.get("noteboard", []),
                stage1_receipts=state.get("stage1_receipts", []),
                claims=list(state.get("claim_ledger", [])) + new_claims,
                evidence=list(state.get("evidence_refs", [])) + new_evidence,
                section_issues=list(state.get("section_issues", [])) + new_issues,
                section_drafts=list(state.get("section_drafts", [])) + new_drafts,
                latest_observation=latest_observation,
                consistency_issues=list(state.get("consistency_issues", [])),
                limit_notes=_stage1_conflict_limit_notes(list(state.get("stage1_unresolved_conflicts", []))),
                stage1_conflict_cases=list(state.get("stage1_conflict_cases", [])),
                stage1_resolved_conflicts=list(state.get("stage1_resolved_conflicts", [])),
                stage1_unresolved_conflicts=list(state.get("stage1_unresolved_conflicts", [])),
            ),
        }
        emit_stage(
            state,
            "process_sections_node",
            "completed",
            section_count=len(new_drafts),
            claim_count=len(new_claims),
            evidence_count=len(new_evidence),
            issue_count=len(new_issues),
        )
        return output

    def graph_context_node(state: ReportState) -> Dict[str, Any]:
        task = state.get("active_task")
        if task is None:
            return dict(state)

        emit_stage(state, "graph_context_node", "started", section_id=task.section_id)
        entity_ids = dedupe_str_list(list(state.get("primary_entities", [])) + list(task.entity_ids))[:max_graph_entities]
        aliases: List[str] = []
        handles: List[str] = []
        domains: List[str] = []
        for entity_id in entity_ids:
            cached = entity_signal_cache.get(entity_id)
            if cached is None:
                cached = graph_context_signals(mcp_client, state["run_id"], [entity_id])
                entity_signal_cache[entity_id] = cached
            item_aliases, item_handles, item_domains = cached
            aliases.extend(item_aliases)
            handles.extend(item_handles)
            domains.extend(item_domains)
        query_hints = dedupe_str_list(list(task.query_hints) + aliases + handles + domains)
        emit_stage(
            state,
            "graph_context_node",
            "completed",
            section_id=task.section_id,
            entity_count=len(entity_ids),
            query_hint_count=len(query_hints),
        )
        return {"active_task": task.model_copy(update={"entity_ids": entity_ids, "query_hints": query_hints})}

    def vector_retrieve_node(state: ReportState) -> Dict[str, Any]:
        task = state.get("active_task")
        if task is None:
            return dict(state)

        emit_stage(state, "vector_retrieve_node", "started", section_id=task.section_id)
        queries = dedupe_str_list(build_section_queries(task, section_query_llm, run_id=state["run_id"]))[:max_vector_queries]
        hits: List[Dict[str, Any]] = []
        graph_hits = graph_multi_entity_query(mcp_client, state["run_id"], task.entity_ids)
        for query in queries:
            cache_key = f"{state['run_id']}::{query.strip().lower()}"
            cached_rows = vector_query_cache.get(cache_key)
            if cached_rows is None:
                cached_rows = vector_multi_query(mcp_client, state["run_id"], [query], k=vector_k)
                vector_query_cache[cache_key] = cached_rows
            hits.extend(cached_rows)
        hits.extend(graph_hits)
        hits = _dedupe_hit_rows(hits)
        emit_stage(
            state,
            "vector_retrieve_node",
            "completed",
            section_id=task.section_id,
            query_count=len(queries),
            hit_count=len(hits),
            graph_hit_count=len(graph_hits),
        )
        return {"section_hits": hits}

    def evidence_pack_node(state: ReportState) -> Dict[str, Any]:
        task = state.get("active_task")
        if task is None:
            return dict(state)

        emit_stage(state, "evidence_pack_node", "started", section_id=task.section_id)
        packed = pack_evidence(task.section_id, state.get("section_hits", []), k=vector_k, section_context=task)
        emit_stage(state, "evidence_pack_node", "completed", section_id=task.section_id, evidence_count=len(packed))
        return {"section_evidence_buffer": packed}

    def extract_claims_node(state: ReportState) -> Dict[str, Any]:
        task = state.get("active_task")
        if task is None:
            return dict(state)

        emit_stage(state, "extract_claims_node", "started", section_id=task.section_id)
        evidence = state.get("section_evidence_buffer", [])
        if section_claim_llm is None or not evidence:
            claims = fallback_claims(task, evidence)
            emit_stage(
                state,
                "extract_claims_node",
                "completed",
                section_id=task.section_id,
                claim_count=len(claims),
                reason=("llm_unavailable" if section_claim_llm is None else "no_evidence"),
            )
            return {"section_claims_buffer": claims}

        payload = {
            "section": task.model_dump(),
            "evidence": [item.model_dump() for item in evidence],
            "output_schema": {
                "claims": [
                    {
                        "claim_id": "string",
                        "text": "string",
                        "subject_name": "string",
                        "about_primary_subject": "boolean",
                        "confidence": "number",
                        "impact": "low|medium|high",
                        "evidence_keys": ["string"],
                        "conflict_flags": ["string"],
                    }
                ]
            },
        }
        try:
            parsed = invoke_complete_json(
                section_claim_llm,
                REPORT_SECTION_CLAIMS_SYSTEM_PROMPT,
                payload,
                temperature=0.1,
                timeout=_stage2_worker_timeout(),
                run_id=state["run_id"],
                operation="stage2.claim_refine",
                metadata={"sectionId": task.section_id},
            )
            raw_claims = parsed.get("claims")
            if isinstance(raw_claims, list):
                claims = [ClaimModel.model_validate({**item, "section_id": task.section_id}) for item in raw_claims]
                emit_stage(state, "extract_claims_node", "completed", section_id=task.section_id, claim_count=len(claims), source="llm")
                return {"section_claims_buffer": claims}
        except Exception:
            logger.exception("Stage 2 claim extraction failed")

        claims = fallback_claims(task, evidence)
        emit_stage(state, "extract_claims_node", "completed", section_id=task.section_id, claim_count=len(claims), source="fallback")
        return {"section_claims_buffer": claims}

    def verify_claims_node(state: ReportState) -> Dict[str, Any]:
        task = state.get("active_task")
        if task is None:
            return dict(state)

        emit_stage(state, "verify_claims_node", "started", section_id=task.section_id)
        evidence = state.get("section_evidence_buffer", [])
        claims = state.get("section_claims_buffer", [])
        verified, issues = verify_claims_for_task(
            task,
            evidence,
            claims,
            primary_entities=list(state.get("primary_entities", [])),
        )

        emit_stage(
            state,
            "verify_claims_node",
            "completed",
            section_id=task.section_id,
            claim_count=len(verified),
            issue_count=len(issues),
        )
        return {"section_claims_buffer": verified, "section_issues_buffer": issues}

    def draft_section_node(state: ReportState) -> Dict[str, Any]:
        task = state.get("active_task")
        if task is None:
            return dict(state)

        emit_stage(state, "draft_section_node", "started", section_id=task.section_id)
        claims = state.get("section_claims_buffer", [])
        evidence = state.get("section_evidence_buffer", [])
        section_draft = SectionDraftModel(
            section_id=task.section_id,
            title=task.title,
            content=draft_section_content(state["run_id"], task, claims, evidence, section_draft_llm),
            citation_keys=[item.citation_key for item in evidence],
        )
        pending = list(state.get("pending_section_tasks", []))
        next_task = pending.pop(0) if pending else None
        output = {
            "section_drafts": list(state.get("section_drafts", [])) + [section_draft],
            "claim_ledger": list(state.get("claim_ledger", [])) + claims,
            "evidence_refs": list(state.get("evidence_refs", [])) + evidence,
            "section_issues": list(state.get("section_issues", [])) + state.get("section_issues_buffer", []),
            "pending_section_tasks": pending,
            "active_task": next_task,
            "section_hits": [],
            "section_evidence_buffer": [],
            "section_claims_buffer": [],
            "section_issues_buffer": [],
            "report_memory": build_report_memory(
                question=state.get("prompt", ""),
                report_type=state.get("report_type", "person"),
                primary_entities=state.get("primary_entities", []),
                primary_target_contract=state.get("primary_target_contract"),
                noteboard=state.get("noteboard", []),
                stage1_receipts=state.get("stage1_receipts", []),
                claims=list(state.get("claim_ledger", [])) + claims,
                evidence=list(state.get("evidence_refs", [])) + evidence,
                section_issues=list(state.get("section_issues", [])) + state.get("section_issues_buffer", []),
                section_drafts=list(state.get("section_drafts", [])) + [section_draft],
                latest_observation=(evidence[0].snippet if evidence else section_draft.content[:240]),
                consistency_issues=list(state.get("consistency_issues", [])),
                limit_notes=_stage1_conflict_limit_notes(list(state.get("stage1_unresolved_conflicts", []))),
                stage1_conflict_cases=list(state.get("stage1_conflict_cases", [])),
                stage1_resolved_conflicts=list(state.get("stage1_resolved_conflicts", [])),
                stage1_unresolved_conflicts=list(state.get("stage1_unresolved_conflicts", [])),
            ),
        }
        emit_stage(
            state,
            "draft_section_node",
            "completed",
            section_id=task.section_id,
            citation_count=len(section_draft.citation_keys),
            next_section=(next_task.section_id if next_task else None),
        )
        return output

    def route_next_section_or_reduce(state: ReportState) -> str:
        return "graph_context_node" if state.get("active_task") else "reduce_sections_node"

    def reduce_sections_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "reduce_sections_node", "started")
        ordered_ids = [task.section_id for task in state.get("outline", [])]
        merged_drafts = latest_draft_per_section(state.get("section_drafts", []), ordered_ids)
        merged_claims = dedupe_claims(state.get("claim_ledger", []))
        merged_evidence = dedupe_evidence(state.get("evidence_refs", []))
        merged_issues = dedupe_str_list(state.get("section_issues", []))
        stage1_conflict_issues = _stage1_conflict_issue_models(list(state.get("stage1_unresolved_conflicts", [])))
        consistency_issues = _merge_consistency_issues(
            stage1_conflict_issues,
            run_consistency_validator(merged_drafts, merged_claims, merged_evidence),
        )
        report_memory = build_report_memory(
            question=state.get("prompt", ""),
            report_type=state.get("report_type", "person"),
            primary_entities=state.get("primary_entities", []),
            primary_target_contract=state.get("primary_target_contract"),
            noteboard=state.get("noteboard", []),
            stage1_receipts=state.get("stage1_receipts", []),
            claims=merged_claims,
            evidence=merged_evidence,
            section_issues=merged_issues,
            section_drafts=merged_drafts,
            latest_observation=state.get("report_memory").latest_observation if state.get("report_memory") else "",
            consistency_issues=consistency_issues,
            limit_notes=_stage1_conflict_limit_notes(list(state.get("stage1_unresolved_conflicts", []))),
            stage1_conflict_cases=list(state.get("stage1_conflict_cases", [])),
            stage1_resolved_conflicts=list(state.get("stage1_resolved_conflicts", [])),
            stage1_unresolved_conflicts=list(state.get("stage1_unresolved_conflicts", [])),
        )
        next_state = {
            "section_drafts": merged_drafts,
            "claim_ledger": merged_claims,
            "evidence_refs": merged_evidence,
            "section_issues": merged_issues,
            "consistency_issues": consistency_issues,
            "contradiction_query_hints": dedupe_str_list(
                contradiction_query_hints(consistency_issues)
                + _stage1_conflict_query_hints(list(state.get("stage1_conflict_cases", [])))
            ),
            "report_memory": report_memory.model_copy(update={"consistency_issues": consistency_issues}),
        }
        if persist_draft_snapshots:
            try:
                persist_report_snapshot(
                    run_id=state["run_id"],
                    report_type=state.get("report_type", "person"),
                    status="draft",
                    refine_round=int(state.get("refine_round", 0)),
                    quality_ok=bool(state.get("quality_ok", False)),
                    final_report="",
                    evidence_appendix="",
                    section_drafts=merged_drafts,
                    claim_ledger=merged_claims,
                    evidence_refs=merged_evidence,
                )
            except Exception:
                logger.exception("Stage 2 snapshot persistence failed")
        emit_stage(
            state,
            "reduce_sections_node",
            "completed",
            section_count=len(merged_drafts),
            claim_count=len(merged_claims),
            evidence_count=len(merged_evidence),
        )
        return next_state

    def final_reflection_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "final_reflection_node", "started")
        outline = list(state.get("outline", []))
        drafts = list(state.get("section_drafts", []))
        report_memory = state.get("report_memory")
        consistency_issues = list(state.get("consistency_issues", []))
        draft_lookup = {item.section_id: item for item in drafts}
        missing_required = [item.section_id for item in outline if item.required and not draft_lookup.get(item.section_id)]

        reflections: List[SectionReflectionModel] = []
        quality_ok_from_llm = False
        llm_reflections_usable = False

        if final_reflection_llm is not None:
            payload = {
                "report_type": state.get("report_type", "person"),
                "primary_target_contract": state.get("primary_target_contract").model_dump() if state.get("primary_target_contract") else {},
                "outline": [item.model_dump() for item in outline],
                "section_drafts": [item.model_dump() for item in drafts],
                "section_issues": state.get("section_issues", []),
                "report_memory": (report_memory.model_dump() if report_memory is not None else {}),
                "consistency_issues": [item.model_dump() for item in consistency_issues],
                "output_schema": {
                    "quality_ok": "boolean",
                    "sections": [
                        {
                            "section_id": "string",
                            "status": "ok|needs_revision|missing",
                            "critique": "string",
                            "next_step_suggestion": "string",
                            "query_hints": ["string"],
                        }
                    ],
                },
            }
            try:
                parsed = invoke_complete_json(
                    final_reflection_llm,
                    REPORT_SECTION_REFLECTION_SYSTEM_PROMPT,
                    payload,
                    temperature=0.1,
                    timeout=_stage2_final_timeout(),
                    run_id=state["run_id"],
                    operation="stage2.final_reflection",
                )
                raw_sections = parsed.get("sections")
                if isinstance(raw_sections, list):
                    parsed_reflections = [SectionReflectionModel.model_validate(item) for item in raw_sections]
                    if parsed_reflections:
                        reflections = parsed_reflections
                        quality_ok_from_llm = bool(parsed.get("quality_ok", False))
                        llm_reflections_usable = True
                    else:
                        logger.warning(
                            "Stage 2 final reflection returned no usable sections; falling back to heuristics",
                            extra={"run_id": state["run_id"]},
                        )
            except Exception:
                logger.exception("Stage 2 final reflection failed")

        reflection_by_section = {item.section_id: item for item in reflections}
        for section_id in missing_required:
            draft = draft_lookup.get(section_id)
            reflection_by_section[section_id] = SectionReflectionModel(
                section_id=section_id,
                status="missing",
                critique="Required section is missing from the current report draft.",
                current_content=(draft.content if draft else ""),
                reflection_source="final_reflection_node",
                next_step_suggestion="Retrieve evidence aligned with the section objective and draft the missing section with concrete citations.",
                query_hints=["overview", "timeline", "official profile"],
            )

        if not reflections:
            for task in outline:
                draft = draft_lookup.get(task.section_id)
                if draft is None:
                    continue
                draft_text = draft.content.strip()
                if len(draft_text) < 350 or not draft.citation_keys:
                    reflection_by_section[task.section_id] = SectionReflectionModel(
                        section_id=task.section_id,
                        status="needs_revision",
                        reflection_source="final_reflection_node",
                        critique="Section is too thin or lacks enough explicit cited detail for the objective.",
                        current_content=draft.content,
                        next_step_suggestion="Expand this section with more evidence-backed specifics, chronology, and direct identifiers tied to the section objective.",
                        query_hints=list(task.query_hints)[:3],
                    )
                else:
                    reflection_by_section.setdefault(task.section_id, SectionReflectionModel(section_id=task.section_id, status="ok", reflection_source="final_reflection_node"))

        for task in outline:
            draft = draft_lookup.get(task.section_id)
            reflection = reflection_by_section.get(task.section_id)
            if reflection is None:
                reflection_by_section[task.section_id] = SectionReflectionModel(
                    section_id=task.section_id,
                    status="ok" if draft and draft.content.strip() else "missing",
                    current_content=(draft.content if draft else ""),
                    reflection_source="final_reflection_node",
                )
            elif not reflection.current_content and draft is not None:
                reflection_by_section[task.section_id] = reflection.model_copy(update={"current_content": draft.content})

        primary_entities = list(state.get("primary_entities", []))
        claims_by_section: Dict[str, List[ClaimModel]] = {}
        for claim in state.get("claim_ledger", []):
            claims_by_section.setdefault(claim.section_id, []).append(claim)
        for task in outline:
            draft = draft_lookup.get(task.section_id)
            if draft is None:
                continue
            drift_subject = detect_section_anchor_drift(
                task,
                draft.content,
                claims_by_section.get(task.section_id, []),
                primary_entities,
            )
            if not drift_subject:
                continue
            reflection_by_section[task.section_id] = SectionReflectionModel(
                section_id=task.section_id,
                status="needs_revision",
                reflection_source="final_reflection_node",
                critique=f"Section drifted away from the declared target and is dominated by {drift_subject}.",
                current_content=draft.content,
                next_step_suggestion=f"Rewrite this section so it stays centered on {primary_entities[0] if primary_entities else 'the primary subject'} and treats {drift_subject} only as anchored context when directly relevant.",
                query_hints=dedupe_str_list(list(task.query_hints) + [primary_entities[0]] if primary_entities else list(task.query_hints))[:4],
            )

        normalized_reflections = [
            reflection_by_section.get(task.section_id)
            for task in outline
            if reflection_by_section.get(task.section_id) is not None
        ]
        has_targets = any(item.status != "ok" for item in normalized_reflections if item is not None)
        quality_ok = (quality_ok_from_llm if llm_reflections_usable else True) and not has_targets
        emit_stage(
            state,
            "final_reflection_node",
            "completed",
            quality_ok=quality_ok,
            targeted_count=len([item for item in normalized_reflections if item is not None and item.status != "ok"]),
        )
        return {"section_reflections": [item for item in normalized_reflections if item is not None], "quality_ok": quality_ok}

    def quality_gate_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "quality_gate_node", "started")
        outline = state.get("outline", [])
        drafts = state.get("section_drafts", [])
        claims = state.get("claim_ledger", [])
        evidence_refs = state.get("evidence_refs", [])
        section_issues = list(state.get("section_issues", []))
        section_reflections = list(state.get("section_reflections", []))
        report_memory = state.get("report_memory")
        consistency_issues = list(state.get("consistency_issues", []))

        draft_ids = {item.section_id for item in drafts if item.content.strip()}
        required_ids = {item.section_id for item in outline if item.required}
        missing = sorted(required_ids - draft_ids)
        if missing:
            section_issues.append(f"Missing required sections: {', '.join(missing)}")

        revision_targets = [item for item in section_reflections if item.status == "needs_revision"]
        missing_targets = [item.section_id for item in section_reflections if item.status == "missing"]
        for reflection in revision_targets:
            critique = reflection.critique or "Section needs revision."
            section_issues.append(f"{reflection.section_id}: reflection revision required: {critique}")
        if missing_targets:
            section_issues.append(f"Reflection flagged missing sections: {', '.join(sorted(set(missing_targets)))}")

        section_issues.extend(
            build_depth_quality_issues(
                report_type=state.get("report_type", "person"),
                primary_entities=state.get("primary_entities", []),
                stage1_receipts=state.get("stage1_receipts", []),
                section_drafts=drafts,
            )
        )

        evidence_by_section: Dict[str, List[EvidenceRefModel]] = {}
        for item in evidence_refs:
            evidence_by_section.setdefault(item.section_id, []).append(item)
        for draft in drafts:
            content = draft.content.strip()
            if not content:
                continue
            if _looks_like_template_section(content):
                section_issues.append(f"{draft.section_id}: template-style fallback content was persisted instead of analyst prose.")
            section_citation_count = _section_inline_citation_count(content)
            if section_citation_count == 0 and evidence_by_section.get(draft.section_id):
                section_issues.append(f"{draft.section_id}: evidence-backed section is missing inline citation markers.")

        for claim in claims:
            if claim.impact == "high" and not claim.evidence_keys:
                section_issues.append(f"{claim.section_id}: high-impact claim missing evidence: {claim.claim_id}")
            if claim.conflict_flags:
                section_issues.append(f"{claim.section_id}: conflict flagged: {claim.claim_id}")

        # Prevent a dead loop when drafting succeeded but retrieval could not produce evidence.
        no_evidence_or_claims = bool(drafts) and not claims and not evidence_refs
        if no_evidence_or_claims:
            section_issues.append("No evidence/claims extracted; finalized with draft-only sections.")

        if evidence_refs:
            resolvable_url_count = len([item for item in evidence_refs if item.source_url])
            evidence_object_key_count = len(
                [
                    item
                    for item in evidence_refs
                    if item.evidence_object_key
                    or (isinstance(item.object_ref, dict) and (item.object_ref.get("objectKey") or item.object_ref.get("object_key")))
                ]
            )
            source_url_ratio = resolvable_url_count / max(1, len(evidence_refs))
            object_key_ratio = evidence_object_key_count / max(1, len(evidence_refs))
            if source_url_ratio < 0.95:
                section_issues.append(
                    f"Citation linkage quality gate: source URL coverage {source_url_ratio:.2f} below required 0.95."
                )
            if object_key_ratio < 1.0:
                section_issues.append(
                    f"Citation linkage quality gate: evidence object key coverage {object_key_ratio:.2f} below required 1.00."
                )

            unique_urls = {item.source_url for item in evidence_refs if item.source_url}
            unique_domains = {(item.domain or "").lower() for item in evidence_refs if (item.source_url or item.domain)}
            unique_domains = {item for item in unique_domains if item}
            if len(unique_urls) < 30 or len(unique_domains) < 10:
                section_issues.append(
                    "Retrieval diversity gate: below target (need >=30 unique URLs and >=10 unique domains) for final person report."
                )

            section_evidence_map: Dict[str, List[EvidenceRefModel]] = {}
            for item in evidence_refs:
                section_evidence_map.setdefault(item.section_id, []).append(item)
            for draft in drafts:
                section_items = section_evidence_map.get(draft.section_id, [])
                has_high_relevance = any(
                    float(item.relevance_score or 0.0) >= 0.08
                    and (
                        bool(item.evidence_object_key)
                        or (isinstance(item.object_ref, dict) and (item.object_ref.get("objectKey") or item.object_ref.get("object_key")))
                    )
                    for item in section_items
                )
                if draft.content.strip() and not has_high_relevance:
                    section_issues.append(
                        f"{draft.section_id}: section-level evidence gate failed (missing high-relevance citation tied to run evidence)."
                    )

        coverage = report_memory.coverage if report_memory is not None else build_coverage_ledger(
            state.get("report_type", "person"),
            claims,
            evidence_refs,
            drafts,
            section_issues,
        )
        if consistency_issues:
            for issue in consistency_issues:
                section_issues.append(f"Consistency issue: {issue.description}")

        quality_ok = not no_evidence_or_claims and bool(state.get("quality_ok", False)) and (
            not missing
            and not revision_targets
            and not any("high-impact claim missing evidence" in item for item in section_issues)
            and not any("Citation linkage quality gate:" in item for item in section_issues)
            and not any("Retrieval diversity gate:" in item for item in section_issues)
            and not any("section-level evidence gate failed" in item for item in section_issues)
            and not any("template-style fallback content" in item for item in section_issues)
            and not any("missing inline citation markers" in item for item in section_issues)
            and not consistency_issues
            and coverage_is_complete(coverage, state.get("report_type", "person"))
        )
        output = {
            "quality_ok": quality_ok,
            "missing_section_ids": sorted(set(missing + missing_targets)),
            "section_issues": dedupe_str_list(section_issues),
            "report_memory": (
                report_memory.model_copy(
                    update={
                        "coverage": coverage,
                        "limits": dedupe_str_list(list(report_memory.limits) + section_issues),
                        "consistency_issues": consistency_issues,
                    }
                )
                if report_memory is not None
                else report_memory
            ),
        }
        emit_stage(
            state,
            "quality_gate_node",
            "completed",
            quality_ok=quality_ok,
            missing_count=len(missing),
            issue_count=len(output["section_issues"]),
        )
        return output

    def quality_route(state: ReportState) -> str:
        if state.get("quality_ok"):
            return "finalize_report_node"
        if state.get("refine_round", 0) >= state.get("max_refine_rounds", 2):
            return "finalize_report_node"
        if any(item.status != "ok" for item in state.get("section_reflections", [])):
            return "prepare_section_revisions_node"
        return "refine_retrieval_node"

    def prepare_section_revisions_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "prepare_section_revisions_node", "started")
        targets = [item for item in state.get("section_reflections", []) if item.status != "ok"]
        target_ids = {item.section_id for item in targets}
        query_hints = list(state.get("query_hints", []))
        for item in targets:
            query_hints.extend(item.query_hints)
        output = {
            "query_hints": dedupe_str_list(query_hints),
            "refine_round": state.get("refine_round", 0) + 1,
            "section_drafts": [item for item in state.get("section_drafts", []) if item.section_id not in target_ids],
            "claim_ledger": [item for item in state.get("claim_ledger", []) if item.section_id not in target_ids],
            "evidence_refs": [item for item in state.get("evidence_refs", []) if item.section_id not in target_ids],
            "section_issues": [
                item
                for item in state.get("section_issues", [])
                if not any(item.startswith(f"{section_id}:") for section_id in target_ids)
                and not item.startswith("Missing required sections:")
                and not item.startswith("Reflection flagged missing sections:")
            ],
            "missing_section_ids": sorted(target_ids),
            "section_hits": [],
            "section_evidence_buffer": [],
            "section_claims_buffer": [],
            "section_issues_buffer": [],
            "consistency_issues": [],
        }
        emit_stage(
            state,
            "prepare_section_revisions_node",
            "completed",
            refine_round=output["refine_round"],
            target_count=len(target_ids),
        )
        return output

    def refine_retrieval_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "refine_retrieval_node", "started")
        hints = list(state.get("query_hints", []))
        for issue in state.get("section_issues", []):
            if "missing evidence" in issue:
                hints.extend(["site:linkedin.com", "site:x.com", "profile", "official"])
            if "Missing required sections" in issue:
                hints.extend(["overview", "identity", "timeline"])
            if "Consistency issue:" in issue:
                hints.extend(state.get("contradiction_query_hints", []))
        output = {
            "query_hints": dedupe_str_list(hints),
            "refine_round": state.get("refine_round", 0) + 1,
            "section_drafts": [],
            "claim_ledger": [],
            "evidence_refs": [],
            "section_issues": [],
            "section_reflections": [],
            "section_hits": [],
            "section_evidence_buffer": [],
            "section_claims_buffer": [],
            "section_issues_buffer": [],
            "consistency_issues": [],
        }
        emit_stage(
            state,
            "refine_retrieval_node",
            "completed",
            refine_round=output["refine_round"],
            query_hint_count=len(output["query_hints"]),
        )
        return output

    def finalize_report_node(state: ReportState) -> Dict[str, Any]:
        emit_stage(state, "finalize_report_node", "started")
        final_report = assemble_final_report(state, final_report_llm)
        appendix = assemble_evidence_appendix(state.get("evidence_refs", []))
        snapshot_status = "ready" if bool(state.get("quality_ok", False)) else "failed"
        try:
            persist_report_snapshot(
                run_id=state["run_id"],
                report_type=state.get("report_type", "person"),
                status=snapshot_status,
                refine_round=int(state.get("refine_round", 0)),
                quality_ok=bool(state.get("quality_ok", False)),
                final_report=final_report,
                evidence_appendix=appendix,
                section_drafts=state.get("section_drafts", []),
                claim_ledger=state.get("claim_ledger", []),
                evidence_refs=state.get("evidence_refs", []),
            )
        except Exception as exc:
            logger.exception("Stage 2 final persistence failed")
            raise RuntimeError(f"Stage 2 final persistence failed: {exc}") from exc
        emit_run_event(
            state["run_id"],
            "REPORT_READY" if snapshot_status == "ready" else "REPORT_FAILED",
            {
                "component": "report_subgraph",
                "report_type": state.get("report_type", "person"),
                "quality_ok": bool(state.get("quality_ok", False)),
                "refine_round": int(state.get("refine_round", 0)),
                "status": snapshot_status,
            },
        )
        output = {
            "final_report": final_report,
            "evidence_appendix": appendix,
            "done": True,
        }
        emit_stage(
            state,
            "finalize_report_node",
            "completed",
            report_length=len(final_report),
            appendix_length=len(appendix),
        )
        return output

    graph.add_node("report_init_node", report_init_node)
    graph.add_node("build_outline_node", build_outline_node)
    graph.add_node("section_router_node", section_router_node)
    graph.add_node("process_sections_node", process_sections_node)
    graph.add_node("graph_context_node", graph_context_node)
    graph.add_node("vector_retrieve_node", vector_retrieve_node)
    graph.add_node("evidence_pack_node", evidence_pack_node)
    graph.add_node("extract_claims_node", extract_claims_node)
    graph.add_node("verify_claims_node", verify_claims_node)
    graph.add_node("draft_section_node", draft_section_node)
    graph.add_node("reduce_sections_node", reduce_sections_node)
    graph.add_node("final_reflection_node", final_reflection_node)
    graph.add_node("quality_gate_node", quality_gate_node)
    graph.add_node("prepare_section_revisions_node", prepare_section_revisions_node)
    graph.add_node("refine_retrieval_node", refine_retrieval_node)
    graph.add_node("finalize_report_node", finalize_report_node)

    graph.set_entry_point("report_init_node")
    graph.add_edge("report_init_node", "build_outline_node")
    graph.add_edge("build_outline_node", "section_router_node")
    graph.add_conditional_edges("section_router_node", route_after_section_router, ["process_sections_node", "reduce_sections_node"])
    graph.add_edge("process_sections_node", "reduce_sections_node")
    graph.add_edge("reduce_sections_node", "final_reflection_node")
    graph.add_edge("final_reflection_node", "quality_gate_node")
    graph.add_conditional_edges(
        "quality_gate_node",
        quality_route,
        ["prepare_section_revisions_node", "refine_retrieval_node", "finalize_report_node"],
    )
    graph.add_edge("prepare_section_revisions_node", "section_router_node")
    graph.add_edge("refine_retrieval_node", "section_router_node")
    graph.add_edge("finalize_report_node", END)
    return graph


def _normalize_outline(outline: List[SectionTaskModel], max_sections: int) -> List[SectionTaskModel]:
    deduped: List[SectionTaskModel] = []
    seen_ids: set[str] = set()
    for item in outline:
        section_id = item.section_id.strip()
        if not section_id or section_id in seen_ids:
            continue
        seen_ids.add(section_id)
        deduped.append(item)
        if len(deduped) >= max_sections:
            break
    return deduped


def _dedupe_hit_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        doc_id = str(row.get("document_id") or row.get("documentId") or "")
        snippet = str(row.get("snippet") or row.get("text") or "")
        key = f"{doc_id}|{snippet[:80]}"
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())


def run_report_subgraph(
    run_id: str,
    prompt: str,
    noteboard: List[str],
    stage1_receipts: List[ToolReceipt],
    stage1_conflict_cases: List[Any] | None = None,
    stage1_resolved_conflicts: List[Any] | None = None,
    stage1_unresolved_conflicts: List[Any] | None = None,
    max_refine_rounds: int = 1,
    primary_target_contract: PrimaryTargetContractModel | None = None,
    stage2_model_config: Stage2ModelConfig | None = None,
) -> ReportResult:
    load_env()
    ensure_run_exists(run_id, prompt)
    emit_run_event(run_id, "STAGE2_STARTED", {"component": "report_subgraph"})

    final_llm: OpenRouterLLM | None = None
    section_llm: OpenRouterLLM | None = None
    role_llms: Dict[str, OpenRouterLLM] = {}
    resolved_primary_target_contract = primary_target_contract or load_primary_target_contract(run_id)
    resolved_stage2_model_config = _resolve_stage2_model_config(stage2_model_config)
    if os.getenv("OPENROUTER_API_KEY"):
        llm_cache: Dict[str, OpenRouterLLM] = {}

        def _llm_for_model(model_name: str) -> OpenRouterLLM | None:
            cleaned = str(model_name or "").strip()
            if not cleaned:
                return None
            cached = llm_cache.get(cleaned)
            if cached is None:
                cached = OpenRouterLLM(model=cleaned)
                llm_cache[cleaned] = cached
            return cached

        final_llm = _llm_for_model(resolved_stage2_model_config.final_report_model)
        section_llm = _llm_for_model(resolved_stage2_model_config.section_draft_model)
        for role, model_name in {
            "outline": resolved_stage2_model_config.outline_model,
            "section_query": resolved_stage2_model_config.section_query_model,
            "section_claim": resolved_stage2_model_config.section_claim_model,
            "section_draft": resolved_stage2_model_config.section_draft_model,
            "final_reflection": resolved_stage2_model_config.final_reflection_model,
            "final_report": resolved_stage2_model_config.final_report_model,
        }.items():
            llm = _llm_for_model(model_name)
            if llm is not None:
                role_llms[role] = llm

    mcp_client = RoutedMcpClient()
    mcp_client.start()
    try:
        graph = build_report_graph(
            mcp_client,
            section_llm=section_llm,
            final_llm=final_llm,
            role_llms=role_llms,
        )
        checkpointer: Any | None = None
        try:
            from langgraph.checkpoint.memory import MemorySaver  # type: ignore

            if os.getenv("STAGE2_CHECKPOINTER", "memory").lower() == "memory":
                checkpointer = MemorySaver()
        except Exception:
            checkpointer = None

        state = make_initial_report_state(
            run_id=run_id,
            prompt=prompt,
            noteboard=noteboard,
            stage1_receipts=stage1_receipts,
            stage1_conflict_cases=stage1_conflict_cases,
            stage1_resolved_conflicts=stage1_resolved_conflicts,
            stage1_unresolved_conflicts=stage1_unresolved_conflicts,
            max_refine_rounds=max_refine_rounds,
            primary_target_contract=resolved_primary_target_contract,
        )
        compiled = graph.compile(checkpointer=checkpointer) if checkpointer is not None else graph.compile()
        invoke_cfg = {"configurable": {"thread_id": run_id}} if checkpointer is not None else None
        final_state = compiled.invoke(state, config=invoke_cfg)
        final_report_memory = final_state.get("report_memory") or state.get("report_memory")
        result = ReportResult(
            run_id=run_id,
            report_type=final_state.get("report_type", "person"),
            final_report=final_state.get("final_report", ""),
            evidence_appendix=final_state.get("evidence_appendix", ""),
            section_drafts=final_state.get("section_drafts", []),
            claim_ledger=final_state.get("claim_ledger", []),
            evidence_refs=final_state.get("evidence_refs", []),
            quality_ok=bool(final_state.get("quality_ok", False)),
            refine_round=int(final_state.get("refine_round", 0)),
            report_memory=final_report_memory,
            primary_target_contract=final_state.get("primary_target_contract") or resolved_primary_target_contract,
        )
        emit_run_event(
            run_id,
            "STAGE2_COMPLETED",
            {
                "component": "report_subgraph",
                "quality_ok": result.quality_ok,
                "refine_round": result.refine_round,
                "section_count": len(result.section_drafts),
                "claim_count": len(result.claim_ledger),
                "evidence_count": len(result.evidence_refs),
            },
        )
        return result
    except Exception as exc:
        emit_run_event(
            run_id,
            "STAGE2_FAILED",
            {"component": "report_subgraph", "error": str(exc)},
        )
        raise
    finally:
        mcp_client.close()
