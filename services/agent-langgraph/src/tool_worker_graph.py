from __future__ import annotations

import json
import os
import re
import hashlib
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from mcp_client import McpClientProtocol
from openrouter_llm import OpenRouterLLM
from receipt_store import insert_artifact, insert_artifact_summary, insert_run_note, insert_tool_receipt
from run_events import emit_run_event
from system_prompts import (
    ACADEMIC_IDENTITY_TOOL_SUMMARY_SYSTEM_PROMPT,
    ARCHIVE_DIFF_TOOL_SUMMARY_SYSTEM_PROMPT,
    ARXIV_TOOL_SUMMARY_SYSTEM_PROMPT,
    BUSINESS_ROLE_TOOL_SUMMARY_SYSTEM_PROMPT,
    CONFERENCE_TOOL_SUMMARY_SYSTEM_PROMPT,
    DOMAIN_WHOIS_TOOL_SUMMARY_SYSTEM_PROMPT,
    GITHUB_TOOL_SUMMARY_SYSTEM_PROMPT,
    IDENTITY_EXPANSION_TOOL_SUMMARY_SYSTEM_PROMPT,
    GITLAB_TOOL_SUMMARY_SYSTEM_PROMPT,
    GRANT_TOOL_SUMMARY_SYSTEM_PROMPT,
    GOOGLE_SERP_PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT,
    GRAPH_INGEST_SYSTEM_PROMPT,
    PACKAGE_REGISTRY_TOOL_SUMMARY_SYSTEM_PROMPT,
    PATENT_TOOL_SUMMARY_SYSTEM_PROMPT,
    PERSONAL_SITE_TOOL_SUMMARY_SYSTEM_PROMPT,
    PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT,
    PUBMED_TOOL_SUMMARY_SYSTEM_PROMPT,
    SANCTIONS_TOOL_SUMMARY_SYSTEM_PROMPT,
    VECTOR_INGEST_SYSTEM_PROMPT,
    WAYBACK_TOOL_SUMMARY_SYSTEM_PROMPT,
    WORKER_TOOL_SUMMARY_SYSTEM_PROMPT,
    WORKER_SUMMARIZE_RECEIPT_SYSTEM_PROMPT,
)
from target_normalization import extract_person_targets, sanitize_search_tool_arguments
from logger import get_logger
from env import load_env

logger = get_logger(__name__)
INGEST_TOOL_NAMES = {"ingest_text", "ingest_graph_entity", "ingest_graph_entities", "ingest_graph_relations"}
URL_IN_TEXT_REGEX = re.compile(r"https?://[^\s)>\"]+")
X_HANDLE_REGEX = re.compile(r"(?<!\w)@([A-Za-z0-9_]{3,32})")
PHONE_IN_TEXT_REGEX = re.compile(r"(?:\+\d{1,3}[\s().-]?)?(?:\(?\d{2,4}\)?[\s.-]){2,}\d{2,4}")

TOOL_CONFIDENCE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "fetch_url": {"type": "utility", "confidence": 0.9},
    "osint_maigret_username": {"type": "username_recon", "confidence": 0.8},
    "osint_amass_domain": {"type": "domain_recon", "confidence": 0.9},
    "osint_whatweb_target": {"type": "web_tech", "confidence": 0.85},
    "osint_exiftool_extract": {"type": "file_meta", "confidence": 0.95},
    "osint_holehe_email": {"type": "email_recon", "confidence": 0.5},
    "osint_theharvester_email_domain": {"type": "domain_recon", "confidence": 0.6},
    "osint_reconng_domain": {"type": "domain_recon", "confidence": 0.55},
    "osint_spiderfoot_scan": {"type": "scan_recon", "confidence": 0.65},
    "osint_sublist3r_domain": {"type": "domain_recon", "confidence": 0.6},
    "osint_sherlock_username": {"type": "username_recon", "confidence": 0.45},
    "osint_whatsmyname_username": {"type": "username_recon", "confidence": 0.45},
    "osint_phoneinfoga_number": {"type": "phone_recon", "confidence": 0.4},
    "osint_dnsdumpster_domain": {"type": "domain_recon", "confidence": 0.45},
    "osint_maltego_manual": {"type": "manual", "confidence": None},
    "osint_foca_manual": {"type": "manual", "confidence": None},
    "person_search": {"type": "person_recon", "confidence": 0.65},
    "tavily_research": {"type": "deep_research", "confidence": 0.86},
    "tavily_person_search": {"type": "web_search", "confidence": 0.82},
    "x_get_user_posts_api": {"type": "social_posts", "confidence": 0.8},
    "linkedin_download_html_ocr": {"type": "social_profile_capture", "confidence": 0.75},
    "google_serp_person_search": {"type": "web_search", "confidence": 0.7},
    "arxiv_search_and_download": {"type": "research_papers", "confidence": 0.85},
    "github_identity_search": {"type": "code_identity", "confidence": 0.9},
    "gitlab_identity_search": {"type": "code_identity", "confidence": 0.85},
    "personal_site_search": {"type": "personal_site", "confidence": 0.8},
    "package_registry_search": {"type": "package_registry", "confidence": 0.8},
    "npm_author_search": {"type": "package_registry", "confidence": 0.78},
    "crates_author_search": {"type": "package_registry", "confidence": 0.76},
    "wayback_fetch_url": {"type": "archive_lookup", "confidence": 0.8},
    "open_corporates_search": {"type": "business_registry", "confidence": 0.82},
    "company_officer_search": {"type": "business_role", "confidence": 0.82},
    "company_filing_search": {"type": "business_filing", "confidence": 0.78},
    "sec_person_search": {"type": "business_sec", "confidence": 0.76},
    "director_disclosure_search": {"type": "business_director", "confidence": 0.74},
    "domain_whois_search": {"type": "business_domain", "confidence": 0.8},
    "wayback_domain_timeline_search": {"type": "archive_timeline", "confidence": 0.8},
    "historical_bio_diff": {"type": "archive_diff", "confidence": 0.85},
    "sanctions_watchlist_search": {"type": "sanctions_check", "confidence": 1.0},
    "alias_variant_generator": {"type": "identity_expansion", "confidence": 0.9},
    "username_permutation_search": {"type": "identity_expansion", "confidence": 0.8},
    "cross_platform_profile_resolver": {"type": "identity_resolution", "confidence": 0.85},
    "institution_directory_search": {"type": "institution_directory", "confidence": 0.6},
    "email_pattern_inference": {"type": "contact_inference", "confidence": 0.7},
    "contact_page_extractor": {"type": "contact_extraction", "confidence": 0.75},
    "reddit_user_search": {"type": "social_profile", "confidence": 0.8},
    "mastodon_profile_search": {"type": "social_profile", "confidence": 0.75},
    "substack_author_search": {"type": "social_profile", "confidence": 0.72},
    "medium_author_search": {"type": "social_profile", "confidence": 0.72},
    "coauthor_graph_search": {"type": "relationship_graph", "confidence": 0.8},
    "org_staff_page_search": {"type": "relationship_org", "confidence": 0.72},
    "board_member_overlap_search": {"type": "relationship_overlap", "confidence": 0.82},
    "shared_contact_pivot_search": {"type": "relationship_contact", "confidence": 0.75},
    "orcid_search": {"type": "academic_identity", "confidence": 0.85},
    "semantic_scholar_search": {"type": "academic_identity", "confidence": 0.8},
    "dblp_author_search": {"type": "academic_identity", "confidence": 0.8},
    "pubmed_author_search": {"type": "academic_publications", "confidence": 0.7},
    "grant_search_person": {"type": "academic_grants", "confidence": 0.7},
    "conference_profile_search": {"type": "academic_conferences", "confidence": 0.75},
    # Temporarily disabled until PatentSearch API integration is implemented.
    # "patent_search_person": {"type": "academic_patents", "confidence": 0.65},
    # Temporarily disabled until non-stub implementations exist.
    # "google_scholar_profile_search": {"type": "academic_stub", "confidence": 0.2},
    # "researchgate_profile_search": {"type": "academic_stub", "confidence": 0.2},
    # "ssrn_author_search": {"type": "academic_stub", "confidence": 0.2},
}


class ToolReceipt(BaseModel):
    run_id: str
    tool_name: str
    tool_type: str | None = None
    confidence_score: float | None = None
    arguments: Dict[str, Any] = Field(default_factory=dict)
    argument_signature: str = ""
    ok: bool
    summary: str
    artifact_ids: List[str] = Field(default_factory=list)
    document_ids: List[str] = Field(default_factory=list)
    key_facts: List[Dict[str, Any]] = Field(default_factory=list)
    vector_upserts: Dict[str, Any] = Field(default_factory=dict)
    graph_upserts: Dict[str, Any] = Field(default_factory=dict)
    next_hints: List[str] = Field(default_factory=list)


class ToolWorkerState(TypedDict):
    run_id: str
    tool_name: str
    arguments: Dict[str, Any]
    ok: bool
    result: Dict[str, Any]
    tool_result_summary: str
    vector_ingest_result: Dict[str, Any]
    graph_ingest_result: Dict[str, Any]
    receipt_llm_result: Dict[str, Any]
    receipt: ToolReceipt | None


@dataclass
class ToolWorkerResult:
    receipt: ToolReceipt
    result: Dict[str, Any]


def build_tool_worker_graph(mcp_client: McpClientProtocol) -> StateGraph:
    graph = StateGraph(ToolWorkerState)

    def emit_stage(state: ToolWorkerState, stage: str, status: str, **payload: Any) -> None:
        emit_run_event(
            state["run_id"],
            f"TOOL_WORKER_STAGE_{status.upper()}",
            {"tool": state["tool_name"], "stage": stage, **payload},
        )

    def execute_tool(state: ToolWorkerState) -> Dict[str, Any]:
        emit_stage(state, "execute_tool", "started")
        arguments = _normalize_tool_arguments(state["tool_name"], state["arguments"])
        emit_run_event(state["run_id"], "TOOL_WORKER_STARTED", {"tool": state["tool_name"]})
        logger.info("Tool worker executing", extra={"tool": state["tool_name"], "run_id": state["run_id"]})
        result = mcp_client.call_tool(state["tool_name"], arguments)
        emit_stage(state, "execute_tool", "completed", ok=result.ok)
        return {"arguments": arguments, "ok": result.ok, "result": result.content}

    def summarize_tool_result(state: ToolWorkerState) -> Dict[str, Any]:
        # Phase 1: convert raw tool output into normalized plain text for all downstream steps.
        emit_stage(state, "summarize_tool_result", "started")
        summary_text = _summarize_tool_output_for_ingestion(
            tool_name=state["tool_name"],
            arguments=state["arguments"],
            result=state.get("result", {}),
            ok=bool(state.get("ok", False)),
        )
        emit_stage(
            state,
            "summarize_tool_result",
            "completed",
            summary_length=len(summary_text),
        )
        return {"tool_result_summary": summary_text}

    def vector_ingest_worker(state: ToolWorkerState) -> Dict[str, Any]:
        # Phase 2a: vector ingestion is always derived from normalized tool_result_summary text.
        if not _should_run_post_ingest(state["tool_name"], bool(state.get("ok", False))):
            emit_stage(state, "vector_ingest_worker", "completed", skipped=True)
            return {"vector_ingest_result": {}}
        llm = _build_tool_llm()
        emit_stage(state, "vector_ingest_worker", "started")
        try:
            result = _run_vector_ingest_worker(
                llm=llm,
                mcp_client=mcp_client,
                run_id=state["run_id"],
                tool_name=state["tool_name"],
                arguments=state["arguments"],
                result=state.get("result", {}),
                tool_result_summary=state.get("tool_result_summary", ""),
            )
        except Exception as exc:
            logger.exception(
                "Vector ingest worker failed",
                extra={"tool": state["tool_name"], "run_id": state["run_id"]},
            )
            emit_stage(state, "vector_ingest_worker", "failed", error=str(exc))
            emit_run_event(
                state["run_id"],
                "TOOL_POST_INGEST_FAILED",
                {"tool": state["tool_name"], "phase": "vector", "error": str(exc)},
            )
            result = {}
        else:
            emit_stage(
                state,
                "vector_ingest_worker",
                "completed",
                has_result=bool(result),
                keys=sorted(result.keys()),
            )
        return {"vector_ingest_result": result}

    def graph_ingest_worker(state: ToolWorkerState) -> Dict[str, Any]:
        # Phase 2b: graph ingestion is always derived from normalized tool_result_summary text.
        if not _should_run_post_ingest(state["tool_name"], bool(state.get("ok", False))):
            emit_stage(state, "graph_ingest_worker", "completed", skipped=True)
            return {"graph_ingest_result": {}}
        llm = _build_tool_llm()
        emit_stage(state, "graph_ingest_worker", "started")
        try:
            result = _run_graph_ingest_worker(
                llm=llm,
                mcp_client=mcp_client,
                run_id=state["run_id"],
                tool_name=state["tool_name"],
                arguments=state["arguments"],
                result=state.get("result", {}),
                tool_result_summary=state.get("tool_result_summary", ""),
            )
        except Exception as exc:
            logger.exception(
                "Graph ingest worker failed",
                extra={"tool": state["tool_name"], "run_id": state["run_id"]},
            )
            emit_stage(state, "graph_ingest_worker", "failed", error=str(exc))
            emit_run_event(
                state["run_id"],
                "TOOL_POST_INGEST_FAILED",
                {"tool": state["tool_name"], "phase": "graph", "error": str(exc)},
            )
            result = {}
        else:
            emit_stage(
                state,
                "graph_ingest_worker",
                "completed",
                has_result=bool(result),
                keys=sorted(result.keys()),
            )
        return {"graph_ingest_result": result}

    def receipt_summarize_worker(state: ToolWorkerState) -> Dict[str, Any]:
        # Phase 3: construct planner receipt from normalized text + graph deltas.
        if not _should_run_post_ingest(state["tool_name"], bool(state.get("ok", False))):
            emit_stage(state, "receipt_summarize_worker", "completed", skipped=True)
            return {"receipt_llm_result": {}}
        llm = _build_tool_llm()
        if llm is None:
            emit_stage(state, "receipt_summarize_worker", "completed", skipped=True, reason="llm_unavailable")
            return {"receipt_llm_result": {}}
        emit_stage(state, "receipt_summarize_worker", "started")
        try:
            result = _run_receipt_summarize_worker(
                llm=llm,
                tool_name=state["tool_name"],
                tool_result_summary=state.get("tool_result_summary", ""),
                graph_ingest_result=state.get("graph_ingest_result", {}),
            )
        except Exception as exc:
            emit_stage(state, "receipt_summarize_worker", "failed", error=str(exc))
            result = {}
        else:
            emit_stage(
                state,
                "receipt_summarize_worker",
                "completed",
                has_result=bool(result),
                keys=sorted(result.keys()),
            )
        return {"receipt_llm_result": result}

    def persist_receipt(state: ToolWorkerState) -> Dict[str, Any]:
        emit_stage(state, "persist_receipt", "started")
        tool_type, confidence_score = _get_tool_metadata(state["tool_name"])
        argument_signature = tool_argument_signature(state["tool_name"], state["arguments"])
        summary, key_facts, vector_upserts, graph_upserts, next_hints = _summarize_result(
            state["tool_name"], state["arguments"], state.get("result", {}), state.get("ok", False)
        )
        normalized_text = str(state.get("tool_result_summary", "") or "").strip()
        if normalized_text:
            summary = _summary_from_normalized_text(normalized_text, summary)
        llm_enrichment = state.get("receipt_llm_result", {})
        summary = llm_enrichment.get("summary") or summary
        llm_key_facts = llm_enrichment.get("key_facts")
        llm_next_hints = llm_enrichment.get("next_hints")
        if isinstance(llm_key_facts, list) and llm_key_facts:
            key_facts = llm_key_facts
        if isinstance(llm_next_hints, list):
            next_hints = _dedupe_str_list(next_hints + [str(item) for item in llm_next_hints])

        vector_upserts.update(_vector_upsert_from_result(state.get("vector_ingest_result", {})))
        graph_upserts.update(_graph_upsert_from_result(state.get("graph_ingest_result", {})))
        if tool_type:
            key_facts.insert(0, {"toolType": tool_type})
            graph_upserts["toolType"] = tool_type
        if confidence_score is not None:
            key_facts.insert(1 if tool_type else 0, {"confidenceScore": confidence_score})
            graph_upserts["confidenceScore"] = confidence_score
            vector_upserts["confidenceScore"] = confidence_score
            next_hints.append(f"confidence:{confidence_score:.2f}")
            summary = _append_confidence_line(summary, confidence_score)

        artifact_ids, document_ids, summary_id = _store_artifacts_and_summary(
            state["run_id"],
            state["tool_name"],
            state["arguments"],
            state.get("result", {}),
            summary,
            key_facts,
            confidence_score,
        )

        receipt = ToolReceipt(
            run_id=state["run_id"],
            tool_name=state["tool_name"],
            tool_type=tool_type,
            confidence_score=confidence_score,
            arguments=dict(state["arguments"]),
            argument_signature=argument_signature,
            ok=bool(state.get("ok", False)),
            summary=summary,
            artifact_ids=artifact_ids,
            document_ids=document_ids,
            key_facts=key_facts,
            vector_upserts=vector_upserts,
            graph_upserts=graph_upserts,
            next_hints=next_hints,
        )

        insert_tool_receipt(
            run_id=state["run_id"],
            tool_name=state["tool_name"],
            ok=bool(state.get("ok", False)),
            arguments=state["arguments"],
            summary_id=summary_id,
            artifact_ids=artifact_ids,
            vector_upserts=vector_upserts,
            graph_upserts=graph_upserts,
            next_hints=next_hints,
        )

        note = _note_from_receipt(receipt)
        if note:
            insert_run_note(state["run_id"], note, _citations_from_receipt(receipt))
        logger.info("Tool worker receipt stored", extra={"tool": state["tool_name"], "ok": receipt.ok})

        emit_run_event(state["run_id"], "TOOL_RECEIPT_READY", {"tool": state["tool_name"], "ok": receipt.ok})
        emit_stage(
            state,
            "persist_receipt",
            "completed",
            artifact_count=len(artifact_ids),
            document_count=len(document_ids),
        )
        return {"receipt": receipt}

    graph.add_node("execute_tool", execute_tool)
    graph.add_node("summarize_tool_result", summarize_tool_result)
    graph.add_node("vector_ingest_worker", vector_ingest_worker)
    graph.add_node("graph_ingest_worker", graph_ingest_worker)
    graph.add_node("receipt_summarize_worker", receipt_summarize_worker)
    graph.add_node("persist_receipt", persist_receipt)
    graph.set_entry_point("execute_tool")
    graph.add_edge("execute_tool", "summarize_tool_result")
    graph.add_edge("summarize_tool_result", "vector_ingest_worker")
    graph.add_edge("summarize_tool_result", "graph_ingest_worker")
    graph.add_edge("vector_ingest_worker", "receipt_summarize_worker")
    graph.add_edge("graph_ingest_worker", "receipt_summarize_worker")
    # Persist only after the receipt stage so planner-facing output reflects ingestion deltas.
    graph.add_edge("receipt_summarize_worker", "persist_receipt")
    graph.add_edge("persist_receipt", END)

    return graph


def _build_tool_llm() -> OpenRouterLLM | None:
    load_env()
    if os.getenv("LANGGRAPH_ENABLE_WORKER_LLM", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    if not os.getenv("OPENROUTER_API_KEY"):
        return None
    model = (
        os.getenv("OPENROUTER_WORKER_MODEL")
        or os.getenv("OPENROUTER_TOOL_MODEL")
        or os.getenv("OPENROUTER_MODEL")
    )
    return OpenRouterLLM(model=model)


def _should_run_post_ingest(tool_name: str, ok: bool) -> bool:
    return ok and tool_name not in INGEST_TOOL_NAMES


def _summarize_tool_output_for_ingestion(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    ok: bool,
) -> str:
    fallback = _build_tool_result_text(tool_name, arguments, result)
    if not ok:
        return fallback
    llm = _build_tool_llm()
    if llm is None:
        return fallback
    payload = {
        "tool_name": tool_name,
        "arguments": arguments,
        "result": result,
        "output_schema": {"summary_text": "string"},
    }
    try:
        parsed = llm.complete_json(_tool_summary_prompt(tool_name), payload, temperature=0.1, timeout=30)
    except Exception:
        return fallback
    summary_text = parsed.get("summary_text")
    if isinstance(summary_text, str) and summary_text.strip():
        return summary_text.strip()
    return fallback


def _tool_summary_prompt(tool_name: str) -> str:
    if tool_name == "person_search":
        return PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"google_serp_person_search", "tavily_person_search", "tavily_research"}:
        return GOOGLE_SERP_PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "arxiv_search_and_download":
        return ARXIV_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "github_identity_search":
        return GITHUB_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "gitlab_identity_search":
        return GITLAB_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "personal_site_search":
        return PERSONAL_SITE_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"package_registry_search", "npm_author_search", "crates_author_search"}:
        return PACKAGE_REGISTRY_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "wayback_fetch_url":
        return WAYBACK_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"wayback_domain_timeline_search", "historical_bio_diff"}:
        return ARCHIVE_DIFF_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"open_corporates_search", "company_officer_search", "company_filing_search", "sec_person_search", "director_disclosure_search"}:
        return BUSINESS_ROLE_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "domain_whois_search":
        return DOMAIN_WHOIS_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "sanctions_watchlist_search":
        return SANCTIONS_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {
        "alias_variant_generator",
        "username_permutation_search",
        "cross_platform_profile_resolver",
        "institution_directory_search",
        "email_pattern_inference",
        "contact_page_extractor",
        "reddit_user_search",
        "mastodon_profile_search",
        "substack_author_search",
        "medium_author_search",
    }:
        return IDENTITY_EXPANSION_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {
        "coauthor_graph_search",
        "org_staff_page_search",
        "board_member_overlap_search",
        "shared_contact_pivot_search",
    }:
        return WORKER_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"orcid_search", "semantic_scholar_search", "dblp_author_search"}:
        return ACADEMIC_IDENTITY_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "pubmed_author_search":
        return PUBMED_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "grant_search_person":
        return GRANT_TOOL_SUMMARY_SYSTEM_PROMPT
    # Temporarily disabled until PatentSearch API integration is implemented.
    # if tool_name == "patent_search_person":
    #     return PATENT_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name == "conference_profile_search":
        return CONFERENCE_TOOL_SUMMARY_SYSTEM_PROMPT
    return WORKER_TOOL_SUMMARY_SYSTEM_PROMPT


def run_tool_worker(
    mcp_client: McpClientProtocol,
    run_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
) -> ToolWorkerResult:
    graph = build_tool_worker_graph(mcp_client)
    state: ToolWorkerState = {
        "run_id": run_id,
        "tool_name": tool_name,
        "arguments": arguments,
        "ok": False,
        "result": {},
        "tool_result_summary": "",
        "vector_ingest_result": {},
        "graph_ingest_result": {},
        "receipt_llm_result": {},
        "receipt": None,
    }
    final_state = graph.compile().invoke(state)
    receipt = final_state.get("receipt")
    if receipt is None:
        raise RuntimeError("Tool worker did not return a receipt")
    return ToolWorkerResult(receipt=receipt, result=final_state.get("result", {}))


def tool_argument_signature(tool_name: str, arguments: Dict[str, Any]) -> str:
    payload = {
        key: _canonical_argument_value(value)
        for key, value in sorted(arguments.items())
        if key != "runId"
    }
    return f"{tool_name}|{json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)}"


def _canonical_argument_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _canonical_argument_value(nested)
            for key, nested in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_canonical_argument_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _run_vector_ingest_worker(
    llm: OpenRouterLLM | None,
    mcp_client: McpClientProtocol,
    run_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    tool_result_summary: str,
) -> Dict[str, Any]:
    seed_text = tool_result_summary or _build_tool_result_text(tool_name, arguments, result)
    seed_args: Dict[str, Any] = {
        "runId": run_id,
        "text": seed_text,
        "title": f"{tool_name} result",
    }
    source_url = _extract_source_url(arguments, result)
    if source_url:
        seed_args["sourceUrl"] = source_url
    evidence_ref = _extract_evidence(result)
    if evidence_ref:
        seed_args["evidenceJson"] = evidence_ref

    llm_args = seed_args
    if llm is not None:
        try:
            llm_args = llm.refine_tool_arguments(VECTOR_INGEST_SYSTEM_PROMPT, "ingest_text", seed_args)
        except Exception:
            llm_args = seed_args

    if not isinstance(llm_args, dict):
        llm_args = seed_args
    llm_args = _coerce_json_argument_fields(
        llm_args,
        seed_args,
        json_keys=("evidenceJson",),
    )
    llm_args["runId"] = run_id
    normalized = _normalize_tool_arguments("ingest_text", llm_args)
    tool_result = mcp_client.call_tool("ingest_text", normalized)
    if not tool_result.ok:
        raise RuntimeError(f"ingest_text failed: {tool_result.content}")
    if not isinstance(tool_result.content, dict):
        raise RuntimeError("ingest_text returned non-dict content")
    return tool_result.content


def _run_graph_ingest_worker(
    llm: OpenRouterLLM | None,
    mcp_client: McpClientProtocol,
    run_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    tool_result_summary: str,
) -> Dict[str, Any]:
    seed_args: Dict[str, Any] = {
        "runId": run_id,
        "entityType": "Snippet",
        "entityId": _stable_snippet_entity_id(run_id, tool_name, result, tool_result_summary),
        "propertiesJson": {
            "sourceTool": tool_name,
            "toolSummary": tool_result_summary,
            "toolArgsJson": json.dumps(arguments, sort_keys=True),
        },
    }

    evidence_ref = _extract_evidence(result)
    if evidence_ref:
        seed_args["evidenceJson"] = {"objectRef": evidence_ref}

    llm_args = seed_args
    if llm is not None:
        try:
            llm_args = llm.refine_tool_arguments(
                GRAPH_INGEST_SYSTEM_PROMPT, "ingest_graph_entity", seed_args
            )
        except Exception:
            llm_args = seed_args

    if not isinstance(llm_args, dict):
        llm_args = seed_args
    llm_args = _coerce_json_argument_fields(
        llm_args,
        seed_args,
        json_keys=("propertiesJson", "evidenceJson", "relationsJson"),
    )
    llm_args["runId"] = run_id
    if llm_args.get("entityType") == "Snippet":
        llm_args.setdefault("entityId", seed_args["entityId"])
    normalized = _normalize_tool_arguments("ingest_graph_entity", llm_args)
    if not _ingest_graph_entity_has_merge_key(normalized):
        normalized = _normalize_tool_arguments("ingest_graph_entity", seed_args)
    tool_result = mcp_client.call_tool("ingest_graph_entity", normalized)
    if not tool_result.ok:
        raise RuntimeError(f"ingest_graph_entity failed: {tool_result.content}")
    if not isinstance(tool_result.content, dict):
        raise RuntimeError("ingest_graph_entity returned non-dict content")
    return tool_result.content


def _stable_snippet_entity_id(
    run_id: str,
    tool_name: str,
    result: Dict[str, Any],
    tool_result_summary: str,
) -> str:
    evidence = _extract_evidence(result) or {}
    stable_bits = [
        str(evidence.get("documentId") or ""),
        str(evidence.get("objectKey") or ""),
        str(result.get("documentId") or ""),
        str(result.get("sourceUrl") or ""),
        tool_result_summary[:400],
    ]
    digest = hashlib.sha256("|".join(stable_bits).encode("utf-8")).hexdigest()[:16]
    return f"snippet:{run_id}:{tool_name}:{digest}"


def _coerce_json_argument_fields(
    refined_args: Dict[str, Any],
    seed_args: Dict[str, Any],
    json_keys: tuple[str, ...],
) -> Dict[str, Any]:
    output = dict(refined_args)
    for key in json_keys:
        value = output.get(key)
        if isinstance(value, (dict, list)):
            continue
        if isinstance(value, str):
            try:
                json.loads(value)
                continue
            except json.JSONDecodeError:
                pass
        if key in seed_args:
            output[key] = seed_args[key]
        else:
            output.pop(key, None)
    return output


def _parse_json_object_argument(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _ingest_graph_entity_has_merge_key(arguments: Dict[str, Any]) -> bool:
    entity_id = arguments.get("entityId")
    if isinstance(entity_id, str) and entity_id.strip():
        return True

    entity_type = str(arguments.get("entityType") or "").strip()
    properties = _parse_json_object_argument(arguments.get("propertiesJson"))

    if entity_type == "Location":
        return any(properties.get(key) for key in ("location_id", "name", "address"))
    if entity_type == "Email":
        return any(properties.get(key) for key in ("address_normalized", "address", "email"))
    if entity_type == "Domain":
        return any(properties.get(key) for key in ("name_normalized", "name", "domain"))
    if entity_type in {"Person", "Organization"}:
        return any(
            properties.get(key)
            for key in ("name_normalized", "name", "person_id", "org_id")
        )
    if entity_type == "Article":
        return any(properties.get(key) for key in ("uri_normalized", "uri", "url"))

    return any(
        properties.get(key)
        for key in ("person_id", "org_id", "location_id", "email", "address", "uri", "name")
    )


def _run_receipt_summarize_worker(
    llm: OpenRouterLLM,
    tool_name: str,
    tool_result_summary: str,
    graph_ingest_result: Dict[str, Any],
) -> Dict[str, Any]:
    payload = {
        "tool_name": tool_name,
        "tool_result_summary": tool_result_summary,
        "graph_ingest_result": graph_ingest_result,
        "output_schema": {
            "summary": "string",
            "key_facts": "array<object>",
            "next_hints": "array<string>",
        },
    }
    try:
        parsed = llm.complete_json(WORKER_SUMMARIZE_RECEIPT_SYSTEM_PROMPT, payload, temperature=0.1, timeout=30)
    except Exception:
        return {}

    summary = parsed.get("summary")
    key_facts = parsed.get("key_facts")
    next_hints = parsed.get("next_hints")

    output: Dict[str, Any] = {}
    if isinstance(summary, str) and summary.strip():
        output["summary"] = summary.strip()
    if isinstance(key_facts, list):
        output["key_facts"] = [item for item in key_facts if isinstance(item, dict)]
    if isinstance(next_hints, list):
        output["next_hints"] = [str(item) for item in next_hints if str(item).strip()]
    return output


def _summarize_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    ok: bool,
) -> tuple[str, List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], List[str]]:
    if not ok:
        error_text = result.get("error") if isinstance(result, dict) else None
        summary = f"{tool_name} failed" + (f": {error_text}" if error_text else ".")
        return summary, [], {}, {}, []

    key_facts: List[Dict[str, Any]] = []
    vector_upserts: Dict[str, Any] = {}
    graph_upserts: Dict[str, Any] = {}
    next_hints: List[str] = []
    command_issue = _extract_command_issue(result)

    if tool_name == "fetch_url":
        url = arguments.get("url")
        summary = "Fetched URL and stored raw content."
        key_facts.append({"documentId": result.get("documentId"), "url": url})
        key_facts.append(
            {
                "contentType": result.get("contentType"),
                "sizeBytes": result.get("sizeBytes"),
                "statusCode": result.get("statusCode"),
                "finalUrl": result.get("finalUrl"),
            }
        )
        title = result.get("title")
        if isinstance(title, str) and title.strip():
            key_facts.append({"title": title.strip()})
        same_host_links = result.get("sameHostLinks")
        if isinstance(same_host_links, list):
            normalized_links = [
                str(item).strip()
                for item in same_host_links
                if isinstance(item, str) and str(item).strip()
            ]
            if normalized_links:
                key_facts.append({"sameHostLinkCount": len(normalized_links)})
                next_hints = _dedupe_str_list(normalized_links)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "ingest_text":
        summary = "Ingested text into chunks and vectors."
        key_facts.append({"documentId": result.get("documentId"), "chunkCount": result.get("chunkCount")})
        vector_upserts = {
            "count": result.get("vectorCount"),
            "collection": result.get("collection"),
            "embeddingModel": result.get("embeddingModel"),
        }
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "ingest_graph_entity":
        summary = "Ingested graph entity and relationships."
        key_facts.append({"entityType": result.get("entityType"), "relationCount": result.get("relationCount")})
        graph_upserts = {"relationCount": result.get("relationCount")}
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "ingest_graph_entities":
        summary = "Ingested graph entities in batch."
        key_facts.append({"count": result.get("count")})
        graph_upserts = {"count": result.get("count")}
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "ingest_graph_relations":
        summary = "Linked graph relations."
        key_facts.append({"count": result.get("count")})
        graph_upserts = {"count": result.get("count")}
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name.startswith("osint_") and tool_name not in {
        "osint_amass_domain",
        "osint_theharvester_email_domain",
        "osint_reconng_domain",
        "osint_spiderfoot_scan",
        "osint_maigret_username",
    }:
        summary = f"Executed {tool_name}."
        if isinstance(result, dict):
            for key in (
                "foundCount",
                "usedServiceCount",
                "breachCount",
                "subdomainCount",
                "recordCountApprox",
                "openPortCount",
                "post_count",
                "tweet_count",
                "returncode",
            ):
                if key in result:
                    key_facts.append({key: result.get(key)})

        if command_issue:
            summary = f"{tool_name} returned no usable results: {command_issue}"
            return summary, key_facts, vector_upserts, graph_upserts, next_hints
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "osint_amass_domain":
        domain = result.get("domain") or arguments.get("domain")
        subdomains = _extract_string_list(result.get("subdomains"))
        summary = f"Ran Amass for {domain or 'target domain'}."
        if command_issue:
            summary = f"Amass could not enumerate {domain or 'the domain'}: {command_issue}"
        elif subdomains:
            summary = (
                f"Amass found {len(subdomains)} subdomain(s) for {domain or 'the domain'}; "
                f"top examples: {', '.join(subdomains[:5])}."
            )
            next_hints = subdomains[:10]
        else:
            summary = f"Amass found no subdomains for {domain or 'the domain'}."
        key_facts.append({"domain": domain, "subdomainCount": result.get("subdomainCount", len(subdomains))})
        if subdomains:
            key_facts.append({"subdomains": subdomains[:10]})
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "osint_theharvester_email_domain":
        domain = result.get("domain") or arguments.get("domain")
        generated_files = _extract_string_list(result.get("generatedFiles"))
        emails = _extract_strings_from_text(result.get("stdout", ""), kind="email")
        domains = _extract_strings_from_text(result.get("stdout", ""), kind="domain")
        summary = f"Ran theHarvester for {domain or 'the domain'}."
        if command_issue:
            summary = f"theHarvester could not collect data for {domain or 'the domain'}: {command_issue}"
        elif emails or domains or generated_files:
            summary = (
                f"theHarvester collected {len(emails)} email(s) and {len(domains)} domain/host candidate(s) "
                f"for {domain or 'the domain'}."
            )
        else:
            summary = f"theHarvester returned no usable findings for {domain or 'the domain'}."
        key_facts.append({"domain": domain, "generatedFileCount": len(generated_files)})
        if emails:
            key_facts.append({"emails": emails[:10]})
        if domains:
            key_facts.append({"domains": domains[:10]})
        next_hints = _dedupe_str_list(emails[:10] + domains[:10])
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "osint_reconng_domain":
        domain = result.get("domain") or arguments.get("domain")
        module = result.get("module") or arguments.get("module")
        hosts = _extract_table_row_values(result.get("stdout", ""), {"hosts", "host"})
        contacts = _extract_table_row_values(result.get("stdout", ""), {"contacts", "contact"})
        summary = f"Ran Recon-ng module {module or 'unknown'} for {domain or 'the domain'}."
        if command_issue:
            summary = f"Recon-ng did not yield usable output for {domain or 'the domain'}: {command_issue}"
        elif hosts or contacts:
            summary = (
                f"Recon-ng surfaced {len(hosts)} host(s) and {len(contacts)} contact candidate(s) "
                f"for {domain or 'the domain'}."
            )
        else:
            summary = f"Recon-ng completed for {domain or 'the domain'} but returned no host/contact rows."
        key_facts.append({"domain": domain, "module": module})
        if hosts:
            key_facts.append({"hosts": hosts[:10]})
        if contacts:
            key_facts.append({"contacts": contacts[:10]})
        next_hints = _dedupe_str_list(hosts[:10] + contacts[:10])
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "osint_spiderfoot_scan":
        target = result.get("target") or arguments.get("target")
        summary = f"Ran SpiderFoot scan for {target or 'the target'}."
        if command_issue:
            summary = f"SpiderFoot did not produce usable output for {target or 'the target'}: {command_issue}"
        elif result.get("ok") is False or result.get("error"):
            summary = f"SpiderFoot returned no usable findings for {target or 'the target'}."
        key_facts.append({"target": target, "returncode": result.get("returncode")})
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "osint_maigret_username":
        username = arguments.get("username")
        parsed = result.get("parsed")
        parsed_count = len(parsed) if isinstance(parsed, dict) else 0
        profile_urls = _extract_url_candidates(result)
        related_handles = _extract_related_handles(result)
        summary = f"Ran Maigret for @{username}."
        if command_issue:
            summary = f"Maigret could not profile @{username}: {command_issue}"
        elif parsed_count or profile_urls:
            summary = (
                f"Maigret found {parsed_count or len(profile_urls)} claimed profile candidate(s) for @{username}; "
                f"sample pivots: {', '.join(profile_urls[:5] or related_handles[:5])}."
            )
        else:
            summary = f"Maigret returned no claimed profiles for @{username}."
        key_facts.append({"username": username, "claimedProfileCount": parsed_count})
        if profile_urls:
            key_facts.append({"profileUrls": profile_urls[:10]})
        if related_handles:
            key_facts.append({"relatedHandles": related_handles[:10]})
        next_hints = _dedupe_str_list(profile_urls[:10] + related_handles[:10])
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "person_search":
        target_name = str(result.get("name") or arguments.get("name") or "").strip()
        summary = f"Searched public web sources for {target_name or 'the target person'}."
        key_facts.append({"name": result.get("name"), "count": result.get("count")})
        results = result.get("results")
        if isinstance(results, list):
            urls = [
                str(item.get("url")).strip()
                for item in results
                if isinstance(item, dict) and isinstance(item.get("url"), str) and str(item.get("url")).strip()
            ]
            combined_text = " ".join(
                str(item.get("extracted_text") or item.get("snippet") or "")
                for item in results
                if isinstance(item, dict)
            )
            emails = _extract_strings_from_text(combined_text, kind="email")
            phones = _extract_phone_numbers_from_text(combined_text)
            related_people = _extract_related_people(combined_text, exclude_names=[target_name])
            history_terms = _extract_history_markers(combined_text)
            if urls:
                key_facts.append({"profileUrls": urls[:10]})
            if emails:
                key_facts.append({"emails": emails[:10]})
            if phones:
                key_facts.append({"phones": phones[:10]})
            if related_people:
                key_facts.append({"relatedPeople": related_people[:10]})
            if history_terms:
                key_facts.append({"historySignals": history_terms[:8]})
            next_hints = _dedupe_str_list(urls[:10] + emails[:10] + phones[:10] + related_people[:10])
            summary = (
                f"Searched public web sources for {target_name or 'the target person'} and reviewed {len(results)} result page(s); "
                f"pivots include {', '.join((related_people or emails or phones or urls)[:5])}."
            )
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {"github_identity_search", "gitlab_identity_search", "personal_site_search", "package_registry_search", "npm_author_search", "crates_author_search"}:
        summary, technical_facts, next_hints = _summarize_technical_tool_result(tool_name, arguments, result)
        key_facts.extend(technical_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "wayback_fetch_url":
        summary, archive_facts, next_hints = _summarize_wayback_tool_result(arguments, result)
        key_facts.extend(archive_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {"wayback_domain_timeline_search", "historical_bio_diff"}:
        summary, archive_facts, next_hints = _summarize_archive_tool_result(tool_name, arguments, result)
        key_facts.extend(archive_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {"open_corporates_search", "company_officer_search", "company_filing_search", "sec_person_search", "director_disclosure_search", "domain_whois_search"}:
        summary, business_facts, next_hints = _summarize_business_tool_result(tool_name, arguments, result)
        key_facts.extend(business_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "sanctions_watchlist_search":
        summary, safety_facts, next_hints = _summarize_sanctions_tool_result(arguments, result)
        key_facts.extend(safety_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {
        "alias_variant_generator",
        "username_permutation_search",
        "cross_platform_profile_resolver",
        "institution_directory_search",
        "email_pattern_inference",
        "contact_page_extractor",
    }:
        summary, identity_facts, next_hints = _summarize_identity_expansion_tool_result(tool_name, arguments, result)
        key_facts.extend(identity_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {
        "reddit_user_search",
        "mastodon_profile_search",
        "substack_author_search",
        "medium_author_search",
    }:
        summary, social_facts, next_hints = _summarize_social_tool_result(tool_name, arguments, result)
        key_facts.extend(social_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {
        "coauthor_graph_search",
        "org_staff_page_search",
        "board_member_overlap_search",
        "shared_contact_pivot_search",
    }:
        summary, relationship_facts, next_hints = _summarize_relationship_tool_result(tool_name, arguments, result)
        key_facts.extend(relationship_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "x_get_user_posts_api":
        username = result.get("username") or arguments.get("username")
        summary = f"Fetched recent X posts for @{username}."
        key_facts.append({"username": username, "outputPath": result.get("output_path")})
        nested = result.get("result")
        if isinstance(nested, dict):
            for key in ("tweet_count", "post_count"):
                if key in nested:
                    key_facts.append({key: nested.get(key)})
            user = nested.get("user")
            if isinstance(user, dict):
                display_name = user.get("name")
                if display_name:
                    key_facts.append({"displayName": display_name})
            tweets = nested.get("tweets")
            if isinstance(tweets, list):
                tweet_urls = _build_x_status_urls(nested.get("user"), tweets)
                mentioned_handles = _extract_handles_from_tweets(tweets)
                external_urls = _extract_url_candidates(tweets)
                if tweet_urls:
                    key_facts.append({"tweetUrls": tweet_urls[:5]})
                if mentioned_handles:
                    key_facts.append({"mentionedHandles": mentioned_handles[:10]})
                next_hints = _dedupe_str_list(tweet_urls[:5] + external_urls[:10] + mentioned_handles[:10])
                summary = (
                    f"Fetched {len(tweets)} recent X post(s) for @{username}; "
                    f"mentions/pivots include {', '.join((mentioned_handles or external_urls or tweet_urls)[:5])}."
                )
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "linkedin_download_html_ocr":
        profile = result.get("profile") or arguments.get("profile")
        summary = f"Captured LinkedIn HTML/OCR artifacts for {profile or 'profile'}."
        if command_issue:
            summary = f"LinkedIn capture did not produce usable artifacts for {profile or 'profile'}: {command_issue}"
        key_facts.append({"profile": profile, "fileCount": result.get("file_count")})
        key_facts.append({"outputDir": result.get("output_dir")})
        if profile:
            next_hints = [str(profile)]
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {"google_serp_person_search", "tavily_person_search", "tavily_research"}:
        if tool_name == "tavily_research":
            search_label = "Tavily research"
        elif tool_name == "tavily_person_search":
            search_label = "Tavily person search"
        else:
            search_label = "Google SERP person search"
        target_name = str(result.get("target_name") or arguments.get("target_name") or "").strip()
        if not target_name and tool_name == "tavily_research":
            target_name = str(result.get("input") or arguments.get("input") or arguments.get("query") or "").strip()
        summary = f"Ran {search_label} for {target_name or 'the target'} and archived result pages."
        key_facts.append({"targetName": result.get("target_name"), "outputDir": result.get("output_dir")})
        if tool_name == "tavily_research":
            key_facts.append({"requestId": result.get("request_id"), "status": result.get("status")})
        rows = result.get("extracted_results")
        if isinstance(rows, list):
            urls = [
                str(item.get("url")).strip()
                for item in rows
                if isinstance(item, dict) and isinstance(item.get("url"), str) and str(item.get("url")).strip()
            ]
            combined_text = " ".join(
                str(item.get("title") or "") + " " + str(item.get("extracted_text") or "")
                for item in rows
                if isinstance(item, dict)
            )
            related_people = _extract_related_people(combined_text, exclude_names=[target_name])
            emails = _extract_strings_from_text(combined_text, kind="email")
            phones = _extract_phone_numbers_from_text(combined_text)
            source_types = _extract_serp_source_types(rows)
            if urls:
                key_facts.append({"sourceUrls": urls[:10]})
            if source_types:
                key_facts.append({"sourceTypes": source_types[:8]})
            if related_people:
                key_facts.append({"relatedPeople": related_people[:10]})
            if emails:
                key_facts.append({"emails": emails[:10]})
            if phones:
                key_facts.append({"phones": phones[:10]})
            next_hints = _dedupe_str_list(urls[:10] + related_people[:10] + emails[:10] + phones[:10])
            summary = (
                f"Ran {search_label} for {target_name or 'the target'} and found {len(rows)} archived result(s); "
                f"source types include {', '.join(source_types[:4] or ['public profiles'])}."
            )
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name == "arxiv_search_and_download":
        summary = "Queried arXiv and downloaded matched papers."
        key_facts.append({"outputDir": result.get("output_dir"), "metadataPath": result.get("metadata_path")})
        nested = result.get("metadata")
        if isinstance(nested, dict):
            for key in ("search_query", "total_available", "collected_count", "downloaded_count"):
                if key in nested:
                    key_facts.append({key: nested.get(key)})
        entries = result.get("extracted_entries")
        if isinstance(entries, list):
            coauthors = _extract_arxiv_coauthors(entries, exclude_names=[str(arguments.get("author") or "")])
            affiliations = _extract_arxiv_affiliations(entries)
            paper_urls = [
                str(item.get("pdf_url")).strip()
                for item in entries
                if isinstance(item, dict) and isinstance(item.get("pdf_url"), str) and str(item.get("pdf_url")).strip()
            ]
            if coauthors:
                key_facts.append({"coauthors": coauthors[:10]})
            if affiliations:
                key_facts.append({"affiliations": affiliations[:10]})
            if paper_urls:
                key_facts.append({"paperUrls": paper_urls[:10]})
            next_hints = _dedupe_str_list(coauthors[:10] + affiliations[:10] + paper_urls[:10])
            summary = (
                f"Queried arXiv and reviewed {len(entries)} matched paper(s); "
                f"co-author or affiliation pivots include {', '.join((coauthors or affiliations or paper_urls)[:5])}."
            )
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {
        "orcid_search",
        "semantic_scholar_search",
        "dblp_author_search",
        "pubmed_author_search",
        "grant_search_person",
        "conference_profile_search",
        # Temporarily disabled until PatentSearch API integration is implemented.
        # "patent_search_person",
        # Temporarily disabled until non-stub implementations exist.
        # "google_scholar_profile_search",
        # "researchgate_profile_search",
        # "ssrn_author_search",
    }:
        summary, candidate_facts, next_hints = _summarize_academic_tool_result(tool_name, arguments, result)
        key_facts.extend(candidate_facts)
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    summary = f"Executed {tool_name}."
    return summary, key_facts, vector_upserts, graph_upserts, next_hints


def _store_artifacts_and_summary(
    run_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    summary: str,
    key_facts: List[Dict[str, Any]],
    confidence_score: float | None,
) -> tuple[List[str], List[str], str | None]:
    artifact_ids: List[str] = []
    document_ids: List[str] = []
    summary_id: str | None = None

    evidence = _extract_evidence(result)
    if evidence:
        artifact_id = insert_artifact(
            run_id=run_id,
            tool_name=tool_name,
            kind="tool_result",
            document_id=evidence.get("documentId"),
            bucket=evidence.get("bucket"),
            object_key=evidence.get("objectKey"),
            version_id=evidence.get("versionId"),
            etag=evidence.get("etag"),
            size_bytes=evidence.get("sizeBytes"),
            content_type=evidence.get("contentType"),
            sha256=evidence.get("sha256"),
        )
        artifact_ids.append(artifact_id)
        if evidence.get("documentId"):
            document_ids.append(str(evidence.get("documentId")))

    evidence_refs = _extract_evidence_refs_from_arguments(tool_name, arguments)
    for ref in evidence_refs:
        artifact_id = insert_artifact(
            run_id=run_id,
            tool_name=tool_name,
            kind="evidence_ref",
            document_id=ref.get("documentId"),
            bucket=ref.get("bucket"),
            object_key=ref.get("objectKey"),
            version_id=ref.get("versionId"),
            etag=ref.get("etag"),
            size_bytes=None,
            content_type=None,
            sha256=None,
        )
        artifact_ids.append(artifact_id)
        if ref.get("documentId"):
            document_ids.append(str(ref.get("documentId")))

    if tool_name == "fetch_url" and result.get("documentId"):
        artifact_id = insert_artifact(
            run_id=run_id,
            tool_name=tool_name,
            kind=result.get("sourceType") or "raw",
            document_id=result.get("documentId"),
            bucket=result.get("bucket"),
            object_key=result.get("objectKey"),
            version_id=result.get("versionId"),
            etag=result.get("etag"),
            size_bytes=result.get("sizeBytes"),
            content_type=result.get("contentType"),
            sha256=result.get("sha256"),
        )
        artifact_ids.append(artifact_id)
        document_ids.append(str(result.get("documentId")))

    if tool_name == "ingest_text" and result.get("documentId"):
        artifact_id = insert_artifact(
            run_id=run_id,
            tool_name=tool_name,
            kind="text",
            document_id=result.get("documentId"),
            sha256=None,
        )
        artifact_ids.append(artifact_id)
        document_ids.append(str(result.get("documentId")))

    if artifact_ids:
        summary_id = insert_artifact_summary(
            artifact_ids[0],
            summary,
            key_facts,
            confidence=confidence_score,
        )

    return artifact_ids, document_ids, summary_id


def _summarize_academic_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    query = result.get("query") if isinstance(result.get("query"), dict) else {}
    candidates = result.get("candidates") if isinstance(result.get("candidates"), list) else []
    records = result.get("records") if isinstance(result.get("records"), list) else []
    status = result.get("status")
    message = result.get("message")

    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []
    compact_candidates: List[Dict[str, Any]] = []

    for candidate in candidates[:5]:
        if not isinstance(candidate, dict):
            continue
        evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), list) else []
        evidence_urls = [
            str(item.get("url")).strip()
            for item in evidence
            if isinstance(item, dict) and isinstance(item.get("url"), str) and str(item.get("url")).strip()
        ]
        affiliations = _extract_string_list(candidate.get("affiliations"))
        topics = _extract_string_list(candidate.get("topics"))
        external_ids = candidate.get("external_ids") if isinstance(candidate.get("external_ids"), dict) else {}
        works_summary = candidate.get("works_summary") if isinstance(candidate.get("works_summary"), dict) else {}
        compact_candidate = {
            "canonical_name": candidate.get("canonical_name"),
            "source": candidate.get("source"),
            "source_id": candidate.get("source_id"),
            "confidence": candidate.get("confidence"),
            "affiliations": affiliations[:6],
            "topics": topics[:6],
            "external_ids": external_ids,
            "works_summary": works_summary,
            "evidence": evidence[:3] if isinstance(evidence, list) else [],
        }
        if candidate.get("homepage"):
            compact_candidate["homepage"] = candidate.get("homepage")
        if candidate.get("profile_url"):
            compact_candidate["profile_url"] = candidate.get("profile_url")
        compact_candidates.append(compact_candidate)
        next_hints.extend(evidence_urls[:3])
        next_hints.extend(affiliations[:3])
        next_hints.extend(topics[:3])

    if compact_candidates:
        compact_candidates.sort(key=lambda item: float(item.get("confidence") or 0.0), reverse=True)
        top = compact_candidates[0]
        summary = (
            f"{tool_name} returned {len(compact_candidates)} academic candidate(s) for "
            f"{query.get('person_name') or arguments.get('person_name') or 'the target'}; "
            f"top candidate {top.get('canonical_name') or 'unknown'} at confidence {top.get('confidence')}."
        )
        key_facts.append({"candidates": compact_candidates})
        top_ids = top.get("external_ids")
        if isinstance(top_ids, dict) and top_ids:
            key_facts.append({"externalIds": top_ids})
    elif isinstance(status, str) and status:
        summary = f"{tool_name} returned status {status}" + (f": {message}" if isinstance(message, str) and message else ".")
    else:
        summary = f"{tool_name} returned no academic candidates."

    compact_records: List[Dict[str, Any]] = []
    for record in records[:10]:
        if not isinstance(record, dict):
            continue
        compact_record = {}
        for key in ("grant_id", "patent_id", "pmid", "title", "venue", "year", "journal", "institution", "agency", "url", "fiscal_year", "filing_date"):
            if key in record:
                compact_record[key] = record.get(key)
        if compact_record:
            compact_records.append(compact_record)
            if isinstance(record.get("url"), str) and str(record.get("url")).strip():
                next_hints.append(str(record.get("url")).strip())

    if compact_records:
        key_facts.append({"records": compact_records})
        summary = f"{summary.rstrip('.')} Includes {len(compact_records)} structured record(s)."
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_technical_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []

    profile_url = str(result.get("profile_url") or "").strip()
    if profile_url:
        key_facts.append({"profileUrl": profile_url})
        next_hints.append(profile_url)

    organizations = result.get("organizations") if isinstance(result.get("organizations"), list) else []
    compact_orgs = []
    for item in organizations[:10]:
        if not isinstance(item, dict):
            continue
        compact = {}
        for key in ("name", "url", "relation"):
            if key in item:
                compact[key] = item.get(key)
        if compact:
            compact_orgs.append(compact)
            if isinstance(item.get("url"), str) and str(item.get("url")).strip():
                next_hints.append(str(item.get("url")).strip())
            if isinstance(item.get("name"), str) and str(item.get("name")).strip():
                next_hints.append(str(item.get("name")).strip())
    if compact_orgs:
        key_facts.append({"organizations": compact_orgs})

    repositories = result.get("repositories") if isinstance(result.get("repositories"), list) else []
    compact_repos = []
    for item in repositories[:8]:
        if not isinstance(item, dict):
            continue
        compact = {}
        for key in ("name", "url", "language", "stars", "updated_at"):
            if key in item:
                compact[key] = item.get(key)
        if compact:
            compact_repos.append(compact)
            if isinstance(item.get("url"), str) and str(item.get("url")).strip():
                next_hints.append(str(item.get("url")).strip())
    if compact_repos:
        key_facts.append({"repositories": compact_repos})

    publications = result.get("publications") if isinstance(result.get("publications"), list) else []
    if publications:
        key_facts.append({"publications": publications[:8]})

    contact_signals = result.get("contact_signals") if isinstance(result.get("contact_signals"), list) else []
    compact_contacts = []
    for item in contact_signals[:10]:
        if not isinstance(item, dict):
            continue
        compact = {}
        for key in ("type", "value", "source"):
            if key in item:
                compact[key] = item.get(key)
        if compact:
            compact_contacts.append(compact)
            if isinstance(item.get("value"), str) and str(item.get("value")).strip():
                next_hints.append(str(item.get("value")).strip())
    if compact_contacts:
        key_facts.append({"contactSignals": compact_contacts})

    external_links = result.get("external_links") if isinstance(result.get("external_links"), list) else []
    compact_links = []
    for item in external_links[:12]:
        if not isinstance(item, dict):
            continue
        compact = {}
        for key in ("type", "url"):
            if key in item:
                compact[key] = item.get(key)
        if compact:
            compact_links.append(compact)
            if isinstance(item.get("url"), str) and str(item.get("url")).strip():
                next_hints.append(str(item.get("url")).strip())
    if compact_links:
        key_facts.append({"externalLinks": compact_links})

    evidence = result.get("evidence") if isinstance(result.get("evidence"), list) else []
    compact_evidence = []
    for item in evidence[:5]:
        if not isinstance(item, dict):
            continue
        compact = {}
        for key in ("url", "snippet", "retrieved_at"):
            if key in item:
                compact[key] = item.get(key)
        if compact:
            compact_evidence.append(compact)
            if isinstance(item.get("url"), str) and str(item.get("url")).strip():
                next_hints.append(str(item.get("url")).strip())
    if compact_evidence:
        key_facts.append({"evidence": compact_evidence})

    if tool_name in {"github_identity_search", "gitlab_identity_search"}:
        username = str(result.get("username") or arguments.get("username") or "").strip()
        display_name = str(result.get("display_name") or "").strip()
        blog_url = ""
        for item in compact_links:
            if str(item.get("type") or "").strip().lower() == "blog" and isinstance(item.get("url"), str):
                blog_url = str(item["url"]).strip()
                break
        if username:
            key_facts.append({"username": username})
            next_hints.append(username)
        if display_name:
            key_facts.append({"displayName": display_name})
        if blog_url:
            key_facts.append({"blogUrl": blog_url})
            next_hints.append(blog_url)
        summary = (
            f"Resolved {'GitHub' if tool_name == 'github_identity_search' else 'GitLab'} profile {username or profile_url or 'candidate'} with "
            f"{len(compact_repos)} repository item(s) and {len(compact_orgs)} organization link(s)."
        )
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name in {"package_registry_search", "npm_author_search", "crates_author_search"}:
        summary = (
            f"Resolved {tool_name} package-author search with {len(publications)} publication(s), "
            f"{len(compact_repos)} repository link(s), and {len(compact_orgs)} organization/namespace signal(s)."
        )
        return summary, key_facts, _dedupe_str_list(next_hints)

    site_title = str(result.get("site_title") or "").strip()
    technologies = result.get("detected_technologies") if isinstance(result.get("detected_technologies"), list) else []
    if site_title:
        key_facts.append({"siteTitle": site_title})
    if technologies:
        key_facts.append({"detectedTechnologies": [str(item) for item in technologies[:8]]})
    summary = (
        f"Resolved personal site {profile_url or arguments.get('url') or arguments.get('domain') or 'candidate'}"
        f"{f' ({site_title})' if site_title else ''}; found {len(compact_contacts)} contact signal(s) and "
        f"{len(compact_links)} external link(s)."
    )
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_wayback_tool_result(
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []
    original_url = str(result.get("original_url") or arguments.get("url") or "").strip()
    archived_url = str(result.get("archived_url") or "").strip()
    first_archived_at = result.get("first_archived_at")
    last_archived_at = result.get("last_archived_at")
    if result.get("timestamp"):
        key_facts.append({"timestamp": result.get("timestamp")})
    if result.get("extracted_text"):
        key_facts.append({"extractedText": str(result.get("extracted_text"))[:500]})
    if result.get("earliest_archived_url"):
        key_facts.append({"earliestArchivedUrl": result.get("earliest_archived_url")})
        next_hints.append(str(result.get("earliest_archived_url")))
    if result.get("latest_archived_url"):
        key_facts.append({"latestArchivedUrl": result.get("latest_archived_url")})
    if result.get("earliest_extracted_text"):
        key_facts.append({"earliestExtractedText": str(result.get("earliest_extracted_text"))[:500]})
    if result.get("latest_extracted_text"):
        key_facts.append({"latestExtractedText": str(result.get("latest_extracted_text"))[:500]})
    snapshots = result.get("snapshots") if isinstance(result.get("snapshots"), list) else []
    compact_snapshots: List[Dict[str, Any]] = []
    for item in snapshots[:8]:
        if not isinstance(item, dict):
            continue
        compact = {}
        for key in ("timestamp", "original_url", "archived_url", "status_code", "mime_type"):
            if key in item:
                compact[key] = item.get(key)
        if compact:
            compact_snapshots.append(compact)
            if isinstance(item.get("archived_url"), str) and str(item.get("archived_url")).strip():
                next_hints.append(str(item.get("archived_url")).strip())
    if original_url:
        key_facts.append({"originalUrl": original_url})
    if archived_url:
        key_facts.append({"archivedUrl": archived_url})
        next_hints.append(archived_url)
    if first_archived_at:
        key_facts.append({"firstArchivedAt": first_archived_at})
    if last_archived_at:
        key_facts.append({"lastArchivedAt": last_archived_at})
    if compact_snapshots:
        key_facts.append({"snapshots": compact_snapshots})
    summary = (
        f"Wayback returned {len(compact_snapshots)} snapshot(s) for {original_url or 'the target URL'}"
        f"{f'; latest snapshot {last_archived_at}' if last_archived_at else ''}."
    )
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_business_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []

    if tool_name == "open_corporates_search":
        company_name = str(result.get("company_name") or arguments.get("company_name") or "").strip()
        company_number = str(result.get("company_number") or "").strip()
        jurisdiction = str(result.get("jurisdiction") or "").strip()
        source_url = str(result.get("source_url") or "").strip()
        officers = result.get("officers") if isinstance(result.get("officers"), list) else []
        if company_name:
            key_facts.append({"companyName": company_name})
        if company_number:
            key_facts.append({"companyNumber": company_number})
        if jurisdiction:
            key_facts.append({"jurisdiction": jurisdiction})
        if source_url:
            key_facts.append({"sourceUrl": source_url})
            next_hints.append(source_url)
        if result.get("registered_address"):
            key_facts.append({"registeredAddress": result.get("registered_address")})
        compact_officers = []
        for item in officers[:10]:
            if not isinstance(item, dict):
                continue
            compact = {key: item.get(key) for key in ("name", "position", "start_date", "end_date") if key in item}
            if compact:
                compact_officers.append(compact)
                if isinstance(item.get("name"), str) and str(item.get("name")).strip():
                    next_hints.append(str(item.get("name")).strip())
        if compact_officers:
            key_facts.append({"officers": compact_officers})
        summary = f"Resolved company {company_name or 'candidate'} in {jurisdiction or 'unknown jurisdiction'} with {len(compact_officers)} officer record(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "company_officer_search":
        roles = result.get("roles") if isinstance(result.get("roles"), list) else []
        compact_roles = []
        for item in roles[:10]:
            if not isinstance(item, dict):
                continue
            compact = {key: item.get(key) for key in ("company_name", "company_number", "jurisdiction", "role", "start_date", "end_date", "source_url") if key in item}
            if compact:
                compact_roles.append(compact)
                if isinstance(item.get("company_name"), str) and str(item.get("company_name")).strip():
                    next_hints.append(str(item.get("company_name")).strip())
                if isinstance(item.get("source_url"), str) and str(item.get("source_url")).strip():
                    next_hints.append(str(item.get("source_url")).strip())
        if compact_roles:
            key_facts.append({"roles": compact_roles})
        summary = f"Resolved {len(compact_roles)} company officer/director role(s) for {arguments.get('person_name') or 'the target'}."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "company_filing_search":
        filings = result.get("filings") if isinstance(result.get("filings"), list) else []
        compact_filings = []
        for item in filings[:12]:
            if not isinstance(item, dict):
                continue
            compact = {key: item.get(key) for key in ("filing_type", "filing_date", "description", "document_url") if key in item}
            if compact:
                compact_filings.append(compact)
                if isinstance(item.get("document_url"), str) and str(item.get("document_url")).strip():
                    next_hints.append(str(item.get("document_url")).strip())
        if compact_filings:
            key_facts.append({"filings": compact_filings})
        summary = f"Resolved {len(compact_filings)} company filing record(s) for {result.get('company_number') or arguments.get('company_number') or arguments.get('cik') or 'the company'}."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "sec_person_search":
        companies = result.get("companies") if isinstance(result.get("companies"), list) else []
        roles = result.get("roles") if isinstance(result.get("roles"), list) else []
        insider_filings = result.get("insider_filings") if isinstance(result.get("insider_filings"), list) else []
        if companies:
            key_facts.append({"companies": [str(item) for item in companies[:10]]})
            next_hints.extend([str(item) for item in companies[:10]])
        if roles:
            key_facts.append({"roles": roles[:10]})
        if insider_filings:
            key_facts.append({"insiderFilings": insider_filings[:10]})
            for item in insider_filings[:10]:
                if isinstance(item, dict) and isinstance(item.get("source_url"), str) and str(item.get("source_url")).strip():
                    next_hints.append(str(item.get("source_url")).strip())
        if result.get("cik"):
            key_facts.append({"cik": result.get("cik")})
        summary = f"SEC search surfaced {len(roles)} role record(s) and {len(insider_filings)} insider filing(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "director_disclosure_search":
        directorships = result.get("directorships") if isinstance(result.get("directorships"), list) else []
        if directorships:
            key_facts.append({"directorships": directorships[:10]})
            for item in directorships[:10]:
                if isinstance(item, dict) and isinstance(item.get("company"), str) and str(item.get("company")).strip():
                    next_hints.append(str(item.get("company")).strip())
        if result.get("source_url"):
            key_facts.append({"sourceUrl": result.get("source_url")})
            next_hints.append(str(result.get("source_url")))
        summary = f"Director disclosure extraction returned {len(directorships)} structured directorship record(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    domain = str(result.get("domain") or arguments.get("domain") or "").strip()
    if domain:
        key_facts.append({"domain": domain})
        next_hints.append(domain)
    if result.get("registrant_org"):
        key_facts.append({"registrantOrg": result.get("registrant_org")})
        next_hints.append(str(result.get("registrant_org")))
    if result.get("registration_date"):
        key_facts.append({"registrationDate": result.get("registration_date")})
    name_servers = result.get("name_servers") if isinstance(result.get("name_servers"), list) else []
    if name_servers:
        key_facts.append({"nameServers": [str(item) for item in name_servers[:10]]})
    if result.get("source_url"):
        key_facts.append({"sourceUrl": result.get("source_url")})
        next_hints.append(str(result.get("source_url")))
    summary = f"Domain WHOIS resolved {domain or 'the domain'} with {len(name_servers)} nameserver record(s)."
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_archive_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []
    if tool_name == "wayback_domain_timeline_search":
        domain = str(result.get("domain") or arguments.get("domain") or "").strip()
        snapshots = result.get("snapshots") if isinstance(result.get("snapshots"), list) else []
        compact = []
        for item in snapshots[:15]:
            if not isinstance(item, dict):
                continue
            compact.append({key: item.get(key) for key in ("timestamp", "status") if key in item})
        if domain:
            key_facts.append({"domain": domain})
            next_hints.append(domain)
        if compact:
            key_facts.append({"snapshots": compact})
        summary = f"Wayback timeline search found {len(compact)} snapshot(s) for {domain or 'the domain'}."
        return summary, key_facts, _dedupe_str_list(next_hints)

    changes = result.get("changes") if isinstance(result.get("changes"), list) else []
    compact_changes = []
    for item in changes[:10]:
        if not isinstance(item, dict):
            continue
        compact = {key: item.get(key) for key in ("field", "old", "new", "timestamp_range") if key in item}
        if compact:
            compact_changes.append(compact)
            if isinstance(item.get("new"), str) and str(item.get("new")).strip():
                next_hints.append(str(item.get("new")).strip())
    if compact_changes:
        key_facts.append({"changes": compact_changes})
    summary = f"Historical bio diff identified {len(compact_changes)} structured change(s)."
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_sanctions_tool_result(
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    matches = result.get("matches") if isinstance(result.get("matches"), list) else []
    compact_matches = []
    for item in matches[:10]:
        if not isinstance(item, dict):
            continue
        compact = {key: item.get(key) for key in ("name", "program", "country", "source") if key in item}
        if compact:
            compact_matches.append(compact)
    if compact_matches:
        key_facts.append({"matches": compact_matches})
    summary = (
        f"Sanctions watchlist search found {len(compact_matches)} exact match(es) for "
        f"{arguments.get('person_name') or arguments.get('name') or 'the query'}."
    )
    return summary, key_facts, []


def _summarize_identity_expansion_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []

    if tool_name == "alias_variant_generator":
        variants = result.get("variants") if isinstance(result.get("variants"), list) else []
        if variants:
            key_facts.append({"variants": [str(item) for item in variants[:20]]})
            next_hints.extend([str(item) for item in variants[:20]])
        summary = f"Generated {len(variants)} alias variant(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "username_permutation_search":
        hits = result.get("platform_hits") if isinstance(result.get("platform_hits"), list) else []
        compact_hits = []
        for item in hits[:10]:
            if not isinstance(item, dict):
                continue
            compact = {key: item.get(key) for key in ("platform", "url", "status") if key in item}
            if compact:
                compact_hits.append(compact)
                if isinstance(item.get("url"), str) and str(item.get("url")).strip():
                    next_hints.append(str(item.get("url")).strip())
        if compact_hits:
            key_facts.append({"platformHits": compact_hits})
        summary = f"Username permutation search found {len(compact_hits)} direct platform hit(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "cross_platform_profile_resolver":
        matched_profiles = result.get("matched_profiles") if isinstance(result.get("matched_profiles"), list) else []
        if matched_profiles:
            key_facts.append({"matchedProfiles": matched_profiles[:10]})
        if result.get("resolved_identity_id"):
            key_facts.append({"resolvedIdentityId": result.get("resolved_identity_id")})
        if isinstance(result.get("canonical_identity"), dict):
            key_facts.append({"canonical_identity": result.get("canonical_identity")})
        if isinstance(result.get("disambiguation_evidence"), list):
            key_facts.append({"disambiguation_evidence": result.get("disambiguation_evidence")[:10]})
        summary = f"Cross-platform resolver matched {len(matched_profiles)} profile(s) at confidence {result.get('confidence', 0.0)}."
        return summary, key_facts, []

    if tool_name == "institution_directory_search":
        if result.get("institution"):
            key_facts.append({"institution": result.get("institution")})
        if result.get("email"):
            key_facts.append({"email": result.get("email")})
            next_hints.append(str(result.get("email")))
        if result.get("profile_url"):
            key_facts.append({"profileUrl": result.get("profile_url")})
            next_hints.append(str(result.get("profile_url")))
        summary = f"Institution directory search completed for {arguments.get('person_name') or arguments.get('name') or 'the target'}."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "email_pattern_inference":
        patterns = result.get("patterns") if isinstance(result.get("patterns"), list) else []
        if patterns:
            key_facts.append({"patterns": [str(item) for item in patterns[:10]]})
            next_hints.extend([str(item) for item in patterns[:10]])
        summary = f"Inferred {len(patterns)} email pattern candidate(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    pages = result.get("pages") if isinstance(result.get("pages"), list) else []
    emails = result.get("emails") if isinstance(result.get("emails"), list) else []
    if pages:
        key_facts.append({"pages": pages[:10]})
        for item in pages[:10]:
            if isinstance(item, dict) and isinstance(item.get("url"), str) and str(item.get("url")).strip():
                next_hints.append(str(item.get("url")).strip())
    if emails:
        key_facts.append({"emails": [str(item) for item in emails[:10]]})
        next_hints.extend([str(item) for item in emails[:10]])
    summary = f"Contact page extractor found {len(emails)} public email(s) across {len(pages)} page(s)."
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_social_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []

    if tool_name == "reddit_user_search":
        username = result.get("username") or arguments.get("username")
        if result.get("profile_url"):
            key_facts.append({"profileUrl": result.get("profile_url")})
            next_hints.append(str(result.get("profile_url")))
        if username:
            key_facts.append({"username": username})
        if result.get("bio"):
            key_facts.append({"bio": result.get("bio")})
        subreddits = result.get("subreddits") if isinstance(result.get("subreddits"), list) else []
        if subreddits:
            key_facts.append({"subreddits": [str(item) for item in subreddits[:10]]})
        summary = f"Reddit profile lookup completed for @{username or 'target'}."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "mastodon_profile_search":
        if result.get("profile_url"):
            key_facts.append({"profileUrl": result.get("profile_url")})
            next_hints.append(str(result.get("profile_url")))
        if result.get("username"):
            key_facts.append({"username": result.get("username")})
        if result.get("bio"):
            key_facts.append({"bio": result.get("bio")})
        summary = (
            f"Mastodon profile lookup completed for "
            f"{result.get('username') or arguments.get('username') or 'target'} on {result.get('instance') or arguments.get('instance') or 'instance'}."
        )
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "substack_author_search":
        if result.get("profile_url"):
            key_facts.append({"profileUrl": result.get("profile_url")})
            next_hints.append(str(result.get("profile_url")))
        if result.get("author_name"):
            key_facts.append({"displayName": result.get("author_name")})
        articles = result.get("articles") if isinstance(result.get("articles"), list) else []
        if articles:
            key_facts.append({"articleUrls": [str(item) for item in articles[:10]]})
            next_hints.extend([str(item) for item in articles[:10]])
        social_links = result.get("social_links") if isinstance(result.get("social_links"), list) else []
        if social_links:
            key_facts.append({"externalLinks": social_links[:10]})
            next_hints.extend(
                [str(item.get("url")).strip() for item in social_links[:10] if isinstance(item, dict) and str(item.get("url") or "").strip()]
            )
        emails = result.get("emails") if isinstance(result.get("emails"), list) else []
        if emails:
            key_facts.append({"emails": [str(item) for item in emails[:10]]})
            next_hints.extend([str(item) for item in emails[:10]])
        summary = f"Substack author search found {len(articles)} article URL(s) and {len(emails)} email signal(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if result.get("profile_url"):
        key_facts.append({"profileUrl": result.get("profile_url")})
        next_hints.append(str(result.get("profile_url")))
    if result.get("username"):
        key_facts.append({"username": result.get("username")})
    articles = result.get("articles") if isinstance(result.get("articles"), list) else []
    if articles:
        key_facts.append({"articleUrls": [str(item) for item in articles[:10]]})
        next_hints.extend([str(item) for item in articles[:10]])
    linked_accounts = result.get("linked_accounts") if isinstance(result.get("linked_accounts"), list) else []
    if linked_accounts:
        key_facts.append({"externalLinks": linked_accounts[:10]})
        next_hints.extend(
            [str(item.get("url")).strip() for item in linked_accounts[:10] if isinstance(item, dict) and str(item.get("url") or "").strip()]
        )
    summary = f"Medium author search found {len(articles)} article URL(s) for @{result.get('username') or arguments.get('username') or 'target'}."
    return summary, key_facts, _dedupe_str_list(next_hints)


def _summarize_relationship_tool_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[str, List[Dict[str, Any]], List[str]]:
    key_facts: List[Dict[str, Any]] = []
    next_hints: List[str] = []

    if tool_name == "coauthor_graph_search":
        coauthors = result.get("coauthors") if isinstance(result.get("coauthors"), list) else []
        venues = result.get("shared_venues") if isinstance(result.get("shared_venues"), list) else []
        if coauthors:
            key_facts.append({"coauthors": coauthors[:20]})
            next_hints.extend(
                [str(item.get("name")).strip() for item in coauthors[:20] if isinstance(item, dict) and str(item.get("name") or "").strip()]
            )
        if venues:
            key_facts.append({"sharedVenues": venues[:20]})
        summary = f"Coauthor graph search found {len(coauthors)} coauthor(s) and {len(venues)} shared venue(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "org_staff_page_search":
        staff = result.get("staff") if isinstance(result.get("staff"), list) else []
        if result.get("org_url"):
            key_facts.append({"profileUrl": result.get("org_url")})
            next_hints.append(str(result.get("org_url")))
        if staff:
            key_facts.append({"staff": staff[:20]})
            next_hints.extend(
                [str(item.get("name")).strip() for item in staff[:20] if isinstance(item, dict) and str(item.get("name") or "").strip()]
            )
        summary = f"Org staff page search extracted {len(staff)} staff entrie(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    if tool_name == "board_member_overlap_search":
        overlaps = result.get("overlaps") if isinstance(result.get("overlaps"), list) else []
        if overlaps:
            key_facts.append({"overlaps": overlaps[:20]})
            next_hints.extend(
                [str(item.get("name")).strip() for item in overlaps[:20] if isinstance(item, dict) and str(item.get("name") or "").strip()]
            )
        summary = f"Board overlap search found {len(overlaps)} overlapping board/director profile(s)."
        return summary, key_facts, _dedupe_str_list(next_hints)

    shared_domains = result.get("shared_domains") if isinstance(result.get("shared_domains"), list) else []
    shared_organizations = result.get("shared_organizations") if isinstance(result.get("shared_organizations"), list) else []
    shared_addresses = result.get("shared_addresses") if isinstance(result.get("shared_addresses"), list) else []
    if shared_domains:
        key_facts.append({"sharedDomains": shared_domains[:20]})
        next_hints.extend(
            [str(item.get("domain")).strip() for item in shared_domains[:20] if isinstance(item, dict) and str(item.get("domain") or "").strip()]
        )
    if shared_organizations:
        key_facts.append({"sharedOrganizations": shared_organizations[:20]})
    if shared_addresses:
        key_facts.append({"sharedAddresses": shared_addresses[:20]})
    summary = (
        f"Shared contact pivot search found {len(shared_domains)} shared domain(s), "
        f"{len(shared_organizations)} shared organization(s), and {len(shared_addresses)} shared address(es)."
    )
    return summary, key_facts, _dedupe_str_list(next_hints)


def _note_from_receipt(receipt: ToolReceipt) -> str | None:
    if not receipt.ok:
        return None
    if receipt.tool_name == "fetch_url" and receipt.document_ids:
        return f"Fetched content → document {receipt.document_ids[0]}"
    if receipt.tool_name == "ingest_text" and receipt.document_ids:
        chunks = None
        for fact in receipt.key_facts:
            if "chunkCount" in fact:
                chunks = fact.get("chunkCount")
        if chunks is not None:
            return f"Ingested text → document {receipt.document_ids[0]} ({chunks} chunks)"
        return f"Ingested text → document {receipt.document_ids[0]}"
    if receipt.tool_name == "ingest_graph_entity":
        return "Ingested graph entity"
    if receipt.tool_name == "ingest_graph_entities":
        return "Ingested graph entities"
    if receipt.tool_name == "ingest_graph_relations":
        return "Linked graph relations"
    return receipt.summary


def _append_confidence_line(summary: str, confidence_score: float) -> str:
    if "Confidence score:" in summary:
        return summary
    return f"{summary} Confidence score: {confidence_score:.2f}."


def _summary_from_normalized_text(normalized_text: str, fallback: str) -> str:
    """Keep receipt summaries grounded in Phase-1 normalized text."""
    stripped = normalized_text.lstrip()
    if stripped.startswith("{") and '"tool"' in stripped and '"result"' in stripped:
        return fallback

    lines = [line.strip() for line in normalized_text.splitlines() if line.strip()]
    if not lines:
        return fallback

    # When worker LLM summarization is disabled, normalized_text may just be pretty-printed JSON.
    # In that case keep the domain-specific fallback summary instead of flooding the noteboard.
    jsonish_prefixes = ('{', '}', '[', ']', '"tool"', '"arguments"', '"result"')
    jsonish_lines = sum(1 for line in lines[:12] if line.startswith(jsonish_prefixes))
    if jsonish_lines >= max(3, min(6, len(lines[:12]))):
        return fallback

    # Prefer high-signal normalized sections; cap size to keep planner notes compact.
    selected = lines[:10]
    summary = " | ".join(selected)
    return summary[:1400] if len(summary) > 1400 else summary


def _get_tool_metadata(tool_name: str) -> tuple[str | None, float | None]:
    metadata = TOOL_CONFIDENCE_REGISTRY.get(tool_name, {})
    tool_type = metadata.get("type")
    confidence = metadata.get("confidence")
    normalized_type = tool_type if isinstance(tool_type, str) and tool_type else None
    normalized_confidence = float(confidence) if isinstance(confidence, (float, int)) else None
    return normalized_type, normalized_confidence


def _citations_from_receipt(receipt: ToolReceipt) -> List[Dict[str, Any]]:
    citations: List[Dict[str, Any]] = []
    for doc_id in receipt.document_ids:
        citations.append({"documentId": doc_id})
    return citations


def _build_tool_result_text(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> str:
    payload = {
        "tool": tool_name,
        "arguments": arguments,
        "result": result,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _extract_source_url(arguments: Dict[str, Any], result: Dict[str, Any]) -> str | None:
    for key in ("url", "profile", "target", "sourceUrl"):
        value = arguments.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value
    if isinstance(result, dict):
        for key in ("url", "sourceUrl"):
            value = result.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value
    return None


def _vector_upsert_from_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    output: Dict[str, Any] = {}
    for key in ("vectorCount", "chunkCount", "collection", "embeddingModel", "documentId"):
        if key in result:
            output[key] = result.get(key)
    return output


def _graph_upsert_from_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    output: Dict[str, Any] = {}
    for key in ("entityType", "relationCount", "count"):
        if key in result:
            output[key] = result.get(key)
    return output


def _extract_evidence(result: Dict[str, Any]) -> Dict[str, Any] | None:
    evidence = result.get("evidence") if isinstance(result, dict) else None
    if not isinstance(evidence, dict):
        return None
    if evidence.get("bucket") and evidence.get("objectKey"):
        return evidence
    if evidence.get("documentId"):
        return evidence
    return None


def _extract_command_issue(result: Dict[str, Any]) -> str | None:
    if not isinstance(result, dict):
        return None

    error_value = result.get("error")
    if isinstance(error_value, str) and error_value.strip():
        return error_value.strip()

    returncode = result.get("returncode")
    if isinstance(returncode, int) and returncode != 0:
        stderr = result.get("stderr")
        if isinstance(stderr, str) and stderr.strip():
            first_line = next((line.strip() for line in stderr.splitlines() if line.strip()), "")
            if first_line:
                return first_line[:240]
        return f"command exited with status {returncode}"

    return None


def _extract_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return _dedupe_str_list([str(item).strip() for item in value if isinstance(item, str) and str(item).strip()])


def _extract_url_candidates(value: Any) -> List[str]:
    found: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, str):
            found.extend(URL_IN_TEXT_REGEX.findall(node))
            return
        if isinstance(node, dict):
            for nested in node.values():
                walk(nested)
            return
        if isinstance(node, list):
            for nested in node:
                walk(nested)

    walk(value)
    return _dedupe_str_list(found)


def _extract_handles_from_tweets(tweets: Any) -> List[str]:
    handles: List[str] = []
    if not isinstance(tweets, list):
        return handles
    for tweet in tweets:
        if not isinstance(tweet, dict):
            continue
        text = tweet.get("text")
        if not isinstance(text, str):
            continue
        handles.extend([f"@{match}" for match in X_HANDLE_REGEX.findall(text)])
    return _dedupe_str_list(handles)


def _extract_related_handles(result: Dict[str, Any]) -> List[str]:
    handles: List[str] = []
    parsed = result.get("parsed")
    if isinstance(parsed, dict):
        for item in parsed.values():
            if not isinstance(item, dict):
                continue
            status = item.get("status")
            if isinstance(status, dict):
                ids = status.get("ids")
                if isinstance(ids, dict):
                    for value in ids.values():
                        if isinstance(value, str) and 2 < len(value) <= 32 and " " not in value:
                            handles.append(value)
    return _dedupe_str_list(handles)


def _build_x_status_urls(user: Any, tweets: Any) -> List[str]:
    if not isinstance(user, dict) or not isinstance(tweets, list):
        return []
    username = user.get("username")
    if not isinstance(username, str) or not username.strip():
        return []

    urls: List[str] = []
    for tweet in tweets:
        if not isinstance(tweet, dict):
            continue
        tweet_id = tweet.get("id")
        if isinstance(tweet_id, str) and tweet_id.strip():
            urls.append(f"https://x.com/{username}/status/{tweet_id}")
    return _dedupe_str_list(urls)


def _extract_phone_numbers_from_text(text: Any) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    return _dedupe_str_list(
        [
            match.strip()
            for match in PHONE_IN_TEXT_REGEX.findall(text)
            if match.strip() and any(ch in match for ch in " +-.()")
        ]
    )


def _extract_related_people(text: Any, exclude_names: List[str] | None = None) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    excluded = {item.strip().casefold() for item in (exclude_names or []) if isinstance(item, str) and item.strip()}
    candidates = []
    for name in extract_person_targets(text):
        if name.casefold() in excluded:
            continue
        candidates.append(name)
    return _dedupe_str_list(candidates)


def _extract_history_markers(text: Any) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    markers = []
    for pattern in (
        r"\b(?:advisor|advised by|co-author|coauthor|collaborator|lab|department|university|phd|graduate|student)\b",
        r"\b(?:worked at|joined|previously|former|history|publication|paper|research)\b",
        r"\b(?:crime|arrest|charged|lawsuit|court|case|convicted|sanction)\b",
    ):
        markers.extend(re.findall(pattern, text, re.IGNORECASE))
    return _dedupe_str_list([item.lower() for item in markers])


def _extract_serp_source_types(rows: Any) -> List[str]:
    source_types: List[str] = []
    if not isinstance(rows, list):
        return source_types
    for item in rows:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        host = urlparse(url).hostname or ""
        normalized = host.lower()
        if normalized.startswith("www."):
            normalized = normalized[4:]
        if normalized:
            source_types.append(normalized)
    return _dedupe_str_list(source_types)


def _extract_arxiv_coauthors(entries: Any, exclude_names: List[str] | None = None) -> List[str]:
    coauthors: List[str] = []
    excluded = {item.strip().casefold() for item in (exclude_names or []) if isinstance(item, str) and item.strip()}
    if not isinstance(entries, list):
        return coauthors
    for item in entries:
        if not isinstance(item, dict):
            continue
        authors = item.get("authors")
        if isinstance(authors, list):
            for author in authors:
                if not isinstance(author, str) or not author.strip():
                    continue
                if author.strip().casefold() in excluded:
                    continue
                coauthors.append(author.strip())
            continue
        if isinstance(authors, str) and authors.strip():
            for author in re.split(r"\s*,\s*|\s+and\s+", authors.strip()):
                if not author or author.casefold() in excluded:
                    continue
                coauthors.append(author)
    return _dedupe_str_list(coauthors)


def _extract_arxiv_affiliations(entries: Any) -> List[str]:
    affiliations: List[str] = []
    if not isinstance(entries, list):
        return affiliations
    for item in entries:
        if not isinstance(item, dict):
            continue
        value = item.get("affiliations")
        if isinstance(value, list):
            affiliations.extend([str(entry).strip() for entry in value if isinstance(entry, str) and str(entry).strip()])
        elif isinstance(value, str) and value.strip():
            affiliations.extend([part.strip() for part in re.split(r"\s*;\s*|\s*,\s*", value) if part.strip()])
    return _dedupe_str_list(affiliations)


def _extract_strings_from_text(text: Any, kind: str) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    if kind == "email":
        pattern = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
    elif kind == "domain":
        pattern = re.compile(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", re.IGNORECASE)
    else:
        return []
    return _dedupe_str_list(pattern.findall(text))


def _extract_table_row_values(text: Any, labels: set[str]) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    values: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or ":" not in stripped:
            continue
        left, right = stripped.split(":", 1)
        if left.strip().lower() in labels and right.strip():
            values.append(right.strip())
    return _dedupe_str_list(values)


def _normalize_tool_arguments(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(arguments)

    if tool_name == "ingest_text":
        source_url = normalized.get("sourceUrl")
        if source_url is None or (isinstance(source_url, str) and not source_url.strip()):
            normalized.pop("sourceUrl", None)
        if "evidence" in normalized and "evidenceJson" not in normalized:
            evidence_value = normalized.pop("evidence")
            if isinstance(evidence_value, (dict, list)):
                normalized["evidenceJson"] = json.dumps(evidence_value)
        evidence_json = normalized.get("evidenceJson")
        if isinstance(evidence_json, (dict, list)):
            normalized["evidenceJson"] = json.dumps(evidence_json)

    if tool_name == "ingest_graph_entity":
        for key in ("propertiesJson", "evidenceJson", "relationsJson"):
            value = normalized.get(key)
            if isinstance(value, (dict, list)):
                normalized[key] = json.dumps(value)

    if tool_name == "ingest_graph_entities":
        value = normalized.get("entitiesJson")
        if isinstance(value, (dict, list)):
            normalized["entitiesJson"] = json.dumps(value)

    if tool_name == "ingest_graph_relations":
        value = normalized.get("relationsJson")
        if isinstance(value, (dict, list)):
            normalized["relationsJson"] = json.dumps(value)

    return sanitize_search_tool_arguments(tool_name, normalized)


def _extract_evidence_refs_from_arguments(tool_name: str, arguments: Dict[str, Any]) -> List[Dict[str, Any]]:
    evidence_refs: List[Dict[str, Any]] = []

    def add_ref(ref: Any) -> None:
        if not isinstance(ref, dict):
            return
        if (ref.get("bucket") and ref.get("objectKey")) or ref.get("documentId"):
            evidence_refs.append(ref)

    def parse_json(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return value

    if tool_name == "ingest_text":
        evidence = parse_json(arguments.get("evidenceJson"))
        add_ref(evidence)

    if tool_name == "ingest_graph_entity":
        evidence = parse_json(arguments.get("evidenceJson"))
        if isinstance(evidence, dict):
            add_ref(evidence.get("objectRef"))
        relations = parse_json(arguments.get("relationsJson"))
        if isinstance(relations, list):
            for rel in relations:
                if isinstance(rel, dict):
                    add_ref(rel.get("evidenceRef"))

    if tool_name == "ingest_graph_entities":
        entities = parse_json(arguments.get("entitiesJson"))
        if isinstance(entities, list):
            for entity in entities:
                if not isinstance(entity, dict):
                    continue
                evidence = entity.get("evidence")
                if isinstance(evidence, dict):
                    add_ref(evidence.get("objectRef"))
                relations = entity.get("relations")
                if isinstance(relations, list):
                    for rel in relations:
                        if isinstance(rel, dict):
                            add_ref(rel.get("evidenceRef"))

    if tool_name == "ingest_graph_relations":
        relations = parse_json(arguments.get("relationsJson"))
        if isinstance(relations, list):
            for rel in relations:
                if isinstance(rel, dict):
                    add_ref(rel.get("evidenceRef"))

    return _dedupe_evidence_refs(evidence_refs)


def _dedupe_evidence_refs(refs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    unique: List[Dict[str, Any]] = []
    for ref in refs:
        key = f"{ref.get('bucket')}|{ref.get('objectKey')}|{ref.get('versionId')}|{ref.get('documentId')}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(ref)
    return unique


def _dedupe_str_list(values: List[str]) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for item in values:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output
