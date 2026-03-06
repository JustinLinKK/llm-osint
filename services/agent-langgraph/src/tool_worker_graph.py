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
from openrouter_llm import OpenRouterLLM, get_openrouter_timeout, invoke_complete_json
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
    GRAPH_CONSTRUCTION_SYSTEM_PROMPT,
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
    "tavily_person_search": {"type": "web_search", "confidence": 0.9},
    "extract_webpage": {"type": "web_extract", "confidence": 0.88},
    "crawl_webpage": {"type": "web_crawl", "confidence": 0.82},
    "map_webpage": {"type": "web_map", "confidence": 0.8},
    "x_get_user_posts_api": {"type": "social_posts", "confidence": 0.8},
    "linkedin_download_html_ocr": {"type": "social_profile_capture", "confidence": 0.75},
    "google_serp_person_search": {"type": "web_search", "confidence": 0.7},
    "arxiv_search_and_download": {"type": "research_papers", "confidence": 0.85},
    "arxiv_paper_ingest": {"type": "research_paper", "confidence": 0.92},
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
    worker_llm = _build_tool_llm()

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
            llm=worker_llm,
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
        emit_stage(state, "vector_ingest_worker", "started")
        try:
            result = _run_vector_ingest_worker(
                llm=worker_llm,
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
        # Phase 2b: graph ingestion uses normalized summary text plus deterministic raw-result normalization.
        if not _should_run_post_ingest(state["tool_name"], bool(state.get("ok", False))):
            emit_stage(state, "graph_ingest_worker", "completed", skipped=True)
            return {"graph_ingest_result": {}}
        emit_stage(state, "graph_ingest_worker", "started")
        try:
            result = _run_graph_ingest_worker(
                llm=worker_llm,
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
        if worker_llm is None:
            emit_stage(state, "receipt_summarize_worker", "completed", skipped=True, reason="llm_unavailable")
            return {"receipt_llm_result": {}}
        emit_stage(state, "receipt_summarize_worker", "started")
        try:
            result = _run_receipt_summarize_worker(
                llm=worker_llm,
                run_id=state["run_id"],
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
            key_facts = _merge_key_fact_lists(key_facts, llm_key_facts)
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


def _openrouter_worker_timeout() -> float:
    return get_openrouter_timeout(
        "OPENROUTER_WORKER_TIMEOUT_SECONDS",
        get_openrouter_timeout("OPENROUTER_TIMEOUT_SECONDS", 120.0),
    )


def _should_run_post_ingest(tool_name: str, ok: bool) -> bool:
    return ok and tool_name not in INGEST_TOOL_NAMES


def _summarize_tool_output_for_ingestion(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    ok: bool,
    llm: OpenRouterLLM | None = None,
) -> str:
    fallback = _build_tool_result_text(tool_name, arguments, result)
    if not ok:
        return fallback
    if llm is None:
        return fallback
    payload = {
        "tool_name": tool_name,
        "arguments": arguments,
        "result": result,
        "output_schema": {"summary_text": "string"},
    }
    run_id = arguments.get("runId")
    try:
        parsed = invoke_complete_json(
            llm,
            _tool_summary_prompt(tool_name),
            payload,
            temperature=0.1,
            timeout=_openrouter_worker_timeout(),
            run_id=(str(run_id) if isinstance(run_id, str) else None),
            operation=f"tool_worker.summary.{tool_name}",
        )
    except Exception:
        return fallback
    summary_text = parsed.get("summary_text")
    if isinstance(summary_text, str) and summary_text.strip():
        return _validate_summary_text(tool_name, summary_text.strip(), arguments, result)
    return fallback


def _tool_summary_prompt(tool_name: str) -> str:
    if tool_name == "person_search":
        return PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"google_serp_person_search", "tavily_person_search", "tavily_research", "extract_webpage", "crawl_webpage", "map_webpage"}:
        return GOOGLE_SERP_PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT
    if tool_name in {"arxiv_search_and_download", "arxiv_paper_ingest"}:
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
            llm_args = llm.refine_tool_arguments(
                VECTOR_INGEST_SYSTEM_PROMPT,
                "ingest_text",
                seed_args,
                run_id=run_id,
            )
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
    if llm is not None:
        extracted_graph = _extract_graph_construction_payload(
            llm=llm,
            run_id=run_id,
            tool_name=tool_name,
            arguments=arguments,
            result=result,
            tool_result_summary=tool_result_summary,
        )
        entities, relations = _build_graph_construction_batches(
            run_id=run_id,
            tool_name=tool_name,
            arguments=arguments,
            result=result,
            extracted_graph=extracted_graph,
        )
        if entities:
            entity_tool_result = mcp_client.call_tool(
                "ingest_graph_entities",
                _normalize_tool_arguments(
                    "ingest_graph_entities",
                    {"runId": run_id, "entitiesJson": entities},
                ),
            )
            if not entity_tool_result.ok:
                raise RuntimeError(f"ingest_graph_entities failed: {entity_tool_result.content}")
            if not isinstance(entity_tool_result.content, dict):
                raise RuntimeError("ingest_graph_entities returned non-dict content")

            relation_result: Dict[str, Any] = {}
            if relations:
                relation_tool_result = mcp_client.call_tool(
                    "ingest_graph_relations",
                    _normalize_tool_arguments(
                        "ingest_graph_relations",
                        {"runId": run_id, "relationsJson": relations},
                    ),
                )
                if not relation_tool_result.ok:
                    raise RuntimeError(f"ingest_graph_relations failed: {relation_tool_result.content}")
                if not isinstance(relation_tool_result.content, dict):
                    raise RuntimeError("ingest_graph_relations returned non-dict content")
                relation_result = relation_tool_result.content

            return {
                "entityCount": len(entities),
                "relationCount": len(relations),
                "entities": entity_tool_result.content.get("entities", []),
                "entityWarnings": entity_tool_result.content.get("warnings", []),
                "relationWarnings": relation_result.get("warnings", []),
                "graphSchema": "person_context_v2",
            }

    return _run_legacy_graph_ingest_worker(
        llm=llm,
        mcp_client=mcp_client,
        run_id=run_id,
        tool_name=tool_name,
        arguments=arguments,
        result=result,
        tool_result_summary=tool_result_summary,
    )


def _run_legacy_graph_ingest_worker(
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
                GRAPH_INGEST_SYSTEM_PROMPT,
                "ingest_graph_entity",
                seed_args,
                run_id=run_id,
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


def _extract_graph_construction_payload(
    llm: OpenRouterLLM,
    run_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    tool_result_summary: str,
) -> Dict[str, Any]:
    graph_result = _normalize_tool_result_for_graph(tool_name, arguments, result)
    payload = {
        "tool_name": tool_name,
        "arguments": arguments,
        "result": graph_result,
        "tool_result_summary": tool_result_summary,
        "output_schema": {
            "entities": [
                {
                    "canonical_name": "string",
                    "type": "string",
                    "alt_names": ["string"],
                    "attributes": ["string"],
                }
            ],
            "relations": [
                {
                    "src": "string",
                    "dst": "string",
                    "canonical_name": "string",
                    "rel_type": "string",
                    "alt_names": ["string"],
                }
            ],
        },
    }
    parsed = invoke_complete_json(
        llm,
        GRAPH_CONSTRUCTION_SYSTEM_PROMPT,
        payload,
        temperature=0.1,
        timeout=_openrouter_worker_timeout(),
        run_id=run_id,
        operation=f"tool_worker.graph_extract.{tool_name}",
    )
    return parsed if isinstance(parsed, dict) else {}


def _build_graph_construction_batches(
    run_id: str,
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
    extracted_graph: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    graph_result = _normalize_tool_result_for_graph(tool_name, arguments, result)
    raw_entities = extracted_graph.get("entities")
    raw_relations = extracted_graph.get("relations")
    if not isinstance(raw_entities, list):
        raw_entities = []
    if not isinstance(raw_relations, list):
        raw_relations = []
    supplemental_entities, supplemental_relations = _supplemental_graph_components_from_result(tool_name, arguments, graph_result)
    raw_entities = list(raw_entities) + supplemental_entities
    raw_relations = list(raw_relations) + supplemental_relations

    evidence_ref = _extract_evidence(graph_result)
    grouped_entities: Dict[str, Dict[str, Any]] = {}
    signature_to_bucket: Dict[str, str] = {}
    for row in raw_entities:
        if not isinstance(row, dict):
            continue
        row_attributes = [str(item).strip() for item in (row.get("attributes") or []) if str(item).strip()]
        row_type = str(row.get("type") or "").strip() or "Unknown"
        names = _graph_unique_strings(
            [
                row.get("canonical_name"),
                *list(row.get("alt_names") or []),
                *_graph_attribute_values(row_attributes, "title", "label", "page_title", "site_title", "display_title"),
                *_graph_auto_aliases(row_type, [row.get("canonical_name"), *list(row.get("alt_names") or [])]),
            ]
        )
        if not names:
            continue
        canonical_name = _choose_graph_canonical_name(names, row_type)
        normalized_key = _normalize_graph_name(canonical_name)
        if not normalized_key:
            continue
        entity_type = _canonical_graph_entity_type(
            row_type,
            canonical_name,
            row_attributes,
        )
        attributes = row_attributes
        merge_keys = _graph_entity_merge_keys(
            entity_type,
            canonical_name,
            [name for name in names if _normalize_graph_name(name) != normalized_key],
            attributes,
        )
        bucket_key = next((signature_to_bucket[key] for key in merge_keys if key in signature_to_bucket), normalized_key)
        bucket = grouped_entities.setdefault(
            bucket_key,
            {
                "names": [],
                "types": [],
                "attributes": [],
                "canonical_name": canonical_name,
                "entity_type": entity_type,
                "merge_keys": [],
            },
        )
        bucket["names"].extend(names)
        bucket["types"].append(entity_type)
        bucket["attributes"].extend(attributes)
        bucket["merge_keys"].extend(merge_keys)
        for key in merge_keys:
            signature_to_bucket[key] = bucket_key

    entities: List[Dict[str, Any]] = []
    entity_lookup: Dict[str, Dict[str, Any]] = {}
    for bucket in grouped_entities.values():
        names = _graph_unique_strings(bucket["names"])
        entity_type = _majority_value(bucket["types"], fallback="Unknown")
        canonical_name = _choose_graph_canonical_name(names, entity_type)
        attributes = _graph_unique_strings(bucket["attributes"])
        merge_keys = _graph_entity_merge_keys(
            entity_type,
            canonical_name,
            [name for name in names if _normalize_graph_name(name) != _normalize_graph_name(canonical_name)],
            attributes,
        )
        canonical_id = _canonical_graph_node_id(entity_type, canonical_name)
        run_scoped_id = _stable_graph_node_id(run_id, entity_type, canonical_name)
        entity_payload = {
            "node_id": run_scoped_id,
            "canonical_id": canonical_id,
            "run_scoped_id": run_scoped_id,
            "run_id": run_id,
            "external_context": False,
            "type": entity_type,
            "raw_type": entity_type,
            "canonical_name": canonical_name,
            "alt_names": [name for name in names if _normalize_graph_name(name) != _normalize_graph_name(canonical_name)],
            "attributes": attributes,
            "merge_keys": _dedupe_str_list([*bucket.get("merge_keys", []), *merge_keys]),
            "osint_bucket": _infer_osint_bucket(entity_type, canonical_name, attributes),
            "source_tools": [tool_name],
        }
        if evidence_ref:
            entity_payload["evidence"] = {"objectRef": evidence_ref}
        entities.append(entity_payload)
        lookup_names = _graph_unique_strings([canonical_name, *entity_payload["alt_names"], *_graph_auto_aliases(entity_type, [canonical_name, *entity_payload["alt_names"]])])
        for name in lookup_names:
            alias_key = _normalize_graph_name(str(name))
            if alias_key:
                entity_lookup[alias_key] = entity_payload

    relations: List[Dict[str, Any]] = []
    seen_relations: set[str] = set()
    for row in raw_relations:
        if not isinstance(row, dict):
            continue
        src_name = _normalize_graph_name(str(row.get("src") or ""))
        dst_name = _normalize_graph_name(str(row.get("dst") or ""))
        if not src_name or not dst_name:
            continue
        src_entity = entity_lookup.get(src_name)
        dst_entity = entity_lookup.get(dst_name)
        if src_entity is None or dst_entity is None:
            continue
        rel_type = _canonical_graph_relation_type(
            str(row.get("rel_type") or "").strip() or "RELATED_TO",
            str(src_entity.get("type") or ""),
            str(dst_entity.get("type") or ""),
        )
        canonical_name = str(row.get("canonical_name") or rel_type).strip() or rel_type
        alt_names = _graph_unique_strings(row.get("alt_names") or [])
        fingerprint = "|".join(
            [
                str(src_entity["node_id"]),
                str(dst_entity["node_id"]),
                _normalize_graph_name(rel_type),
                _normalize_graph_name(canonical_name),
            ]
        )
        if fingerprint in seen_relations:
            continue
        seen_relations.add(fingerprint)
        relation_payload = {
            "edge_id": _stable_graph_edge_id(
                str(src_entity["node_id"]),
                str(dst_entity["node_id"]),
                rel_type,
                canonical_name,
            ),
            "canonical_id": _canonical_graph_edge_id(
                str(src_entity.get("canonical_id") or src_entity["node_id"]),
                str(dst_entity.get("canonical_id") or dst_entity["node_id"]),
                rel_type,
                canonical_name,
            ),
            "run_scoped_id": _stable_graph_edge_id(
                str(src_entity["node_id"]),
                str(dst_entity["node_id"]),
                rel_type,
                canonical_name,
            ),
            "run_id": run_id,
            "external_context": False,
            "src_id": src_entity["node_id"],
            "dst_id": dst_entity["node_id"],
            "src_canonical_id": src_entity.get("canonical_id"),
            "dst_canonical_id": dst_entity.get("canonical_id"),
            "rel_type": rel_type,
            "raw_relation_type": str(row.get("rel_type") or rel_type),
            "canonical_name": canonical_name,
            "alt_names": alt_names,
            "source_tool": tool_name,
        }
        if evidence_ref:
            relation_payload["evidenceRef"] = evidence_ref
        relations.append(relation_payload)

    for entity in entities:
        entity_type = str(entity.get("type") or "")
        canonical_name = str(entity.get("canonical_name") or "").strip()
        if entity_type not in {"Website", "Document"} or not canonical_name.startswith(("http://", "https://")):
            continue
        host = (urlparse(canonical_name).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        domain_entity = entity_lookup.get(_normalize_graph_name(host))
        if domain_entity is None:
            continue
        rel_type = "HAS_DOMAIN"
        fingerprint = "|".join(
            [
                str(entity["node_id"]),
                str(domain_entity["node_id"]),
                rel_type,
                rel_type,
            ]
        )
        if fingerprint in seen_relations:
            continue
        seen_relations.add(fingerprint)
        relation_payload = {
            "edge_id": _stable_graph_edge_id(
                str(entity["node_id"]),
                str(domain_entity["node_id"]),
                rel_type,
                rel_type,
            ),
            "canonical_id": _canonical_graph_edge_id(
                str(entity.get("canonical_id") or entity["node_id"]),
                str(domain_entity.get("canonical_id") or domain_entity["node_id"]),
                rel_type,
                rel_type,
            ),
            "run_scoped_id": _stable_graph_edge_id(
                str(entity["node_id"]),
                str(domain_entity["node_id"]),
                rel_type,
                rel_type,
            ),
            "run_id": run_id,
            "external_context": False,
            "src_id": entity["node_id"],
            "dst_id": domain_entity["node_id"],
            "src_canonical_id": entity.get("canonical_id"),
            "dst_canonical_id": domain_entity.get("canonical_id"),
            "rel_type": rel_type,
            "raw_relation_type": rel_type,
            "canonical_name": rel_type,
            "alt_names": [],
            "source_tool": tool_name,
        }
        if evidence_ref:
            relation_payload["evidenceRef"] = evidence_ref
        relations.append(relation_payload)

    return entities, relations


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
        tool_name,
        tool_result_summary[:400],
    ]
    digest = hashlib.sha256("|".join(stable_bits).encode("utf-8")).hexdigest()[:16]
    return f"snippet:{tool_name}:{digest}"


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
            except (json.JSONDecodeError, TypeError):
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
        except (json.JSONDecodeError, TypeError):
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


def _normalize_graph_name(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = re.sub(r"[\W_]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _graph_unique_strings(values: List[Any]) -> List[str]:
    output: List[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        normalized = _normalize_graph_name(text)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(text)
    return output


def _graph_is_url_candidate(value: str) -> bool:
    return str(value or "").strip().startswith(("http://", "https://"))


def _graph_looks_like_domain(value: str) -> bool:
    return bool(re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}", str(value or "").strip(), re.IGNORECASE))


def _graph_looks_like_person_name(value: str) -> bool:
    text = str(value or "").strip()
    if not text or _graph_is_url_candidate(text) or _graph_looks_like_domain(text):
        return False
    if re.search(r"[@/:]|\b(?:github|linkedin|researchgate|duckduckgo)\b", text, re.IGNORECASE):
        return False
    tokens = _normalize_graph_name(text).split()
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    if any(len(token) == 1 for token in tokens):
        return False
    return all(re.fullmatch(r"[a-z][a-z'-]*", token) for token in tokens)


def _graph_looks_like_phone(value: str) -> bool:
    return bool(re.fullmatch(r"\+?[0-9][0-9().\-\s]{6,}[0-9]", str(value or "").strip()))


def _graph_score_name_candidate(entity_type: str, value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return float("-inf")
    normalized = _normalize_graph_name(text)
    family = _graph_entity_family(_canonical_graph_entity_type(entity_type, text, []))
    preferred_type = _canonical_graph_entity_type(entity_type, "", [])
    preferred_normalized = _normalize_graph_name(preferred_type)
    score = 0.0
    if re.search(r"snippet:|duckduckgo|github_identity_search|gitlab_identity_search", text, re.IGNORECASE):
        score -= 280.0
    if not _graph_is_url_candidate(text) and not _graph_looks_like_domain(text):
        score += 50.0
    if _graph_is_url_candidate(text):
        score -= 0.0 if family in {"digital", "repository"} else 220.0
    if _graph_looks_like_domain(text):
        score -= 0.0 if family == "digital" else 140.0
    score -= float(max(0, len(text) - 40))
    tokens = [token for token in normalized.split() if token]
    if family == "person":
        if _graph_looks_like_person_name(text):
            score += 260.0
        if len(tokens) >= 2 and len(tokens) <= 4:
            score += 40.0
        if re.search(r"[@/:]|\d", text):
            score -= 160.0
    elif family == "role":
        # Prefer human-readable role phrasing like "X at Y" over "X Y" aliases.
        if " at " in f" {normalized} ":
            score += 8.0
    elif family == "org":
        if any(token in normalized for token in ("university", "college", "institute", "school", "company", "corporation", "lab", "startup", "agency")):
            score += 220.0
        if len(tokens) >= 2:
            score += 25.0
        if re.fullmatch(r"\(?[A-Z0-9]{2,10}\)?", text):
            score -= 50.0
    elif family == "publication":
        if not _graph_is_url_candidate(text) and len(tokens) >= 4:
            score += 180.0
        if not _graph_is_url_candidate(text) and len(text) >= 20:
            score += 40.0
    elif family == "digital":
        if _graph_is_url_candidate(text):
            score += 200.0
        if _graph_looks_like_domain(text):
            score += 160.0
        if _graph_looks_like_phone(text):
            score += 140.0
        if preferred_normalized == "website" and not _graph_is_url_candidate(text) and not _graph_looks_like_domain(text):
            score += 240.0
            if any(token in normalized for token in ("profile", "website", "site", "homepage", "official", "page")):
                score += 60.0
    elif family == "repository":
        if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", text):
            score += 220.0
        if _graph_is_url_candidate(text):
            score += 120.0
    elif family == "contact":
        if _graph_looks_like_phone(text) or text.startswith("@") or "@" in text or _graph_is_url_candidate(text):
            score += 200.0
        if "contact" in normalized or "surface" in normalized:
            score += 120.0
    elif family == "credential":
        if any(token in normalized for token in ("phd", "doctor", "master", "bachelor", "degree", "credential")):
            score += 200.0
        if " from " in text.lower() or " at " in text.lower():
            score += 80.0
    elif family == "experience":
        if " at " in text.lower():
            score += 180.0
        if any(token in normalized for token in ("engineer", "student", "scientist", "founder", "director", "research")):
            score += 80.0
    elif family == "affiliation":
        if " with " in text.lower() or " at " in text.lower():
            score += 180.0
    elif family == "timelineevent":
        if any(token in normalized for token in ("started", "joined", "graduated", "published", "founded", "appointed")):
            score += 160.0
        if re.search(r"\b(19|20)\d{2}\b", text):
            score += 80.0
    elif family == "occupation":
        if len(tokens) >= 1 and len(tokens) <= 4:
            score += 120.0
    elif family == "image":
        if _graph_is_url_candidate(text):
            score += 180.0
        elif preferred_normalized == "imageobject" and len(tokens) >= 2:
            score += 220.0
    return score


def _choose_graph_canonical_name(values: List[Any], entity_type: str = "Unknown") -> str:
    candidates = _graph_unique_strings(values)
    if not candidates:
        return "unknown"
    best = candidates[0]
    best_score = _graph_score_name_candidate(entity_type, best)
    family = _graph_entity_family(_canonical_graph_entity_type(entity_type, best, []))
    for candidate in candidates[1:]:
        score = _graph_score_name_candidate(entity_type, candidate)
        if score > best_score:
            best = candidate
            best_score = score
            family = _graph_entity_family(_canonical_graph_entity_type(entity_type, best, []))
            continue
        if score == best_score:
            # For people and roles, prefer the more complete (often longer) form when scores tie.
            # For other entity types, shorter names are typically more canonical.
            if family in {"person", "role"}:
                if len(candidate) > len(best):
                    best = candidate
            else:
                if len(candidate) < len(best):
                    best = candidate
            continue
    return best


def _majority_value(values: List[str], fallback: str) -> str:
    counts: Dict[str, int] = {}
    for value in values:
        normalized = str(value or "").strip()
        if not normalized:
            continue
        counts[normalized] = counts.get(normalized, 0) + 1
    if not counts:
        return fallback
    return sorted(counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]


def _stable_graph_node_id(run_id: str, entity_type: str, canonical_name: str) -> str:
    digest = hashlib.sha256(
        f"{run_id}|{_normalize_graph_name(entity_type)}|{_normalize_graph_name(canonical_name)}".encode("utf-8")
    ).hexdigest()[:20]
    return f"ent_{digest}"


def _canonical_graph_node_id(entity_type: str, canonical_name: str) -> str:
    digest = hashlib.sha256(
        f"{_normalize_graph_name(entity_type)}|{_normalize_graph_name(canonical_name)}".encode("utf-8")
    ).hexdigest()[:20]
    return f"entc_{digest}"


def _stable_graph_edge_id(src_id: str, dst_id: str, rel_type: str, canonical_name: str) -> str:
    digest = hashlib.sha256(
        f"{src_id}|{dst_id}|{_normalize_graph_name(rel_type)}|{_normalize_graph_name(canonical_name)}".encode("utf-8")
    ).hexdigest()[:20]
    return f"rel_{digest}"


def _canonical_graph_edge_id(src_canonical_id: str, dst_canonical_id: str, rel_type: str, canonical_name: str) -> str:
    digest = hashlib.sha256(
        f"{src_canonical_id}|{dst_canonical_id}|{_normalize_graph_name(rel_type)}|{_normalize_graph_name(canonical_name)}".encode("utf-8")
    ).hexdigest()[:20]
    return f"relc_{digest}"


def _infer_osint_bucket(entity_type: str, canonical_name: str, attributes: List[str]) -> str:
    type_text = _normalize_graph_name(entity_type)
    joined = " ".join([type_text, _normalize_graph_name(canonical_name), *[_normalize_graph_name(item) for item in attributes]])
    if any(token in joined for token in ("person", "author", "researcher", "founder", "director", "employee")):
        return "person"
    if any(token in joined for token in ("experience", "credential", "affiliation", "occupation", "timeline event", "timelineevent", "time node", "timenode", "contact point", "contactpoint")):
        return "person"
    if any(token in joined for token in ("organization profile", "org profile", "subject_org")):
        return "organization"
    if any(token in joined for token in ("organization", "company", "institution", "lab", "agency", "university")):
        return "organization"
    if any(token in joined for token in ("domain", "website", "hostname", "subdomain", "repo", "repository", "account", "username", "email", "phone", "imageobject", "image object")):
        return "digital_asset"
    if any(token in joined for token in ("article", "paper", "publication", "patent", "grant", "conference")):
        return "publication"
    if any(token in joined for token in ("topic", "theme", "keyword", "language", "framework", "method")):
        return "topic"
    if any(token in joined for token in ("project", "initiative", "program")):
        return "project"
    if any(token in joined for token in ("award", "fellowship", "prize", "honor")):
        return "award"
    if any(token in joined for token in ("role", "officer", "director", "title", "position")):
        return "role"
    if any(token in joined for token in ("city", "country", "location", "address", "region")):
        return "location"
    if any(token in joined for token in ("snippet", "evidence", "document")):
        return "evidence"
    return "unknown"


def _canonical_graph_entity_type(entity_type: str, canonical_name: str, attributes: List[str]) -> str:
    normalized_type = _normalize_graph_name(entity_type)
    explicit_mapping = {
        "person": "Person",
        "institution": "Institution",
        "organization": "Organization",
        "conference": "Conference",
        "contactpoint": "ContactPoint",
        "contact_point": "ContactPoint",
        "educationalcredential": "EducationalCredential",
        "educational_credential": "EducationalCredential",
        "experience": "Experience",
        "affiliation": "Affiliation",
        "timelineevent": "TimelineEvent",
        "timeline_event": "TimelineEvent",
        "timenode": "TimeNode",
        "time_node": "TimeNode",
        "occupation": "Occupation",
        "imageobject": "ImageObject",
        "image_object": "ImageObject",
        "repository": "Repository",
        "project": "Project",
        "topic": "Topic",
        "language": "Topic",
        "award": "Award",
        "grant": "Grant",
        "patent": "Patent",
        "role": "Role",
        "organizationprofile": "OrganizationProfile",
        "organization_profile": "OrganizationProfile",
        "publication": "Publication",
        "document": "Document",
        "website": "Website",
        "domain": "Domain",
        "email": "Email",
        "phone": "Phone",
        "handle": "Handle",
        "location": "Location",
    }
    if normalized_type in explicit_mapping:
        return explicit_mapping[normalized_type]

    normalized = _normalize_graph_name(" ".join([entity_type, canonical_name, *attributes]))
    if any(token in normalized for token in ("contact point", "contact_type", "contact surface")):
        return "ContactPoint"
    if any(token in normalized for token in ("educational credential", "credential", "degree", "bachelor of", "master of", "doctor of philosophy", "phd")):
        return "EducationalCredential"
    if any(token in normalized for token in ("time node", "time_key")):
        return "TimeNode"
    if any(token in normalized for token in ("timeline event", "milestone", "start_date", "end_date", "tenure_start", "tenure_end", "event_type")):
        return "TimelineEvent"
    if any(token in normalized for token in ("experience", "employment", "work history", "tenure")):
        return "Experience"
    if any(token in normalized for token in ("affiliation", "member of", "membership", "relation")):
        return "Affiliation"
    if any(token in normalized for token in ("occupation", "job family", "profession")):
        return "Occupation"
    if any(token in normalized for token in ("image object", "profile image", "avatar", "headshot")):
        return "ImageObject"
    if any(token in normalized for token in ("organization profile", "org profile", "company overview", "institution overview", "school overview", "lab overview", "subject org")):
        return "OrganizationProfile"
    if "topic kind language" in normalized or "language kind" in normalized:
        return "Topic"
    if any(token in normalized for token in ("orcid", "researcher", "author", "person", "advisor", "coauthor", "employee", "founder", "director")):
        return "Person"
    if any(token in normalized for token in ("university", "college", "institute", "school", "department", "lab", "laboratory")):
        return "Institution"
    if any(token in normalized for token in ("company", "organization", "corp", "llc", "committee", "agency", "firm")):
        return "Organization"
    if any(token in normalized for token in ("conference", "workshop", "symposium", "venue")):
        return "Conference"
    if any(token in normalized for token in ("repository", "repo")):
        return "Repository"
    if any(token in normalized for token in ("project", "framework", "initiative", "program")):
        return "Project"
    if any(token in normalized for token in ("programming language", "spoken language")):
        return "Topic"
    if any(token in normalized for token in ("topic", "theme", "keyword", "method")):
        return "Topic"
    if any(token in normalized for token in ("award", "prize", "fellowship", "honor")):
        return "Award"
    if any(token in normalized for token in ("grant", "award id", "nsf", "nih")):
        return "Grant"
    if any(token in normalized for token in ("patent", "application number", "inventor")):
        return "Patent"
    if any(token in normalized for token in ("role", "position", "title", "officer", "director")):
        return "Role"
    if any(token in normalized for token in ("paper", "publication", "preprint", "journal", "article")):
        return "Publication"
    lower_name = canonical_name.lower()
    if lower_name.startswith(("http://", "https://")):
        parsed = urlparse(lower_name)
        host = (parsed.hostname or "").lower()
        path_parts = [part for part in parsed.path.strip("/").split("/") if part]
        if host.startswith("www."):
            host = host[4:]
        if host in {"github.com", "gitlab.com", "bitbucket.org"} and len(path_parts) >= 2:
            return "Repository"
        if lower_name.endswith(".pdf") or any(token in normalized for token in ("thesis", "dissertation", "cv", "resume", "pdf")):
            return "Document"
        return "Website"
    if re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}", lower_name):
        return "Domain"
    if re.fullmatch(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", canonical_name, re.IGNORECASE):
        return "Email"
    if re.fullmatch(r"\+?[0-9][0-9().\-\s]{6,}[0-9]", canonical_name):
        return "Phone"
    if canonical_name.startswith("@") or "username" in normalized or "handle" in normalized:
        return "Handle"
    if any(token in normalized for token in ("city", "country", "location", "address", "state", "region")):
        return "Location"
    return entity_type if entity_type and entity_type != "Unknown" else "Unknown"


def _canonical_graph_relation_type(rel_type: str, src_type: str, dst_type: str) -> str:
    normalized = _normalize_graph_name(rel_type).replace(" ", "_").upper()
    mapping = {
        "AFFILIATED_WITH": "AFFILIATED_WITH",
        "WORKS_AT": "WORKS_AT",
        "MEMBER_OF": "MEMBER_OF",
        "FOUNDED": "FOUNDED",
        "OFFICER_OF": "OFFICER_OF",
        "DIRECTOR_OF": "DIRECTOR_OF",
        "OWNS": "OWNS",
        "USES_DOMAIN": "HAS_DOMAIN",
        "HAS_DOMAIN": "HAS_DOMAIN",
        "HAS_EMAIL": "HAS_EMAIL",
        "HAS_PHONE": "HAS_PHONE",
        "HAS_HANDLE": "HAS_HANDLE",
        "HAS_PROFILE": "HAS_PROFILE",
        "HAS_DOCUMENT": "HAS_DOCUMENT",
        "HAS_CONTACT_POINT": "HAS_CONTACT_POINT",
        "HAS_CREDENTIAL": "HAS_CREDENTIAL",
        "HAS_EXPERIENCE": "HAS_EXPERIENCE",
        "HAS_AFFILIATION": "HAS_AFFILIATION",
        "HAS_TIMELINE_EVENT": "HAS_TIMELINE_EVENT",
        "MENTIONS_TIMELINE_EVENT": "MENTIONS_TIMELINE_EVENT",
        "IN_TIME_NODE": "IN_TIME_NODE",
        "NEXT_TIME_NODE": "NEXT_TIME_NODE",
        "HAS_OCCUPATION": "HAS_OCCUPATION",
        "HAS_IMAGE": "HAS_IMAGE",
        "HAS_ROLE": "HAS_ROLE",
        "ISSUED_BY": "ISSUED_BY",
        "HOLDS_ROLE": "HOLDS_ROLE",
        "PUBLISHED": "PUBLISHED",
        "PUBLISHED_IN": "PUBLISHED_IN",
        "COAUTHORED_WITH": "COAUTHORED_WITH",
        "COAUTHOR_OF": "COAUTHORED_WITH",
        "MAINTAINS": "MAINTAINS",
        "LOCATED_IN": "LOCATED_IN",
        "KNOWS_LANGUAGE": "KNOWS_LANGUAGE",
        "USES_LANGUAGE": "USES_LANGUAGE",
        "RESEARCHES": "RESEARCHES",
        "FOCUSES_ON": "FOCUSES_ON",
        "HAS_TOPIC": "HAS_TOPIC",
        "HAS_SKILL_TOPIC": "HAS_SKILL_TOPIC",
        "HAS_HOBBY_TOPIC": "HAS_HOBBY_TOPIC",
        "HAS_INTEREST_TOPIC": "HAS_INTEREST_TOPIC",
        "HAS_ORGANIZATION_PROFILE": "HAS_ORGANIZATION_PROFILE",
        "RECEIVED_AWARD": "RECEIVED_AWARD",
        "HAS_GRANT": "HAS_GRANT",
        "HAS_PATENT": "HAS_PATENT",
        "FILED": "FILED",
        "APPEARS_IN_ARCHIVE": "APPEARS_IN_ARCHIVE",
        "STUDIED_AT": "STUDIED_AT",
        "ADVISED_BY": "ADVISED_BY",
        "ABOUT": "ABOUT",
    }
    if normalized in mapping:
        if normalized == "COAUTHOR_OF" and dst_type == "Publication":
            return "PUBLISHED"
        return mapping[normalized]
    if re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", normalized):
        return normalized
    if src_type == "Person" and dst_type in {"Organization", "Institution"}:
        return "AFFILIATED_WITH"
    if dst_type == "Email":
        return "HAS_EMAIL"
    if dst_type == "Phone":
        return "HAS_PHONE"
    if dst_type == "Handle":
        return "HAS_HANDLE"
    if dst_type == "Domain":
        return "HAS_DOMAIN"
    if dst_type == "Website":
        return "HAS_PROFILE"
    if dst_type == "Document":
        return "HAS_DOCUMENT"
    if dst_type == "ContactPoint":
        return "HAS_CONTACT_POINT"
    if dst_type == "EducationalCredential":
        return "HAS_CREDENTIAL"
    if dst_type == "Experience":
        return "HAS_EXPERIENCE"
    if dst_type == "Affiliation":
        return "HAS_AFFILIATION"
    if dst_type == "TimelineEvent":
        return "HAS_TIMELINE_EVENT"
    if dst_type == "TimeNode":
        return "IN_TIME_NODE"
    if dst_type == "Occupation":
        return "HAS_OCCUPATION"
    if dst_type == "ImageObject":
        return "HAS_IMAGE"
    if dst_type == "OrganizationProfile":
        return "HAS_ORGANIZATION_PROFILE"
    if dst_type == "Role":
        return "HAS_ROLE" if src_type == "Experience" else "HOLDS_ROLE"
    if src_type == "Person" and dst_type == "Publication":
        return "PUBLISHED"
    if src_type == "Publication" and dst_type == "Conference":
        return "PUBLISHED_IN"
    if src_type == "EducationalCredential" and dst_type in {"Institution", "Organization"}:
        return "ISSUED_BY"
    if src_type == "Experience" and dst_type in {"Organization", "Institution"}:
        if dst_type == "Institution":
            return "STUDIED_AT" if "student" in normalized or "phd" in normalized else "AFFILIATED_WITH"
        return "WORKS_AT"
    if src_type == "Role" and dst_type in {"Organization", "Institution"}:
        return "AFFILIATED_WITH"
    if src_type == "TimelineEvent":
        return "ABOUT"
    if src_type == "TimeNode" and dst_type == "TimeNode":
        return "NEXT_TIME_NODE"
    if src_type == "Person" and dst_type == "Award":
        return "RECEIVED_AWARD"
    if dst_type == "Grant":
        return "HAS_GRANT"
    if dst_type == "Patent":
        return "HAS_PATENT"
    if dst_type == "Topic":
        if src_type == "Repository":
            return "HAS_TOPIC"
        if src_type == "OrganizationProfile":
            return "FOCUSES_ON"
        if src_type == "Person":
            return "RESEARCHES"
        return "HAS_TOPIC"
    return "RELATED_TO"


def _graph_attribute_values(attributes: List[str], *prefixes: str) -> List[str]:
    values: List[str] = []
    normalized_prefixes = {prefix.strip().lower() for prefix in prefixes if prefix.strip()}
    for attribute in attributes:
        if not isinstance(attribute, str) or ":" not in attribute:
            continue
        key, raw_value = attribute.split(":", 1)
        if key.strip().lower() not in normalized_prefixes:
            continue
        value = raw_value.strip()
        if value:
            values.append(value)
    return _dedupe_str_list(values)


def _graph_auto_aliases(entity_type: str, values: List[str]) -> List[str]:
    aliases: List[str] = []
    family = entity_type.casefold()
    conservative_families = {
        "contactpoint",
        "educationalcredential",
        "experience",
        "affiliation",
        "timelineevent",
        "timenode",
        "occupation",
        "imageobject",
        "organizationprofile",
    }
    stopwords = {"the", "of", "at", "for", "and", "in", "on", "to"}
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        without_parens = re.sub(r"\s*\([^)]*\)\s*", " ", text).strip()
        without_parens = re.sub(r"\s+", " ", without_parens)
        if family not in conservative_families and without_parens and _normalize_graph_name(without_parens) != _normalize_graph_name(text):
            aliases.append(without_parens)
        if family not in conservative_families and " at " in text.lower():
            aliases.append(re.sub(r"\bat\b", "", text, flags=re.IGNORECASE).replace("  ", " ").strip())
        match = re.search(r"\(([^)]+)\)", text)
        if match:
            candidate = match.group(1).strip()
            if family not in conservative_families and re.fullmatch(r"[A-Za-z0-9._-]{2,16}", candidate):
                aliases.append(candidate)
        if family in {"institution", "organization", "conference", "project"}:
            words = [word for word in re.findall(r"[A-Za-z0-9]+", without_parens or text) if word.lower() not in stopwords]
            acronym = "".join(word[0] for word in words if word)
            if 2 <= len(acronym) <= 12:
                aliases.append(acronym.upper())
    return _graph_unique_strings(aliases)


def _graph_entity_family(entity_type: str) -> str:
    normalized = entity_type.casefold()
    if normalized in {"institution", "organization"}:
        return "org"
    if normalized in {"conference"}:
        return "conference"
    if normalized in {"publication", "document"}:
        return "publication"
    if normalized in {"repository"}:
        return "repository"
    if normalized == "language":
        return "topic"
    if normalized in {"website", "domain", "email", "handle", "phone"}:
        return "digital"
    if normalized == "contactpoint":
        return "contact"
    if normalized == "educationalcredential":
        return "credential"
    if normalized == "organizationprofile":
        return "orgprofile"
    if normalized in {"experience", "affiliation", "timelineevent", "timenode", "occupation"}:
        return normalized
    if normalized == "imageobject":
        return "image"
    if normalized in {"topic", "project", "award", "grant", "patent", "role"}:
        return normalized
    return normalized or "unknown"


def _graph_name_signature(value: str) -> str:
    normalized = _normalize_graph_name(re.sub(r"\s*\([^)]*\)\s*", " ", value))
    normalized = re.sub(r"\b(?:the|of|at|for|and|in|on|to)\b", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _graph_repository_key(values: List[str], attributes: List[str]) -> str:
    candidates = list(values) + _graph_attribute_values(attributes, "url", "id")
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        if text.startswith(("http://", "https://")):
            parsed = urlparse(text)
            parts = [part for part in parsed.path.strip("/").split("/") if part]
            if len(parts) >= 2:
                return f"{parts[0].lower()}/{parts[1].lower()}"
        if "/" in text and " " not in text:
            owner, name = text.split("/", 1)
            if owner and name:
                return f"{owner.lower()}/{name.lower()}"
    return ""


def _graph_entity_merge_keys(entity_type: str, canonical_name: str, alt_names: List[str], attributes: List[str]) -> List[str]:
    family = _graph_entity_family(entity_type)
    names = _graph_unique_strings([canonical_name, *alt_names, *_graph_auto_aliases(entity_type, [canonical_name, *alt_names])])
    keys: List[str] = []
    allow_host_merge_key = family in {"org", "conference"}
    for name in names:
        normalized = _normalize_graph_name(name)
        if normalized:
            keys.append(f"name:{family}:{normalized}")
        signature = _graph_name_signature(name)
        if signature and signature != normalized and family in {"org", "conference", "topic", "project"}:
            keys.append(f"sig:{family}:{signature}")
    for value in [canonical_name, *alt_names, *_graph_attribute_values(attributes, "url", "domain", "email", "handle", "username", "id", "doi", "arxiv_id")]:
        text = str(value or "").strip()
        if not text:
            continue
        lower_text = text.lower()
        if lower_text.startswith(("http://", "https://")):
            if family == "digital":
                keys.append(f"url:{lower_text.rstrip('/')}")
            else:
                keys.append(f"url:{family}:{lower_text.rstrip('/')}")
            host = (urlparse(text).hostname or "").lower()
            if host.startswith("www."):
                host = host[4:]
            if host and allow_host_merge_key:
                keys.append(f"host:{family}:{host}")
        elif re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}", lower_text):
            if family == "digital":
                keys.append(f"domain:{lower_text}")
            elif allow_host_merge_key:
                keys.append(f"host:{family}:{lower_text}")
        elif re.fullmatch(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", lower_text):
            keys.append(f"email:{lower_text}")
        elif text.startswith("@") or (family == "digital" and " " not in text and len(text) <= 32):
            keys.append(f"handle:{lower_text.lstrip('@')}")
        elif family == "publication" and len(lower_text) <= 64 and ("/" in lower_text or lower_text.startswith("10.")):
            keys.append(f"pubid:{lower_text}")
    for value in _graph_attribute_values(attributes, "company_number", "cik", "grant_id", "patent_id", "filing_id"):
        text = str(value or "").strip().lower()
        if text:
            keys.append(f"id:{family}:{text}")
    if family in {"contact", "credential", "experience", "affiliation", "timelineevent", "timenode", "occupation", "orgprofile"}:
        subject_values = _graph_attribute_values(attributes, "subject")
        org_values = _graph_attribute_values(attributes, "organization", "institution", "employer", "company", "subject_org")
        role_values = _graph_attribute_values(attributes, "role", "occupation", "degree", "field", "relation", "contact_type", "event_type", "industry", "focus", "time_key")
        date_values = _graph_attribute_values(attributes, "date", "year", "start_date", "end_date", "tenure_start", "tenure_end")
        direct_values = _graph_attribute_values(attributes, "value", "email", "phone", "handle", "username")
        for value in direct_values:
            normalized = str(value or "").strip().lower()
            if normalized:
                keys.append(f"value:{family}:{normalized}")
        if subject_values or org_values or role_values or date_values or direct_values:
            composite = "|".join(
                [
                    _graph_name_signature(canonical_name),
                    _normalize_graph_name(subject_values[0]) if subject_values else "",
                    _normalize_graph_name(org_values[0]) if org_values else "",
                    _normalize_graph_name(role_values[0]) if role_values else "",
                    _normalize_graph_name(date_values[0]) if date_values else "",
                    str(direct_values[0] or "").strip().lower() if direct_values else "",
                ]
            ).strip("|")
            if composite:
                keys.append(f"composite:{family}:{composite}")
    if family == "repository":
        repo_key = _graph_repository_key(names, attributes)
        if repo_key:
            keys.append(f"repo:{repo_key}")
    return _dedupe_str_list(keys)


def _is_graph_noise_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in {"duckduckgo.com", "html.duckduckgo.com", "www.google.com", "google.com", "www.bing.com", "bing.com"}:
        return True
    if host.endswith("linkedin.com") and "/pub/dir/" in parsed.path.lower():
        return True
    if host.endswith("truepeoplesearch.com") and "/find/" in parsed.path.lower():
        return True
    return any(token in parsed.path.lower() for token in ("/search", "/html"))


def _clean_url_candidate(url: str) -> str:
    return str(url or "").strip().rstrip("\"'.,);]>")


def _graph_platform_label(platform: str | None, url: str | None = None) -> str:
    explicit = str(platform or "").strip()
    if explicit:
        lower = explicit.casefold()
        known = {
            "github": "GitHub",
            "gitlab": "GitLab",
            "linkedin": "LinkedIn",
            "researchgate": "ResearchGate",
            "orcid": "ORCID",
            "google scholar": "Google Scholar",
            "scholar": "Google Scholar",
            "personal site": "Personal Site",
        }
        if lower in known:
            return known[lower]
        return explicit.title()
    cleaned = _clean_url_candidate(str(url or ""))
    if not cleaned.startswith(("http://", "https://")):
        return ""
    host = (urlparse(cleaned).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    host_map = {
        "github.com": "GitHub",
        "gitlab.com": "GitLab",
        "linkedin.com": "LinkedIn",
        "researchgate.net": "ResearchGate",
        "orcid.org": "ORCID",
        "scholar.google.com": "Google Scholar",
        "google scholar": "Google Scholar",
        "openreview.net": "OpenReview",
        "arxiv.org": "arXiv",
        "x.com": "X",
        "twitter.com": "X",
    }
    if host in host_map:
        return host_map[host]
    if not host:
        return ""
    host_base = host.split(".")[0].replace("-", " ").replace("_", " ").strip()
    return host_base.title()


def _graph_semantic_resource_title(
    entity_type: str,
    url: str,
    *,
    owner_name: str | None = None,
    subject_name: str | None = None,
    platform: str | None = None,
    title: str | None = None,
) -> str:
    explicit_title = str(title or "").strip()
    if explicit_title and not explicit_title.startswith(("http://", "https://")):
        return explicit_title
    subject = str(subject_name or owner_name or "").strip()
    platform_label = _graph_platform_label(platform, url)
    if entity_type == "ImageObject" and subject:
        return f"Image of {subject}"
    if entity_type == "Document":
        if subject:
            return f"Document for {subject}"
        return url
    if entity_type == "Website":
        if subject and platform_label:
            return f"{platform_label} profile for {subject}"
        if subject:
            return f"Website for {subject}"
        if platform_label:
            return f"{platform_label} profile"
        return url
    return explicit_title or url


def _graph_role_relation_type(role_text: str) -> str:
    normalized = _normalize_graph_name(role_text)
    if any(token in normalized for token in ("founder", "cofounder")):
        return "FOUNDED"
    if any(token in normalized for token in ("director", "board")):
        return "DIRECTOR_OF"
    if any(token in normalized for token in ("officer", "chief", "ceo", "cto", "cfo", "president", "treasurer", "secretary")):
        return "OFFICER_OF"
    if any(token in normalized for token in ("student", "phd", "doctor of philosophy", "master", "bachelor", "alumn", "graduate")):
        return "STUDIED_AT"
    if any(token in normalized for token in ("engineer", "research", "scientist", "professor", "assistant", "intern", "employee", "staff", "maintainer")):
        return "WORKS_AT"
    return "AFFILIATED_WITH"


def _graph_org_type(org_name: str) -> str:
    normalized = _normalize_graph_name(org_name)
    if any(token in normalized for token in ("university", "college", "institute", "school", "lab", "laboratory", "department")):
        return "Institution"
    return "Organization"


def _graph_date_span_label(start_date: str, end_date: str, year: str = "") -> str:
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    if start_date:
        return f"{start_date} to present"
    if end_date:
        return end_date
    return year


def _graph_normalize_time_token(value: str) -> tuple[str, str, str]:
    text = str(value or "").strip()
    if not text:
        return "", "", ""
    lower = text.casefold()
    if lower in {"present", "current", "now"}:
        return "present", "open", "9999-12-31"
    year_match = re.fullmatch(r"(19|20)\d{2}", text)
    if year_match:
        normalized = year_match.group(0)
        return normalized, "year", f"{normalized}-01-01"
    year_month_match = re.fullmatch(r"((?:19|20)\d{2})[-/](0?[1-9]|1[0-2])", text)
    if year_month_match:
        normalized = f"{year_month_match.group(1)}-{int(year_month_match.group(2)):02d}"
        return normalized, "month", f"{normalized}-01"
    ymd_match = re.fullmatch(r"((?:19|20)\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])", text)
    if ymd_match:
        normalized = f"{ymd_match.group(1)}-{int(ymd_match.group(2)):02d}-{int(ymd_match.group(3)):02d}"
        return normalized, "day", normalized
    iso_embedded = re.search(r"((?:19|20)\d{2})-(0[1-9]|1[0-2])-([0-3]\d)", text)
    if iso_embedded:
        normalized = f"{iso_embedded.group(1)}-{iso_embedded.group(2)}-{iso_embedded.group(3)}"
        return normalized, "day", normalized
    month_map = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }
    month_year_match = re.fullmatch(r"([A-Za-z]{3,9})\s+((?:19|20)\d{2})", text)
    if month_year_match:
        month = month_map.get(month_year_match.group(1).casefold())
        year = month_year_match.group(2)
        if month:
            normalized = f"{year}-{month:02d}"
            return normalized, "month", f"{normalized}-01"
    year_in_text = re.search(r"(19|20)\d{2}", text)
    if year_in_text:
        normalized = year_in_text.group(0)
        return normalized, "year", f"{normalized}-01-01"
    return "", "", ""


def _graph_time_node_parts(date: str = "", start_date: str = "", end_date: str = "") -> tuple[str, List[str], str]:
    normalized_date, date_granularity, date_sort = _graph_normalize_time_token(date)
    normalized_start, start_granularity, start_sort = _graph_normalize_time_token(start_date)
    normalized_end, end_granularity, end_sort = _graph_normalize_time_token(end_date)

    if normalized_start and normalized_end:
        key = f"{normalized_start}__{normalized_end}"
        label = f"{normalized_start} to {normalized_end}"
        granularity = start_granularity or end_granularity or "range"
        sort_key = start_sort or end_sort
    elif normalized_start:
        key = f"{normalized_start}__present"
        label = f"{normalized_start} to present"
        granularity = start_granularity or "range"
        sort_key = start_sort
    elif normalized_date:
        key = normalized_date
        label = normalized_date
        granularity = date_granularity or "point"
        sort_key = date_sort
    elif normalized_end:
        key = f"until__{normalized_end}"
        label = f"until {normalized_end}"
        granularity = end_granularity or "range"
        sort_key = end_sort
    else:
        return "", [], ""

    attrs = [f"time_key: {key}", f"granularity: {granularity}"]
    if normalized_date:
        attrs.append(f"date: {normalized_date}")
    if normalized_start:
        attrs.append(f"start_date: {normalized_start}")
    if normalized_end:
        attrs.append(f"end_date: {normalized_end}")
    return f"Time node {label}", attrs, sort_key


GRAPH_SEARCH_NORMALIZER_TOOL_NAMES = {
    "person_search",
    "google_serp_person_search",
    "tavily_person_search",
    "tavily_research",
}
GRAPH_SEARCH_NOISY_PERSON_TOKENS = {
    "advisor",
    "advisors",
    "anthology",
    "citation",
    "citations",
    "conflicts",
    "dblp",
    "email",
    "emails",
    "generator",
    "github",
    "google",
    "linkedin",
    "name",
    "names",
    "openreview",
    "profile",
    "relations",
    "scholar",
    "semantic",
    "semanticscholar",
    "suggest",
    "url",
    "wikipedia",
}
GRAPH_SEARCH_NOISY_PERSON_PHRASES = (
    "suggest name emails",
    "suggest position advisors",
    "google scholar",
    "semantic scholar",
)
GRAPH_SEARCH_ROLE_PATTERN = re.compile(
    r"\b("
    r"assistant professor|associate professor|postdoctoral researcher|research scientist|"
    r"research assistant|research intern|doctoral candidate|graduate student|"
    r"undergraduate student|software engineer|staff engineer|phd student|"
    r"researcher|scientist|engineer|professor|student|founder|director"
    r")\b",
    re.IGNORECASE,
)
GRAPH_SEARCH_YEAR_RANGE_PATTERN = re.compile(
    r"\b((?:19|20)\d{2})\s*[–-]\s*((?:19|20)\d{2}|present|current|now)\b",
    re.IGNORECASE,
)


def _graph_dedupe_mixed_items(values: List[Any]) -> List[Any]:
    output: List[Any] = []
    seen: set[str] = set()
    for value in values:
        if isinstance(value, str):
            cleaned = str(value).strip()
            if not cleaned:
                continue
            fingerprint = f"s:{cleaned.casefold()}"
            normalized_value: Any = cleaned
        elif isinstance(value, dict):
            cleaned_dict = {
                str(key): nested
                for key, nested in value.items()
                if nested not in (None, "", [], {})
            }
            if not cleaned_dict:
                continue
            fingerprint = f"d:{json.dumps(cleaned_dict, sort_keys=True, ensure_ascii=True, default=str)}"
            normalized_value = cleaned_dict
        else:
            continue
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        output.append(normalized_value)
    return output


def _graph_merge_result_list(result: Dict[str, Any], key: str, additions: List[Any]) -> None:
    existing = result.get(key)
    merged = _graph_dedupe_mixed_items(
        [*(existing if isinstance(existing, list) else []), *additions]
    )
    if merged:
        result[key] = merged


def _graph_org_aliases(org_name: str) -> List[str]:
    text = str(org_name or "").strip()
    normalized = _normalize_graph_name(text)
    aliases: List[str] = []
    if normalized in {"ucsd", "uc san diego", "university of california san diego"}:
        aliases.extend(["UC San Diego", "UCSD", "University of California, San Diego"])
    if normalized in {
        "uiuc",
        "university of illinois urbana champaign",
        "university of illinois at urbana champaign",
    }:
        aliases.extend(["UIUC", "University of Illinois Urbana-Champaign", "University of Illinois at Urbana-Champaign"])
    return [alias for alias in _graph_unique_strings(aliases) if _normalize_graph_name(alias) != normalized]


def _graph_canonical_org_name(org_name: str) -> str:
    text = str(org_name or "").strip()
    normalized = _normalize_graph_name(text)
    if normalized in {"ucsd", "uc san diego", "university of california san diego"}:
        return "University of California, San Diego"
    if normalized in {
        "uiuc",
        "university of illinois urbana champaign",
        "university of illinois at urbana champaign",
    }:
        return "University of Illinois at Urbana-Champaign"
    return text


def _graph_person_name_from_profile_url(url: str) -> str:
    cleaned = _clean_url_candidate(url)
    if not cleaned.startswith(("http://", "https://")):
        return ""
    parsed = urlparse(cleaned)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if not parts:
        return ""
    slug = parts[-1]
    tokens = [token for token in re.split(r"[-_]+", slug) if token]
    name_tokens = [token for token in tokens if re.fullmatch(r"[A-Za-z][A-Za-z']*", token)]
    if len(name_tokens) < 2:
        return ""
    candidate = " ".join(token.title() for token in name_tokens[:4])
    return candidate if _graph_is_valid_search_person_candidate(candidate) else ""


def _graph_is_noisy_search_person_candidate(value: str) -> bool:
    candidate = " ".join(str(value or "").strip().split()).strip(" -,:;|")
    if not candidate:
        return True
    normalized = _normalize_graph_name(candidate)
    if any(phrase in normalized for phrase in GRAPH_SEARCH_NOISY_PERSON_PHRASES):
        return True
    tokens = [token for token in normalized.split() if token]
    if any(token in GRAPH_SEARCH_NOISY_PERSON_TOKENS for token in tokens):
        return True
    return bool(re.search(r"[@/:]|\b(?:search|result|results)\b", candidate, re.IGNORECASE))


def _graph_is_valid_search_person_candidate(value: str) -> bool:
    candidate = str(value or "").strip()
    if not candidate or _graph_is_noisy_search_person_candidate(candidate):
        return False
    return _graph_looks_like_person_name(candidate)


def _graph_collect_row_records(tool_name: str, result: Dict[str, Any]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    raw_rows: List[Any] = []
    if tool_name == "person_search":
        if isinstance(result.get("results"), list):
            raw_rows.extend(result.get("results", [])[:20])
    elif tool_name in GRAPH_SEARCH_NORMALIZER_TOOL_NAMES:
        if isinstance(result.get("extracted_results"), list):
            raw_rows.extend(result.get("extracted_results", [])[:20])
        if tool_name == "tavily_research" and isinstance(result.get("sources"), list):
            raw_rows.extend(result.get("sources", [])[:20])
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        url = _clean_url_candidate(str(row.get("url") or "").strip())
        title = str(row.get("title") or "").strip()
        text = " ".join(
            part
            for part in (
                str(row.get("text") or "").strip(),
                str(row.get("extracted_text") or "").strip(),
                str(row.get("snippet") or "").strip(),
                str(row.get("content") or "").strip(),
                str(row.get("main_text") or "").strip(),
                str(row.get("description") or "").strip(),
            )
            if part
        ).strip()
        if not any([url, title, text]):
            continue
        rows.append({"url": url, "title": title, "text": text})
    return rows


def _graph_title_person_candidates(title: str) -> List[str]:
    cleaned = str(title or "").replace("\u202a", "").replace("\u202c", "").replace("\u200e", "").strip()
    segments = [cleaned]
    segments.extend(re.split(r"\s+[|·–]\s+|\s+-\s+", cleaned))
    candidates: List[str] = []
    for segment in segments[:4]:
        value = segment.strip(" []()")
        if not value:
            continue
        if _graph_is_valid_search_person_candidate(value):
            candidates.append(value)
            continue
        candidates.extend(
            candidate
            for candidate in _extract_related_people(value)
            if _graph_is_valid_search_person_candidate(candidate)
        )
    return _graph_unique_strings(candidates)


def _graph_resolve_primary_person_from_rows(rows: List[Dict[str, str]], fallback_names: List[str]) -> str:
    scores: Dict[str, float] = {}
    for fallback in fallback_names:
        candidate = str(fallback or "").strip()
        if _graph_is_valid_search_person_candidate(candidate):
            scores[candidate] = scores.get(candidate, 0.0) + 1.0
    for row in rows[:12]:
        title = str(row.get("title") or "")
        for candidate in _graph_title_person_candidates(title):
            scores[candidate] = scores.get(candidate, 0.0) + 4.0
        text = str(row.get("text") or "")
        for candidate in _extract_related_people(text)[:10]:
            if _graph_is_valid_search_person_candidate(candidate):
                scores[candidate] = scores.get(candidate, 0.0) + 1.0
    if not scores:
        return ""
    ranked = sorted(
        scores.items(),
        key=lambda item: (item[1], _graph_score_name_candidate("Person", item[0]), -len(item[0]), item[0]),
        reverse=True,
    )
    return ranked[0][0]


def _graph_is_profileish_result_url(url: str, title: str) -> bool:
    cleaned = _clean_url_candidate(url)
    if not cleaned.startswith(("http://", "https://")) or _is_graph_noise_url(cleaned):
        return False
    parsed = urlparse(cleaned)
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.lower()
    if host in {
        "openreview.net",
        "scholar.google.com",
        "dblp.org",
        "researchgate.net",
        "semanticscholar.org",
        "www.semanticscholar.org",
        "aclanthology.org",
        "underline.io",
        "catalyzex.com",
        "linkedin.com",
        "www.linkedin.com",
        "github.com",
    }:
        return True
    if any(token in path for token in ("/profile", "/author", "/citations", "/people/", "/speakers/", "/pid/", "/in/")):
        return True
    return any(_graph_is_valid_search_person_candidate(candidate) for candidate in _graph_title_person_candidates(title))


def _graph_extract_org_mentions_with_positions(text: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    patterns = (
        r"University of California,?\s+San Diego",
        r"University of Illinois(?: at)? Urbana-Champaign",
        r"UC San Diego",
        r"UCSD",
        r"UIUC",
        r"Stealth Startup",
        r"[A-Z][A-Za-z0-9&.'-]*(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,7}\s+(?:University|College|Institute|School|Laboratory|Lab|Startup|Corporation|Corp|Inc|LLC)",
    )
    seen: set[str] = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            raw_name = str(match.group(0) or "").strip(" ,.;|")
            if not raw_name:
                continue
            canonical_name = _graph_canonical_org_name(raw_name)
            normalized_name = _normalize_graph_name(canonical_name)
            if normalized_name not in {
                "university of california san diego",
                "university of illinois at urbana champaign",
                "stealth startup",
            } and any(
                token in normalized_name
                for token in ("linkedin", "openreview", "google scholar", "scholar", "relations", "conflicts", "career", "education")
            ):
                continue
            fingerprint = f"{canonical_name.casefold()}:{match.start()}"
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            candidates.append(
                {
                    "name": canonical_name,
                    "alt_names": _graph_org_aliases(canonical_name) + ([raw_name] if _normalize_graph_name(raw_name) != _normalize_graph_name(canonical_name) else []),
                    "start": match.start(),
                    "end": match.end(),
                }
            )
    return sorted(candidates, key=lambda item: (int(item["start"]), int(item["end"])))


def _graph_clean_field_segment(value: str) -> str:
    text = str(value or "").strip(" ,.;:|·")
    text = re.sub(r"^(?:at|@)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text).strip(" ,.;:|·")
    if not text:
        return ""
    if _graph_is_url_candidate(text) or _graph_looks_like_domain(text):
        return ""
    if len(text) > 80:
        return ""
    return text


def _graph_extract_role_records_from_text(text: str, *, source_url: str = "") -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    org_mentions = _graph_extract_org_mentions_with_positions(text)
    if not org_mentions:
        return records
    for match in GRAPH_SEARCH_ROLE_PATTERN.finditer(text):
        role_title = " ".join(str(match.group(1) or "").split()).strip()
        if not role_title:
            continue
        role_end = match.end()
        org_match = next(
            (
                item
                for item in org_mentions
                if int(item["start"]) >= role_end and int(item["start"]) - role_end <= 120
            ),
            None,
        )
        if org_match is None:
            continue
        field = _graph_clean_field_segment(text[role_end:int(org_match["start"])])
        search_window = text[match.start(): min(len(text), int(org_match["end"]) + 64)]
        date_match = GRAPH_SEARCH_YEAR_RANGE_PATTERN.search(search_window)
        start_date = str(date_match.group(1) or "").strip() if date_match else ""
        end_date = str(date_match.group(2) or "").strip() if date_match else ""
        record = {
            "title": role_title,
            "organization": str(org_match["name"]),
            "source_url": source_url,
        }
        if field:
            record["field"] = field
        if start_date:
            record["start_date"] = start_date
        if end_date:
            record["end_date"] = end_date
        records.append(record)
    return _graph_dedupe_mixed_items(records)


def _graph_extract_affiliation_records_from_text(text: str, *, source_url: str = "", relation: str = "profile affiliation") -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for org_match in _graph_extract_org_mentions_with_positions(text):
        records.append(
            {
                "name": str(org_match["name"]),
                "relation": relation,
                "url": source_url,
            }
        )
    return _graph_dedupe_mixed_items(records)


def _graph_extract_advisors_from_text(text: str, exclude_names: List[str]) -> List[str]:
    advisor_names: List[str] = []
    for match in re.finditer(r"\b(?:phd\s+advisor|advisor|advisors)\b[:\s,]+(.{0,120})", text, re.IGNORECASE):
        snippet = str(match.group(1) or "").strip()
        leading_tokens = re.findall(r"[A-Z][A-Za-z'-]*", snippet)
        for size in range(2, min(4, len(leading_tokens)) + 1):
            candidate = " ".join(leading_tokens[:size]).strip()
            if _graph_is_valid_search_person_candidate(candidate):
                advisor_names.append(candidate)
                break
        advisor_names.extend(
            candidate
            for candidate in _extract_related_people(snippet, exclude_names=exclude_names)[:4]
            if _graph_is_valid_search_person_candidate(candidate)
        )
    return _graph_unique_strings(advisor_names)


def _graph_extract_topics_from_row(row: Dict[str, str], primary_person: str = "") -> List[str]:
    title = str(row.get("title") or "")
    text = str(row.get("text") or "")
    url = str(row.get("url") or "")
    parsed = urlparse(_clean_url_candidate(url))
    host = (parsed.hostname or "").lower()
    segments = [
        segment.strip(" \u202a\u202c")
        for segment in re.split(r"\s+[|·]\s+|\s+-\s+", title)
        if segment.strip(" \u202a\u202c")
    ]
    topics: List[str] = []
    if host == "scholar.google.com":
        for segment in segments:
            normalized = _normalize_graph_name(segment)
            if not normalized or normalized.startswith("verified email at"):
                continue
            if _normalize_graph_name(segment) == _normalize_graph_name(primary_person):
                continue
            if _graph_extract_org_mentions_with_positions(segment):
                continue
            if _graph_looks_like_domain(segment) or _graph_is_url_candidate(segment):
                continue
            if any(token in normalized for token in ("articles", "cited by", "public access", "privacy", "terms", "help", "google scholar", "openreview", "dblp", "linkedin", "semantic scholar")):
                continue
            if 2 <= len(segment.split()) <= 5:
                topics.append(segment)
    verified_email_match = re.search(r"verified email at\s+([A-Za-z0-9.-]+\.[A-Za-z]{2,})", text, re.IGNORECASE)
    if verified_email_match:
        topics.extend(
            segment
            for segment in segments
            if "verified email at" not in segment.casefold()
            and _normalize_graph_name(segment) != _normalize_graph_name(primary_person)
            and not _graph_extract_org_mentions_with_positions(segment)
            and not any(token in _normalize_graph_name(segment) for token in ("google scholar", "openreview", "dblp", "linkedin", "semantic scholar"))
            and 2 <= len(segment.split()) <= 5
        )
    return _graph_unique_strings(topics)


def _normalize_search_like_result_for_graph(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(result)
    rows = _graph_collect_row_records(tool_name, result)
    affiliation_hosts = {
        "openreview.net",
        "scholar.google.com",
        "researchgate.net",
        "underline.io",
        "linkedin.com",
        "www.linkedin.com",
        "dl.acm.org",
        "dblp.org",
        "aclanthology.org",
        "catalyzex.com",
        "academia.edu",
        "independent.academia.edu",
    }
    fallback_names = _graph_unique_strings(
        [
            str(result.get("canonical_name") or "").strip(),
            str(result.get("display_name") or "").strip(),
            str(result.get("target_name") or "").strip(),
            str(result.get("name") or "").strip(),
            str(result.get("input") or "").strip(),
            str(arguments.get("person_name") or "").strip(),
            str(arguments.get("target_name") or "").strip(),
            str(arguments.get("name") or "").strip(),
        ]
    )
    resolved_primary_person = _graph_resolve_primary_person_from_rows(rows, fallback_names)
    if resolved_primary_person:
        normalized["resolved_primary_person"] = resolved_primary_person
        if not str(normalized.get("canonical_name") or "").strip() or _graph_is_noisy_search_person_candidate(str(normalized.get("canonical_name") or "")):
            normalized["canonical_name"] = resolved_primary_person

    external_links: List[Dict[str, Any]] = []
    contact_signals: List[Dict[str, Any]] = []
    organizations: List[Dict[str, Any]] = []
    roles: List[Dict[str, Any]] = []
    education: List[Dict[str, Any]] = []
    advisors: List[Dict[str, Any]] = []
    topics: List[str] = []
    related_org_names_from_roles: set[str] = set()

    for row in rows[:20]:
        url = str(row.get("url") or "")
        title = str(row.get("title") or "")
        body_text = str(row.get("text") or "").strip()
        text = " ".join(part for part in (title, body_text) if part).strip()
        parsed_url = urlparse(_clean_url_candidate(url))
        host = (parsed_url.hostname or "").lower()
        if _graph_is_profileish_result_url(url, title):
            external_links.append(
                {
                    "type": _graph_platform_label(None, url) or "profile",
                    "url": url,
                    "title": title,
                }
            )
        for email in _extract_strings_from_text(text, kind="email"):
            contact_signals.append({"type": "email", "value": email})
        for phone in _extract_phone_numbers_from_text(text):
            contact_signals.append({"type": "phone", "value": phone})
        if host == "scholar.google.com":
            for segment in re.split(r"\s+[|·]\s+|\s+-\s+", title):
                normalized_segment = _normalize_graph_name(segment)
                if normalized_segment in {"uc san diego", "ucsd", "university of california san diego"}:
                    organizations.append(
                        {
                            "name": "University of California, San Diego",
                            "relation": "scholar affiliation",
                            "url": url,
                        }
                    )
                elif normalized_segment in {
                    "uiuc",
                    "university of illinois urbana champaign",
                    "university of illinois at urbana champaign",
                }:
                    organizations.append(
                        {
                            "name": "University of Illinois at Urbana-Champaign",
                            "relation": "scholar affiliation",
                            "url": url,
                        }
                    )
        if host.endswith("linkedin.com") and re.search(r"\bStealth Startup\b", body_text, re.IGNORECASE):
            organizations.append({"name": "Stealth Startup", "relation": "member", "url": url})

        row_roles = _graph_extract_role_records_from_text(body_text or text, source_url=url)
        roles.extend(row_roles)
        for row_role in row_roles:
            org_name = str(row_role.get("organization") or "").strip()
            if org_name:
                related_org_names_from_roles.add(_normalize_graph_name(org_name))
            if _graph_is_education_relation(
                _graph_role_relation_type(str(row_role.get("title") or "")),
                str(row_role.get("title") or ""),
            ):
                education_record = {
                    "degree": row_role.get("title"),
                    "institution": row_role.get("organization"),
                    "source_url": row_role.get("source_url"),
                }
                if row_role.get("field"):
                    education_record["field"] = row_role.get("field")
                if row_role.get("start_date"):
                    education_record["start_date"] = row_role.get("start_date")
                if row_role.get("end_date"):
                    education_record["end_date"] = row_role.get("end_date")
                education.append(education_record)

        row_orgs = (
            []
            if host.endswith("linkedin.com") or host not in affiliation_hosts
            else _graph_extract_affiliation_records_from_text(
                body_text or text,
                source_url=url,
                relation="profile affiliation",
            )
        )
        for row_org in row_orgs:
            org_name = str(row_org.get("name") or "").strip()
            if _normalize_graph_name(org_name) in related_org_names_from_roles:
                continue
            organizations.append(row_org)

        for advisor_name in _graph_extract_advisors_from_text(body_text or text, exclude_names=[resolved_primary_person]):
            advisors.append({"name": advisor_name})
        topics.extend(_graph_extract_topics_from_row(row, primary_person=resolved_primary_person))

    _graph_merge_result_list(normalized, "external_links", external_links)
    _graph_merge_result_list(normalized, "contact_signals", contact_signals)
    _graph_merge_result_list(normalized, "organizations", organizations)
    _graph_merge_result_list(normalized, "roles", roles)
    _graph_merge_result_list(normalized, "education", education)
    _graph_merge_result_list(normalized, "advisors", advisors)
    _graph_merge_result_list(normalized, "topics", topics)
    return normalized


def _normalize_linkedin_result_for_graph(arguments: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(result)
    profile_url = _clean_url_candidate(str(result.get("profile") or arguments.get("profile") or "").strip())
    if profile_url:
        normalized["profile_url"] = profile_url
        if not str(normalized.get("canonical_name") or "").strip():
            slug_name = _graph_person_name_from_profile_url(profile_url)
            if slug_name:
                normalized["canonical_name"] = slug_name

    contact_signals: List[Dict[str, Any]] = []
    external_links: List[Dict[str, Any]] = []
    contact_info = result.get("contact_info") if isinstance(result.get("contact_info"), dict) else {}
    if contact_info:
        for email in _extract_string_list(contact_info.get("emails"))[:10]:
            contact_signals.append({"type": "email", "value": email})
        for phone in _extract_string_list(contact_info.get("phones"))[:10]:
            contact_signals.append({"type": "phone", "value": phone})
        for website in _extract_string_list(contact_info.get("websites"))[:10]:
            contact_signals.append({"type": "website", "value": website, "platform": "Personal Site"})
        for profile in _extract_string_list(contact_info.get("profiles"))[:10]:
            external_links.append({"type": _graph_platform_label(None, profile) or "profile", "url": profile})
        overlay_url = _clean_url_candidate(str(contact_info.get("overlay_url") or "").strip())
        if overlay_url:
            external_links.append({"type": "LinkedIn", "url": overlay_url})

    rows: List[Dict[str, str]] = []
    for page in result.get("extracted_pages", []) if isinstance(result.get("extracted_pages"), list) else []:
        if not isinstance(page, dict):
            continue
        rows.append(
            {
                "url": profile_url,
                "title": str(page.get("file") or "").strip(),
                "text": str(page.get("extracted_text") or "").strip(),
            }
        )
    if rows:
        derived = _normalize_search_like_result_for_graph("person_search", arguments, {"results": rows, **normalized})
        for key in ("resolved_primary_person", "canonical_name", "organizations", "roles", "education", "advisors", "topics"):
            if key in derived and derived.get(key):
                normalized[key] = derived[key]
        if not str(normalized.get("headline") or "").strip():
            combined_text = " ".join(str(row.get("text") or "") for row in rows)
            headline_match = re.search(r"\bbuilds?\s+[A-Za-z0-9 ,'-]{10,120}", combined_text, re.IGNORECASE)
            if headline_match:
                normalized["headline"] = " ".join(headline_match.group(0).split()).strip(" ,.;")
            if re.search(r"\bStealth Startup\b", combined_text):
                _graph_merge_result_list(
                    normalized,
                    "organizations",
                    [{"name": "Stealth Startup", "relation": "member", "url": profile_url}],
                )
            location_match = re.search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3},\s*[A-Z]{2})\b", combined_text)
            if location_match:
                contact_signals.append({"type": "location", "value": location_match.group(1)})

    _graph_merge_result_list(normalized, "contact_signals", contact_signals)
    _graph_merge_result_list(normalized, "external_links", external_links)
    return normalized


def _normalize_tool_result_for_graph(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    if tool_name in GRAPH_SEARCH_NORMALIZER_TOOL_NAMES:
        return _normalize_search_like_result_for_graph(tool_name, arguments, result)
    if tool_name == "linkedin_download_html_ocr":
        return _normalize_linkedin_result_for_graph(arguments, result)
    return result


def _graph_primary_people_from_context(tool_name: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> List[str]:
    ordered_candidates = (
        [
            str(result.get("resolved_primary_person") or "").strip(),
            str(result.get("display_name") or "").strip(),
            str(result.get("canonical_name") or "").strip(),
            str(arguments.get("person_name") or "").strip(),
            str(arguments.get("target_name") or "").strip(),
            str(arguments.get("name") or "").strip() if tool_name in {"person_search", "tavily_person_search"} else "",
            str(arguments.get("author") or "").strip(),
            str(result.get("target_name") or "").strip(),
            str(result.get("name") or "").strip(),
            str(result.get("input") or "").strip() if tool_name == "tavily_research" else "",
        ]
        if tool_name in GRAPH_SEARCH_NORMALIZER_TOOL_NAMES or tool_name == "linkedin_download_html_ocr"
        else [
            str(arguments.get("person_name") or "").strip(),
            str(arguments.get("target_name") or "").strip(),
            str(arguments.get("name") or "").strip() if tool_name in {"person_search", "tavily_person_search"} else "",
            str(arguments.get("author") or "").strip(),
            str(result.get("display_name") or "").strip(),
            str(result.get("canonical_name") or "").strip(),
        ]
    )
    candidates = []
    for candidate in ordered_candidates:
        if not candidate:
            continue
        if tool_name in GRAPH_SEARCH_NORMALIZER_TOOL_NAMES or tool_name == "linkedin_download_html_ocr":
            if not _graph_is_valid_search_person_candidate(candidate):
                continue
        candidates.append(candidate)
    primary_people = _graph_unique_strings(candidates)
    if primary_people:
        return primary_people
    query = result.get("query")
    if isinstance(query, dict):
        query_candidates = [
            str(query.get("person_name") or "").strip(),
            str(query.get("name") or "").strip(),
            str(query.get("author") or "").strip(),
        ]
        if tool_name in GRAPH_SEARCH_NORMALIZER_TOOL_NAMES or tool_name == "linkedin_download_html_ocr":
            return _graph_unique_strings(
                [candidate for candidate in query_candidates if _graph_is_valid_search_person_candidate(candidate)]
            )
        return _graph_unique_strings(query_candidates)
    return []


def _graph_is_education_relation(rel_type: str, role_text: str) -> bool:
    normalized = _normalize_graph_name(role_text)
    if rel_type == "STUDIED_AT":
        return True
    return any(token in normalized for token in ("student", "phd", "doctor of philosophy", "master", "bachelor", "mba", "alumn", "graduate", "degree"))


def _supplemental_graph_components_from_result(
    tool_name: str,
    arguments: Dict[str, Any],
    result: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    entities_by_key: Dict[str, Dict[str, Any]] = {}
    relations_by_key: Dict[str, Dict[str, Any]] = {}
    consumed_urls: set[str] = set()
    time_node_sort_keys: Dict[str, str] = {}

    def add_entity(canonical_name: str, entity_type: str, *, alt_names: List[str] | None = None, attributes: List[str] | None = None) -> str | None:
        names = _graph_unique_strings([canonical_name, *(alt_names or [])])
        if not names:
            return None
        attrs = _graph_unique_strings(attributes or [])
        provisional_canonical = _choose_graph_canonical_name(names, entity_type)
        normalized_type = _canonical_graph_entity_type(entity_type, provisional_canonical, attrs)
        canonical = _choose_graph_canonical_name(names, normalized_type)
        merge_keys = _graph_entity_merge_keys(normalized_type, canonical, [name for name in names if _normalize_graph_name(name) != _normalize_graph_name(canonical)], attrs)
        key = merge_keys[0] if merge_keys else f"{normalized_type}:{_normalize_graph_name(canonical)}"
        current = entities_by_key.get(key)
        if current is None:
            current = {
                "canonical_name": canonical,
                "type": normalized_type,
                "alt_names": [],
                "attributes": [],
                "merge_keys": merge_keys,
            }
            entities_by_key[key] = current
        current["alt_names"] = _graph_unique_strings([*(current.get("alt_names") or []), *[name for name in names if _normalize_graph_name(name) != _normalize_graph_name(canonical)]])
        current["attributes"] = _graph_unique_strings([*(current.get("attributes") or []), *attrs])
        current["merge_keys"] = _dedupe_str_list([*(current.get("merge_keys") or []), *merge_keys])
        return canonical

    def add_relation(src: str | None, dst: str | None, rel_type: str, *, canonical_name: str | None = None, alt_names: List[str] | None = None) -> None:
        if not src or not dst:
            return
        src_name = str(src).strip()
        dst_name = str(dst).strip()
        if not src_name or not dst_name:
            return
        key = "|".join([_normalize_graph_name(src_name), _normalize_graph_name(dst_name), _normalize_graph_name(rel_type), _normalize_graph_name(canonical_name or rel_type)])
        if key in relations_by_key:
            return
        relations_by_key[key] = {
            "src": src_name,
            "dst": dst_name,
            "canonical_name": str(canonical_name or rel_type).strip() or rel_type,
            "rel_type": rel_type,
            "alt_names": _graph_unique_strings(alt_names or []),
        }

    def add_time_node(*, date: str = "", start_date: str = "", end_date: str = "") -> str | None:
        canonical, attrs, sort_key = _graph_time_node_parts(date=date, start_date=start_date, end_date=end_date)
        if not canonical:
            return None
        time_node = add_entity(canonical, "TimeNode", attributes=attrs)
        if time_node and sort_key:
            previous = time_node_sort_keys.get(time_node)
            if not previous or sort_key < previous:
                time_node_sort_keys[time_node] = sort_key
        return time_node

    def link_entity_to_time_node(entity_name: str | None, *, date: str = "", start_date: str = "", end_date: str = "") -> str | None:
        if not entity_name:
            return None
        time_node = add_time_node(date=date, start_date=start_date, end_date=end_date)
        if time_node:
            add_relation(entity_name, time_node, "IN_TIME_NODE")
        return time_node

    def add_profile(
        owner_name: str | None,
        url: str | None,
        *,
        platform: str | None = None,
        relation_type: str = "HAS_PROFILE",
        title: str | None = None,
        subject_name: str | None = None,
    ) -> None:
        cleaned = _clean_url_candidate(str(url or ""))
        if not cleaned or _is_graph_noise_url(cleaned):
            return
        consumed_urls.add(cleaned.lower())
        profile_type = "Document" if cleaned.lower().endswith(".pdf") else "Website"
        resource_title = _graph_semantic_resource_title(
            profile_type,
            cleaned,
            owner_name=owner_name,
            subject_name=subject_name,
            platform=platform,
            title=title,
        )
        attrs = [f"url: {cleaned}"]
        if platform:
            attrs.append(f"platform: {platform}")
        if resource_title and resource_title != cleaned:
            attrs.append(f"title: {resource_title}")
        profile_node = add_entity(
            resource_title or cleaned,
            profile_type,
            alt_names=[cleaned] if resource_title and resource_title != cleaned else None,
            attributes=attrs,
        )
        host = (urlparse(cleaned).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host and profile_node:
            add_entity(host, "Domain", attributes=[f"host: {host}"])
            add_relation(profile_node, host, "HAS_DOMAIN")
        if owner_name and profile_node:
            owner_relation_type = "HAS_DOCUMENT" if relation_type == "HAS_PROFILE" and profile_type == "Document" else relation_type
            add_relation(owner_name, profile_node, owner_relation_type)

    def add_image_object(owner_name: str | None, url: str | None, *, label: str | None = None) -> None:
        cleaned = _clean_url_candidate(str(url or ""))
        if not owner_name or not cleaned or _is_graph_noise_url(cleaned):
            return
        consumed_urls.add(cleaned.lower())
        image_name = label or cleaned
        add_entity(
            image_name,
            "ImageObject",
            attributes=[
                f"subject: {owner_name}",
                f"url: {cleaned}",
                "image_type: profile",
            ],
        )
        add_relation(owner_name, image_name, "HAS_IMAGE")
        add_profile(image_name, cleaned, relation_type="HAS_PROFILE", title=f"Image URL for {owner_name}", subject_name=owner_name)

    def ensure_contact_hub(owner_name: str | None) -> str | None:
        if not owner_name:
            return None
        hub_name = f"Contact surface for {owner_name}"
        add_entity(hub_name, "ContactPoint", attributes=[f"subject: {owner_name}", "contact_type: aggregate"])
        add_relation(owner_name, hub_name, "HAS_CONTACT_POINT")
        return hub_name

    def add_contact_point(owner_name: str | None, contact_type: str, value: str | None, *, platform: str | None = None) -> str | None:
        text = str(value or "").strip()
        if not owner_name or not text:
            return None
        contact_hub = ensure_contact_hub(owner_name)
        contact_type_normalized = str(contact_type or "").strip().lower() or "contact"
        if (contact_type_normalized in {"profile", "site", "website", "url"} or text.startswith(("http://", "https://"))) and _is_graph_noise_url(text):
            return None
        if contact_type_normalized in {"profile", "site", "website", "url"} or text.startswith(("http://", "https://")):
            contact_label = f"{platform or contact_type_normalized}: {text}"
        elif platform and contact_type_normalized in {"handle", "username"}:
            contact_label = f"{platform}: {text}"
        else:
            contact_label = text
        add_entity(
            contact_label,
            "ContactPoint",
            attributes=[
                f"subject: {owner_name}",
                f"contact_type: {contact_type_normalized}",
                f"value: {text}",
                *( [f"platform: {platform}"] if platform else [] ),
            ],
        )
        if contact_hub:
            add_relation(contact_hub, contact_label, "HAS_CONTACT_POINT")
        if contact_type_normalized == "email":
            add_entity(text, "Email", attributes=[f"email: {text}"])
            add_relation(contact_label, text, "HAS_EMAIL")
        elif contact_type_normalized == "phone":
            add_entity(text, "Phone", attributes=[f"phone: {text}"])
            add_relation(contact_label, text, "HAS_PHONE")
        elif contact_type_normalized in {"handle", "username"}:
            handle_value = text if text.startswith("@") else f"@{text}"
            add_entity(handle_value, "Handle", attributes=[f"username: {text.lstrip('@')}"])
            add_relation(contact_label, handle_value, "HAS_HANDLE")
        elif contact_type_normalized in {"profile", "site", "website", "url"} or text.startswith(("http://", "https://")):
            semantic_title = _graph_semantic_resource_title(
                "Website",
                text,
                owner_name=contact_label,
                subject_name=owner_name,
                platform=platform,
            )
            add_profile(contact_label, text, platform=platform, title=semantic_title, subject_name=owner_name)
        return contact_label

    def topic_kind_for_relation(relation_type: str, explicit_kind: str = "") -> str:
        if explicit_kind:
            return explicit_kind
        mapping = {
            "HAS_SKILL_TOPIC": "skill",
            "HAS_HOBBY_TOPIC": "hobby",
            "HAS_INTEREST_TOPIC": "interest",
            "RESEARCHES": "research",
            "FOCUSES_ON": "domain",
        }
        return mapping.get(str(relation_type or "").strip().upper(), "")

    def add_topic_bundle(owner_name: str | None, topics: List[str], relation_type: str, *, topic_kind: str = "") -> None:
        resolved_kind = topic_kind_for_relation(relation_type, topic_kind)
        for topic in _graph_unique_strings(topics)[:12]:
            attrs = [f"topic_kind: {resolved_kind}"] if resolved_kind else []
            add_entity(topic, "Topic", attributes=attrs)
            if owner_name:
                add_relation(owner_name, topic, relation_type)

    def add_organization_profile(
        org_name: str,
        *,
        org_url: str | None = None,
        summary: str | None = None,
        focus_values: List[str] | None = None,
        industry: str | None = None,
        why_relevant: str | None = None,
    ) -> str | None:
        if not org_name:
            return None
        if not any([summary, industry, why_relevant, *(focus_values or [])]):
            return None
        profile_name = f"Profile of {org_name}"
        attrs = [f"subject_org: {org_name}"]
        if summary:
            attrs.append(f"summary: {summary}")
        if industry:
            attrs.append(f"industry: {industry}")
        if why_relevant:
            attrs.append(f"why_relevant: {why_relevant}")
        for focus_value in focus_values or []:
            attrs.append(f"focus: {focus_value}")
        canonical = add_entity(profile_name, "OrganizationProfile", attributes=_graph_unique_strings(attrs))
        if canonical:
            add_relation(org_name, canonical, "HAS_ORGANIZATION_PROFILE")
            if org_url:
                add_profile(
                    canonical,
                    org_url,
                    relation_type="HAS_PROFILE",
                    title=f"Official website for {org_name}",
                    subject_name=org_name,
                )
            for focus_value in _graph_unique_strings(focus_values or [])[:8]:
                add_entity(focus_value, "Topic", attributes=["topic_kind: domain"])
                add_relation(canonical, focus_value, "FOCUSES_ON")
            if industry:
                industry_node = add_entity(industry, "Topic", attributes=["topic_kind: industry"])
                if industry_node:
                    add_relation(canonical, industry_node, "FOCUSES_ON")
        return canonical

    def add_organization_context(org_name: str, *, org_url: str | None = None, summary: str | None = None, focus_values: List[str] | None = None, industry: str | None = None, why_relevant: str | None = None, extra_attributes: List[str] | None = None) -> str | None:
        if not org_name:
            return None
        canonical_org_name = _graph_canonical_org_name(org_name)
        attrs: List[str] = []
        if org_url:
            attrs.append(f"url: {org_url}")
        if summary:
            attrs.append(f"summary: {summary}")
        if industry:
            attrs.append(f"industry: {industry}")
        if why_relevant:
            attrs.append(f"why_relevant: {why_relevant}")
        for focus_value in focus_values or []:
            attrs.append(f"focus: {focus_value}")
        attrs.extend(extra_attributes or [])
        canonical = add_entity(
            canonical_org_name,
            _graph_org_type(canonical_org_name),
            alt_names=_graph_unique_strings([org_name, *_graph_org_aliases(canonical_org_name)]),
            attributes=_graph_unique_strings(attrs),
        )
        if canonical and org_url:
            add_profile(
                canonical,
                org_url,
                platform=str(result.get("platform") or "").strip() or None,
                title=f"Official website for {canonical}",
                subject_name=canonical,
            )
        if canonical:
            add_organization_profile(
                canonical,
                org_url=org_url or None,
                summary=summary or None,
                focus_values=focus_values or [],
                industry=industry or None,
                why_relevant=why_relevant or None,
            )
            for focus_value in _graph_unique_strings(focus_values or [])[:8]:
                add_entity(focus_value, "Topic", attributes=["topic_kind: domain"])
                add_relation(canonical, focus_value, "FOCUSES_ON")
        return canonical

    def add_timeline_event(
        owner_name: str | None,
        label: str,
        *,
        date: str = "",
        start_date: str = "",
        end_date: str = "",
        event_type: str = "",
        related_entities: List[str] | None = None,
        mention_sources: List[str] | None = None,
    ) -> str | None:
        if not owner_name or not label:
            return None
        timeline_name = label
        span_label = _graph_date_span_label(start_date, end_date, date)
        if span_label:
            timeline_name = f"{label} ({span_label})"
        attrs = [f"subject: {owner_name}"]
        if date:
            attrs.append(f"date: {date}")
        if start_date:
            attrs.append(f"start_date: {start_date}")
        if end_date:
            attrs.append(f"end_date: {end_date}")
        if event_type:
            attrs.append(f"event_type: {event_type}")
        event_name = add_entity(timeline_name, "TimelineEvent", attributes=attrs)
        if event_name:
            add_relation(owner_name, event_name, "HAS_TIMELINE_EVENT")
            link_entity_to_time_node(event_name, date=date, start_date=start_date, end_date=end_date)
            for related in related_entities or []:
                if related:
                    add_relation(event_name, related, "ABOUT")
            for source in _graph_unique_strings(mention_sources or [])[:6]:
                add_relation(source, event_name, "MENTIONS_TIMELINE_EVENT")
        return event_name

    def add_occupation(owner_name: str | None, occupation_name: str) -> str | None:
        text = str(occupation_name or "").strip()
        if not owner_name or not text:
            return None
        occupation = add_entity(text, "Occupation")
        if occupation:
            add_relation(owner_name, occupation, "HAS_OCCUPATION")
        return occupation

    def add_affiliation_context(owner_name: str | None, org_name: str, *, relation: str = "", org_url: str = "", why_relevant: str = "", summary: str = "", focus_values: List[str] | None = None, start_date: str = "", end_date: str = "") -> str | None:
        if not owner_name or not org_name:
            return None
        org_canonical = add_organization_context(
            org_name,
            org_url=org_url or None,
            summary=summary or None,
            focus_values=focus_values or [],
            why_relevant=why_relevant or None,
        )
        rel_label = relation or "affiliation"
        affiliation_name = f"{rel_label.title()} affiliation with {org_name}"
        attrs = [
            f"subject: {owner_name}",
            f"relation: {rel_label}",
            f"organization: {org_name}",
        ]
        if why_relevant:
            attrs.append(f"why_relevant: {why_relevant}")
        if start_date:
            attrs.append(f"start_date: {start_date}")
        if end_date:
            attrs.append(f"end_date: {end_date}")
        affiliation = add_entity(affiliation_name, "Affiliation", attributes=attrs)
        if affiliation:
            add_relation(owner_name, affiliation, "HAS_AFFILIATION")
            link_entity_to_time_node(affiliation, start_date=start_date, end_date=end_date)
            if org_canonical:
                aff_rel = "MEMBER_OF" if rel_label in {"member", "owner", "maintainer"} else "AFFILIATED_WITH"
                add_relation(affiliation, org_canonical, aff_rel, canonical_name=rel_label or aff_rel)
                add_timeline_event(
                    owner_name,
                    f"Affiliation period at {org_name}",
                    start_date=start_date,
                    end_date=end_date,
                    event_type="affiliation",
                    related_entities=[affiliation, org_canonical],
                )
        return affiliation

    def add_credential_context(owner_name: str | None, degree: str, institution: str, *, field: str = "", start_date: str = "", end_date: str = "", status: str = "", source_url: str = "") -> str | None:
        if not owner_name or not degree or not institution:
            return None
        institution_canonical = add_organization_context(institution, org_url=source_url or None)
        field_text = f" in {field}" if field else ""
        span = _graph_date_span_label(start_date, end_date)
        credential_name = f"{degree}{field_text} from {institution}"
        if span:
            credential_name = f"{credential_name} ({span})"
        attrs = [
            f"subject: {owner_name}",
            f"degree: {degree}",
            f"institution: {institution}",
        ]
        if field:
            attrs.append(f"field: {field}")
        if start_date:
            attrs.append(f"start_date: {start_date}")
        if end_date:
            attrs.append(f"end_date: {end_date}")
        if status:
            attrs.append(f"status: {status}")
        credential = add_entity(credential_name, "EducationalCredential", attributes=attrs)
        if credential:
            add_relation(owner_name, credential, "HAS_CREDENTIAL")
            link_entity_to_time_node(credential, start_date=start_date, end_date=end_date)
            if institution_canonical:
                add_relation(credential, institution_canonical, "ISSUED_BY")
            if field:
                add_entity(field, "Topic", attributes=["topic_kind: research"])
                add_relation(credential, field, "HAS_TOPIC")
            add_timeline_event(
                owner_name,
                f"Education period at {institution}",
                start_date=start_date,
                end_date=end_date,
                event_type="education",
                related_entities=[credential, institution_canonical] if institution_canonical else [credential],
            )
            if source_url:
                add_profile(credential, source_url, relation_type="HAS_DOCUMENT", title=f"Document for {credential}", subject_name=owner_name)
        return credential

    def add_experience_context(owner_name: str | None, role_title: str, org_name: str, *, relation_type: str, start_date: str = "", end_date: str = "", status: str = "", source_url: str = "", summary: str = "", why_relevant: str = "", org_focus: List[str] | None = None, org_summary: str = "", extra_org_attributes: List[str] | None = None) -> str | None:
        if not owner_name or (not role_title and not org_name):
            return None
        org_canonical = add_organization_context(
            org_name,
            org_url=source_url or None,
            summary=org_summary or summary or None,
            focus_values=org_focus or [],
            why_relevant=why_relevant or None,
            extra_attributes=extra_org_attributes or [],
        ) if org_name else None
        role_name = None
        if role_title:
            role_attrs = [f"organization: {org_name}"] if org_name else []
            if start_date:
                role_attrs.append(f"start_date: {start_date}")
            if end_date:
                role_attrs.append(f"end_date: {end_date}")
            if status:
                role_attrs.append(f"status: {status}")
            role_name = add_entity(f"{role_title} at {org_name}" if org_name else role_title, "Role", attributes=role_attrs)
            add_occupation(owner_name, role_title)
        span = _graph_date_span_label(start_date, end_date)
        experience_name = role_title or relation_type.replace("_", " ").title()
        if org_name:
            connector = "at" if relation_type in {"WORKS_AT", "OFFICER_OF", "DIRECTOR_OF", "FOUNDED"} else "with"
            if relation_type == "STUDIED_AT":
                connector = "at"
            experience_name = f"{experience_name} {connector} {org_name}"
        if span:
            experience_name = f"{experience_name} ({span})"
        attrs = [f"subject: {owner_name}"]
        if role_title:
            attrs.append(f"role: {role_title}")
        if org_name:
            attrs.append(f"organization: {org_name}")
        if start_date:
            attrs.append(f"start_date: {start_date}")
        if end_date:
            attrs.append(f"end_date: {end_date}")
        if status:
            attrs.append(f"status: {status}")
        if summary:
            attrs.append(f"summary: {summary}")
        if why_relevant:
            attrs.append(f"why_relevant: {why_relevant}")
        experience = add_entity(experience_name, "Experience", attributes=attrs)
        if experience:
            add_relation(owner_name, experience, "HAS_EXPERIENCE")
            link_entity_to_time_node(experience, start_date=start_date, end_date=end_date)
            if role_name:
                add_relation(experience, role_name, "HAS_ROLE")
                add_relation(owner_name, role_name, "HOLDS_ROLE")
            if org_canonical:
                add_relation(experience, org_canonical, relation_type, canonical_name=role_title or relation_type)
            add_timeline_event(
                owner_name,
                f"Experience period at {org_name}" if org_name else f"Experience period: {role_title or relation_type.replace('_', ' ').title()}",
                start_date=start_date,
                end_date=end_date,
                event_type="experience",
                related_entities=[value for value in [experience, role_name, org_canonical] if value],
            )
            if source_url:
                add_profile(experience, source_url, relation_type="HAS_DOCUMENT", title=f"Document for {experience}", subject_name=owner_name)
        return experience

    def infer_org_name_from_url(url: str) -> str:
        cleaned = _clean_url_candidate(url)
        if not cleaned.startswith(("http://", "https://")):
            return ""
        host = (urlparse(cleaned).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host

    def default_role_title_for_relation(relation_type: str) -> str:
        mapping = {
            "DIRECTOR_OF": "Director",
            "FOUNDED": "Founder",
            "OFFICER_OF": "Officer",
            "STUDIED_AT": "Student",
            "WORKS_AT": "Staff member",
        }
        return mapping.get(relation_type, relation_type.replace("_", " ").title())

    def add_person_role_context(
        person_name: str | None,
        role_title: str,
        org_name: str,
        *,
        relation_type: str | None = None,
        start_date: str = "",
        end_date: str = "",
        status: str = "",
        source_url: str = "",
        summary: str = "",
        why_relevant: str = "",
        org_focus: List[str] | None = None,
        org_summary: str = "",
        extra_org_attributes: List[str] | None = None,
    ) -> None:
        if not person_name or not org_name:
            return
        resolved_relation_type = relation_type or _graph_role_relation_type(role_title)
        normalized_role_title = role_title or default_role_title_for_relation(resolved_relation_type)
        add_entity(person_name, "Person")
        add_experience_context(
            person_name,
            normalized_role_title,
            org_name,
            relation_type=resolved_relation_type,
            start_date=start_date,
            end_date=end_date,
            status=status,
            source_url=source_url,
            summary=summary,
            why_relevant=why_relevant,
            org_focus=org_focus or [],
            org_summary=org_summary,
            extra_org_attributes=extra_org_attributes or [],
        )

    def publication_topics(publication: Dict[str, Any]) -> List[str]:
        if not isinstance(publication, dict):
            return []
        return _graph_unique_strings(
            [
                *_extract_string_list(publication.get("topics"))[:10],
                *_extract_string_list(publication.get("keywords"))[:10],
                *_extract_string_list(publication.get("field_keywords"))[:10],
                *_extract_string_list(publication.get("research_areas"))[:10],
                *_extract_string_list(publication.get("methods_keywords"))[:10],
                *_extract_string_list(publication.get("abstract_keywords"))[:10],
                *_extract_string_list(publication.get("categories"))[:10],
                str(publication.get("topic") or "").strip(),
                str(publication.get("field") or "").strip(),
                str(publication.get("discipline") or "").strip(),
            ]
        )

    primary_people = _graph_primary_people_from_context(tool_name, arguments, result)
    if not primary_people:
        query = result.get("query")
        if isinstance(query, dict):
            primary_people = _graph_primary_people_from_context(
                tool_name,
                arguments,
                {"query": query, **result},
            )
    primary_person = primary_people[0] if primary_people else None
    for person_name in primary_people[:3]:
        add_entity(person_name, "Person")
    timeline_mention_sources: List[str] = []

    def register_timeline_mention_source(node_name: str | None, url: str = "", platform: str = "") -> None:
        if not node_name:
            return
        platform_text = str(platform or "").strip().casefold()
        host = (urlparse(_clean_url_candidate(url)).hostname or "").casefold()
        if host.startswith("www."):
            host = host[4:]
        if platform_text in {"linkedin", "x", "twitter"} or host in {"linkedin.com", "x.com", "twitter.com"}:
            timeline_mention_sources.append(node_name)

    username = str(result.get("username") or arguments.get("username") or "").strip().lstrip("@")
    if username:
        handle_node = add_contact_point(
            primary_person,
            "handle",
            f"@{username}",
            platform=str(result.get("platform") or "").strip() or None,
        )
        register_timeline_mention_source(
            handle_node,
            platform=str(result.get("platform") or "").strip(),
        )

    profile_url = str(
        result.get("profile_url")
        or result.get("profileUrl")
        or arguments.get("profile_url")
        or arguments.get("profile")
        or ""
    ).strip()
    if profile_url and primary_person:
        profile_node = add_contact_point(
            primary_person,
            "profile",
            profile_url,
            platform=str(result.get("platform") or "").strip() or None,
        )
        register_timeline_mention_source(
            profile_node,
            url=profile_url,
            platform=str(result.get("platform") or "").strip(),
        )
    if tool_name == "x_get_user_posts_api" and username and primary_person:
        x_profile_url = f"https://x.com/{username}"
        x_profile_node = add_contact_point(primary_person, "profile", x_profile_url, platform="X")
        register_timeline_mention_source(x_profile_node, url=x_profile_url, platform="X")

    for link in result.get("external_links", []) if isinstance(result.get("external_links"), list) else []:
        if not isinstance(link, dict):
            continue
        link_url = str(link.get("url") or "").strip()
        link_platform = str(link.get("type") or result.get("platform") or "").strip() or None
        link_node = add_contact_point(
            primary_person,
            "profile",
            link_url,
            platform=link_platform,
        )
        register_timeline_mention_source(link_node, url=link_url, platform=str(link_platform or ""))

    for image_key in ("image_url", "avatar_url", "photo_url", "profile_image_url", "image"):
        image_url = str(result.get(image_key) or "").strip()
        if image_url:
            add_image_object(primary_person, image_url, label=f"{primary_person} image" if primary_person else None)

    for signal in result.get("contact_signals", []) if isinstance(result.get("contact_signals"), list) else []:
        if not isinstance(signal, dict):
            continue
        signal_type = str(signal.get("type") or "").strip().lower()
        value = str(signal.get("value") or "").strip()
        if not value:
            continue
        if signal_type == "email":
            add_contact_point(primary_person, "email", value)
        elif signal_type == "phone":
            add_contact_point(primary_person, "phone", value)
        elif signal_type in {"profile", "url", "website", "site"}:
            signal_platform = str(signal.get("platform") or "").strip() or None
            signal_node = add_contact_point(primary_person, "profile", value, platform=signal_platform)
            register_timeline_mention_source(signal_node, url=value, platform=str(signal_platform or ""))
        elif signal_type in {"company", "organization", "institution"}:
            add_affiliation_context(
                primary_person,
                value,
                relation=signal_type,
                why_relevant="public contact signal",
            )
        elif signal_type == "location":
            add_entity(value, "Location")
            if primary_person:
                add_relation(primary_person, value, "LOCATED_IN")

    advisor_names: List[str] = []
    for key in ("advisors", "advisor", "mentors"):
        value = result.get(key)
        if isinstance(value, str):
            advisor_names.extend(
                candidate
                for candidate in _extract_related_people(value, exclude_names=primary_people)[:4]
                if _graph_is_valid_search_person_candidate(candidate)
            )
        elif isinstance(value, list):
            for item in value[:8]:
                if isinstance(item, dict):
                    name = str(item.get("name") or item.get("person_name") or "").strip()
                    if _graph_is_valid_search_person_candidate(name):
                        advisor_names.append(name)
                elif isinstance(item, str) and _graph_is_valid_search_person_candidate(item):
                    advisor_names.append(item.strip())
    for advisor_name in _graph_unique_strings(advisor_names)[:8]:
        if advisor_name.casefold() == str(primary_person or "").casefold():
            continue
        add_entity(advisor_name, "Person")
        if primary_person:
            add_relation(primary_person, advisor_name, "ADVISED_BY", canonical_name="advisor")

    for occupation_value in _graph_unique_strings(
        [
            str(result.get("headline") or "").strip(),
            str(result.get("current_role") or "").strip(),
            str(result.get("title") or "").strip() if isinstance(result.get("title"), str) else "",
        ]
    ):
        if len(occupation_value.split()) <= 8 and not occupation_value.startswith(("http://", "https://")):
            add_occupation(primary_person, occupation_value)

    for org in result.get("organizations", []) if isinstance(result.get("organizations"), list) else []:
        if not isinstance(org, dict):
            continue
        org_name = str(org.get("name") or "").strip()
        org_url = str(org.get("url") or "").strip()
        relation = str(org.get("relation") or "").strip().lower()
        org_summary = str(org.get("summary") or org.get("description") or org.get("about") or "").strip()
        org_industry = str(org.get("industry") or "").strip()
        start_date = str(org.get("start_date") or "").strip()
        end_date = str(org.get("end_date") or "").strip()
        focus_values = _graph_unique_strings(
            [
                *_extract_string_list(org.get("focus"))[:6],
                *_extract_string_list(org.get("topics"))[:6],
                *_extract_string_list(org.get("research_areas"))[:6],
            ]
        )
        if not org_name and org_url.startswith(("http://", "https://")):
            path_parts = [part for part in urlparse(org_url).path.strip("/").split("/") if part]
            if path_parts:
                org_name = path_parts[-1]
        if not org_name:
            continue
        relation_type = _graph_role_relation_type(relation) if relation else ""
        if relation and relation_type != "AFFILIATED_WITH":
            add_person_role_context(
                primary_person,
                relation,
                org_name,
                relation_type=relation_type,
                start_date=start_date,
                end_date=end_date,
                source_url=org_url,
                summary=org_summary,
                why_relevant=f"{relation} relationship",
                org_focus=focus_values,
                org_summary=org_summary,
            )
            if _graph_is_education_relation(relation_type, relation):
                add_credential_context(
                    primary_person,
                    relation,
                    org_name,
                    start_date=start_date,
                    end_date=end_date,
                    source_url=org_url,
                )
        else:
            add_affiliation_context(
                primary_person,
                org_name,
                relation=relation or "affiliation",
                org_url=org_url,
                why_relevant=f"{relation} relationship" if relation else "",
                summary=org_summary,
                focus_values=focus_values,
                start_date=start_date,
                end_date=end_date,
            )
        add_organization_context(
            org_name,
            org_url=org_url or None,
            summary=org_summary or None,
            focus_values=focus_values,
            industry=org_industry or None,
        )

    for repo in result.get("repositories", []) if isinstance(result.get("repositories"), list) else []:
        if not isinstance(repo, dict):
            continue
        repo_name = str(repo.get("name") or "").strip()
        repo_url = _clean_url_candidate(str(repo.get("url") or "").strip())
        repo_attributes = [f"url: {repo_url}"] if repo_url else []
        if repo_name or repo_url:
            repo_id = repo_name or repo_url
            add_entity(repo_id, "Repository", attributes=repo_attributes)
            if primary_person:
                add_relation(primary_person, repo_id, "MAINTAINS")
            if repo_url:
                consumed_urls.add(repo_url.lower())

    add_topic_bundle(
        primary_person,
        _graph_unique_strings(
            [
                *_extract_string_list(result.get("topics"))[:12],
                *_extract_string_list(result.get("research_interests"))[:12],
                *_extract_string_list(result.get("field_keywords"))[:12],
                *_extract_string_list(arguments.get("field_keywords"))[:12],
            ]
        ),
        "RESEARCHES",
        topic_kind="research",
    )
    add_topic_bundle(
        primary_person,
        _graph_unique_strings(
            [
                *_extract_string_list(result.get("skills"))[:12],
                *_extract_string_list(result.get("skill_set"))[:12],
                *_extract_string_list(result.get("technical_skills"))[:12],
            ]
        ),
        "HAS_SKILL_TOPIC",
        topic_kind="skill",
    )
    add_topic_bundle(
        primary_person,
        _graph_unique_strings(
            [
                *_extract_string_list(result.get("hobbies"))[:12],
            ]
        ),
        "HAS_HOBBY_TOPIC",
        topic_kind="hobby",
    )
    add_topic_bundle(
        primary_person,
        _graph_unique_strings(
            [
                *_extract_string_list(result.get("interests"))[:12],
                *_extract_string_list(result.get("personal_interests"))[:12],
                *_extract_string_list(result.get("extracurriculars"))[:12],
            ]
        ),
        "HAS_INTEREST_TOPIC",
        topic_kind="interest",
    )

    publication_records: List[Dict[str, Any]] = []
    for key in ("publications", "records", "papers", "extracted_entries"):
        values = result.get(key)
        if isinstance(values, list):
            publication_records.extend([item for item in values if isinstance(item, dict)])
    metadata = result.get("metadata")
    if isinstance(metadata, dict):
        entries = metadata.get("entries")
        if isinstance(entries, list):
            publication_records.extend([item for item in entries if isinstance(item, dict)])

    for publication in publication_records[:24]:
        title = str(publication.get("title") or publication.get("name") or "").strip()
        arxiv_id = str(publication.get("arxiv_id") or publication.get("id") or "").strip()
        pub_name = title or (f"arXiv:{arxiv_id}" if arxiv_id else "")
        if not pub_name:
            continue
        pub_attributes: List[str] = []
        if arxiv_id:
            pub_attributes.append(f"arxiv_id: {arxiv_id}")
        year = str(publication.get("year") or publication.get("published") or "").strip()
        if year:
            pub_attributes.append(f"year: {year[:4]}")
        venue = str(publication.get("venue") or publication.get("journal") or publication.get("conference") or "").strip()
        if venue:
            pub_attributes.append(f"venue: {venue}")
        pub_url = _clean_url_candidate(str(publication.get("url") or publication.get("pdf_url") or "").strip())
        if pub_url:
            pub_attributes.append(f"url: {pub_url}")
            consumed_urls.add(pub_url.lower())
        add_entity(pub_name, "Publication", attributes=pub_attributes)
        link_entity_to_time_node(pub_name, date=year[:4] if year else "")
        if primary_person:
            add_relation(primary_person, pub_name, "PUBLISHED")
        authors = []
        author_values = publication.get("authors") or publication.get("coauthors") or publication.get("author_names") or []
        if isinstance(author_values, list):
            for author in author_values:
                if isinstance(author, str):
                    authors.append(author.strip())
                elif isinstance(author, dict) and isinstance(author.get("name"), str):
                    authors.append(str(author.get("name")).strip())
        elif isinstance(author_values, str):
            authors.extend([part.strip() for part in re.split(r"\s*,\s*|\s+and\s+", author_values) if part.strip()])
        for author_name in _graph_unique_strings(authors)[:12]:
            add_entity(author_name, "Person")
            add_relation(author_name, pub_name, "PUBLISHED")
            if primary_person and author_name.casefold() != primary_person.casefold():
                add_relation(primary_person, author_name, "COAUTHORED_WITH")
        pub_topics = publication_topics(publication)
        for topic in pub_topics[:12]:
            add_entity(topic, "Topic", attributes=["topic_kind: research"])
            add_relation(pub_name, topic, "HAS_TOPIC")
            if primary_person:
                add_relation(primary_person, topic, "RESEARCHES")
            for author_name in _graph_unique_strings(authors)[:12]:
                add_relation(author_name, topic, "RESEARCHES")
        affiliations = publication.get("affiliations")
        if isinstance(affiliations, list):
            for affiliation in [str(item).strip() for item in affiliations if isinstance(item, str) and str(item).strip()][:8]:
                add_affiliation_context(
                    primary_person,
                    affiliation,
                    relation="publication affiliation",
                    why_relevant=f"publication affiliation for {pub_name}",
                )
        elif isinstance(affiliations, str) and affiliations.strip():
            for affiliation in [part.strip() for part in re.split(r"\s*;\s*|\s*,\s*", affiliations) if part.strip()][:8]:
                add_affiliation_context(
                    primary_person,
                    affiliation,
                    relation="publication affiliation",
                    why_relevant=f"publication affiliation for {pub_name}",
                )
        if venue:
            add_entity(venue, "Conference", attributes=[f"year: {year[:4]}"] if year else [])
            add_relation(pub_name, venue, "PUBLISHED_IN")
        if primary_person and year:
            add_timeline_event(
                primary_person,
                f"Published {pub_name}",
                date=year[:4],
                event_type="publication",
                related_entities=[pub_name, venue] if venue else [pub_name],
            )

    for candidate in result.get("candidates", []) if isinstance(result.get("candidates"), list) else []:
        if not isinstance(candidate, dict):
            continue
        candidate_name = str(candidate.get("canonical_name") or candidate.get("name") or "").strip()
        if not candidate_name:
            continue
        add_entity(candidate_name, "Person")
        if primary_person and candidate_name.casefold() != primary_person.casefold():
            add_relation(primary_person, candidate_name, "RELATED_TO", canonical_name="candidate_match")
        profile_url = str(candidate.get("profile_url") or candidate.get("homepage") or "").strip()
        if profile_url:
            add_profile(candidate_name, profile_url, subject_name=candidate_name)
        affiliations = candidate.get("affiliations")
        if isinstance(affiliations, list):
            for affiliation in [str(item).strip() for item in affiliations if isinstance(item, str) and str(item).strip()][:8]:
                add_affiliation_context(
                    candidate_name,
                    affiliation,
                    relation="candidate affiliation",
                    why_relevant=f"candidate evidence for {candidate_name}",
                )
        add_topic_bundle(
            candidate_name,
            _graph_unique_strings(
                [
                    *_extract_string_list(candidate.get("topics"))[:10],
                    *_extract_string_list(candidate.get("research_interests"))[:10],
                ]
            ),
            "RESEARCHES",
            topic_kind="research",
        )
        add_topic_bundle(
            candidate_name,
            _graph_unique_strings(
                [
                    *_extract_string_list(candidate.get("skills"))[:10],
                    *_extract_string_list(candidate.get("technical_skills"))[:10],
                    *_extract_string_list(candidate.get("skill_set"))[:10],
                ]
            ),
            "HAS_SKILL_TOPIC",
            topic_kind="skill",
        )
        add_topic_bundle(
            candidate_name,
            _graph_unique_strings(
                [
                    *_extract_string_list(candidate.get("hobbies"))[:10],
                ]
            ),
            "HAS_HOBBY_TOPIC",
            topic_kind="hobby",
        )
        add_topic_bundle(
            candidate_name,
            _graph_unique_strings(
                [
                    *_extract_string_list(candidate.get("interests"))[:10],
                    *_extract_string_list(candidate.get("personal_interests"))[:10],
                    *_extract_string_list(candidate.get("extracurriculars"))[:10],
                ]
            ),
            "HAS_INTEREST_TOPIC",
            topic_kind="interest",
        )
        for evidence_item in candidate.get("evidence", []) if isinstance(candidate.get("evidence"), list) else []:
            if not isinstance(evidence_item, dict):
                continue
            title = str(evidence_item.get("title") or "").strip()
            if title:
                add_entity(title, "Publication", attributes=[f"url: {str(evidence_item.get('url') or '').strip()}"] if evidence_item.get("url") else [])
                add_relation(candidate_name, title, "PUBLISHED")

    collaboration = result.get("collaborationGraph")
    if isinstance(collaboration, dict):
        for node in collaboration.get("nodes", []) if isinstance(collaboration.get("nodes"), list) else []:
            if not isinstance(node, dict):
                continue
            node_name = str(node.get("label") or node.get("id") or "").strip()
            node_type = str(node.get("type") or "Unknown").strip()
            if node_name:
                normalized_type = "Publication" if node_type.casefold() == "paper" else ("Conference" if node_type.casefold() == "venue" else node_type)
                add_entity(node_name, normalized_type)
        for edge in collaboration.get("edges", []) if isinstance(collaboration.get("edges"), list) else []:
            if not isinstance(edge, dict):
                continue
            rel = str(edge.get("rel") or "").strip() or "RELATED_TO"
            src = str(edge.get("src") or "").strip()
            dst = str(edge.get("dst") or "").strip()
            add_relation(src, dst, rel)

    for coauthor in result.get("coauthors", []) if isinstance(result.get("coauthors"), list) else []:
        if not isinstance(coauthor, dict):
            continue
        name = str(coauthor.get("name") or "").strip()
        if not name:
            continue
        add_entity(name, "Person")
        if primary_person:
            add_relation(primary_person, name, "COAUTHORED_WITH")
        email = str(coauthor.get("email") or "").strip()
        if email:
            add_contact_point(name, "email", email)

    for contact in result.get("author_contacts", []) if isinstance(result.get("author_contacts"), list) else []:
        if not isinstance(contact, dict):
            continue
        name = str(contact.get("name") or "").strip()
        if not name:
            continue
        add_entity(name, "Person")
        if primary_person and name.casefold() != primary_person.casefold():
            add_relation(primary_person, name, "COAUTHORED_WITH")
        email = str(contact.get("email") or "").strip()
        if email:
            add_contact_point(name, "email", email)

    for venue in result.get("shared_venues", []) if isinstance(result.get("shared_venues"), list) else []:
        venue_name = str(venue.get("venue") or "").strip() if isinstance(venue, dict) else ""
        if not venue_name:
            continue
        add_entity(venue_name, "Conference")
        if primary_person:
            add_relation(primary_person, venue_name, "PUBLISHED_IN")

    award_records: List[Any] = []
    for key in ("awards", "honors", "fellowships"):
        values = result.get(key)
        if isinstance(values, list):
            award_records.extend(values[:12])
    for award in award_records:
        if isinstance(award, str):
            award_name = award.strip()
            award_meta: Dict[str, Any] = {}
        elif isinstance(award, dict):
            award_name = str(award.get("title") or award.get("name") or award.get("award") or "").strip()
            award_meta = award
        else:
            continue
        if not award_name:
            continue
        award_attributes = []
        issuer = str(award_meta.get("issuer") or award_meta.get("organization") or award_meta.get("institution") or "").strip()
        year = str(award_meta.get("year") or award_meta.get("date") or "").strip()
        award_url = _clean_url_candidate(str(award_meta.get("url") or "").strip())
        if issuer:
            award_attributes.append(f"issuer: {issuer}")
        if year:
            award_attributes.append(f"year: {year[:4]}")
        if award_url:
            award_attributes.append(f"url: {award_url}")
        add_entity(award_name, "Award", attributes=award_attributes)
        if primary_person:
            add_relation(primary_person, award_name, "RECEIVED_AWARD")
        if issuer:
            issuer_name = add_organization_context(issuer)
            if issuer_name:
                add_relation(award_name, issuer_name, "AFFILIATED_WITH")
        if award_url:
            add_profile(award_name, award_url, relation_type="HAS_DOCUMENT", title=f"Document for {award_name}", subject_name=award_name)
        if primary_person and year:
            add_timeline_event(
                primary_person,
                f"Received {award_name}",
                date=year[:4],
                event_type="award",
                related_entities=[award_name],
            )

    project_records: List[Any] = []
    for key in ("projects", "frameworks", "initiatives"):
        values = result.get(key)
        if isinstance(values, list):
            project_records.extend(values[:12])
    for project in project_records:
        if isinstance(project, str):
            project_name = project.strip()
            project_meta: Dict[str, Any] = {}
        elif isinstance(project, dict):
            project_name = str(project.get("title") or project.get("name") or project.get("project") or "").strip()
            project_meta = project
        else:
            continue
        if not project_name:
            continue
        project_attributes = []
        project_url = _clean_url_candidate(str(project_meta.get("url") or project_meta.get("source_url") or "").strip())
        if project_url:
            project_attributes.append(f"url: {project_url}")
        add_entity(project_name, "Project", attributes=project_attributes)
        if primary_person:
            add_relation(primary_person, project_name, "RELATED_TO", canonical_name="project_focus")
        for topic in _extract_string_list(project_meta.get("topics"))[:8]:
            add_entity(topic, "Topic", attributes=["topic_kind: domain"])
            add_relation(project_name, topic, "HAS_TOPIC")
        if project_url:
            add_profile(project_name, project_url, title=f"Project page for {project_name}", subject_name=project_name)

    grant_records: List[Any] = []
    for key in ("grants", "nih_records", "nsf_records"):
        values = result.get(key)
        if isinstance(values, list):
            grant_records.extend(values[:12])
    for grant in grant_records:
        if isinstance(grant, str):
            grant_name = grant.strip()
            grant_meta: Dict[str, Any] = {}
        elif isinstance(grant, dict):
            grant_name = str(
                grant.get("title")
                or grant.get("name")
                or grant.get("award_title")
                or grant.get("project_title")
                or grant.get("id")
                or ""
            ).strip()
            grant_meta = grant
        else:
            continue
        if not grant_name:
            continue
        grant_attributes = []
        grant_id = str(grant_meta.get("id") or grant_meta.get("award_id") or "").strip()
        if grant_id:
            grant_attributes.append(f"grant_id: {grant_id}")
        institution = str(grant_meta.get("institution") or grant_meta.get("organization") or grant_meta.get("org_name") or "").strip()
        if institution:
            grant_attributes.append(f"institution: {institution}")
        grant_url = _clean_url_candidate(str(grant_meta.get("url") or grant_meta.get("source_url") or "").strip())
        if grant_url:
            grant_attributes.append(f"url: {grant_url}")
        add_entity(grant_name, "Grant", attributes=grant_attributes)
        if primary_person:
            add_relation(primary_person, grant_name, "HAS_GRANT")
        if institution:
            institution_name = add_organization_context(institution)
            if institution_name:
                add_relation(grant_name, institution_name, "AFFILIATED_WITH")
        if grant_url:
            add_profile(grant_name, grant_url, relation_type="HAS_DOCUMENT", title=f"Document for {grant_name}", subject_name=grant_name)

    patent_records = result.get("patents") if isinstance(result.get("patents"), list) else []
    for patent in patent_records[:12]:
        if isinstance(patent, str):
            patent_name = patent.strip()
            patent_meta: Dict[str, Any] = {}
        elif isinstance(patent, dict):
            patent_name = str(
                patent.get("title")
                or patent.get("name")
                or patent.get("patent_title")
                or patent.get("application_title")
                or patent.get("application_number")
                or ""
            ).strip()
            patent_meta = patent
        else:
            continue
        if not patent_name:
            continue
        patent_attributes = []
        patent_id = str(patent_meta.get("patent_id") or patent_meta.get("application_number") or patent_meta.get("publication_number") or "").strip()
        if patent_id:
            patent_attributes.append(f"patent_id: {patent_id}")
        patent_url = _clean_url_candidate(str(patent_meta.get("url") or patent_meta.get("source_url") or "").strip())
        if patent_url:
            patent_attributes.append(f"url: {patent_url}")
        add_entity(patent_name, "Patent", attributes=patent_attributes)
        if primary_person:
            add_relation(primary_person, patent_name, "HAS_PATENT")
        if patent_url:
            add_profile(patent_name, patent_url, relation_type="HAS_DOCUMENT", title=f"Document for {patent_name}", subject_name=patent_name)

    staff_records = result.get("staff") if isinstance(result.get("staff"), list) else []
    staff_org_name = str(
        result.get("org_name")
        or arguments.get("org_name")
        or arguments.get("organization")
        or arguments.get("institution")
        or arguments.get("company_name")
        or ""
    ).strip() or infer_org_name_from_url(str(result.get("org_url") or arguments.get("org_url") or ""))
    for staff_member in staff_records[:20]:
        if not isinstance(staff_member, dict):
            continue
        staff_name = str(staff_member.get("name") or "").strip()
        staff_title = str(staff_member.get("title") or staff_member.get("role") or "Staff member").strip()
        staff_url = _clean_url_candidate(str(staff_member.get("source_url") or staff_member.get("url") or "").strip())
        staff_summary = str(staff_member.get("summary") or staff_member.get("bio") or "").strip()
        staff_topics = _graph_unique_strings(
            [
                *_extract_string_list(staff_member.get("topics"))[:6],
                *_extract_string_list(staff_member.get("research_areas"))[:6],
            ]
        )
        if not staff_name or not staff_org_name:
            continue
        add_person_role_context(
            staff_name,
            staff_title,
            staff_org_name,
            relation_type=_graph_role_relation_type(staff_title or "staff member"),
            source_url=staff_url,
            summary=staff_summary,
            why_relevant="reported staff or team entry",
            org_focus=staff_topics,
            org_summary=staff_summary,
        )
        if staff_topics:
            add_topic_bundle(staff_name, staff_topics, "RESEARCHES", topic_kind="research")

    role_records: List[Dict[str, Any]] = []
    for key in ("roles", "positions", "experience", "employments", "directorships"):
        values = result.get(key)
        if isinstance(values, list):
            role_records.extend([item for item in values[:20] if isinstance(item, dict)])
    for role in role_records:
        role_title = str(
            role.get("role")
            or role.get("title")
            or role.get("position")
            or role.get("committee_role")
            or ""
        ).strip()
        if not role_title and isinstance(role.get("committee_roles"), list):
            role_title = ", ".join(
                [
                    str(item).strip()
                    for item in role.get("committee_roles")[:4]
                    if isinstance(item, str) and str(item).strip()
                ]
            )
        org_name = str(
            role.get("company_name")
            or role.get("company")
            or role.get("organization")
            or role.get("institution")
            or role.get("employer")
            or role.get("org")
            or ""
        ).strip()
        if not role_title and not org_name:
            continue
        role_relation_type = _graph_role_relation_type(role_title)
        start_date = str(role.get("start_date") or role.get("tenure_start") or "").strip()
        end_date = str(role.get("end_date") or role.get("tenure_end") or "").strip()
        status = str(role.get("status") or "").strip()
        role_url = _clean_url_candidate(str(role.get("source_url") or role.get("url") or "").strip())
        org_summary = str(role.get("summary") or role.get("description") or role.get("about") or "").strip()
        org_focus = _graph_unique_strings(
            [
                *_extract_string_list(role.get("focus"))[:6],
                *_extract_string_list(role.get("topics"))[:6],
                *_extract_string_list(role.get("research_areas"))[:6],
            ]
        )
        extra_org_attributes = []
        for attr_key in ("jurisdiction", "company_number", "cik"):
            value = str(role.get(attr_key) or "").strip()
            if value:
                extra_org_attributes.append(f"{attr_key}: {value}")
        if role_title and org_name:
            add_experience_context(
                primary_person,
                role_title,
                org_name,
                relation_type=role_relation_type,
                start_date=start_date,
                end_date=end_date,
                status=status,
                source_url=role_url,
                summary=org_summary,
                why_relevant="reported role or experience",
                org_focus=org_focus,
                org_summary=org_summary,
                extra_org_attributes=extra_org_attributes,
            )
            if _graph_is_education_relation(role_relation_type, role_title):
                degree_field = str(role.get("field") or role.get("discipline") or "").strip()
                add_credential_context(
                    primary_person,
                    role_title,
                    org_name,
                    field=degree_field,
                    start_date=start_date,
                    end_date=end_date,
                    status=status,
                    source_url=role_url,
                )
        elif org_name:
            add_affiliation_context(
                primary_person,
                org_name,
                relation=role_relation_type.lower(),
                org_url=role_url,
                why_relevant="reported organization linkage",
                summary=org_summary,
                focus_values=org_focus,
                start_date=start_date,
                end_date=end_date,
            )

    education_records: List[Any] = []
    for key in ("education", "degrees", "credentials", "schools", "education_history"):
        values = result.get(key)
        if isinstance(values, list):
            education_records.extend(values[:16])
    for credential in education_records:
        if isinstance(credential, str):
            degree = credential.strip()
            institution = ""
            credential_meta: Dict[str, Any] = {}
        elif isinstance(credential, dict):
            degree = str(
                credential.get("degree")
                or credential.get("title")
                or credential.get("credential")
                or credential.get("program")
                or credential.get("study")
                or ""
            ).strip()
            institution = str(
                credential.get("institution")
                or credential.get("school")
                or credential.get("organization")
                or credential.get("university")
                or ""
            ).strip()
            credential_meta = credential
        else:
            continue
        if not degree and not institution:
            continue
        if not institution and " at " in degree.lower():
            left, _, right = degree.partition(" at ")
            degree = left.strip()
            institution = right.strip()
        if not institution:
            institution = str(arguments.get("institution") or "").strip()
        field = str(credential_meta.get("field") or credential_meta.get("discipline") or credential_meta.get("major") or "").strip()
        start_date = str(credential_meta.get("start_date") or "").strip()
        end_date = str(credential_meta.get("end_date") or credential_meta.get("graduation_date") or credential_meta.get("year") or "").strip()
        status = str(credential_meta.get("status") or "").strip()
        source_url = _clean_url_candidate(str(credential_meta.get("source_url") or credential_meta.get("url") or "").strip())
        if institution:
            add_credential_context(
                primary_person,
                degree or "Education",
                institution,
                field=field,
                start_date=start_date,
                end_date=end_date,
                status=status,
                source_url=source_url,
            )

    timeline_records: List[Any] = []
    for key in ("timeline", "history", "milestones", "events"):
        values = result.get(key)
        if isinstance(values, list):
            timeline_records.extend(values[:20])
    for timeline_item in timeline_records:
        if isinstance(timeline_item, str):
            label = timeline_item.strip()
            timeline_meta: Dict[str, Any] = {}
        elif isinstance(timeline_item, dict):
            label = str(
                timeline_item.get("label")
                or timeline_item.get("title")
                or timeline_item.get("event")
                or timeline_item.get("description")
                or ""
            ).strip()
            timeline_meta = timeline_item
        else:
            continue
        if not label:
            continue
        related_entities: List[str] = []
        related_org = str(timeline_meta.get("organization") or timeline_meta.get("institution") or "").strip()
        if related_org:
            org_canonical = add_organization_context(related_org)
            if org_canonical:
                related_entities.append(org_canonical)
        related_person = str(timeline_meta.get("person") or "").strip()
        if related_person and related_person.casefold() != str(primary_person or "").casefold():
            add_entity(related_person, "Person")
            related_entities.append(related_person)
        add_timeline_event(
            primary_person,
            label,
            date=str(timeline_meta.get("date") or timeline_meta.get("year") or "").strip(),
            start_date=str(timeline_meta.get("start_date") or "").strip(),
            end_date=str(timeline_meta.get("end_date") or "").strip(),
            event_type=str(timeline_meta.get("type") or timeline_meta.get("event_type") or "").strip(),
            related_entities=related_entities,
            mention_sources=timeline_mention_sources if tool_name in {"linkedin_download_html_ocr", "x_get_user_posts_api"} else None,
        )

    if tool_name == "x_get_user_posts_api":
        nested = result.get("result") if isinstance(result.get("result"), dict) else {}
        tweets = nested.get("tweets") if isinstance(nested.get("tweets"), list) else []
        for tweet in tweets[:12]:
            if not isinstance(tweet, dict):
                continue
            post_text = str(
                tweet.get("full_text")
                or tweet.get("text")
                or tweet.get("content")
                or ""
            ).strip()
            created_at = str(
                tweet.get("created_at")
                or tweet.get("date")
                or tweet.get("timestamp")
                or ""
            ).strip()
            if not post_text and not created_at:
                continue
            normalized_text = re.sub(r"\s+", " ", post_text).strip()
            if len(normalized_text) > 96:
                normalized_text = f"{normalized_text[:93].rstrip()}..."
            label = f"X post: {normalized_text}" if normalized_text else "X timeline post"
            add_timeline_event(
                primary_person,
                label,
                date=created_at,
                event_type="social_post",
                mention_sources=timeline_mention_sources,
            )

    company_name = str(result.get("company_name") or arguments.get("company_name") or "").strip()
    company_context_attributes = []
    for attr_key in ("company_number", "jurisdiction", "incorporation_date", "status", "cik", "registered_address"):
        value = str(result.get(attr_key) or "").strip()
        if value:
            company_context_attributes.append(f"{attr_key}: {value}")
    if company_name:
        add_organization_context(
            company_name,
            org_url=str(result.get("source_url") or "").strip() or None,
            summary=str(result.get("summary") or result.get("description") or "").strip() or None,
            focus_values=_extract_string_list(result.get("topics"))[:8],
            industry=str(result.get("industry") or "").strip() or None,
            extra_attributes=company_context_attributes,
        )
        if primary_person and not role_records:
            add_affiliation_context(
                primary_person,
                company_name,
                relation="company linkage",
                org_url=str(result.get("source_url") or "").strip(),
                why_relevant="company returned by current tool",
            )
        company_url = _clean_url_candidate(str(result.get("source_url") or "").strip())
        if company_url:
            add_profile(company_name, company_url, title=f"Official website for {company_name}", subject_name=company_name)

    officer_records = result.get("officers") if isinstance(result.get("officers"), list) else []
    for officer in officer_records[:20]:
        if not isinstance(officer, dict):
            continue
        officer_name = str(officer.get("name") or officer.get("person_name") or "").strip()
        officer_role = str(officer.get("position") or officer.get("role") or "Officer").strip()
        officer_start = str(officer.get("start_date") or officer.get("tenure_start") or "").strip()
        officer_end = str(officer.get("end_date") or officer.get("tenure_end") or "").strip()
        officer_org_name = str(
            officer.get("company_name")
            or officer.get("company")
            or officer.get("organization")
            or company_name
            or arguments.get("company_name")
            or ""
        ).strip()
        if not officer_name or not officer_org_name:
            continue
        add_person_role_context(
            officer_name,
            officer_role,
            officer_org_name,
            relation_type=_graph_role_relation_type(officer_role or "officer"),
            start_date=officer_start,
            end_date=officer_end,
            source_url=_clean_url_candidate(str(result.get("source_url") or officer.get("source_url") or "").strip()),
            why_relevant="reported company officer or management record",
            org_focus=_extract_string_list(result.get("topics"))[:8],
            org_summary=str(result.get("summary") or result.get("description") or "").strip(),
            extra_org_attributes=company_context_attributes,
        )

    overlap_records = result.get("overlaps") if isinstance(result.get("overlaps"), list) else []
    for overlap in overlap_records[:20]:
        if not isinstance(overlap, dict):
            continue
        overlap_name = str(overlap.get("name") or overlap.get("person_name") or "").strip()
        companies = _extract_string_list(overlap.get("companies"))[:8]
        overlap_roles = _extract_string_list(overlap.get("roles"))[:4]
        if not overlap_name:
            continue
        for company in companies:
            role_title = overlap_roles[0] if overlap_roles else "Director"
            add_person_role_context(
                overlap_name,
                role_title,
                company,
                relation_type=_graph_role_relation_type(role_title or "director"),
                why_relevant="reported overlapping board or director membership",
            )

    filing_records = result.get("filings") if isinstance(result.get("filings"), list) else []
    for filing in filing_records[:16]:
        if not isinstance(filing, dict):
            continue
        filing_url = _clean_url_candidate(str(filing.get("document_url") or filing.get("url") or filing.get("source_url") or "").strip())
        filing_title = str(filing.get("description") or filing.get("filing_type") or filing.get("form") or filing_url or "").strip()
        if not filing_title:
            continue
        filing_attributes = []
        filing_type = str(filing.get("filing_type") or filing.get("form") or "").strip()
        filing_date = str(filing.get("filing_date") or filing.get("date") or "").strip()
        if filing_type:
            filing_attributes.append(f"filing_type: {filing_type}")
        if filing_date:
            filing_attributes.append(f"filing_date: {filing_date}")
        if filing_url:
            filing_attributes.append(f"url: {filing_url}")
        add_entity(filing_title, "Document", attributes=filing_attributes)
        if company_name:
            add_relation(company_name, filing_title, "FILED", canonical_name=filing_type or "FILED")
        if filing_url:
            consumed_urls.add(filing_url.lower())

    for affiliation in _extract_arxiv_affiliations(result.get("extracted_entries"))[:10]:
        add_affiliation_context(
            primary_person,
            affiliation,
            relation="arxiv affiliation",
            why_relevant="derived from arXiv author metadata",
        )
    for coauthor in _extract_arxiv_coauthors(result.get("extracted_entries"), exclude_names=primary_people)[:12]:
        add_entity(coauthor, "Person")
        if primary_person:
            add_relation(primary_person, coauthor, "COAUTHORED_WITH")

    for url in _extract_url_candidates(result)[:20]:
        cleaned = _clean_url_candidate(url)
        if not cleaned or cleaned.lower() in consumed_urls or _is_graph_noise_url(cleaned):
            continue
        lower_url = cleaned.lower()
        entity_type = "Document" if lower_url.endswith(".pdf") or any(token in lower_url for token in ("/thesis", "/dissertation", "cv.pdf")) else "Website"
        attributes = [f"url: {cleaned}"]
        if entity_type == "Document" and any(token in lower_url for token in ("thesis", "dissertation")):
            attributes.append("document_type: thesis")
        add_entity(cleaned, entity_type, attributes=attributes)
        host = (urlparse(cleaned).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host:
            add_entity(host, "Domain", attributes=[f"host: {host}"])
            add_relation(cleaned, host, "HAS_DOMAIN")

    ordered_time_nodes = sorted(
        [
            (sort_key, name)
            for name, sort_key in time_node_sort_keys.items()
            if isinstance(name, str) and name.strip() and isinstance(sort_key, str) and sort_key.strip()
        ],
        key=lambda item: (item[0], _normalize_graph_name(item[1])),
    )
    for index in range(len(ordered_time_nodes) - 1):
        current_node = ordered_time_nodes[index][1]
        next_node = ordered_time_nodes[index + 1][1]
        if current_node != next_node:
            add_relation(current_node, next_node, "NEXT_TIME_NODE")

    return list(entities_by_key.values()), list(relations_by_key.values())


def _run_receipt_summarize_worker(
    llm: OpenRouterLLM,
    run_id: str,
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
        parsed = invoke_complete_json(
            llm,
            WORKER_SUMMARIZE_RECEIPT_SYSTEM_PROMPT,
            payload,
            temperature=0.1,
            timeout=_openrouter_worker_timeout(),
            run_id=run_id,
            operation=f"tool_worker.receipt_summary.{tool_name}",
        )
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
            related_people = _extract_related_people_from_search_rows(results, target_name)
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
        contact_info = result.get("contact_info") if isinstance(result.get("contact_info"), dict) else {}
        if contact_info:
            emails = _extract_string_list(contact_info.get("emails"))
            phones = _extract_string_list(contact_info.get("phones"))
            websites = _extract_string_list(contact_info.get("websites"))
            profiles = _extract_string_list(contact_info.get("profiles"))
            overlay_url = str(contact_info.get("overlay_url") or "").strip()
            if emails:
                key_facts.append({"emails": emails[:10]})
            if phones:
                key_facts.append({"phones": phones[:10]})
            if websites:
                key_facts.append({"sourceUrls": websites[:10]})
            if profiles:
                key_facts.append({"profileUrls": profiles[:10]})
            if overlay_url:
                key_facts.append({"contactOverlayUrl": overlay_url})
            if emails or phones or websites or profiles:
                summary = (
                    f"Captured LinkedIn HTML/OCR artifacts for {profile or 'profile'} and extracted contact signals "
                    f"(emails={len(emails)}, phones={len(phones)}, websites={len(websites)}, profiles={len(profiles)})."
                )
            next_hints = _dedupe_str_list(
                ([str(profile)] if profile else [])
                + ([overlay_url] if overlay_url else [])
                + emails[:10]
                + phones[:10]
                + websites[:10]
                + profiles[:10]
            )
        elif profile:
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
        elif tool_name == "tavily_person_search":
            key_facts.append({"requestId": result.get("request_id"), "responseTime": result.get("response_time")})
        rows = result.get("extracted_results")
        if isinstance(rows, list):
            urls: List[str] = []
            for item in rows:
                if not isinstance(item, dict):
                    continue
                raw_url = item.get("url")
                if not isinstance(raw_url, str):
                    continue
                normalized_url = _clean_url_candidate(raw_url)
                if normalized_url.startswith(("http://", "https://")):
                    urls.append(normalized_url)
            urls = _dedupe_str_list(urls)
            combined_text = " ".join(
                str(item.get("title") or "") + " " + str(item.get("extracted_text") or "")
                for item in rows
                if isinstance(item, dict)
            )
            related_people = _extract_related_people_from_search_rows(rows, target_name)
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

    if tool_name in {"extract_webpage", "crawl_webpage", "map_webpage"}:
        if tool_name == "extract_webpage":
            action_label = "Tavily webpage extract"
        elif tool_name == "crawl_webpage":
            action_label = "Tavily site crawl"
        else:
            action_label = "Tavily site map"
        source_url = str(result.get("url") or arguments.get("url") or "").strip()
        summary = f"Ran {action_label} for {source_url or 'the target URL'}."
        key_facts.append({"url": source_url or None, "outputDir": result.get("output_dir")})
        if tool_name == "map_webpage":
            urls = _dedupe_str_list(_extract_string_list(result.get("urls")))
            key_facts.append({"resultsFound": result.get("results_found")})
            if urls:
                key_facts.append({"sourceUrls": urls[:25]})
                next_hints = urls[:25]
                summary = f"Mapped {len(urls)} in-scope URL(s) via Tavily for {source_url or 'the target URL'}."
            return summary, key_facts, vector_upserts, graph_upserts, next_hints

        pages = result.get("extracted_pages")
        page_rows = [item for item in pages if isinstance(item, dict)] if isinstance(pages, list) else []
        page_urls: List[str] = []
        combined_text = ""
        for page in page_rows:
            page_url = _clean_url_candidate(str(page.get("url") or "").strip())
            if page_url.startswith(("http://", "https://")):
                page_urls.append(page_url)
            combined_text += " " + str(page.get("title") or "") + " " + str(page.get("extracted_text") or "")
        page_urls = _dedupe_str_list(page_urls)
        related_people = _extract_related_people_from_search_rows(
            page_rows,
            str(arguments.get("target_name") or "").strip(),
        )
        emails = _extract_strings_from_text(combined_text, kind="email")
        phones = _extract_phone_numbers_from_text(combined_text)
        key_facts.append({"resultsFound": result.get("results_found")})
        if page_urls:
            key_facts.append({"sourceUrls": page_urls[:15]})
        if related_people:
            key_facts.append({"relatedPeople": related_people[:10]})
        if emails:
            key_facts.append({"emails": emails[:10]})
        if phones:
            key_facts.append({"phones": phones[:10]})
        if tool_name == "crawl_webpage":
            next_hints = page_urls[:15]
            summary = f"Crawled {len(page_rows)} page(s) via Tavily for {source_url or 'the target URL'}."
        else:
            summary = f"Extracted {len(page_rows)} page(s) via Tavily for {source_url or 'the target URL'}."
        return summary, key_facts, vector_upserts, graph_upserts, next_hints

    if tool_name in {"arxiv_search_and_download", "arxiv_paper_ingest"}:
        summary = "Queried arXiv and collected paper evidence."
        key_facts.append({"outputDir": result.get("output_dir"), "metadataPath": result.get("metadata_path")})
        nested = result.get("metadata")
        if isinstance(nested, dict):
            for key in ("search_query", "total_available", "collected_count", "downloaded_count"):
                if key in nested:
                    key_facts.append({key: nested.get(key)})
        entries = result.get("extracted_entries")
        if isinstance(entries, list):
            excluded_names = [
                str(arguments.get("author") or "").strip(),
                str(arguments.get("author_hint") or "").strip(),
                str(arguments.get("person_name") or "").strip(),
                str(arguments.get("name") or "").strip(),
            ]
            coauthors = _extract_arxiv_coauthors(entries, exclude_names=excluded_names)
            affiliations = _extract_arxiv_affiliations(entries)
            paper_urls = [
                str(item.get("pdf_url")).strip()
                for item in entries
                if isinstance(item, dict) and isinstance(item.get("pdf_url"), str) and str(item.get("pdf_url")).strip()
            ]
            if tool_name == "arxiv_paper_ingest":
                topics = _extract_string_list(result.get("topics"))
                emails = _extract_string_list(result.get("emails"))
                author_contacts = result.get("author_contacts") if isinstance(result.get("author_contacts"), list) else []
                if topics:
                    key_facts.append({"topics": topics[:10]})
                if emails:
                    key_facts.append({"emails": emails[:10]})
                if author_contacts:
                    key_facts.append({"author_contacts": author_contacts[:10]})
                paper = result.get("paper") if isinstance(result.get("paper"), dict) else {}
                paper_title = str(paper.get("title") or "").strip()
                if paper_title:
                    key_facts.append({"paperTitle": paper_title})
            if coauthors:
                key_facts.append({"coauthors": coauthors[:10]})
            if affiliations:
                key_facts.append({"affiliations": affiliations[:10]})
            if paper_urls:
                key_facts.append({"paperUrls": paper_urls[:10]})
            extra_hints: List[str] = []
            if tool_name == "arxiv_paper_ingest":
                extra_hints.extend(_extract_string_list(result.get("emails"))[:10])
                extra_hints.extend(_extract_string_list(result.get("topics"))[:10])
            next_hints = _dedupe_str_list(coauthors[:10] + affiliations[:10] + paper_urls[:10] + extra_hints[:10])
            if tool_name == "arxiv_paper_ingest":
                summary = (
                    f"Fetched one arXiv paper and extracted {len(coauthors)} co-author pivot(s), "
                    f"{len(_extract_string_list(result.get('emails')))} email signal(s), and "
                    f"{len(_extract_string_list(result.get('topics')))} topic signal(s)."
                )
            else:
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

    for artifact in _extract_artifact_documents(result):
        artifact_id = insert_artifact(
            run_id=run_id,
            tool_name=tool_name,
            kind="artifact_document",
            document_id=artifact.get("documentId"),
            bucket=artifact.get("bucket"),
            object_key=artifact.get("objectKey"),
            version_id=artifact.get("versionId"),
            etag=artifact.get("etag"),
            size_bytes=artifact.get("sizeBytes"),
            content_type=artifact.get("contentType"),
            sha256=artifact.get("sha256"),
        )
        artifact_ids.append(artifact_id)
        if artifact.get("documentId"):
            document_ids.append(str(artifact.get("documentId")))

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

    return artifact_ids, _dedupe_str_list(document_ids), summary_id


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
        for key in ("name", "url", "stars", "updated_at"):
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
            core_members = [
                str(item.get("name")).strip()
                for item in coauthors[:12]
                if isinstance(item, dict) and str(item.get("name") or "").strip()
            ]
            # Minimal clustering signal for downstream reporting. This is intentionally simple:
            # it reflects a "core collaborator set" when we only have a ranked coauthor list.
            if len(core_members) >= 3:
                key_facts.append({"clusters": [{"label": "Core coauthors", "members": core_members[:10], "representative_works": []}]})
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


def _validate_summary_text(tool_name: str, summary_text: str, arguments: Dict[str, Any], result: Dict[str, Any]) -> str:
    present = summary_text.lower()
    payload = {"arguments": arguments, "result": result}
    missing: List[str] = []
    urls = _extract_url_candidates(payload)
    emails = _extract_strings_from_text(json.dumps(payload, ensure_ascii=False, default=str), kind="email")
    years = _extract_year_candidates(payload)
    orgs = _extract_org_like_candidates(payload)

    if urls and not any(url.lower() in present for url in urls[:5]):
        missing.extend(urls[:3])
    if emails and not any(email.lower() in present for email in emails[:3]):
        missing.extend(emails[:2])
    if years and not any(year in summary_text for year in years[:3]):
        missing.extend(years[:2])
    if tool_name in {"institution_directory_search", "orcid_search", "semantic_scholar_search", "dblp_author_search"}:
        if orgs and not any(org.lower() in present for org in orgs[:3]):
            missing.extend(orgs[:2])
    for marker in ("orcid", "doi", "pmid", "arxiv", "openreview", "dblp"):
        if marker in json.dumps(payload, ensure_ascii=False, default=str).lower() and marker not in present:
            missing.append(marker)
    missing = _dedupe_str_list([item for item in missing if item])
    if not missing:
        return summary_text
    output = f"{summary_text} PIVOTS: {'; '.join(missing[:6])}"
    return output[:2500]


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
    source_keys = (
        "url",
        "profile",
        "target",
        "sourceUrl",
        "source_url",
        "profile_url",
        "profileUrl",
        "repo_url",
        "org_url",
        "site_url",
        "filing_url",
        "blog",
        "homepage",
    )
    for key in source_keys:
        value = arguments.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value
    if isinstance(result, dict):
        for key in source_keys:
            value = result.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value

        # Best-effort: some discovery tools return lists of source URLs rather than a single URL.
        for list_key in (
            "sourceUrls",
            "source_urls",
            "paperUrls",
            "paper_urls",
            "urls",
            "url_list",
        ):
            values = result.get(list_key)
            if not isinstance(values, list):
                continue
            for item in values:
                if isinstance(item, str) and item.startswith(("http://", "https://")):
                    return item
                if isinstance(item, dict):
                    candidate = item.get("url") or item.get("sourceUrl") or item.get("pdf_url") or item.get("pdfUrl")
                    if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                        return candidate

        rows = result.get("extracted_results")
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                candidate = row.get("url") or row.get("sourceUrl") or row.get("pdf_url") or row.get("pdfUrl")
                if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                    return candidate

        entries = result.get("extracted_entries")
        if isinstance(entries, list):
            for row in entries:
                if not isinstance(row, dict):
                    continue
                candidate = row.get("pdf_url") or row.get("pdfUrl") or row.get("url") or row.get("sourceUrl")
                if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                    return candidate
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
    for key in ("entityType", "relationCount", "count", "entityCount", "graphSchema"):
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


def _extract_artifact_documents(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(result, dict):
        return []
    rows = result.get("artifactDocuments")
    if not isinstance(rows, list):
        rows = result.get("rawArtifacts")
    if not isinstance(rows, list):
        return []

    artifact_documents: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("documentId") or (row.get("bucket") and row.get("objectKey")):
            artifact_documents.append(row)
    return _dedupe_evidence_refs(artifact_documents)


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
    return _dedupe_str_list([_clean_url_candidate(item) for item in found if _clean_url_candidate(item)])


def _extract_year_candidates(value: Any) -> List[str]:
    found: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, str):
            found.extend(re.findall(r"\b(?:19|20)\d{2}\b", node))
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


def _extract_org_like_candidates(value: Any) -> List[str]:
    blob = json.dumps(value, ensure_ascii=False, default=str)
    pattern = re.compile(
        r"\b(?:[A-Z][A-Za-z&.-]+(?:\s+[A-Z][A-Za-z&.-]+){0,5})\s+(?:University|College|Institute|School|Department|Lab|Laboratory|Company|Inc|LLC|Corp)\b"
    )
    return _dedupe_str_list([item.strip() for item in pattern.findall(blob) if item.strip()])


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


SEARCH_RELATED_PERSON_RELATION_REGEX = re.compile(
    r"\b("
    r"advisor|advisors|advisee|advisees|author|authors|coauthor|coauthors|collaborator|collaborators|"
    r"colleague|colleagues|mentor|mentors|lab member|lab members|team member|team members|"
    r"works with|worked with|member of|director|officer|founder"
    r")\b",
    re.IGNORECASE,
)
SEARCH_RELATED_PERSON_ANCHOR_REGEX = re.compile(
    r"\b("
    r"paper|papers|publication|publications|preprint|thesis|dissertation|university|lab|laboratory|"
    r"institute|department|faculty|research|profile|biography|homepage|cv|curriculum vitae"
    r")\b",
    re.IGNORECASE,
)


def _target_person_aliases(target_name: str) -> List[str]:
    aliases = extract_person_targets(target_name or "")
    cleaned_target = str(target_name or "").strip()
    if cleaned_target:
        aliases.append(cleaned_target)
    return _dedupe_str_list([item for item in aliases if item.strip()])


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


def _search_row_supports_related_person(
    *,
    url: str,
    title: str,
    text: str,
    target_name: str,
) -> bool:
    blob = " ".join(part for part in (title, text) if isinstance(part, str) and part.strip())
    if not blob:
        return False
    target_aliases = _target_person_aliases(target_name)
    same_page_as_target = any(alias.lower() in blob.lower() for alias in target_aliases if alias)
    profileish_url = bool(url and _graph_is_profileish_result_url(url, title))
    explicit_relation = bool(SEARCH_RELATED_PERSON_RELATION_REGEX.search(blob))
    supporting_anchor = bool(SEARCH_RELATED_PERSON_ANCHOR_REGEX.search(blob))
    return explicit_relation or profileish_url or (same_page_as_target and supporting_anchor)


def _extract_related_people_from_search_rows(rows: Any, target_name: str) -> List[str]:
    if not isinstance(rows, list):
        return []
    excluded = _target_person_aliases(target_name)
    candidates: List[str] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        title = str(item.get("title") or "").strip()
        text = str(item.get("extracted_text") or item.get("snippet") or item.get("text") or "").strip()
        if not _search_row_supports_related_person(url=url, title=title, text=text, target_name=target_name):
            continue
        blob = " ".join(part for part in (title, text) if part)
        candidates.extend(_extract_related_people(blob, exclude_names=excluded)[:6])
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
            except (json.JSONDecodeError, TypeError):
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


def _merge_key_fact_lists(base_facts: List[Dict[str, Any]], llm_facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for bucket in (base_facts, llm_facts):
        for fact in bucket:
            if not isinstance(fact, dict) or len(fact) != 1:
                continue
            key = next(iter(fact.keys()))
            value = fact.get(key)
            fingerprint = f"{key}|{json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)}"
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            merged.append(fact)
    return merged


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
