from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from mcp_client import McpClientProtocol
from openrouter_llm import OpenRouterLLM
from receipt_store import insert_artifact, insert_artifact_summary, insert_run_note, insert_tool_receipt
from run_events import emit_run_event
from system_prompts import SYSTEM_PROMPTS
from logger import get_logger
from env import load_env

logger = get_logger(__name__)


class ToolReceipt(BaseModel):
    run_id: str
    tool_name: str
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
    receipt: ToolReceipt | None


@dataclass
class ToolWorkerResult:
    receipt: ToolReceipt


def build_tool_worker_graph(mcp_client: McpClientProtocol) -> StateGraph:
    graph = StateGraph(ToolWorkerState)

    def execute_tool(state: ToolWorkerState) -> ToolWorkerState:
        arguments = _maybe_refine_arguments(state["tool_name"], state["arguments"])
        arguments = _normalize_tool_arguments(state["tool_name"], arguments)
        emit_run_event(state["run_id"], "TOOL_WORKER_STARTED", {"tool": state["tool_name"]})
        logger.info("Tool worker executing", extra={"tool": state["tool_name"], "run_id": state["run_id"]})
        result = mcp_client.call_tool(state["tool_name"], arguments)
        return {**state, "arguments": arguments, "ok": result.ok, "result": result.content}

    def summarize_and_persist(state: ToolWorkerState) -> ToolWorkerState:
        summary, key_facts, vector_upserts, graph_upserts, next_hints = _summarize_result(
            state["tool_name"], state["arguments"], state.get("result", {}), state.get("ok", False)
        )

        artifact_ids, document_ids, summary_id = _store_artifacts_and_summary(
            state["run_id"],
            state["tool_name"],
            state["arguments"],
            state.get("result", {}),
            summary,
            key_facts,
        )

        receipt = ToolReceipt(
            run_id=state["run_id"],
            tool_name=state["tool_name"],
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
        return {**state, "receipt": receipt}

    graph.add_node("execute_tool", execute_tool)
    graph.add_node("summarize_and_persist", summarize_and_persist)
    graph.set_entry_point("execute_tool")
    graph.add_edge("execute_tool", "summarize_and_persist")
    graph.add_edge("summarize_and_persist", END)

    return graph


def _maybe_refine_arguments(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    load_env()
    system_prompt = SYSTEM_PROMPTS.get(tool_name)
    if not system_prompt:
        return arguments
    if not os.getenv("OPENROUTER_API_KEY"):
        return arguments

    try:
        model = os.getenv("OPENROUTER_TOOL_MODEL") or os.getenv("OPENROUTER_MODEL")
        llm = OpenRouterLLM(model=model)
        return llm.refine_tool_arguments(system_prompt, tool_name, arguments)
    except Exception:
        return arguments


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
        "receipt": None,
    }
    final_state = graph.compile().invoke(state)
    receipt = final_state.get("receipt")
    if receipt is None:
        raise RuntimeError("Tool worker did not return a receipt")
    return ToolWorkerResult(receipt=receipt)


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

    if tool_name == "fetch_url":
        url = arguments.get("url")
        summary = "Fetched URL and stored raw content."
        key_facts.append({"documentId": result.get("documentId"), "url": url})
        key_facts.append({"contentType": result.get("contentType"), "sizeBytes": result.get("sizeBytes")})
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

    if tool_name.startswith("osint_"):
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
        summary_id = insert_artifact_summary(artifact_ids[0], summary, key_facts)

    return artifact_ids, document_ids, summary_id


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
    if receipt.tool_name.startswith("osint_"):
        return f"OSINT run completed: {receipt.tool_name}"
    return receipt.summary


def _citations_from_receipt(receipt: ToolReceipt) -> List[Dict[str, Any]]:
    citations: List[Dict[str, Any]] = []
    for doc_id in receipt.document_ids:
        citations.append({"documentId": doc_id})
    return citations


def _extract_evidence(result: Dict[str, Any]) -> Dict[str, Any] | None:
    evidence = result.get("evidence") if isinstance(result, dict) else None
    if not isinstance(evidence, dict):
        return None
    if evidence.get("bucket") and evidence.get("objectKey"):
        return evidence
    if evidence.get("documentId"):
        return evidence
    return None


def _normalize_tool_arguments(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(arguments)

    if tool_name == "ingest_text":
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

    return normalized


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
