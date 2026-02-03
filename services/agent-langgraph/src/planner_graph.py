from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, TypedDict

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from mcp_client import StdioMcpClient
from openrouter_llm import OpenRouterLLM
from run_events import emit_run_event

URL_REGEX = re.compile(r"https?://[^\s\]]+")


class ToolPlanItem(BaseModel):
    tool: str
    arguments: Dict[str, Any]
    rationale: str


class PlannerState(TypedDict):
    run_id: str
    prompt: str
    inputs: List[str]
    seed_urls: List[str]
    tool_plan: List[ToolPlanItem]
    rationale: str
    documents_created: List[str]
    tool_results: List[Dict[str, Any]]
    iteration: int
    max_iterations: int
    done: bool
    enough_info: bool
    noteboard: List[str]


@dataclass
class PlannerResult:
    run_id: str
    tool_plan: List[ToolPlanItem]
    documents_created: List[str]
    rationale: str
    tool_results: List[Dict[str, Any]]
    iterations: int
    noteboard: List[str]


def build_planner_graph(mcp_client: StdioMcpClient, llm: OpenRouterLLM | None = None) -> StateGraph:
    graph = StateGraph(PlannerState)

    def analyze_input(state: PlannerState) -> PlannerState:
        prompt_urls = _extract_urls(state.get("prompt", ""))
        input_urls: List[str] = []
        for item in state.get("inputs", []):
            input_urls.extend(_extract_urls(item))

        seed_urls = _dedupe(prompt_urls + input_urls)
        return {
            **state,
            "seed_urls": seed_urls,
            "iteration": 0,
            "done": False,
        }

    def plan_tools(state: PlannerState) -> PlannerState:
        seed_urls = list(state.get("seed_urls", []))
        rationale = ""
        enough_info = False

        if llm is not None:
            tool_catalog = [
                {
                    "name": "fetch_url",
                    "description": "Fetch a URL via HTTP GET and store raw response",
                    "args": {"runId": "uuid", "url": "string"},
                }
            ]
            try:
                prompt = _inject_noteboard(state.get("prompt", ""), state.get("noteboard", []))
                result = llm.plan_tools(prompt, state.get("inputs", []), tool_catalog)
                rationale = result.get("rationale", "")
                enough_info = bool(result.get("enough_info", False))
                llm_urls = [url for url in result.get("urls", []) if isinstance(url, str)]
                seed_urls = _dedupe(seed_urls + llm_urls)
            except Exception:
                rationale = "LLM planning failed, using heuristic URL extraction."

        plan: List[ToolPlanItem] = []
        for url in seed_urls:
            plan.append(
                ToolPlanItem(
                    tool="fetch_url",
                    arguments={"runId": state["run_id"], "url": url},
                    rationale=f"Fetch seed URL for evidence collection: {url}",
                )
            )

        return {**state, "tool_plan": plan, "rationale": rationale, "enough_info": enough_info}

    def explain_plan(state: PlannerState) -> PlannerState:
        rationale = state.get("rationale") or (
            "No URLs found in input. Planner will wait for more seeds."
            if not state.get("tool_plan")
            else "\n".join([item.rationale for item in state["tool_plan"]])
        )
        emit_run_event(state["run_id"], "TOOLS_SELECTED", {"rationale": rationale, "tools": [item.model_dump() for item in state.get("tool_plan", [])]})
        return {**state, "rationale": rationale}

    def execute_tools(state: PlannerState) -> PlannerState:
        results: List[Dict[str, Any]] = []
        documents_created = list(state.get("documents_created", []))
        noteboard = list(state.get("noteboard", []))

        for item in state.get("tool_plan", []):
            result = mcp_client.call_tool(item.tool, item.arguments)
            payload = {"tool": item.tool, "arguments": item.arguments, "ok": result.ok, "result": result.content}
            results.append(payload)
            if result.ok:
                document_id = result.content.get("documentId")
                if document_id:
                    documents_created.append(document_id)
                note = _format_tool_note(item.tool, result.content)
                if note:
                    noteboard.append(note)

        noteboard = _trim_noteboard(noteboard)
        emit_run_event(
            state["run_id"],
            "NOTEBOARD_UPDATED",
            {"notes": noteboard},
        )
        return {
            **state,
            "tool_results": results,
            "documents_created": documents_created,
            "noteboard": noteboard,
        }

    def decide_stop_or_refine(state: PlannerState) -> PlannerState:
        iteration = state.get("iteration", 0) + 1
        done = (
            iteration >= state.get("max_iterations", 1)
            or not state.get("tool_plan")
            or state.get("enough_info", False)
        )
        return {**state, "iteration": iteration, "done": done}

    def should_continue(state: PlannerState) -> str:
        return END if state.get("done") else "plan_tools"

    graph.add_node("analyze_input", analyze_input)
    graph.add_node("plan_tools", plan_tools)
    graph.add_node("explain_plan", explain_plan)
    graph.add_node("execute_tools", execute_tools)
    graph.add_node("decide_stop_or_refine", decide_stop_or_refine)

    graph.set_entry_point("analyze_input")
    graph.add_edge("analyze_input", "plan_tools")
    graph.add_edge("plan_tools", "explain_plan")
    graph.add_edge("explain_plan", "execute_tools")
    graph.add_edge("execute_tools", "decide_stop_or_refine")
    graph.add_conditional_edges("decide_stop_or_refine", should_continue)

    return graph


def run_planner(
    run_id: str,
    prompt: str,
    inputs: List[str] | None = None,
    max_iterations: int = 1,
) -> PlannerResult:
    emit_run_event(run_id, "PLANNER_STARTED", {})
    llm: OpenRouterLLM | None = None
    if os.getenv("OPENROUTER_API_KEY"):
        llm = OpenRouterLLM()

    mcp_client = StdioMcpClient()
    mcp_client.start()

    try:
        graph = build_planner_graph(mcp_client, llm)
        state: PlannerState = {
            "run_id": run_id,
            "prompt": prompt,
            "inputs": inputs or [],
            "seed_urls": [],
            "tool_plan": [],
            "rationale": "",
            "documents_created": [],
            "tool_results": [],
            "iteration": 0,
            "max_iterations": max_iterations,
            "done": False,
            "enough_info": False,
            "noteboard": [],
        }

        final_state = graph.compile().invoke(state)
        return PlannerResult(
            run_id=run_id,
            tool_plan=final_state.get("tool_plan", []),
            documents_created=final_state.get("documents_created", []),
            rationale=final_state.get("rationale", ""),
            tool_results=final_state.get("tool_results", []),
            iterations=final_state.get("iteration", 0),
            noteboard=final_state.get("noteboard", []),
        )
    finally:
        mcp_client.close()


def _extract_urls(text: str) -> List[str]:
    return URL_REGEX.findall(text or "")


def _dedupe(items: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in items:
        normalized = item.strip().rstrip(".,)")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _format_tool_note(tool_name: str, result: Dict[str, Any]) -> str | None:
    if tool_name == "fetch_url":
        document_id = result.get("documentId")
        source_type = result.get("sourceType")
        url = result.get("url") or result.get("sourceUrl")
        if document_id:
            return f"Fetched {source_type or 'content'} from {url or 'url'} → document {document_id}"
    if tool_name == "ingest_text":
        document_id = result.get("documentId")
        chunk_count = result.get("chunkCount")
        if document_id:
            return f"Ingested text → document {document_id} ({chunk_count} chunks)"
    return None


def _trim_noteboard(notes: List[str], max_items: int = 20) -> List[str]:
    if len(notes) <= max_items:
        return notes
    return notes[-max_items:]


def _inject_noteboard(prompt: str, notes: List[str]) -> str:
    if not notes:
        return prompt
    summary = "\n".join(f"- {note}" for note in notes)
    return f"{prompt}\n\nNoteboard (key findings so far):\n{summary}"
