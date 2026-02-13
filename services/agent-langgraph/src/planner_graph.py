from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, TypedDict

import psycopg
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from mcp_client import McpClientProtocol, RoutedMcpClient
from openrouter_llm import OpenRouterLLM
from run_events import emit_run_event
from system_prompts import WORK_PLANNER_SYSTEM_PROMPT
from tool_worker_graph import ToolReceipt, run_tool_worker
from logger import get_logger
from env import load_env

logger = get_logger(__name__)

URL_REGEX = re.compile(r"https?://[^\s\]]+")
EMAIL_REGEX = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
DOMAIN_REGEX = re.compile(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}\b", re.IGNORECASE)
USERNAME_REGEX = re.compile(r"(?<!\w)@([A-Za-z0-9_]{3,32})")
PHONE_REGEX = re.compile(r"(?:\+\d{1,3}[\s-]?)?(?:\(?\d{3}\)?[\s-]?)\d{3}[\s-]?\d{4}")
IPV4_REGEX = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b")


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
    tool_receipts: List[ToolReceipt]
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
    tool_receipts: List[ToolReceipt]
    iterations: int
    noteboard: List[str]


def build_planner_graph(mcp_client: McpClientProtocol, llm: OpenRouterLLM | None = None) -> StateGraph:
    graph = StateGraph(PlannerState)

    def analyze_input(state: PlannerState) -> PlannerState:
        prompt_urls = _extract_urls(state.get("prompt", ""))
        input_urls: List[str] = []
        for item in state.get("inputs", []):
            input_urls.extend(_extract_urls(item))

        seed_urls = _dedupe(prompt_urls + input_urls)
        logger.info("Planner input analyzed", extra={"seed_urls": seed_urls})
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
                },
                {
                    "name": "osint_sherlock_username",
                    "description": "Username enumeration across public social platforms",
                    "args": {"runId": "uuid", "username": "string"},
                },
                {
                    "name": "osint_maigret_username",
                    "description": "Deep username profiling and metadata extraction",
                    "args": {"runId": "uuid", "username": "string"},
                },
                {
                    "name": "osint_whatsmyname_username",
                    "description": "Username checks against WhatsMyName site list",
                    "args": {"runId": "uuid", "username": "string", "maxSites": "int"},
                },
                {
                    "name": "osint_holehe_email",
                    "description": "Check account presence for an email across services",
                    "args": {"runId": "uuid", "email": "string"},
                },
                # Disabled by default (requires API key).
                # Get HIBP key: https://haveibeenpwned.com/API/Key
                # {
                #     "name": "osint_hibp_email",
                #     "description": "Check email breaches via Have I Been Pwned API",
                #     "args": {"runId": "uuid", "email": "string"},
                # },
                {
                    "name": "osint_theharvester_email_domain",
                    "description": "Collect emails/hosts from a domain",
                    "args": {"runId": "uuid", "domain": "string", "source": "string", "limit": "int"},
                },
                {
                    "name": "osint_reconng_domain",
                    "description": "Run Recon-ng module against a domain source",
                    "args": {"runId": "uuid", "domain": "string", "module": "string"},
                },
                {
                    "name": "osint_spiderfoot_scan",
                    "description": "Run SpiderFoot scan for broad asset and identity pivots",
                    "args": {"runId": "uuid", "target": "string", "modules": "string|array"},
                },
                {
                    "name": "osint_amass_domain",
                    "description": "Passive domain intelligence and subdomain expansion",
                    "args": {"runId": "uuid", "domain": "string", "passive": "boolean"},
                },
                {
                    "name": "osint_sublist3r_domain",
                    "description": "Subdomain enumeration for a domain",
                    "args": {"runId": "uuid", "domain": "string"},
                },
                {
                    "name": "osint_dnsdumpster_domain",
                    "description": "Passive DNS footprint expansion via DNSDumpster",
                    "args": {"runId": "uuid", "domain": "string"},
                },
                {
                    "name": "osint_maltego_manual",
                    "description": "Manual Maltego workflow placeholder for relationship mapping",
                    "args": {"runId": "uuid"},
                },
                {
                    "name": "osint_foca_manual",
                    "description": "Manual FOCA workflow placeholder for document metadata",
                    "args": {"runId": "uuid"},
                },
                # Disabled by default (requires API key).
                # Get Shodan key: https://account.shodan.io/
                # {
                #     "name": "osint_shodan_host",
                #     "description": "Host exposure lookup via Shodan API",
                #     "args": {"runId": "uuid", "host": "string"},
                # },
                {
                    "name": "osint_whatweb_target",
                    "description": "Web technology fingerprinting",
                    "args": {"runId": "uuid", "target": "string"},
                },
                {
                    "name": "osint_phoneinfoga_number",
                    "description": "Phone number metadata intelligence",
                    "args": {"runId": "uuid", "number": "string"},
                },
                {
                    "name": "osint_exiftool_extract",
                    "description": "Metadata extraction from a local file",
                    "args": {"runId": "uuid", "path": "string"},
                },
            ]
            try:
                prompt = _inject_noteboard(state.get("prompt", ""), state.get("noteboard", []))
                result = llm.plan_tools(
                    prompt,
                    state.get("inputs", []),
                    tool_catalog,
                    system_prompt=WORK_PLANNER_SYSTEM_PROMPT,
                )
                rationale = result.get("rationale", "")
                enough_info = bool(result.get("enough_info", False))
                llm_urls = [url for url in result.get("urls", []) if isinstance(url, str)]
                seed_urls = _dedupe(seed_urls + llm_urls)
            except Exception as exc:
                logger.error("Planner LLM failed", extra={"error": str(exc)})
                rationale = "LLM planning failed, using heuristic URL extraction."

        plan: List[ToolPlanItem] = []
        emails = _extract_emails_from_state(state)
        domains = _extract_domains_from_state(state)
        usernames = _extract_usernames_from_state(state)
        phones = _extract_phone_numbers_from_state(state)
        ipv4_hosts = _extract_ipv4_from_state(state)

        for url in seed_urls:
            plan.append(
                ToolPlanItem(
                    tool="fetch_url",
                    arguments={"runId": state["run_id"], "url": url},
                    rationale=f"Fetch seed URL for evidence collection: {url}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_whatweb_target",
                    arguments={"runId": state["run_id"], "target": url},
                    rationale=f"Fingerprint web technologies for target: {url}",
                )
            )

        for email in emails:
            plan.append(
                ToolPlanItem(
                    tool="osint_holehe_email",
                    arguments={"runId": state["run_id"], "email": email},
                    rationale=f"Pivot on email account registrations: {email}",
                )
            )
            # Disabled by default (requires API key).
            # Get HIBP key: https://haveibeenpwned.com/API/Key
            # plan.append(
            #     ToolPlanItem(
            #         tool="osint_hibp_email",
            #         arguments={"runId": state["run_id"], "email": email},
            #         rationale=f"Check email breach exposure: {email}",
            #     )
            # )

        for domain in domains:
            plan.append(
                ToolPlanItem(
                    tool="osint_theharvester_email_domain",
                    arguments={"runId": state["run_id"], "domain": domain, "source": "all", "limit": 100},
                    rationale=f"Collect domain intel via theHarvester: {domain}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_reconng_domain",
                    arguments={"runId": state["run_id"], "domain": domain},
                    rationale=f"Run Recon-ng domain module: {domain}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_spiderfoot_scan",
                    arguments={"runId": state["run_id"], "target": domain},
                    rationale=f"Run broad SpiderFoot scan for target: {domain}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_amass_domain",
                    arguments={"runId": state["run_id"], "domain": domain, "passive": True},
                    rationale=f"Expand passive subdomain footprint: {domain}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_sublist3r_domain",
                    arguments={"runId": state["run_id"], "domain": domain},
                    rationale=f"Enumerate additional subdomains: {domain}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_dnsdumpster_domain",
                    arguments={"runId": state["run_id"], "domain": domain},
                    rationale=f"Map DNS footprint via DNSDumpster: {domain}",
                )
            )

        for username in usernames:
            plan.append(
                ToolPlanItem(
                    tool="osint_sherlock_username",
                    arguments={"runId": state["run_id"], "username": username},
                    rationale=f"Enumerate username presence across platforms: {username}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_maigret_username",
                    arguments={"runId": state["run_id"], "username": username},
                    rationale=f"Profile username metadata with Maigret: {username}",
                )
            )
            plan.append(
                ToolPlanItem(
                    tool="osint_whatsmyname_username",
                    arguments={"runId": state["run_id"], "username": username},
                    rationale=f"Verify username manually across known patterns: {username}",
                )
            )

        for phone in phones:
            plan.append(
                ToolPlanItem(
                    tool="osint_phoneinfoga_number",
                    arguments={"runId": state["run_id"], "number": phone},
                    rationale=f"Collect phone intelligence metadata: {phone}",
                )
            )

        # Disabled by default (requires API key).
        # Get Shodan key: https://account.shodan.io/
        # for host in ipv4_hosts:
        #     plan.append(
        #         ToolPlanItem(
        #             tool="osint_shodan_host",
        #             arguments={"runId": state["run_id"], "host": host},
        #             rationale=f"Check exposed services for host: {host}",
        #         )
        #     )

        plan = _dedupe_tool_plan(plan)

        logger.info("Planner tool plan created", extra={"count": len(plan)})
        return {**state, "tool_plan": plan, "rationale": rationale, "enough_info": enough_info}

    def explain_plan(state: PlannerState) -> PlannerState:
        rationale = state.get("rationale") or (
            "No URLs found in input. Planner will wait for more seeds."
            if not state.get("tool_plan")
            else "\n".join([item.rationale for item in state["tool_plan"]])
        )
        emit_run_event(state["run_id"], "TOOLS_SELECTED", {"rationale": rationale, "tools": [item.model_dump() for item in state.get("tool_plan", [])]})
        logger.info("Planner plan explained", extra={"tool_count": len(state.get("tool_plan", []))})
        return {**state, "rationale": rationale}

    def execute_tools(state: PlannerState) -> PlannerState:
        receipts: List[ToolReceipt] = []
        documents_created = list(state.get("documents_created", []))
        noteboard = list(state.get("noteboard", []))

        for item in state.get("tool_plan", []):
            worker_result = run_tool_worker(mcp_client, state["run_id"], item.tool, item.arguments)
            receipt = worker_result.receipt
            receipts.append(receipt)
            for document_id in receipt.document_ids:
                if document_id:
                    documents_created.append(document_id)
            note = _format_receipt_note(receipt)
            if note:
                noteboard.append(note)
            logger.info("Planner executed tool", extra={"tool": item.tool, "ok": receipt.ok})

        noteboard = _trim_noteboard(noteboard)
        emit_run_event(
            state["run_id"],
            "NOTEBOARD_UPDATED",
            {"notes": noteboard},
        )
        logger.info("Planner noteboard updated", extra={"note_count": len(noteboard)})
        return {
            **state,
            "tool_receipts": receipts,
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
    load_env()
    emit_run_event(run_id, "PLANNER_STARTED", {})
    llm: OpenRouterLLM | None = None
    if os.getenv("OPENROUTER_API_KEY"):
        planner_model = os.getenv("OPENROUTER_PLANNER_MODEL") or os.getenv("OPENROUTER_MODEL")
        llm = OpenRouterLLM(model=planner_model)

    run_title = _derive_run_title(prompt, inputs or [], llm)
    _persist_run_title(run_id, run_title)
    emit_run_event(run_id, "RUN_TITLE_SET", {"title": run_title})

    mcp_client = RoutedMcpClient()
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
            "tool_receipts": [],
            "iteration": 0,
            "max_iterations": max_iterations,
            "done": False,
            "enough_info": False,
            "noteboard": [],
        }

        final_state = graph.compile().invoke(state)
        logger.info("Planner run complete", extra={"run_id": run_id, "iterations": final_state.get("iteration", 0)})
        return PlannerResult(
            run_id=run_id,
            tool_plan=final_state.get("tool_plan", []),
            documents_created=final_state.get("documents_created", []),
            rationale=final_state.get("rationale", ""),
            tool_receipts=final_state.get("tool_receipts", []),
            iterations=final_state.get("iteration", 0),
            noteboard=final_state.get("noteboard", []),
        )
    finally:
        mcp_client.close()


def _extract_urls(text: str) -> List[str]:
    return URL_REGEX.findall(text or "")


def _extract_emails(text: str) -> List[str]:
    return EMAIL_REGEX.findall(text or "")


def _extract_domains(text: str) -> List[str]:
    return DOMAIN_REGEX.findall(text or "")


def _extract_usernames(text: str) -> List[str]:
    return USERNAME_REGEX.findall(text or "")


def _extract_phone_numbers(text: str) -> List[str]:
    raw = PHONE_REGEX.findall(text or "")
    numbers: List[str] = []
    for item in raw:
        normalized = item.strip()
        if normalized:
            numbers.append(normalized)
    return numbers


def _extract_ipv4(text: str) -> List[str]:
    return IPV4_REGEX.findall(text or "")


def _extract_domains_from_state(state: PlannerState) -> List[str]:
    domains: List[str] = []
    combined = [state.get("prompt", "")] + list(state.get("inputs", []))
    for item in combined:
        domains.extend(_extract_domains(item))
        for url in _extract_urls(item):
            host = _domain_from_url(url)
            if host:
                domains.append(host)
    return _dedupe(domains)


def _extract_emails_from_state(state: PlannerState) -> List[str]:
    emails: List[str] = []
    combined = [state.get("prompt", "")] + list(state.get("inputs", []))
    for item in combined:
        emails.extend(_extract_emails(item))
    return _dedupe(emails)


def _extract_usernames_from_state(state: PlannerState) -> List[str]:
    usernames: List[str] = []
    combined = [state.get("prompt", "")] + list(state.get("inputs", []))
    for item in combined:
        usernames.extend(_extract_usernames(item))
    return _dedupe(usernames)


def _extract_phone_numbers_from_state(state: PlannerState) -> List[str]:
    numbers: List[str] = []
    combined = [state.get("prompt", "")] + list(state.get("inputs", []))
    for item in combined:
        numbers.extend(_extract_phone_numbers(item))
    return _dedupe(numbers)


def _extract_ipv4_from_state(state: PlannerState) -> List[str]:
    hosts: List[str] = []
    combined = [state.get("prompt", "")] + list(state.get("inputs", []))
    for item in combined:
        hosts.extend(_extract_ipv4(item))
    return _dedupe(hosts)


def _domain_from_url(url: str) -> str | None:
    match = re.match(r"^https?://([^/:?#]+)", url.strip(), re.IGNORECASE)
    if not match:
        return None
    host = match.group(1).lower().strip()
    if host.startswith("www."):
        return host[4:]
    return host


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


def _dedupe_tool_plan(plan: List[ToolPlanItem]) -> List[ToolPlanItem]:
    seen: set[str] = set()
    deduped: List[ToolPlanItem] = []
    for item in plan:
        key = f"{item.tool}|{json_like(item.arguments)}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def json_like(value: Dict[str, Any]) -> str:
    try:
        return str(sorted(value.items()))
    except Exception:
        return str(value)


def _format_receipt_note(receipt: ToolReceipt) -> str | None:
    if not receipt.ok:
        return None
    if receipt.tool_name == "fetch_url" and receipt.document_ids:
        return f"Fetched content → document {receipt.document_ids[0]}"
    if receipt.tool_name == "ingest_text" and receipt.document_ids:
        chunk_count = None
        for fact in receipt.key_facts:
            if "chunkCount" in fact:
                chunk_count = fact.get("chunkCount")
        if chunk_count is not None:
            return f"Ingested text → document {receipt.document_ids[0]} ({chunk_count} chunks)"
        return f"Ingested text → document {receipt.document_ids[0]}"
    if receipt.tool_name == "ingest_graph_entity":
        return "Ingested graph entity"
    return receipt.summary


def _trim_noteboard(notes: List[str], max_items: int = 20) -> List[str]:
    if len(notes) <= max_items:
        return notes
    return notes[-max_items:]


def _inject_noteboard(prompt: str, notes: List[str]) -> str:
    if not notes:
        return prompt
    summary = "\n".join(f"- {note}" for note in notes)
    return f"{prompt}\n\nNoteboard (key findings so far):\n{summary}"


def _derive_run_title(prompt: str, inputs: List[str], llm: OpenRouterLLM | None) -> str:
    if llm is not None:
        try:
            title = llm.generate_run_title(prompt, inputs)
            if title:
                return title
        except Exception as exc:
            logger.warning("Run title generation failed", extra={"error": str(exc)})

    normalized = " ".join((prompt or "").strip().split())
    if not normalized:
        return "Untitled investigation"
    return normalized[:160]


def _persist_run_title(run_id: str, title: str) -> None:
    dsn = os.getenv("DATABASE_URL", "postgresql://osint:osint@postgres:5432/osint")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE runs
                SET title = %s
                WHERE run_id = %s
                  AND (title IS NULL OR btrim(title) = '')
                """,
                (title, run_id),
            )
