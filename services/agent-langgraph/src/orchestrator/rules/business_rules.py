from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple

from orchestrator.rules.academic_rules import PRIORITY_HIGH, PRIORITY_MEDIUM, add_task_if_new, prune_dedupe_store


BUSINESS_SEARCH_TOOLS = {
    "open_corporates_search",
    "company_officer_search",
    "company_filing_search",
    "sec_person_search",
    "director_disclosure_search",
    "domain_whois_search",
}
TECHNICAL_COMPANY_SOURCE_TOOLS = {
    "github_identity_search",
    "gitlab_identity_search",
    "package_registry_search",
    "npm_author_search",
    "crates_author_search",
}


@dataclass(frozen=True)
class BusinessTask:
    tool_name: str
    payload: Dict[str, Any]
    priority: int
    reason: str
    dedupe_key: str


class ReceiptLike(Protocol):
    tool_name: str
    ok: bool
    summary: str
    key_facts: List[Dict[str, Any]]


def _extract_fact_list(receipt: ReceiptLike, key: str) -> List[Any]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get(key), list):
            return list(fact.get(key) or [])
    return []


def _extract_fact_value(receipt: ReceiptLike, key: str) -> Any:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and key in fact:
            return fact.get(key)
    return None


def _extract_company_candidates(receipt: ReceiptLike) -> List[str]:
    companies: List[str] = []
    if receipt.tool_name in TECHNICAL_COMPANY_SOURCE_TOOLS:
        for item in _extract_fact_list(receipt, "organizations"):
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                if name:
                    companies.append(name.lstrip("@"))
    if receipt.tool_name == "sec_person_search":
        for item in _extract_fact_list(receipt, "companies"):
            if isinstance(item, str) and item.strip():
                companies.append(item.strip())
    if receipt.tool_name == "company_officer_search":
        for item in _extract_fact_list(receipt, "roles"):
            if isinstance(item, dict):
                name = str(item.get("company_name") or "").strip()
                if name:
                    companies.append(name)
    return list(dict.fromkeys(companies))


def derive_business_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ReceiptLike],
    primary_person_targets: List[str],
    iteration: int,
    dedupe_store: Dict[str, int],
) -> Tuple[List[BusinessTask], Dict[str, int], List[str]]:
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks: List[BusinessTask] = []
    notes: List[str] = []
    primary_name = primary_person_targets[:1][0] if primary_person_targets else ""

    for receipt in [item for item in receipts if item.ok]:
        for company_name in _extract_company_candidates(receipt)[:5]:
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="open_corporates_search",
                payload={"runId": run_id, "company_name": company_name, "max_results": 5},
                priority=PRIORITY_MEDIUM,
                reason="Company or org signal detected; resolve stable corporate registry identity.",
            )

        if receipt.tool_name == "open_corporates_search":
            company_name = str(_extract_fact_value(receipt, "companyName") or "").strip()
            company_number = str(_extract_fact_value(receipt, "companyNumber") or "").strip()
            jurisdiction = str(_extract_fact_value(receipt, "jurisdiction") or "").strip().lower()
            registered_address = str(_extract_fact_value(receipt, "registeredAddress") or "").strip()
            if primary_name and company_name:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="company_officer_search",
                    payload={"runId": run_id, "person_name": primary_name, "jurisdiction_code": jurisdiction or "", "max_results": 10},
                    priority=PRIORITY_HIGH,
                    reason="Resolved company; search officer roles for the target person.",
                )
            if company_number and jurisdiction:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="company_filing_search",
                    payload={"runId": run_id, "company_number": company_number, "jurisdiction_code": jurisdiction, "max_results": 10},
                    priority=PRIORITY_HIGH,
                    reason="Resolved company; fetch filing history.",
                )
            if jurisdiction.startswith("us") and (company_name or primary_name):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="sec_person_search",
                    payload={"runId": run_id, "person_name": primary_name, "company_name": company_name, "max_results": 10},
                    priority=PRIORITY_MEDIUM,
                    reason="US company detected; search SEC involvement.",
                )
            if "." in registered_address and " " not in registered_address.split(".")[-1]:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="domain_whois_search",
                    payload={"runId": run_id, "domain": registered_address.split()[-1], "max_results": 5},
                    priority=PRIORITY_MEDIUM,
                    reason="Registered address contains a domain-like signal; resolve RDAP ownership.",
                )
            if company_name:
                notes.append(f"Corporate registry match resolved: {company_name}")

        if receipt.tool_name == "company_officer_search":
            for role in _extract_fact_list(receipt, "roles")[:5]:
                if not isinstance(role, dict):
                    continue
                company_number = str(role.get("company_number") or "").strip()
                jurisdiction = str(role.get("jurisdiction") or "").strip().lower()
                if company_number and jurisdiction:
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="company_filing_search",
                        payload={"runId": run_id, "company_number": company_number, "jurisdiction_code": jurisdiction, "max_results": 10},
                        priority=PRIORITY_MEDIUM,
                        reason="Officer role detected; fetch company filing history.",
                    )
                if jurisdiction.startswith("us"):
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="sec_person_search",
                        payload={"runId": run_id, "person_name": primary_name, "company_name": str(role.get('company_name') or ''), "max_results": 10},
                        priority=PRIORITY_MEDIUM,
                        reason="US officer role detected; search SEC involvement.",
                    )

        if receipt.tool_name == "company_filing_search":
            for filing in _extract_fact_list(receipt, "filings")[:8]:
                if not isinstance(filing, dict):
                    continue
                filing_type = str(filing.get("filing_type") or "").upper()
                document_url = str(filing.get("document_url") or "").strip()
                if "DEF 14A" in filing_type or "PROXY" in filing_type:
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="director_disclosure_search",
                        payload={"runId": run_id, "filing_url": document_url, "company_name": "", "max_results": 5},
                        priority=PRIORITY_MEDIUM,
                        reason="Proxy filing detected; extract director disclosure details.",
                    )

        if receipt.tool_name == "sec_person_search":
            for role in _extract_fact_list(receipt, "roles")[:5]:
                if not isinstance(role, dict):
                    continue
                source_url = str(role.get("source_url") or "").strip()
                if source_url and str(role.get("form") or "").upper() == "DEF 14A":
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="director_disclosure_search",
                        payload={"runId": run_id, "filing_url": source_url, "company_name": str(role.get("company") or ""), "max_results": 5},
                        priority=PRIORITY_MEDIUM,
                        reason="SEC proxy filing detected; extract structured director disclosures.",
                    )

    wrapped = [
        BusinessTask(
            tool_name=task.tool_name,
            payload=task.payload,
            priority=task.priority,
            reason=task.reason,
            dedupe_key=task.dedupe_key,
        )
        for task in tasks
    ]
    return wrapped, dedupe_store, notes
