from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple

from orchestrator.rules.academic_rules import PRIORITY_MEDIUM, add_task_if_new, prune_dedupe_store


ACADEMIC_RELATIONSHIP_TOOLS = {
    "orcid_search",
    "semantic_scholar_search",
    "dblp_author_search",
    "pubmed_author_search",
    "grant_search_person",
    "conference_profile_search",
    "arxiv_search_and_download",
}
BUSINESS_RELATIONSHIP_TOOLS = {
    "open_corporates_search",
    "company_officer_search",
    "director_disclosure_search",
    "sec_person_search",
}


@dataclass(frozen=True)
class RelationshipTask:
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


def _fact_value(receipt: ReceiptLike, key: str) -> Any:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and key in fact:
            return fact.get(key)
    return None


def _fact_list(receipt: ReceiptLike, key: str) -> List[Any]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get(key), list):
            return list(fact.get(key) or [])
    return []


def _publication_like_data(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    publications: List[Dict[str, Any]] = []
    for key in ("publications", "records", "papers", "extracted_entries"):
        for item in _fact_list(receipt, key):
            if isinstance(item, dict):
                publications.append(item)
    return publications


def derive_relationship_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ReceiptLike],
    primary_person_targets: List[str],
    iteration: int,
    dedupe_store: Dict[str, int],
) -> Tuple[List[RelationshipTask], Dict[str, int], List[str]]:
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks: List[RelationshipTask] = []
    notes: List[str] = []
    primary_name = primary_person_targets[:1][0] if primary_person_targets else ""

    for receipt in [item for item in receipts if item.ok]:
        organizations = [item for item in _fact_list(receipt, "organizations") if isinstance(item, dict)]
        for organization in organizations[:5]:
            org_url = str(organization.get("url") or "").strip()
            org_name = str(organization.get("name") or "").strip()
            if org_url.startswith(("http://", "https://")):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="org_staff_page_search",
                    payload={"runId": run_id, "org_url": org_url},
                    priority=PRIORITY_MEDIUM,
                    reason="Organization URL detected; fetch staff/team page structure.",
                )
                if org_name:
                    notes.append(f"Queued org staff lookup for {org_name}.")

        if receipt.tool_name in ACADEMIC_RELATIONSHIP_TOOLS and primary_name:
            publication_data = _publication_like_data(receipt)
            if publication_data:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="coauthor_graph_search",
                    payload={"runId": run_id, "person_name": primary_name, "publication_data": publication_data[:30]},
                    priority=PRIORITY_MEDIUM,
                    reason="Academic/publication signal detected; derive coauthor overlap graph.",
                )

        if receipt.tool_name in BUSINESS_RELATIONSHIP_TOOLS:
            roles: List[Dict[str, Any]] = []
            for key in ("roles", "officers", "directorships"):
                roles.extend([item for item in _fact_list(receipt, key) if isinstance(item, dict)])
            if roles:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="board_member_overlap_search",
                    payload={"runId": run_id, "roles": roles[:30]},
                    priority=PRIORITY_MEDIUM,
                    reason="Officer/director roles detected; check for overlapping board memberships.",
                )

        emails = [str(item).strip() for item in _fact_list(receipt, "emails") if isinstance(item, str) and str(item).strip()]
        emails.extend([str(item).strip() for item in _fact_list(receipt, "patterns") if isinstance(item, str) and str(item).strip()])
        contact_signals = [item for item in _fact_list(receipt, "contactSignals") if isinstance(item, dict)]
        organizations_text = [str(item.get("name")).strip() for item in organizations if str(item.get("name") or "").strip()]
        addresses: List[str] = []
        for key in ("registeredAddress", "officeAddress", "address"):
            value = _fact_value(receipt, key)
            if isinstance(value, str) and value.strip():
                addresses.append(value.strip())

        if emails or contact_signals or organizations_text or addresses:
            contacts = [
                {
                    "email": str(item.get("value") or "").strip(),
                    "organization": str(item.get("source") or "").strip(),
                    "address": "",
                }
                for item in contact_signals[:20]
            ]
            if len(emails) + len(organizations_text) + len(addresses) + len(contacts) >= 2:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="shared_contact_pivot_search",
                    payload={
                        "runId": run_id,
                        "contacts": contacts,
                        "emails": emails[:20],
                        "organizations": organizations_text[:20],
                        "addresses": addresses[:20],
                    },
                    priority=PRIORITY_MEDIUM,
                    reason="Public contact artifacts detected; compare for shared domain/org/address pivots.",
                )

    wrapped = [
        RelationshipTask(
            tool_name=task.tool_name,
            payload=task.payload,
            priority=task.priority,
            reason=task.reason,
            dedupe_key=task.dedupe_key,
        )
        for task in tasks
    ]
    return wrapped, dedupe_store, notes
