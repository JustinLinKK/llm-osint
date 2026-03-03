from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple
from urllib.parse import urlparse

from orchestrator.rules.academic_rules import (
    PRIORITY_HIGH,
    PRIORITY_MEDIUM,
    add_task_if_new,
    prune_dedupe_store,
)


TECHNICAL_SEARCH_TOOLS = {
    "github_identity_search",
    "gitlab_identity_search",
    "personal_site_search",
    "package_registry_search",
    "npm_author_search",
    "crates_author_search",
}


@dataclass(frozen=True)
class TechnicalTask:
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


def _extract_first_url_links(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("externalLinks"), list):
            return [item for item in fact["externalLinks"] if isinstance(item, dict)]
    return []


def _extract_repositories(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("repositories"), list):
            return [item for item in fact["repositories"] if isinstance(item, dict)]
    return []


def _extract_publications(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("publications"), list):
            return [item for item in fact["publications"] if isinstance(item, dict)]
    return []


def _extract_contact_signals(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("contactSignals"), list):
            return [item for item in fact["contactSignals"] if isinstance(item, dict)]
    return []


def _extract_organizations(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("organizations"), list):
            return [item for item in fact["organizations"] if isinstance(item, dict)]
    return []


def _repo_owner(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    owner = path_parts[0] if path_parts else ""
    if host.startswith("www."):
        host = host[4:]
    return host, owner


def derive_technical_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ReceiptLike],
    primary_person_targets: List[str],
    iteration: int,
    dedupe_store: Dict[str, int],
) -> Tuple[List[TechnicalTask], Dict[str, int], List[str]]:
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks: List[TechnicalTask] = []
    notes: List[str] = []
    technical_receipts = [receipt for receipt in receipts if receipt.ok and receipt.tool_name in TECHNICAL_SEARCH_TOOLS]
    primary_name = primary_person_targets[:1][0] if primary_person_targets else ""

    for receipt in technical_receipts:
        if receipt.tool_name == "github_identity_search":
            blog_url = _fact_value(receipt, "blogUrl")
            github_profile = _fact_value(receipt, "profileUrl")
            username = _fact_value(receipt, "username")
            contact_signals = _extract_contact_signals(receipt)
            organizations = _extract_organizations(receipt)

            if isinstance(blog_url, str) and blog_url.strip():
                payload = {"runId": run_id, "name": primary_name, "url": blog_url.strip(), "blog": blog_url.strip()}
                dedupe_before = len(dedupe_store)
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="personal_site_search",
                    payload=payload,
                    priority=PRIORITY_HIGH,
                    reason="GitHub blog URL found; resolve and archive the linked personal site.",
                )
                if len(dedupe_store) > dedupe_before:
                    notes.append("GitHub blog URL detected; queued personal site resolution.")

            for signal in contact_signals:
                signal_type = str(signal.get("type") or "").strip().lower()
                value = str(signal.get("value") or "").strip()
                if signal_type == "email" and value:
                    notes.append(f"Public GitHub email signal detected: {value}")

            if github_profile and isinstance(github_profile, str):
                notes.append(f"Code identity confirmed on GitHub: {github_profile}")
            if username and isinstance(username, str):
                notes.append(f"GitHub username resolved: {username}")
            if organizations:
                notes.append(f"GitHub profile surfaced {len(organizations)} organization affiliation(s).")
            if github_profile and isinstance(github_profile, str):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="wayback_fetch_url",
                    payload={"runId": run_id, "url": github_profile, "max_results": 5},
                    priority=PRIORITY_MEDIUM,
                    reason="Archive the resolved GitHub profile URL in Wayback.",
                )

            if username and isinstance(username, str):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="package_registry_search",
                    payload={"runId": run_id, "username": username, "max_results": 5},
                    priority=PRIORITY_MEDIUM,
                    reason="GitHub username resolved; search package registry maintainer footprint.",
                )

        if receipt.tool_name == "gitlab_identity_search":
            profile_url = _fact_value(receipt, "profileUrl")
            username = _fact_value(receipt, "username")
            if profile_url and isinstance(profile_url, str):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="wayback_fetch_url",
                    payload={"runId": run_id, "url": profile_url, "max_results": 5},
                    priority=PRIORITY_MEDIUM,
                    reason="Archive the resolved GitLab profile URL in Wayback.",
                )
                notes.append(f"Code identity confirmed on GitLab: {profile_url}")
            if username and isinstance(username, str):
                notes.append(f"GitLab username resolved: {username}")

        if receipt.tool_name == "personal_site_search":
            profile_url = _fact_value(receipt, "profileUrl")
            external_links = _extract_first_url_links(receipt)
            github_links = [
                item for item in external_links
                if str(item.get("type") or "").strip().lower() == "github"
                and isinstance(item.get("url"), str)
            ]
            if github_links:
                first_url = str(github_links[0]["url"]).strip()
                payload = {"runId": run_id, "person_name": primary_name, "profile_url": first_url}
                dedupe_before = len(dedupe_store)
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="github_identity_search",
                    payload=payload,
                    priority=PRIORITY_MEDIUM,
                    reason="Personal site linked a GitHub profile; resolve the code identity directly.",
                )
                if len(dedupe_store) > dedupe_before:
                    notes.append("Personal site linked GitHub; queued GitHub identity resolution.")
            gitlab_links = [
                item for item in external_links
                if str(item.get("type") or "").strip().lower() == "gitlab"
                and isinstance(item.get("url"), str)
            ]
            if gitlab_links:
                first_url = str(gitlab_links[0]["url"]).strip()
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="gitlab_identity_search",
                    payload={"runId": run_id, "person_name": primary_name, "profile_url": first_url},
                    priority=PRIORITY_MEDIUM,
                    reason="Personal site linked a GitLab profile; resolve the code identity directly.",
                )
                notes.append("Personal site linked GitLab; queued GitLab identity resolution.")
            if profile_url and isinstance(profile_url, str):
                notes.append(f"Resolved reachable personal site: {profile_url}")
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="wayback_fetch_url",
                    payload={"runId": run_id, "url": profile_url, "max_results": 5},
                    priority=PRIORITY_MEDIUM,
                    reason="Archive the resolved personal site URL in Wayback.",
                )

        if receipt.tool_name in {"package_registry_search", "npm_author_search", "crates_author_search"}:
            repositories = _extract_repositories(receipt)
            publications = _extract_publications(receipt)
            if publications:
                notes.append(f"{receipt.tool_name} surfaced {len(publications)} package publication(s).")
            for repository in repositories[:5]:
                repo_url = str(repository.get("url") or "").strip()
                if not repo_url:
                    continue
                host, owner = _repo_owner(repo_url)
                if host == "github.com":
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="github_identity_search",
                        payload={"runId": run_id, "person_name": primary_name, "repo_url": repo_url, "username": owner or ""},
                        priority=PRIORITY_HIGH,
                        reason="Package repository URL points to GitHub; resolve the repository owner identity.",
                    )
                if host == "gitlab.com":
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="gitlab_identity_search",
                        payload={"runId": run_id, "person_name": primary_name, "profile_url": f"https://gitlab.com/{owner}" if owner else repo_url},
                        priority=PRIORITY_HIGH,
                        reason="Package repository URL points to GitLab; resolve the repository owner identity.",
                    )
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="wayback_fetch_url",
                    payload={"runId": run_id, "url": repo_url, "max_results": 5},
                    priority=PRIORITY_MEDIUM,
                    reason="Archive the repository URL linked from package metadata.",
                )

    wrapped = [
        TechnicalTask(
            tool_name=task.tool_name,
            payload=task.payload,
            priority=task.priority,
            reason=task.reason,
            dedupe_key=task.dedupe_key,
        )
        for task in tasks
    ]
    return wrapped, dedupe_store, notes
