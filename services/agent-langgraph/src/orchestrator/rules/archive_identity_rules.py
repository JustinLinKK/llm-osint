from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple
from urllib.parse import urlparse

from orchestrator.rules.academic_rules import PRIORITY_HIGH, PRIORITY_MEDIUM, add_task_if_new, prune_dedupe_store


@dataclass(frozen=True)
class ArchiveIdentityTask:
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


def _hint_urls(receipt: ReceiptLike) -> List[str]:
    urls: List[str] = []
    for key in ("profileUrl", "sourceUrl"):
        value = _fact_value(receipt, key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            urls.append(value)
    for key in ("externalLinks", "platformHits"):
        for item in _fact_list(receipt, key):
            if isinstance(item, dict):
                url = str(item.get("url") or "").strip()
                if url.startswith(("http://", "https://")):
                    urls.append(url)
    return list(dict.fromkeys(urls))


def _domain_from_url(value: str) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def derive_archive_identity_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ReceiptLike],
    primary_person_targets: List[str],
    iteration: int,
    dedupe_store: Dict[str, int],
) -> Tuple[List[ArchiveIdentityTask], Dict[str, int], List[str]]:
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks: List[ArchiveIdentityTask] = []
    notes: List[str] = []
    primary_name = primary_person_targets[:1][0] if primary_person_targets else ""

    for receipt in [item for item in receipts if item.ok]:
        profile_url = str(_fact_value(receipt, "profileUrl") or "").strip()
        source_url = str(_fact_value(receipt, "sourceUrl") or "").strip()
        username = str(_fact_value(receipt, "username") or "").strip()
        domain = str(_fact_value(receipt, "domain") or "").strip().lower()
        earliest_text = str(_fact_value(receipt, "earliestExtractedText") or "").strip()
        latest_text = str(_fact_value(receipt, "latestExtractedText") or "").strip()
        earliest_url = str(_fact_value(receipt, "earliestArchivedUrl") or "").strip()
        latest_url = str(_fact_value(receipt, "latestArchivedUrl") or "").strip()

        strong_url = profile_url or source_url
        if strong_url.startswith(("http://", "https://")) and receipt.tool_name != "wayback_fetch_url":
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="wayback_fetch_url",
                payload={"runId": run_id, "url": strong_url, "max_results": 5},
                priority=PRIORITY_MEDIUM,
                reason="Strong profile/source URL detected; fetch Wayback archive snapshots.",
            )

        if username and receipt.tool_name != "username_permutation_search":
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="username_permutation_search",
                payload={"runId": run_id, "username": username},
                priority=PRIORITY_MEDIUM,
                reason="Username signal detected; check deterministic cross-platform URL permutations.",
            )
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="reddit_user_search",
                payload={"runId": run_id, "username": username},
                priority=PRIORITY_MEDIUM,
                reason="Username signal detected; resolve direct Reddit profile metadata.",
            )
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="medium_author_search",
                payload={"runId": run_id, "username": username},
                priority=PRIORITY_MEDIUM,
                reason="Username signal detected; check direct Medium author profile.",
            )

        if domain:
            if primary_name:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="email_pattern_inference",
                    payload={"runId": run_id, "domain": domain, "person_name": primary_name},
                    priority=PRIORITY_MEDIUM,
                    reason="Domain signal detected; infer likely email patterns.",
                )
            if not strong_url and domain:
                strong_url = f"https://{domain}"
            if strong_url.startswith(("http://", "https://")):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="contact_page_extractor",
                    payload={"runId": run_id, "site_url": strong_url},
                    priority=PRIORITY_MEDIUM,
                    reason="Domain/site signal detected; extract public contact pages.",
                )

        for hint_url in _hint_urls(receipt):
            host = _domain_from_url(hint_url)
            if host.endswith(".substack.com"):
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="substack_author_search",
                    payload={"runId": run_id, "url": hint_url},
                    priority=PRIORITY_MEDIUM,
                    reason="Substack profile/publication URL detected; resolve author and linkage signals.",
                )
            if host == "medium.com" and "/@" in hint_url:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="medium_author_search",
                    payload={"runId": run_id, "profile_url": hint_url},
                    priority=PRIORITY_MEDIUM,
                    reason="Medium profile URL detected; resolve author profile metadata.",
                )
            if ("/@" in hint_url or host.endswith("mastodon.social") or host.endswith("hachyderm.io")) and host not in {
                "medium.com",
                "www.medium.com",
            }:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="mastodon_profile_search",
                    payload={"runId": run_id, "profile_url": hint_url},
                    priority=PRIORITY_MEDIUM,
                    reason="Mastodon-style profile URL detected; resolve account metadata.",
                )

        if receipt.tool_name == "wayback_fetch_url" and (earliest_text or earliest_url) and (latest_text or latest_url):
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="historical_bio_diff",
                payload={
                    "runId": run_id,
                    "earliest_text": earliest_text,
                    "latest_text": latest_text,
                    "earliest_url": earliest_url,
                    "latest_url": latest_url,
                    "earliest_timestamp": _fact_value(receipt, "firstArchivedAt") or "",
                    "latest_timestamp": _fact_value(receipt, "lastArchivedAt") or "",
                },
                priority=PRIORITY_HIGH,
                reason="Earliest and latest archived snapshots available; compare bio/history changes.",
            )

        if receipt.tool_name in {"tavily_research", "tavily_person_search", "google_serp_person_search", "person_search"}:
            for fact in receipt.key_facts:
                if not isinstance(fact, dict):
                    continue
                for key in ("emails", "profileUrls"):
                    values = fact.get(key)
                    if isinstance(values, list):
                        for item in values:
                            if isinstance(item, str) and item.startswith(("http://", "https://")):
                                add_task_if_new(
                                    tasks,
                                    dedupe_store,
                                    iteration,
                                    tool_name="wayback_fetch_url",
                                    payload={"runId": run_id, "url": item, "max_results": 5},
                                    priority=PRIORITY_MEDIUM,
                                    reason="Profile URL discovered from public search; archive it in Wayback.",
                                )
            if primary_name:
                add_task_if_new(
                    tasks,
                    dedupe_store,
                    iteration,
                    tool_name="sanctions_watchlist_search",
                    payload={"runId": run_id, "person_name": primary_name},
                    priority=PRIORITY_MEDIUM,
                    reason="Run exact-name sanctions watchlist check for the target.",
                )

        if receipt.tool_name == "historical_bio_diff":
            changes = _fact_list(receipt, "changes")
            if changes:
                notes.append(f"Historical archive comparison found {len(changes)} bio change(s).")

    wrapped = [
        ArchiveIdentityTask(
            tool_name=task.tool_name,
            payload=task.payload,
            priority=task.priority,
            reason=task.reason,
            dedupe_key=task.dedupe_key,
        )
        for task in tasks
    ]
    return wrapped, dedupe_store, notes
