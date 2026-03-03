from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, Iterable, List, Protocol, Tuple


T1 = 0.7
PROMOTION_AGREEMENT_THRESHOLD = 2
PRIORITY_HIGH = 100
PRIORITY_MEDIUM = 70
PRIORITY_LOW = 40
DEDUP_TTL_ITERATIONS = 3

BIOMED_TOKENS = {"med", "medicine", "hospital", "biomed", "biomedical", "clinic", "health", "nih", "pubmed"}
CS_TOKENS = {"acl", "neurips", "icml", "ieee", "cvpr", "emnlp", "naacl", "kdd", "sigir", "computer", "machine learning"}
US_TOKENS = {"usa", "united states", "u.s.", "us", "california", "massachusetts", "new york", "texas", "nih", "nsf"}
# Temporarily disabled until PatentSearch API integration is implemented.
# INVENTOR_TOKENS = {"inventor", "invented", "patent", "patents"}
ACADEMIC_SEARCH_TOOLS = {"orcid_search", "semantic_scholar_search", "dblp_author_search", "pubmed_author_search"}


@dataclass(frozen=True)
class AcademicTask:
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


def make_dedupe_key(tool_name: str, payload: Dict[str, Any]) -> str:
    stable_parts = [tool_name]
    for key in sorted(payload):
        if key == "runId":
            continue
        stable_parts.append(f"{key}={payload[key]}")
    return "|".join(stable_parts)


def prune_dedupe_store(store: Dict[str, int], iteration: int) -> Dict[str, int]:
    return {key: expires_at for key, expires_at in store.items() if expires_at > iteration}


def add_task_if_new(
    tasks: List[AcademicTask],
    dedupe_store: Dict[str, int],
    iteration: int,
    *,
    tool_name: str,
    payload: Dict[str, Any],
    priority: int,
    reason: str,
) -> None:
    dedupe_key = make_dedupe_key(tool_name, payload)
    store_key = f"{tool_name}|{dedupe_key}"
    if store_key in dedupe_store:
        return
    dedupe_store[store_key] = iteration + DEDUP_TTL_ITERATIONS
    tasks.append(
        AcademicTask(
            tool_name=tool_name,
            payload=payload,
            priority=priority,
            reason=reason,
            dedupe_key=dedupe_key,
        )
    )


def _flatten_strings(value: Any) -> List[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        output: List[str] = []
        for item in value:
            output.extend(_flatten_strings(item))
        return output
    if isinstance(value, dict):
        output: List[str] = []
        for item in value.values():
            output.extend(_flatten_strings(item))
        return output
    return []


def _receipt_candidates(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("candidates"), list):
            return [item for item in fact["candidates"] if isinstance(item, dict)]
    return []


def _receipt_records(receipt: ReceiptLike) -> List[Dict[str, Any]]:
    for fact in receipt.key_facts:
        if isinstance(fact, dict) and isinstance(fact.get("records"), list):
            return [item for item in fact["records"] if isinstance(item, dict)]
    return []


def _candidate_domains(candidates: Iterable[Dict[str, Any]]) -> List[str]:
    domains: List[str] = []
    for item in candidates:
        for key in ("homepage_domain", "homepage", "profile_url"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                text = value.strip().lower()
                if "://" in text:
                    text = text.split("://", 1)[1]
                domains.append(text.split("/", 1)[0])
    return domains


def _candidate_affiliations(candidates: Iterable[Dict[str, Any]]) -> List[str]:
    affiliations: List[str] = []
    seen: set[str] = set()
    for item in candidates:
        values = item.get("affiliations")
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, str):
                continue
            text = value.strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            affiliations.append(text)
    return affiliations


def _affiliation_domains(values: Iterable[str]) -> List[str]:
    domains: List[str] = []
    seen: set[str] = set()
    for value in values:
        lowered = value.strip().lower()
        if not lowered:
            continue
        for token in re.findall(r"\b[a-z0-9.-]+\.(?:edu|ac\.[a-z]{2,}|org|com)\b", lowered):
            if token not in seen:
                seen.add(token)
                domains.append(token)
    return domains


def _candidate_id_matches(candidates: Iterable[Dict[str, Any]], id_key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in candidates:
        external_ids = item.get("external_ids") if isinstance(item.get("external_ids"), dict) else {}
        value = external_ids.get(id_key) if isinstance(external_ids, dict) else None
        if isinstance(value, str) and value.strip():
            counts[value.strip().lower()] = counts.get(value.strip().lower(), 0) + 1
    return counts


def _best_candidates(receipt: ReceiptLike, threshold: float = T1) -> List[Dict[str, Any]]:
    candidates = _receipt_candidates(receipt)
    output = [item for item in candidates if float(item.get("confidence") or 0.0) >= threshold]
    output.sort(key=lambda item: float(item.get("confidence") or 0.0), reverse=True)
    return output


def _has_token(texts: Iterable[str], token_set: set[str]) -> bool:
    blob = " ".join(texts).lower()
    return any(token in blob for token in token_set)


def derive_academic_follow_up_tasks(
    *,
    run_id: str,
    receipts: List[ReceiptLike],
    primary_person_targets: List[str],
    iteration: int,
    dedupe_store: Dict[str, int],
) -> Tuple[List[AcademicTask], Dict[str, int], List[str]]:
    dedupe_store = prune_dedupe_store(dedupe_store, iteration)
    tasks: List[AcademicTask] = []
    notes: List[str] = []
    academic_receipts = [receipt for receipt in receipts if receipt.ok and receipt.tool_name in ACADEMIC_SEARCH_TOOLS]

    all_candidates: List[Dict[str, Any]] = []
    for receipt in academic_receipts:
        strong_candidates = _best_candidates(receipt)
        all_candidates.extend(strong_candidates)

        if receipt.tool_name == "orcid_search":
            for candidate in strong_candidates[:1]:
                orcid_id = candidate.get("external_ids", {}).get("orcid") if isinstance(candidate.get("external_ids"), dict) else None
                if isinstance(orcid_id, str) and orcid_id.strip():
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="orcid_search",
                        payload={"runId": run_id, "person_name": candidate.get("canonical_name") or primary_person_targets[:1][0], "orcid_id": orcid_id, "fetch_record": True},
                        priority=PRIORITY_HIGH,
                        reason="High-confidence ORCID candidate found; fetch full public record",
                    )

        if receipt.tool_name == "semantic_scholar_search":
            for candidate in strong_candidates[:1]:
                author_id = candidate.get("source_id")
                if isinstance(author_id, str) and author_id.strip():
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="semantic_scholar_search",
                        payload={"runId": run_id, "person_name": candidate.get("canonical_name") or primary_person_targets[:1][0], "author_id": author_id, "fetch_author": True},
                        priority=PRIORITY_HIGH,
                        reason="High-confidence Semantic Scholar candidate found; fetch author profile",
                    )

        if receipt.tool_name == "dblp_author_search":
            for candidate in strong_candidates[:1]:
                pid = candidate.get("external_ids", {}).get("dblp_pid") if isinstance(candidate.get("external_ids"), dict) else candidate.get("source_id")
                if isinstance(pid, str) and pid.strip():
                    add_task_if_new(
                        tasks,
                        dedupe_store,
                        iteration,
                        tool_name="dblp_author_search",
                        payload={"runId": run_id, "person_name": candidate.get("canonical_name") or primary_person_targets[:1][0], "dblp_pid": pid, "fetch_publications": True},
                        priority=PRIORITY_HIGH,
                        reason="High-confidence DBLP candidate found; fetch publication list",
                    )

        topic_texts = _flatten_strings(_receipt_candidates(receipt)) + _flatten_strings(_receipt_records(receipt)) + [receipt.summary]
        primary_name = primary_person_targets[:1][0] if primary_person_targets else ""
        affiliation_hints = _candidate_affiliations(strong_candidates)

        if _has_token(topic_texts, BIOMED_TOKENS):
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="pubmed_author_search",
                payload={"runId": run_id, "person_name": primary_name, "affiliations": affiliation_hints[:5], "max_results": 10},
                priority=PRIORITY_MEDIUM,
                reason="Biomed tokens detected from affiliations/topics; tighten with PubMed",
            )

        if _has_token(topic_texts, CS_TOKENS):
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="conference_profile_search",
                payload={"runId": run_id, "person_name": primary_name, "max_results": 10},
                priority=PRIORITY_MEDIUM,
                reason="Strong CS venue/topic signal detected; aggregate conference appearances",
            )

        if _has_token(topic_texts, US_TOKENS):
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="grant_search_person",
                payload={"runId": run_id, "person_name": primary_name, "affiliations": affiliation_hints[:5], "max_results": 10},
                priority=PRIORITY_MEDIUM,
                reason="US-heavy affiliation/publication signal detected; search NIH/NSF grants",
            )

        for domain in _affiliation_domains(affiliation_hints)[:4]:
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="institution_directory_search",
                payload={"runId": run_id, "person_name": primary_name, "institution_domain": domain},
                priority=PRIORITY_MEDIUM,
                reason="Affiliation domain inferred; check institution directory for identity confirmation.",
            )
            add_task_if_new(
                tasks,
                dedupe_store,
                iteration,
                tool_name="email_pattern_inference",
                payload={"runId": run_id, "person_name": primary_name, "domain": domain},
                priority=PRIORITY_MEDIUM,
                reason="Academic affiliation domain inferred; derive public email patterns.",
            )

        # Temporarily disabled until PatentSearch API integration is implemented.
        # if _has_token(topic_texts, INVENTOR_TOKENS):
        #     add_task_if_new(
        #         tasks,
        #         dedupe_store,
        #         iteration,
        #         tool_name="patent_search_person",
        #         payload={"runId": run_id, "person_name": primary_name, "max_results": 10},
        #         priority=PRIORITY_LOW,
        #         reason="Inventor/patent keyword signal detected; search PatentsView",
        #     )

    orcid_agreement = _candidate_id_matches(all_candidates, "orcid")
    homepage_agreement = {}
    for domain in _candidate_domains(all_candidates):
        homepage_agreement[domain] = homepage_agreement.get(domain, 0) + 1

    if any(count >= PROMOTION_AGREEMENT_THRESHOLD for count in orcid_agreement.values()):
        notes.append("Academic identity promoted to resolved_identity via cross-source ORCID agreement.")
    elif any(count >= PROMOTION_AGREEMENT_THRESHOLD for count in homepage_agreement.values()):
        notes.append("Academic identity promoted to resolved_identity via shared homepage domain across sources.")

    tasks.sort(key=lambda item: item.priority, reverse=True)
    return tasks, dedupe_store, notes
