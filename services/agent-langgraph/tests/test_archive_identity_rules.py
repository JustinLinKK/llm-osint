from __future__ import annotations

from dataclasses import dataclass

from orchestrator.rules.archive_identity_rules import derive_archive_identity_follow_up_tasks


@dataclass
class ReceiptStub:
    run_id: str
    tool_name: str
    ok: bool
    summary: str
    key_facts: list[dict]


def test_profile_url_queues_wayback_and_username_permutation() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="github_identity_search",
        ok=True,
        summary="Resolved GitHub profile.",
        key_facts=[{"profileUrl": "https://github.com/ada"}, {"username": "ada"}],
    )
    tasks, _, _ = derive_archive_identity_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    tool_names = [task.tool_name for task in tasks]
    assert "wayback_fetch_url" in tool_names
    assert "username_permutation_search" in tool_names


def test_domain_signal_queues_email_pattern_and_contact_extractor() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="domain_whois_search",
        ok=True,
        summary="Domain resolved.",
        key_facts=[{"domain": "example.edu"}, {"sourceUrl": "https://rdap.org/domain/example.edu"}],
    )
    tasks, _, _ = derive_archive_identity_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    tool_names = [task.tool_name for task in tasks]
    assert "email_pattern_inference" in tool_names
    assert "contact_page_extractor" in tool_names


def test_wayback_receipt_queues_historical_bio_diff() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="wayback_fetch_url",
        ok=True,
        summary="Wayback returned snapshots.",
        key_facts=[
            {"earliestExtractedText": "Engineer at Acme"},
            {"latestExtractedText": "Director at Example"},
            {"earliestArchivedUrl": "https://web.archive.org/web/20200101/https://example.com"},
            {"latestArchivedUrl": "https://web.archive.org/web/20250101/https://example.com"},
            {"firstArchivedAt": "20200101"},
            {"lastArchivedAt": "20250101"},
        ],
    )
    tasks, _, _ = derive_archive_identity_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "historical_bio_diff" for task in tasks)


def test_ambiguous_acronym_username_does_not_expand_social_identity_chain() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="tavily_person_search",
        ok=True,
        summary="Search returned an ambiguous uppercase handle.",
        key_facts=[
            {"username": "USPS"},
            {"profileUrl": "https://medium.com/@USPS"},
            {"profileUrls": ["https://medium.com/@USPS"]},
        ],
    )
    tasks, _, _ = derive_archive_identity_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Frederick Xinyu Pi"],
        iteration=0,
        dedupe_store={},
    )
    tool_names = [task.tool_name for task in tasks]
    assert "username_permutation_search" not in tool_names
    assert "reddit_user_search" not in tool_names
    assert "medium_author_search" not in tool_names
    assert "wayback_fetch_url" not in tool_names
