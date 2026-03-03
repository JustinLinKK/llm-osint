from __future__ import annotations

from dataclasses import dataclass

from orchestrator.rules.academic_rules import derive_academic_follow_up_tasks, make_dedupe_key


@dataclass
class ReceiptStub:
    run_id: str
    tool_name: str
    ok: bool
    summary: str
    key_facts: list[dict]


def test_make_dedupe_key_ignores_run_id() -> None:
    first = make_dedupe_key("orcid_search", {"runId": "a", "person_name": "Ada Lovelace", "orcid_id": "0000"})
    second = make_dedupe_key("orcid_search", {"runId": "b", "person_name": "Ada Lovelace", "orcid_id": "0000"})
    assert first == second


def test_derive_academic_follow_up_tasks_enqueues_expected_fetches() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="orcid_search",
        ok=True,
        summary="orcid_search returned 1 academic candidate.",
        key_facts=[
            {
                "candidates": [
                    {
                        "canonical_name": "Ada Lovelace",
                        "source": "orcid",
                        "source_id": "0000-0001",
                        "confidence": 0.91,
                        "affiliations": ["Massachusetts General Hospital"],
                        "topics": ["biomedical informatics"],
                        "external_ids": {"orcid": "0000-0001"},
                        "works_summary": {},
                        "evidence": [{"url": "https://orcid.org/0000-0001"}],
                    }
                ]
            }
        ],
    )
    tasks, dedupe, notes = derive_academic_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    tool_names = {item.tool_name for item in tasks}
    assert "orcid_search" in tool_names
    assert "pubmed_author_search" in tool_names
    assert "grant_search_person" in tool_names
    assert dedupe
    assert notes == []


def test_derive_academic_follow_up_tasks_propagates_affiliations() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="dblp_author_search",
        ok=True,
        summary="dblp_author_search returned one strong US academic candidate.",
        key_facts=[
            {
                "candidates": [
                    {
                        "canonical_name": "Geoffrey E. Hinton",
                        "source": "dblp",
                        "source_id": "https://dblp.org/pid/10/3248",
                        "confidence": 0.81,
                        "affiliations": ["Google DeepMind, London, UK", "University of Toronto, Department of Computer Science, ON, Canada"],
                        "topics": ["neurips", "icml"],
                        "external_ids": {"dblp_pid": "https://dblp.org/pid/10/3248"},
                        "works_summary": {},
                        "evidence": [],
                    }
                ]
            }
        ],
    )
    tasks, _, _ = derive_academic_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Geoffrey Hinton"],
        iteration=0,
        dedupe_store={},
    )
    grant_tasks = [task for task in tasks if task.tool_name == "grant_search_person"]
    assert grant_tasks
    assert grant_tasks[0].payload["affiliations"] == [
        "Google DeepMind, London, UK",
        "University of Toronto, Department of Computer Science, ON, Canada",
    ]


def test_derive_academic_follow_up_tasks_adds_directory_and_email_pivots() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="semantic_scholar_search",
        ok=True,
        summary="semantic_scholar_search returned one strong academic candidate.",
        key_facts=[
            {
                "candidates": [
                    {
                        "canonical_name": "Ada Lovelace",
                        "source": "semanticscholar",
                        "source_id": "123",
                        "confidence": 0.88,
                        "affiliations": ["example.edu", "Analytical Engine Institute"],
                        "topics": ["machine learning"],
                        "external_ids": {"semantic_scholar": "123"},
                        "works_summary": {},
                        "evidence": [],
                    }
                ]
            }
        ],
    )
    tasks, _, _ = derive_academic_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    tool_names = {task.tool_name for task in tasks}
    assert "institution_directory_search" in tool_names
    assert "email_pattern_inference" in tool_names
