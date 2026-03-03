from __future__ import annotations

from dataclasses import dataclass

from orchestrator.rules.relationship_rules import derive_relationship_follow_up_tasks


@dataclass
class ReceiptStub:
    run_id: str
    tool_name: str
    ok: bool
    summary: str
    key_facts: list[dict]


def test_org_signal_queues_staff_lookup() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="github_identity_search",
        ok=True,
        summary="Resolved GitHub profile.",
        key_facts=[{"organizations": [{"name": "Acme", "url": "https://github.com/acme"}]}],
    )
    tasks, _, _ = derive_relationship_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "org_staff_page_search" for task in tasks)


def test_academic_signal_queues_coauthor_graph() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="orcid_search",
        ok=True,
        summary="Resolved ORCID profile.",
        key_facts=[{"publications": [{"authors": ["Ada Lovelace", "Grace Hopper"], "venue": "NeurIPS"}]}],
    )
    tasks, _, _ = derive_relationship_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "coauthor_graph_search" for task in tasks)


def test_business_signal_queues_board_overlap() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="company_officer_search",
        ok=True,
        summary="Officer roles found.",
        key_facts=[{"roles": [{"name": "Ada Lovelace", "company_name": "Acme", "role": "Director"}]}],
    )
    tasks, _, _ = derive_relationship_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "board_member_overlap_search" for task in tasks)


def test_contact_signal_queues_shared_contact_pivot() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="contact_page_extractor",
        ok=True,
        summary="Public contacts found.",
        key_facts=[{"emails": ["ada@example.com", "grace@example.com"]}, {"organizations": [{"name": "Example Org"}]}],
    )
    tasks, _, _ = derive_relationship_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "shared_contact_pivot_search" for task in tasks)

