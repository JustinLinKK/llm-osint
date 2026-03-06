from __future__ import annotations

from dataclasses import dataclass

from orchestrator.business_graph import build_business_graph_entities
from orchestrator.rules.business_rules import derive_business_follow_up_tasks


@dataclass
class ReceiptStub:
    run_id: str
    tool_name: str
    ok: bool
    summary: str
    key_facts: list[dict]


def test_business_rules_queue_open_corporates_from_github_org() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="github_identity_search",
        ok=True,
        summary="GitHub org found.",
        key_facts=[
            {"organizations": [{"name": "Acme", "url": "https://github.com/acme", "relation": "member"}]},
        ],
    )

    tasks, _, _ = derive_business_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    assert any(task.tool_name == "open_corporates_search" and task.payload["company_name"] == "Acme" for task in tasks)


def test_business_rules_skip_provider_like_company_names() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="github_identity_search",
        ok=True,
        summary="Noisy org labels found.",
        key_facts=[
            {"organizations": [{"name": "Tavily", "url": "https://example.com/tavily", "relation": "member"}]},
            {"organizations": [{"name": "Google", "url": "https://example.com/google", "relation": "member"}]},
        ],
    )

    tasks, _, _ = derive_business_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    assert not any(task.tool_name == "open_corporates_search" for task in tasks)


def test_business_rules_note_unresolved_stealth_startup_descriptor() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="github_identity_search",
        ok=True,
        summary="Employment descriptor found.",
        key_facts=[
            {"organizations": [{"name": "Stealth Startup", "url": "", "relation": "member"}]},
        ],
    )

    tasks, _, notes = derive_business_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    assert not any(task.tool_name == "open_corporates_search" for task in tasks)
    assert any("descriptor" in note.lower() and "stealth startup" in note.lower() for note in notes)


def test_business_rules_queue_company_filing_after_company_resolution() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="open_corporates_search",
        ok=True,
        summary="Resolved company.",
        key_facts=[
            {"companyName": "Acme Inc."},
            {"companyNumber": "123"},
            {"jurisdiction": "us_ca"},
            {"registeredAddress": "123 Main St"},
        ],
    )

    tasks, _, _ = derive_business_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    tool_names = [task.tool_name for task in tasks]
    assert "company_officer_search" in tool_names
    assert "company_filing_search" in tool_names
    assert "sec_person_search" in tool_names


def test_build_business_graph_entities_emits_officer_and_domain_edges() -> None:
    person_entities = build_business_graph_entities(
        "company_officer_search",
        {"person_name": "Ada Lovelace"},
        {
            "roles": [
                {
                    "company_name": "Acme Inc.",
                    "company_number": "123",
                    "jurisdiction": "us_ca",
                    "role": "Director",
                    "source_url": "https://opencorporates.com/companies/us_ca/123",
                }
            ]
        },
    )
    domain_entities = build_business_graph_entities(
        "domain_whois_search",
        {"domain": "acme.com"},
        {
            "domain": "acme.com",
            "registrant_org": "Acme Inc.",
            "registration_date": "2020-01-01",
            "registrar": "RegistrarCo",
            "name_servers": ["ns1.example.com"],
        },
    )

    assert person_entities[0]["relations"][0]["type"] == "OFFICER_OF"
    assert domain_entities[0]["entityType"] == "Domain"
    assert domain_entities[0]["relations"][0]["type"] == "AFFILIATED_WITH"
