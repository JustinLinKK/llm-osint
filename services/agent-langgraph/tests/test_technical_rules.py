from __future__ import annotations

from dataclasses import dataclass

from orchestrator.coverage import coverage_led_stop_condition
from orchestrator.technical_graph import build_technical_graph_entities
from orchestrator.rules.technical_rules import derive_technical_follow_up_tasks


@dataclass
class ReceiptStub:
    run_id: str
    tool_name: str
    ok: bool
    summary: str
    key_facts: list[dict]


def test_derive_technical_follow_up_tasks_from_github_blog() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="github_identity_search",
        ok=True,
        summary="Resolved GitHub profile ada with repositories and a blog URL.",
        key_facts=[
            {"profileUrl": "https://github.com/ada"},
            {"username": "ada"},
            {"blogUrl": "https://ada.dev"},
            {"contactSignals": [{"type": "email", "value": "ada@example.com", "source": "github_public_profile"}]},
            {"organizations": [{"name": "analytical-engines", "url": "https://github.com/analytical-engines", "relation": "member"}]},
        ],
    )

    tasks, dedupe, notes = derive_technical_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    tool_names = [task.tool_name for task in tasks]
    assert "personal_site_search" in tool_names
    assert "wayback_fetch_url" in tool_names
    personal_site_task = next(task for task in tasks if task.tool_name == "personal_site_search")
    assert personal_site_task.payload["url"] == "https://ada.dev"
    assert dedupe
    assert any("GitHub blog URL" in note for note in notes)


def test_derive_technical_follow_up_tasks_from_personal_site_linked_github() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="personal_site_search",
        ok=True,
        summary="Resolved personal site and found linked GitHub profile.",
        key_facts=[
            {"profileUrl": "https://ada.dev"},
            {"externalLinks": [{"type": "github", "url": "https://github.com/ada"}]},
        ],
    )

    tasks, _, notes = derive_technical_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    tool_names = [task.tool_name for task in tasks]
    assert "github_identity_search" in tool_names
    assert "wayback_fetch_url" in tool_names
    github_task = next(task for task in tasks if task.tool_name == "github_identity_search")
    assert github_task.payload["profile_url"] == "https://github.com/ada"
    assert any("Personal site linked GitHub" in note for note in notes)


def test_coverage_led_stop_condition_requires_identity_history_relationships_and_anchor() -> None:
    base = {"identity": True, "aliases": True, "history": True, "contacts": True, "relationships": True}
    assert not coverage_led_stop_condition({**base, "identity": False, "code_presence": True, "academic": False, "business_roles": False})
    assert not coverage_led_stop_condition({**base, "history": False, "code_presence": True, "academic": False, "business_roles": False})
    assert not coverage_led_stop_condition({**base, "relationships": False, "code_presence": True, "academic": False, "business_roles": False})
    assert not coverage_led_stop_condition({**base, "aliases": False, "code_presence": True, "academic": False, "business_roles": False})
    assert not coverage_led_stop_condition({**base, "contacts": False, "code_presence": True, "academic": False, "business_roles": False})
    assert coverage_led_stop_condition({**base, "code_presence": True, "academic": False, "business_roles": False})
    assert coverage_led_stop_condition({**base, "code_presence": False, "academic": True, "business_roles": False})


def test_package_repository_enqueues_github_and_wayback() -> None:
    receipt = ReceiptStub(
        run_id="run-1",
        tool_name="npm_author_search",
        ok=True,
        summary="npm search found packages with GitHub repositories.",
        key_facts=[
            {"repositories": [{"name": "@acme/widget", "url": "https://github.com/acme/widget"}]},
            {"publications": [{"name": "@acme/widget", "url": "https://www.npmjs.com/package/@acme/widget"}]},
        ],
    )

    tasks, _, _ = derive_technical_follow_up_tasks(
        run_id="run-1",
        receipts=[receipt],
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    tool_names = [task.tool_name for task in tasks]
    assert "github_identity_search" in tool_names
    assert "wayback_fetch_url" in tool_names


def test_build_technical_graph_entities_emits_typed_relations() -> None:
    entities = build_technical_graph_entities(
        "github_identity_search",
        {"username": "ada"},
        {
            "stable_id": "github:42",
            "platform": "github",
            "profile_url": "https://github.com/ada",
            "username": "ada",
            "repositories": [{"name": "ada/engine", "url": "https://github.com/ada/engine", "language": "Python"}],
            "organizations": [{"name": "acme", "url": "https://github.com/acme", "relation": "member"}],
            "publications": [],
        },
    )

    assert len(entities) == 1
    entity = entities[0]
    assert entity["entityType"] == "Person"
    relation_types = {item["type"] for item in entity["relations"]}
    assert "MAINTAINS" in relation_types
    assert "MEMBER_OF" in relation_types


def test_build_wayback_graph_entities_emits_archived_page_relations() -> None:
    entities = build_technical_graph_entities(
        "wayback_fetch_url",
        {"url": "https://ada.dev"},
        {
            "original_url": "https://ada.dev",
            "snapshots": [
                {
                    "timestamp": "20240101000000",
                    "archived_url": "https://web.archive.org/web/20240101000000/https://ada.dev",
                    "mime_type": "text/html",
                }
            ],
        },
    )

    assert len(entities) == 1
    entity = entities[0]
    assert entity["entityType"] == "Article"
    assert entity["relations"][0]["type"] == "APPEARS_IN_ARCHIVE"
