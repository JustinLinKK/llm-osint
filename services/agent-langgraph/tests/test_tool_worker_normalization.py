from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Any, Dict


def _load_tool_worker_graph_module(monkeypatch):
    src_root = Path(__file__).resolve().parents[1] / "src"
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    langgraph_module = types.ModuleType("langgraph")
    langgraph_graph_module = types.ModuleType("langgraph.graph")
    langgraph_graph_module.END = "__END__"
    langgraph_graph_module.StateGraph = object
    monkeypatch.setitem(sys.modules, "langgraph", langgraph_module)
    monkeypatch.setitem(sys.modules, "langgraph.graph", langgraph_graph_module)

    pydantic_module = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **kwargs: Any) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        def model_dump(self) -> Dict[str, Any]:
            return dict(self.__dict__)

    def _field(*args: Any, default_factory=None, **kwargs: Any) -> Any:
        if default_factory is not None:
            return default_factory()
        return kwargs.get("default")

    pydantic_module.BaseModel = _BaseModel
    pydantic_module.Field = _field
    monkeypatch.setitem(sys.modules, "pydantic", pydantic_module)

    mcp_client_module = types.ModuleType("mcp_client")
    mcp_client_module.McpClientProtocol = object
    monkeypatch.setitem(sys.modules, "mcp_client", mcp_client_module)

    openrouter_module = types.ModuleType("openrouter_llm")
    openrouter_module.OpenRouterLLM = object
    openrouter_module.get_openrouter_timeout = lambda env_var, default: default
    openrouter_module.invoke_complete_json = (
        lambda llm, system_prompt, user_payload, *, temperature, timeout, **kwargs: llm.complete_json(
            system_prompt,
            user_payload,
            temperature=temperature,
            timeout=timeout,
            **kwargs,
        )
    )
    monkeypatch.setitem(sys.modules, "openrouter_llm", openrouter_module)

    receipt_store_module = types.ModuleType("receipt_store")
    receipt_store_module.insert_artifact = lambda *args, **kwargs: None
    receipt_store_module.insert_artifact_summary = lambda *args, **kwargs: None
    receipt_store_module.insert_run_note = lambda *args, **kwargs: None
    receipt_store_module.insert_tool_receipt = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "receipt_store", receipt_store_module)

    run_events_module = types.ModuleType("run_events")
    run_events_module.emit_run_event = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "run_events", run_events_module)

    system_prompts_module = types.ModuleType("system_prompts")
    prompt_names = [
        "ACADEMIC_IDENTITY_TOOL_SUMMARY_SYSTEM_PROMPT",
        "ARCHIVE_DIFF_TOOL_SUMMARY_SYSTEM_PROMPT",
        "ARXIV_TOOL_SUMMARY_SYSTEM_PROMPT",
        "BUSINESS_ROLE_TOOL_SUMMARY_SYSTEM_PROMPT",
        "CONFERENCE_TOOL_SUMMARY_SYSTEM_PROMPT",
        "DOMAIN_WHOIS_TOOL_SUMMARY_SYSTEM_PROMPT",
        "GITHUB_TOOL_SUMMARY_SYSTEM_PROMPT",
        "IDENTITY_EXPANSION_TOOL_SUMMARY_SYSTEM_PROMPT",
        "GITLAB_TOOL_SUMMARY_SYSTEM_PROMPT",
        "GRANT_TOOL_SUMMARY_SYSTEM_PROMPT",
        "GOOGLE_SERP_PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT",
        "GRAPH_CONSTRUCTION_SYSTEM_PROMPT",
        "GRAPH_INGEST_SYSTEM_PROMPT",
        "PACKAGE_REGISTRY_TOOL_SUMMARY_SYSTEM_PROMPT",
        "PATENT_TOOL_SUMMARY_SYSTEM_PROMPT",
        "PERSONAL_SITE_TOOL_SUMMARY_SYSTEM_PROMPT",
        "PERSON_SEARCH_TOOL_SUMMARY_SYSTEM_PROMPT",
        "PUBMED_TOOL_SUMMARY_SYSTEM_PROMPT",
        "SANCTIONS_TOOL_SUMMARY_SYSTEM_PROMPT",
        "VECTOR_INGEST_SYSTEM_PROMPT",
        "WAYBACK_TOOL_SUMMARY_SYSTEM_PROMPT",
        "WORKER_TOOL_SUMMARY_SYSTEM_PROMPT",
        "WORKER_SUMMARIZE_RECEIPT_SYSTEM_PROMPT",
    ]
    for name in prompt_names:
        setattr(system_prompts_module, name, name)
    monkeypatch.setitem(sys.modules, "system_prompts", system_prompts_module)

    target_norm_module = types.ModuleType("target_normalization")
    target_norm_module.extract_person_targets = lambda text: []
    target_norm_module.sanitize_search_tool_arguments = lambda tool, arguments, fallback_person_targets=None: arguments
    monkeypatch.setitem(sys.modules, "target_normalization", target_norm_module)

    logger_module = types.ModuleType("logger")
    logger_module.get_logger = lambda name: types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, "logger", logger_module)

    env_module = types.ModuleType("env")
    env_module.load_env = lambda: None
    monkeypatch.setitem(sys.modules, "env", env_module)

    module_path = src_root / "tool_worker_graph.py"
    spec = importlib.util.spec_from_file_location("tool_worker_graph_unit", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "tool_worker_graph_unit", module)
    spec.loader.exec_module(module)
    return module


def test_normalize_ingest_text_drops_null_source_url(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    normalized = tool_worker_graph._normalize_tool_arguments(
        "ingest_text",
        {
            "runId": "run-1",
            "text": "paper summary",
            "sourceUrl": None,
        },
    )

    assert "sourceUrl" not in normalized


def test_extract_source_url_supports_worker_url_variants(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    assert (
        tool_worker_graph._extract_source_url(
            {"profile_url": "https://example.com/profile"},
            {},
        )
        == "https://example.com/profile"
    )
    assert (
        tool_worker_graph._extract_source_url(
            {},
            {"repo_url": "https://github.com/example/project"},
        )
        == "https://github.com/example/project"
    )


def test_run_graph_ingest_worker_batches_entities_and_relations_from_graph_construction(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    class _FakeLLM:
        def complete_json(self, prompt: str, payload: Dict[str, Any], temperature: float = 0.1, timeout: int = 30, **kwargs: Any) -> Dict[str, Any]:
            return {
                "entities": [
                    {
                        "canonical_name": "Ada Lovelace",
                        "type": "Person",
                        "alt_names": ["Augusta Ada Lovelace"],
                        "attributes": ["mathematician"],
                    },
                    {
                        "canonical_name": "Analytical Engine",
                        "type": "Machine",
                        "alt_names": [],
                        "attributes": ["computing engine"],
                    },
                ],
                "relations": [
                    {
                        "src": "Ada Lovelace",
                        "dst": "Analytical Engine",
                        "canonical_name": "wrote notes on",
                        "rel_type": "WROTE_ABOUT",
                        "alt_names": [],
                    }
                ],
            }

    class _FakeToolResult:
        def __init__(self, content: Dict[str, Any]) -> None:
            self.ok = True
            self.content = content

    class _FakeMcpClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, Dict[str, Any]]] = []

        def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> _FakeToolResult:
            self.calls.append((tool_name, arguments))
            return _FakeToolResult({"entityType": arguments.get("entityType"), "relationCount": 0})

    mcp_client = _FakeMcpClient()
    result = {"target_name": "Ada Lovelace", "summary_path": "/tmp/search_results.json"}

    ingest_result = tool_worker_graph._run_graph_ingest_worker(
        llm=_FakeLLM(),
        mcp_client=mcp_client,
        run_id="123e4567-e89b-12d3-a456-426614174000",
        tool_name="google_serp_person_search",
        arguments={"target_name": "Ada Lovelace"},
        result=result,
        tool_result_summary="Ran Google SERP person search for Ada Lovelace and archived result pages.",
    )

    assert len(mcp_client.calls) == 2
    entity_tool_name, entity_args = mcp_client.calls[0]
    relation_tool_name, relation_args = mcp_client.calls[1]
    assert entity_tool_name == "ingest_graph_entities"
    assert relation_tool_name == "ingest_graph_relations"
    entities = json.loads(entity_args["entitiesJson"])
    relations = json.loads(relation_args["relationsJson"])
    assert entities[0]["canonical_name"] == "Augusta Ada Lovelace"
    assert entities[0]["osint_bucket"] == "person"
    assert relations[0]["rel_type"] == "WROTE_ABOUT"
    assert ingest_result["entityCount"] == 2
    assert ingest_result["relationCount"] == 1


def test_graph_ids_are_stable_across_runs(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    first = tool_worker_graph._stable_graph_node_id("run-1", "Person", "Ada Lovelace")
    second = tool_worker_graph._stable_graph_node_id("run-2", "Person", "Ada Lovelace")
    snippet_first = tool_worker_graph._stable_snippet_entity_id(
        "run-1",
        "google_serp_person_search",
        {"sourceUrl": "https://example.com/profile"},
        "Ada Lovelace profile summary",
    )
    snippet_second = tool_worker_graph._stable_snippet_entity_id(
        "run-2",
        "google_serp_person_search",
        {"sourceUrl": "https://example.com/profile"},
        "Ada Lovelace profile summary",
    )

    assert first == second
    assert snippet_first == snippet_second


def test_run_graph_ingest_worker_falls_back_to_legacy_snippet_when_graph_construction_is_empty(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    class _FakeLLM:
        def complete_json(self, prompt: str, payload: Dict[str, Any], temperature: float = 0.1, timeout: int = 30, **kwargs: Any) -> Dict[str, Any]:
            return {"entities": [], "relations": []}

        def refine_tool_arguments(self, prompt: str, tool_name: str, seed_args: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "runId": seed_args["runId"],
                "entityType": "Snippet",
                "entityId": seed_args["entityId"],
                "propertiesJson": seed_args["propertiesJson"],
            }

    class _FakeToolResult:
        def __init__(self, content: Dict[str, Any]) -> None:
            self.ok = True
            self.content = content

    class _FakeMcpClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, Dict[str, Any]]] = []

        def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> _FakeToolResult:
            self.calls.append((tool_name, arguments))
            return _FakeToolResult({"entityType": arguments.get("entityType"), "relationCount": 0})

    mcp_client = _FakeMcpClient()

    tool_worker_graph._run_graph_ingest_worker(
        llm=_FakeLLM(),
        mcp_client=mcp_client,
        run_id="123e4567-e89b-12d3-a456-426614174000",
        tool_name="fetch_url",
        arguments={"url": "https://example.com"},
        result={},
        tool_result_summary="Fetched a generic page with no structured entity extraction.",
    )

    assert len(mcp_client.calls) == 1
    called_tool_name, called_args = mcp_client.calls[0]
    assert called_tool_name == "ingest_graph_entity"
    assert called_args["entityType"] == "Snippet"


def test_merge_key_fact_lists_preserves_tool_specific_and_llm_facts(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    merged = tool_worker_graph._merge_key_fact_lists(
        [{"profileUrl": "https://github.com/FrederickPi"}, {"publications": [{"title": "Paper A"}]}],
        [{"source_urls": ["https://github.com/FrederickPi"]}, {"uncertainties": ["limited directory visibility"]}],
    )

    assert {"profileUrl": "https://github.com/FrederickPi"} in merged
    assert {"publications": [{"title": "Paper A"}]} in merged
    assert {"source_urls": ["https://github.com/FrederickPi"]} in merged
    assert {"uncertainties": ["limited directory visibility"]} in merged


def test_build_graph_construction_batches_merges_aliases_and_expands_semantic_graph(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    entities, relations = tool_worker_graph._build_graph_construction_batches(
        run_id="123e4567-e89b-12d3-a456-426614174000",
        tool_name="github_identity_search",
        arguments={
            "person_name": "Xinyu Pi",
            "field_keywords": ["large language models", "logical reasoning"],
        },
        result={
            "canonical_name": "Xinyu Pi",
            "profile_url": "https://github.com/jerrydwelly",
            "external_links": [
                {"type": "profile", "url": "https://html.duckduckgo.com/html/?q=site%3Agithub.com+%22Xinyu+Pi%22"},
            ],
            "organizations": [
                {
                    "name": "University of Illinois Urbana-Champaign (UIUC)",
                    "summary": "Public research university in Illinois.",
                    "focus": ["computer science", "machine learning"],
                },
                {"name": "University of Illinois at Urbana-Champaign"},
                {"name": "Stealth Startup", "relation": "member", "summary": "Applied AI startup.", "industry": "artificial intelligence"},
            ],
            "repositories": [
                {"name": "logos", "url": "https://github.com/xinyu/logos", "language": "Python"},
            ],
            "top_languages": ["Python", "TypeScript"],
            "spoken_languages": ["English"],
            "topics": ["Large Language Models", "Logical Reasoning"],
            "publications": [
                {
                    "title": "Reasoning Like Program Executors",
                    "year": "2022",
                    "conference": "EMNLP 2024",
                    "authors": ["Xinyu Pi", "Qian Liu"],
                    "affiliations": ["University of Illinois Urbana-Champaign"],
                }
            ],
            "roles": [
                {
                    "title": "PhD student",
                    "organization": "University of California, San Diego",
                    "start_date": "2021",
                    "source_url": "https://example.com/profile",
                }
            ],
        },
        extracted_graph={
            "entities": [
                {
                    "canonical_name": "University of Illinois Urbana-Champaign",
                    "type": "Institution",
                    "alt_names": [],
                    "attributes": [],
                }
            ],
            "relations": [],
        },
    )

    illinois_entities = [
        entity for entity in entities if entity["type"] == "Institution" and "illinois" in entity["canonical_name"].lower()
    ]
    assert len(illinois_entities) == 1
    assert "University of Illinois at Urbana-Champaign" in illinois_entities[0]["alt_names"]
    assert any(key.startswith("sig:org:university illinois urbana champaign") for key in illinois_entities[0]["merge_keys"])

    assert any(entity["type"] == "Repository" and entity["canonical_name"] == "logos" for entity in entities)
    assert any(entity["type"] == "Website" and entity["canonical_name"] == "GitHub profile for Xinyu Pi" for entity in entities)
    assert not any(entity["type"] == "Website" and entity["canonical_name"] == "https://github.com/jerrydwelly" for entity in entities)
    assert any(entity["type"] == "Language" and entity["canonical_name"] == "Python" for entity in entities)
    assert any(entity["type"] == "Language" and entity["canonical_name"] == "English" for entity in entities)
    assert any(entity["type"] == "Topic" and entity["canonical_name"] == "Large Language Models" for entity in entities)
    assert any(entity["type"] == "Publication" and entity["canonical_name"] == "Reasoning Like Program Executors" for entity in entities)
    assert any(entity["type"] == "Role" and entity["canonical_name"] == "PhD student at University of California, San Diego" for entity in entities)
    assert any(entity["type"] == "ContactPoint" and "xinyu pi" in " ".join(entity.get("attributes") or []).lower() for entity in entities)
    assert any(entity["type"] == "OrganizationProfile" and "University of Illinois" in entity["canonical_name"] for entity in entities)
    assert any(entity["type"] == "Experience" and "University of California, San Diego" in entity["canonical_name"] for entity in entities)
    assert any(entity["type"] == "EducationalCredential" and "University of California, San Diego" in entity["canonical_name"] for entity in entities)
    assert any(entity["type"] == "Occupation" and entity["canonical_name"] == "PhD student" for entity in entities)
    assert any(entity["type"] == "TimelineEvent" and "University of California, San Diego" in entity["canonical_name"] for entity in entities)
    assert not any("duckduckgo" in entity["canonical_name"].lower() for entity in entities)

    relation_types = {relation["rel_type"] for relation in relations}
    assert "HAS_CONTACT_POINT" in relation_types
    assert "HAS_EXPERIENCE" in relation_types
    assert "HAS_CREDENTIAL" in relation_types
    assert "HAS_AFFILIATION" in relation_types
    assert "HAS_TIMELINE_EVENT" in relation_types
    assert "HAS_ROLE" in relation_types
    assert "ISSUED_BY" in relation_types
    assert "MAINTAINS" in relation_types
    assert "USES_LANGUAGE" in relation_types
    assert "KNOWS_LANGUAGE" in relation_types
    assert "RESEARCHES" in relation_types
    assert "PUBLISHED" in relation_types
    assert "PUBLISHED_IN" in relation_types
    assert "COAUTHORED_WITH" in relation_types
    assert "HOLDS_ROLE" in relation_types
    assert "STUDIED_AT" in relation_types
    assert "HAS_ORGANIZATION_PROFILE" in relation_types


def test_store_artifacts_and_summary_collects_compact_artifact_documents(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    artifact_calls: list[dict[str, Any]] = []
    tool_worker_graph.insert_artifact = lambda **kwargs: artifact_calls.append(kwargs) or f"artifact-{len(artifact_calls)}"
    tool_worker_graph.insert_artifact_summary = (
        lambda artifact_id, summary, key_facts, confidence=None: f"summary-for-{artifact_id}"
    )

    artifact_ids, document_ids, summary_id = tool_worker_graph._store_artifacts_and_summary(
        run_id="run-1",
        tool_name="arxiv_paper_ingest",
        arguments={"runId": "run-1", "arxiv_id": "2402.04333", "person_name": "Ada Lovelace"},
        result={
            "evidence": {
                "documentId": "doc-json",
                "bucket": "osint-raw",
                "objectKey": "runs/run-1/raw/python/arxiv_paper_ingest/result.json",
            },
            "artifactDocuments": [
                {
                    "documentId": "doc-pdf",
                    "bucket": "osint-raw",
                    "objectKey": "runs/run-1/raw/python/arxiv_paper_ingest/paper.pdf",
                    "contentType": "application/pdf",
                }
            ],
        },
        summary="Fetched one arXiv paper.",
        key_facts=[{"paperTitle": "Reasoning Like Program Executors"}],
        confidence_score=0.92,
    )

    assert summary_id == "summary-for-artifact-1"
    assert artifact_ids == ["artifact-1", "artifact-2"]
    assert document_ids == ["doc-json", "doc-pdf"]
    assert artifact_calls[1]["kind"] == "artifact_document"
    assert artifact_calls[1]["document_id"] == "doc-pdf"


def test_build_graph_construction_batches_adds_coauthor_email_contacts(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    entities, relations = tool_worker_graph._build_graph_construction_batches(
        run_id="123e4567-e89b-12d3-a456-426614174000",
        tool_name="arxiv_paper_ingest",
        arguments={
            "person_name": "Ada Lovelace",
            "arxiv_id": "2402.04333",
        },
        result={
            "paper": {
                "title": "Reasoning Like Program Executors",
                "arxiv_id": "2402.04333",
                "published": "2024-02-07",
                "authors": ["Ada Lovelace", "Alan Turing"],
                "affiliations": ["Analytical Engine Institute"],
                "topics": ["reasoning", "program executors"],
                "pdf_url": "https://arxiv.org/pdf/2402.04333",
            },
            "papers": [
                {
                    "title": "Reasoning Like Program Executors",
                    "arxiv_id": "2402.04333",
                    "published": "2024-02-07",
                    "authors": ["Ada Lovelace", "Alan Turing"],
                    "affiliations": ["Analytical Engine Institute"],
                    "topics": ["reasoning", "program executors"],
                    "pdf_url": "https://arxiv.org/pdf/2402.04333",
                }
            ],
            "topics": ["reasoning", "program executors"],
            "coauthors": [
                {"name": "Alan Turing", "email": "aturing@example.edu", "match_confidence": 0.9},
            ],
            "author_contacts": [
                {"name": "Ada Lovelace", "email": "ada@example.edu", "match_confidence": 0.95},
                {"name": "Alan Turing", "email": "aturing@example.edu", "match_confidence": 0.9},
            ],
        },
        extracted_graph={"entities": [], "relations": []},
    )

    assert any(entity["type"] == "Email" and entity["canonical_name"] == "ada@example.edu" for entity in entities)
    assert any(entity["type"] == "Email" and entity["canonical_name"] == "aturing@example.edu" for entity in entities)
    assert any(entity["type"] == "Topic" and entity["canonical_name"] == "reasoning" for entity in entities)
    assert any(entity["type"] == "Publication" and entity["canonical_name"] == "Reasoning Like Program Executors" for entity in entities)

    relation_types = {relation["rel_type"] for relation in relations}
    assert "COAUTHORED_WITH" in relation_types
    assert "HAS_CONTACT_POINT" in relation_types
    assert "HAS_EMAIL" in relation_types
    assert "HAS_TOPIC" in relation_types
    assert "RESEARCHES" in relation_types

    node_ids = {entity["canonical_name"]: entity["node_id"] for entity in entities}
    assert any(
        relation["rel_type"] == "HAS_TOPIC"
        and relation["src_id"] == node_ids["Reasoning Like Program Executors"]
        and relation["dst_id"] == node_ids["reasoning"]
        for relation in relations
    )
    assert any(
        relation["rel_type"] == "RESEARCHES"
        and relation["src_id"] == node_ids["Alan Turing"]
        and relation["dst_id"] == node_ids["program executors"]
        for relation in relations
    )


def test_build_graph_construction_batches_emits_management_and_staff_histories(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    entities, relations = tool_worker_graph._build_graph_construction_batches(
        run_id="123e4567-e89b-12d3-a456-426614174000",
        tool_name="open_corporates_search",
        arguments={"company_name": "Acme Bio", "org_name": "Acme Bio"},
        result={
            "company_name": "Acme Bio",
            "company_number": "12345",
            "jurisdiction": "us_de",
            "source_url": "https://acmebio.example.com",
            "topics": ["protein engineering"],
            "officers": [
                {"name": "Grace Hopper", "position": "Chief Executive Officer", "start_date": "2020"},
            ],
            "staff": [
                {"name": "Alan Turing", "title": "Research Scientist", "source_url": "https://acmebio.example.com/team"},
            ],
            "overlaps": [
                {"name": "Barbara Liskov", "companies": ["Acme Bio", "Genome Works"], "roles": ["Director"]},
            ],
        },
        extracted_graph={"entities": [], "relations": []},
    )

    assert any(entity["type"] == "Person" and entity["canonical_name"] == "Grace Hopper" for entity in entities)
    assert any(entity["type"] == "Person" and entity["canonical_name"] == "Alan Turing" for entity in entities)
    assert any(entity["type"] == "Person" and entity["canonical_name"] == "Barbara Liskov" for entity in entities)
    assert any(entity["type"] in {"Organization", "Institution"} and entity["canonical_name"] == "Acme Bio" for entity in entities)
    assert any(entity["type"] in {"Organization", "Institution"} and entity["canonical_name"] == "Genome Works" for entity in entities)
    assert any(entity["type"] == "Topic" and entity["canonical_name"] == "protein engineering" for entity in entities)

    relation_types = {relation["rel_type"] for relation in relations}
    assert "OFFICER_OF" in relation_types
    assert "WORKS_AT" in relation_types
    assert "DIRECTOR_OF" in relation_types
    assert "HAS_EXPERIENCE" in relation_types
    assert "HAS_ROLE" in relation_types
    assert "HOLDS_ROLE" in relation_types
