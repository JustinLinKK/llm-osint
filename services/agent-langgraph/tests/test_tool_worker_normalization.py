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
        tool_name="google_serp_person_search",
        arguments={"target_name": "Ada Lovelace"},
        result={"target_name": "Ada Lovelace"},
        tool_result_summary="Ran Google SERP person search for Ada Lovelace and archived result pages.",
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
