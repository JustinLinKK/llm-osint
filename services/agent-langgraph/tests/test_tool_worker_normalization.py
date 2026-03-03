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


def test_run_graph_ingest_worker_falls_back_to_seed_snippet_when_refined_args_lack_merge_key(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    class _FakeLLM:
        def refine_tool_arguments(self, prompt: str, tool_name: str, seed_args: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "runId": seed_args["runId"],
                "entityType": "Person",
                "propertiesJson": json.dumps({"profileUrls": ["https://example.com/profile"]}),
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

    tool_worker_graph._run_graph_ingest_worker(
        llm=_FakeLLM(),
        mcp_client=mcp_client,
        run_id="123e4567-e89b-12d3-a456-426614174000",
        tool_name="google_serp_person_search",
        arguments={"target_name": "Ada Lovelace"},
        result=result,
        tool_result_summary="Ran Google SERP person search for Ada Lovelace and archived result pages.",
    )

    assert len(mcp_client.calls) == 1
    called_tool_name, called_args = mcp_client.calls[0]
    assert called_tool_name == "ingest_graph_entity"
    assert called_args["entityType"] == "Snippet"
    assert called_args["entityId"].startswith("snippet:123e4567-e89b-12d3-a456-426614174000:google_serp_person_search:")


def test_run_graph_ingest_worker_drops_invalid_relations_json(monkeypatch) -> None:
    tool_worker_graph = _load_tool_worker_graph_module(monkeypatch)

    class _FakeLLM:
        def refine_tool_arguments(self, prompt: str, tool_name: str, seed_args: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "runId": seed_args["runId"],
                "entityType": "Snippet",
                "entityId": seed_args["entityId"],
                "propertiesJson": seed_args["propertiesJson"],
                "relationsJson": "{not valid json",
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
    _, called_args = mcp_client.calls[0]
    assert called_args["entityType"] == "Snippet"
    assert "relationsJson" not in called_args
