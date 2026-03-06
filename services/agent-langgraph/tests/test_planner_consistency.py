from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class ReceiptStub:
    run_id: str
    tool_name: str
    ok: bool
    summary: str
    arguments: Dict[str, Any] = field(default_factory=dict)
    argument_signature: str = ""
    artifact_ids: List[str] = field(default_factory=list)
    document_ids: List[str] = field(default_factory=list)
    key_facts: List[Dict[str, Any]] = field(default_factory=list)
    vector_upserts: Dict[str, Any] = field(default_factory=dict)
    graph_upserts: Dict[str, Any] = field(default_factory=dict)
    next_hints: List[str] = field(default_factory=list)
    next_urls: List[str] = field(default_factory=list)
    next_people: List[str] = field(default_factory=list)
    next_orgs: List[str] = field(default_factory=list)
    next_topics: List[str] = field(default_factory=list)
    next_handles: List[str] = field(default_factory=list)
    next_queries: List[str] = field(default_factory=list)


def _load_planner_graph_module(monkeypatch):
    src_root = Path(__file__).resolve().parents[1] / "src"
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    langgraph_module = types.ModuleType("langgraph")
    langgraph_graph_module = types.ModuleType("langgraph.graph")
    langgraph_graph_module.END = "__END__"
    langgraph_graph_module.StateGraph = object
    monkeypatch.setitem(sys.modules, "langgraph", langgraph_module)
    monkeypatch.setitem(sys.modules, "langgraph.graph", langgraph_graph_module)

    run_events_module = types.ModuleType("run_events")
    run_events_module.emit_run_event = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "run_events", run_events_module)

    mcp_client_module = types.ModuleType("mcp_client")
    mcp_client_module.McpClientProtocol = object
    mcp_client_module.RoutedMcpClient = object
    monkeypatch.setitem(sys.modules, "mcp_client", mcp_client_module)

    openrouter_module = types.ModuleType("openrouter_llm")
    openrouter_module.OpenRouterLLM = object
    monkeypatch.setitem(sys.modules, "openrouter_llm", openrouter_module)

    logger_module = types.ModuleType("logger")
    logger_module.get_logger = lambda name: types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
    monkeypatch.setitem(sys.modules, "logger", logger_module)

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

    env_module = types.ModuleType("env")
    env_module.load_env = lambda: None
    monkeypatch.setitem(sys.modules, "env", env_module)

    target_norm_module = types.ModuleType("target_normalization")
    target_norm_module.extract_person_targets = lambda text: ["Ada Lovelace"] if "Ada Lovelace" in (text or "") else []
    target_norm_module.normalize_person_candidate = lambda value: value
    target_norm_module.sanitize_search_tool_arguments = lambda tool, arguments, fallback_person_targets=None: arguments
    monkeypatch.setitem(sys.modules, "target_normalization", target_norm_module)

    tool_worker_module = types.ModuleType("tool_worker_graph")
    tool_worker_module.ToolReceipt = ReceiptStub
    tool_worker_module.run_tool_worker = lambda *args, **kwargs: None
    tool_worker_module.tool_argument_signature = lambda tool_name, arguments: f"{tool_name}|{str(sorted((arguments or {}).items()))}"
    monkeypatch.setitem(sys.modules, "tool_worker_graph", tool_worker_module)

    module_path = src_root / "planner_graph.py"
    spec = importlib.util.spec_from_file_location("planner_graph_consistency", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "planner_graph_consistency", module)
    spec.loader.exec_module(module)
    return module


def test_publication_contradiction_queues_follow_up(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="arxiv_search_and_download",
            ok=True,
            summary="Queried arXiv and reviewed 0 matched paper(s).",
            key_facts=[{"total_available": 0}, {"collected_count": 0}],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="semantic_scholar_search",
            ok=True,
            summary="Semantic Scholar returned a strong author candidate.",
            key_facts=[
                {
                    "candidates": [
                        {
                            "canonical_name": "Ada Lovelace",
                            "works_summary": {"paper_count": 3, "citation_count": 12},
                        }
                    ]
                }
            ],
        ),
    ]

    tasks, _, notes = planner_graph._derive_consistency_follow_up_tasks(
        run_id="run-1",
        receipts=receipts,
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    tool_names = {task.tool_name for task in tasks}
    assert {"semantic_scholar_search", "dblp_author_search", "conference_profile_search"}.issubset(tool_names)
    assert any("arXiv returned zero direct matches" in note for note in notes)


def test_relationship_contradiction_rebuilds_coauthor_graph(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Coauthor graph search did not reveal any collaborators.",
            key_facts=[],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="orcid_search",
            ok=True,
            summary="ORCID returned publications.",
            key_facts=[{"publications": [{"authors": ["Ada Lovelace", "Grace Hopper"], "title": "Analytical Engines"}]}],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="coauthor_graph_search",
            ok=True,
            summary="Relationship signals found.",
            key_facts=[{"coauthors": [{"name": "Grace Hopper", "count": 1}]}],
        ),
    ]

    tasks, _, notes = planner_graph._derive_consistency_follow_up_tasks(
        run_id="run-1",
        receipts=receipts,
        primary_person_targets=["Ada Lovelace"],
        iteration=0,
        dedupe_store={},
    )

    assert any(task.tool_name == "coauthor_graph_search" for task in tasks)
    assert any("collaborator absence claim conflicts" in note for note in notes)


def test_entity_resolution_follow_up_is_queued_from_multiple_profiles(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="github_identity_search",
            ok=True,
            summary="Resolved GitHub profile.",
            key_facts=[{"profileUrl": "https://github.com/fpi"}, {"username": "fpi"}, {"displayName": "Frederick Pi"}],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="semantic_scholar_search",
            ok=True,
            summary="Resolved academic profile.",
            key_facts=[
                {
                    "candidates": [
                        {
                            "canonical_name": "Xinyu Pi",
                            "affiliations": ["ucsd.edu"],
                            "evidence": [{"snippet": "Neural Audio Systems"}],
                        }
                    ]
                }
            ],
        ),
    ]
    tasks, _, notes = planner_graph._derive_entity_resolution_follow_up_tasks(
        run_id="run-1",
        receipts=receipts,
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "cross_platform_profile_resolver" for task in tasks)
    assert any("identity resolution" in note.lower() for note in notes)
