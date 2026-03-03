from __future__ import annotations

import importlib.util
import sys
import threading
import time
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


@dataclass
class ToolWorkerResultStub:
    receipt: ReceiptStub
    result: Dict[str, Any]


class _CompiledGraph:
    def __init__(self, graph: "_StateGraph") -> None:
        self._graph = graph

    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        current = self._graph.entry_point
        working = dict(state)
        while current and current != self._graph.END:
            updates = self._graph.nodes[current](working)
            if isinstance(updates, dict):
                working = updates
            if current in self._graph.conditional_edges:
                current = self._graph.conditional_edges[current](working)
                continue
            current = self._graph.edges.get(current)
        return working


class _StateGraph:
    END = "__END__"

    def __init__(self, _state_type: Any) -> None:
        self.nodes: Dict[str, Any] = {}
        self.edges: Dict[str, str] = {}
        self.conditional_edges: Dict[str, Any] = {}
        self.entry_point: str | None = None

    def add_node(self, name: str, fn: Any) -> None:
        self.nodes[name] = fn

    def set_entry_point(self, name: str) -> None:
        self.entry_point = name

    def add_edge(self, source: str, target: str) -> None:
        self.edges[source] = target

    def add_conditional_edges(self, source: str, fn: Any) -> None:
        self.conditional_edges[source] = fn

    def compile(self) -> _CompiledGraph:
        return _CompiledGraph(self)


def _load_planner_graph_module(monkeypatch):
    src_root = Path(__file__).resolve().parents[1] / "src"
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))

    langgraph_module = types.ModuleType("langgraph")
    langgraph_graph_module = types.ModuleType("langgraph.graph")
    langgraph_graph_module.END = _StateGraph.END
    langgraph_graph_module.StateGraph = _StateGraph
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
    tool_worker_module.tool_argument_signature = (
        lambda tool_name, arguments: f"{tool_name}|{str(sorted((arguments or {}).items()))}"
    )
    monkeypatch.setitem(sys.modules, "tool_worker_graph", tool_worker_module)

    module_path = src_root / "planner_graph.py"
    spec = importlib.util.spec_from_file_location("planner_graph_smoke", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "planner_graph_smoke", module)
    spec.loader.exec_module(module)
    return module


def test_planner_smoke_runs_technical_followups(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any]) -> ToolWorkerResultStub:
        if tool_name == "github_identity_search":
            result = {
                "tool": "github_identity_search",
                "stable_id": "github:42",
                "platform": "github",
                "profile_url": "https://github.com/ada",
                "created_at": "2020-01-01T00:00:00Z",
                "last_active": "2025-02-01T00:00:00Z",
                "organizations": [{"name": "acme", "url": "https://github.com/acme", "relation": "member"}],
                "repositories": [{"name": "ada/engine", "url": "https://github.com/ada/engine", "language": "Python"}],
                "publications": [],
                "contact_signals": [{"type": "email", "value": "ada@example.com", "source": "github_public_profile"}],
                "external_links": [{"type": "profile", "url": "https://github.com/ada"}, {"type": "blog", "url": "https://ada.dev"}],
                "evidence": [{"url": "https://github.com/ada", "snippet": "Ada profile"}],
                "confidence": 0.95,
                "match_features": {"reasons": ["direct username lookup"]},
                "username": "ada",
                "display_name": "Ada Lovelace",
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Resolved GitHub profile ada.",
                key_facts=[
                    {"profileUrl": "https://github.com/ada"},
                    {"username": "ada"},
                    {"blogUrl": "https://ada.dev"},
                    {"organizations": [{"name": "acme", "url": "https://github.com/acme", "relation": "member"}]},
                    {"repositories": [{"name": "ada/engine", "url": "https://github.com/ada/engine", "language": "Python"}]},
                    {"contactSignals": [{"type": "email", "value": "ada@example.com", "source": "github_public_profile"}]},
                ],
                next_hints=["https://github.com/ada", "https://ada.dev"],
            )
            return ToolWorkerResultStub(receipt=receipt, result=result)

        if tool_name == "package_registry_search":
            result = {
                "tool": "package_registry_search",
                "stable_id": "package_registry:aggregate",
                "platform": "package_registries",
                "profile_url": "",
                "created_at": None,
                "last_active": None,
                "organizations": [{"name": "@acme", "url": "https://www.npmjs.com/org/acme", "relation": "owns_namespace"}],
                "repositories": [{"name": "@acme/widget", "url": "https://github.com/acme/widget"}],
                "publications": [{"name": "@acme/widget", "url": "https://www.npmjs.com/package/@acme/widget"}],
                "contact_signals": [{"type": "npm_username", "value": "ada", "source": "npm"}],
                "external_links": [{"type": "npm_package", "url": "https://www.npmjs.com/package/@acme/widget"}],
                "evidence": [{"url": "https://www.npmjs.com/package/@acme/widget"}],
                "confidence": 0.8,
                "match_features": {"reasons": ["aggregated registry search"]},
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Registry search found package publications.",
                key_facts=[
                    {"repositories": [{"name": "@acme/widget", "url": "https://github.com/acme/widget"}]},
                    {"publications": [{"name": "@acme/widget", "url": "https://www.npmjs.com/package/@acme/widget"}]},
                ],
                next_hints=["https://github.com/acme/widget"],
            )
            return ToolWorkerResultStub(receipt=receipt, result=result)

        if tool_name == "personal_site_search":
            result = {
                "tool": "personal_site_search",
                "stable_id": "site:ada.dev",
                "platform": "website",
                "profile_url": "https://ada.dev",
                "created_at": None,
                "last_active": None,
                "organizations": [],
                "repositories": [],
                "publications": [],
                "contact_signals": [{"type": "email", "value": "ada@example.com", "source": "https://ada.dev"}],
                "external_links": [{"type": "github", "url": "https://github.com/ada"}],
                "evidence": [{"url": "https://ada.dev", "snippet": "Ada site"}],
                "confidence": 0.9,
                "match_features": {"reasons": ["direct URL matched"]},
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Resolved personal site.",
                key_facts=[
                    {"profileUrl": "https://ada.dev"},
                    {"externalLinks": [{"type": "github", "url": "https://github.com/ada"}]},
                    {"contactSignals": [{"type": "email", "value": "ada@example.com", "source": "https://ada.dev"}]},
                ],
                next_hints=["https://ada.dev", "https://github.com/ada"],
            )
            return ToolWorkerResultStub(receipt=receipt, result=result)

        if tool_name == "wayback_fetch_url":
            result = {
                "original_url": arguments.get("url"),
                "archived_url": f"https://web.archive.org/web/20240101000000/{arguments.get('url')}",
                "first_archived_at": "20230101000000",
                "last_archived_at": "20240101000000",
                "snapshots": [
                    {
                        "timestamp": "20240101000000",
                        "original_url": arguments.get("url"),
                        "archived_url": f"https://web.archive.org/web/20240101000000/{arguments.get('url')}",
                        "mime_type": "text/html",
                    }
                ],
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Wayback returned 1 snapshot.",
                key_facts=[
                    {"originalUrl": arguments.get("url")},
                    {"archivedUrl": f"https://web.archive.org/web/20240101000000/{arguments.get('url')}"},
                    {"snapshots": result["snapshots"]},
                ],
                next_hints=[f"https://web.archive.org/web/20240101000000/{arguments.get('url')}"],
            )
            return ToolWorkerResultStub(receipt=receipt, result=result)

        if tool_name == "ingest_graph_entities":
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Ingested graph entities in batch.",
                key_facts=[{"count": 1}],
            )
            return ToolWorkerResultStub(receipt=receipt, result={"count": 1})

        receipt = ReceiptStub(run_id=run_id, tool_name=tool_name, ok=True, summary=f"Executed {tool_name}.")
        return ToolWorkerResultStub(receipt=receipt, result={})

    monkeypatch.setattr(planner_graph, "run_tool_worker", fake_run_tool_worker)
    monkeypatch.setattr(planner_graph, "emit_run_event", lambda *args, **kwargs: None)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace and her public code footprint",
        "inputs": ["Ada Lovelace"],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [],
        "latest_tool_receipts": [],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [],
        "iteration": 0,
        "max_iterations": 2,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "next_stage": "stage1",
        "queued_tasks": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    final_state = graph.compile().invoke(state)

    tool_names = [receipt.tool_name for receipt in final_state["tool_receipts"]]
    assert "github_identity_search" in tool_names
    assert "ingest_graph_entities" in tool_names
    queued_tool_names = [item["tool_name"] for item in final_state["queued_tasks"]]
    assert queued_tool_names
    assert final_state["coverage_ledger"]["identity"] is True
    assert final_state["coverage_ledger"]["code_presence"] is True
    assert final_state["next_stage"] == "stage2"


def test_execute_tools_runs_workers_in_parallel_with_max_worker_limit(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    current_running = 0
    max_running = 0
    lock = threading.Lock()

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any]) -> ToolWorkerResultStub:
        nonlocal current_running, max_running
        with lock:
            current_running += 1
            max_running = max(max_running, current_running)
        time.sleep(0.05)
        with lock:
            current_running -= 1
        receipt = ReceiptStub(
            run_id=run_id,
            tool_name=tool_name,
            ok=True,
            summary=f"Executed {tool_name}.",
            arguments=arguments,
        )
        return ToolWorkerResultStub(receipt=receipt, result={})

    monkeypatch.setattr(planner_graph, "run_tool_worker", fake_run_tool_worker)
    monkeypatch.setattr(planner_graph, "emit_run_event", lambda *args, **kwargs: None)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None, max_worker=2)
    execute_tools = graph.nodes["execute_tools"]
    state = {
        "run_id": "run-1",
        "prompt": "",
        "inputs": [],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [
            planner_graph.ToolPlanItem(tool="tool_a", arguments={"runId": "run-1"}, rationale="A"),
            planner_graph.ToolPlanItem(tool="tool_b", arguments={"runId": "run-1"}, rationale="B"),
            planner_graph.ToolPlanItem(tool="tool_c", arguments={"runId": "run-1"}, rationale="C"),
        ],
        "latest_tool_receipts": [],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [],
        "iteration": 0,
        "max_iterations": 1,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "next_stage": "stage1",
        "queued_tasks": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    updated_state = execute_tools(state)

    assert [receipt.tool_name for receipt in updated_state["latest_tool_receipts"]] == [
        "tool_a",
        "tool_b",
        "tool_c",
    ]
    assert max_running == 2


def test_dedupe_tool_plan_merges_personal_site_variants(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    plan = [
        planner_graph.ToolPlanItem(
            tool="personal_site_search",
            arguments={"runId": "run-1", "url": "https://ada.dev"},
            rationale="Resolve the direct personal site URL.",
        ),
        planner_graph.ToolPlanItem(
            tool="personal_site_search",
            arguments={"runId": "run-1", "blog": "https://ada.dev/"},
            rationale="Resolve the linked blog URL.",
        ),
        planner_graph.ToolPlanItem(
            tool="personal_site_search",
            arguments={"runId": "run-1", "domain": "www.ada.dev"},
            rationale="Resolve the discovered personal domain.",
        ),
        planner_graph.ToolPlanItem(
            tool="personal_site_search",
            arguments={"runId": "run-1", "email": "ada@ada.dev"},
            rationale="Resolve the site from the discovered public email domain.",
        ),
    ]

    deduped = planner_graph._dedupe_tool_plan(plan)

    assert len(deduped) == 1
    item = deduped[0]
    assert item.tool == "personal_site_search"
    assert item.arguments["url"] == "https://ada.dev"
    assert item.arguments["blog"] == "https://ada.dev/"
    assert item.arguments["domain"] == "www.ada.dev"
    assert item.arguments["email"] == "ada@ada.dev"
