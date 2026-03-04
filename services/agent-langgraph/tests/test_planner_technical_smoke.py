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
        if tool_name == "tavily_person_search":
            target_name = str(arguments.get("target_name") or "")
            query = str(arguments.get("query") or target_name)
            key_facts = [{"targetName": target_name, "query": query}]
            next_hints: List[str] = []

            if "github" in query.lower():
                key_facts.extend(
                    [
                        {"profileUrl": "https://github.com/ada"},
                        {"username": "ada"},
                        {"displayName": "Ada Lovelace"},
                    ]
                )
                next_hints.extend(["https://github.com/ada", "ada"])

            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary=f"Executed {tool_name}.",
                arguments=arguments,
                key_facts=key_facts,
                next_hints=next_hints,
            )
            return ToolWorkerResultStub(receipt=receipt, result={"query": query, "target_name": target_name})

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
                arguments=arguments,
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
                arguments=arguments,
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
                arguments=arguments,
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
                arguments=arguments,
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
                arguments=arguments,
                key_facts=[{"count": 1}],
            )
            return ToolWorkerResultStub(receipt=receipt, result={"count": 1})

        receipt = ReceiptStub(run_id=run_id, tool_name=tool_name, ok=True, summary=f"Executed {tool_name}.", arguments=arguments)
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
    tavily_github_receipts = [
        receipt
        for receipt in final_state["tool_receipts"]
        if receipt.tool_name == "tavily_person_search"
        and receipt.arguments.get("query") == "Find the public GitHub profile, account, or repositories associated with Ada Lovelace."
    ]
    assert tavily_github_receipts
    assert "github_identity_search" not in tool_names
    assert final_state["coverage_ledger"]["identity"] is True
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


def test_planner_runs_at_least_two_iterations_before_stage2(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    call_count = {"count": 0}

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any]) -> ToolWorkerResultStub:
        call_count["count"] += 1
        receipt = ReceiptStub(
            run_id=run_id,
            tool_name=tool_name,
            ok=True,
            summary=f"{tool_name} completed.",
            arguments=arguments,
            key_facts=[{"target": arguments.get("target_name") or arguments.get("person_name") or arguments.get("username")}],
        )
        return ToolWorkerResultStub(receipt=receipt, result={})

    monkeypatch.setattr(planner_graph, "run_tool_worker", fake_run_tool_worker)
    monkeypatch.setattr(planner_graph, "emit_run_event", lambda *args, **kwargs: None)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace",
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
        "max_iterations": 3,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "next_stage": "stage1",
        "queued_tasks": [],
        "related_entity_candidates": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    final_state = graph.compile().invoke(state)

    assert final_state["iteration"] >= 2
    assert final_state["next_stage"] == "stage2"
    assert call_count["count"] >= 2


def test_inject_noteboard_renders_structured_sections(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    prompt = planner_graph._inject_noteboard(
        "Investigate Ada Lovelace",
        [],
        {
            "evidence": ["Fetched profile page from example.edu."],
            "frontier": ["Discovered in-scope lab staff page."],
            "gaps": ["Current employer still unverified."],
            "follow_ups": ["Queue institutional directory lookup."],
            "depth_candidates": ["Depth candidate: organization Analytical Engine Lab."],
        },
        "Need to verify the employer before stage2.",
        [{"tool_name": "institution_directory_search", "payload": {"name": "Ada Lovelace"}}],
    )

    assert "Evidence collected:" in prompt
    assert "Open leads and frontier:" in prompt
    assert "Known gaps or unresolved questions:" in prompt
    assert "Depth candidates worth expanding:" in prompt
    assert "Next iteration To Do:" in prompt


def test_planner_review_receipts_queues_source_followups_from_search_results(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    planner_review_receipts = graph.nodes["planner_review_receipts"]

    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace",
        "inputs": ["Ada Lovelace"],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [],
        "latest_tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="tavily_person_search",
                ok=True,
                summary="Ran Tavily person search for Ada Lovelace.",
                arguments={"runId": "run-1", "target_name": "Ada Lovelace"},
                key_facts=[
                    {
                        "sourceUrls": [
                            "https://www.example.edu/people/ada-lovelace",
                            "https://arxiv.org/abs/1234.5678",
                            "https://en.wikipedia.org/wiki/Ada_Lovelace",
                        ]
                    }
                ],
                next_hints=[
                    "https://www.example.edu/people/ada-lovelace",
                    "https://arxiv.org/abs/1234.5678",
                    "https://en.wikipedia.org/wiki/Ada_Lovelace",
                ],
            )
        ],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [],
        "iteration": 0,
        "max_iterations": 3,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "next_stage": "stage1",
        "queued_tasks": [],
        "related_entity_candidates": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    updated = planner_review_receipts(state)
    queued_tools = [(item["tool_name"], item["payload"]) for item in updated["queued_tasks"]]

    assert ("fetch_url", {"runId": "run-1", "url": "https://www.example.edu/people/ada-lovelace"}) in queued_tools
    assert (
        "arxiv_paper_ingest",
        {"runId": "run-1", "paper_url": "https://arxiv.org/abs/1234.5678", "author_hint": "Ada Lovelace"},
    ) in queued_tools
    assert not any(payload.get("url") == "https://en.wikipedia.org/wiki/Ada_Lovelace" for tool, payload in queued_tools if tool == "fetch_url")


def test_planner_review_receipts_adds_fetched_host_to_allowed_hosts(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    planner_review_receipts = graph.nodes["planner_review_receipts"]

    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace",
        "inputs": ["Ada Lovelace"],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": ["https://www.acme.com/about"],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [],
        "latest_tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="fetch_url",
                ok=True,
                summary="Fetched URL.",
                arguments={"runId": "run-1", "url": "https://www.acme.com/about"},
                key_facts=[{"finalUrl": "https://www.acme.com/about"}],
                next_hints=["https://www.acme.com/team"],
            )
        ],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [],
        "iteration": 0,
        "max_iterations": 3,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "next_stage": "stage1",
        "queued_tasks": [],
        "related_entity_candidates": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    updated = planner_review_receipts(state)

    assert updated["allowed_hosts"] == ["acme.com"]
    assert updated["pending_urls"] == ["https://www.acme.com/team"]


def test_planner_review_receipts_expands_topics_management_and_org_staff(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    planner_review_receipts = graph.nodes["planner_review_receipts"]

    def fake_extract_person_targets(text: str) -> list[str]:
        matches: list[str] = []
        for name in ("Ada Lovelace", "Grace Hopper", "Alan Turing"):
            if name in (text or ""):
                matches.append(name)
        return matches

    monkeypatch.setattr(planner_graph, "extract_person_targets", fake_extract_person_targets)

    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace",
        "inputs": ["Ada Lovelace"],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [],
        "latest_tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="open_corporates_search",
                ok=True,
                summary="Resolved company Analytical Bio Systems with officer records.",
                arguments={"runId": "run-1", "company_name": "Analytical Bio Systems"},
                key_facts=[
                    {"companyName": "Analytical Bio Systems"},
                    {"officers": [{"name": "Grace Hopper", "position": "Chief Executive Officer"}]},
                ],
                next_hints=["Grace Hopper"],
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="tavily_research",
                ok=True,
                summary="Research found the company website and topic coverage.",
                arguments={"runId": "run-1", "input": "Analytical Bio Systems"},
                key_facts=[
                    {
                        "organizations": [
                            {
                                "name": "Analytical Bio Systems",
                                "url": "https://abio.example.com",
                                "topics": ["computational pathology"],
                            }
                        ]
                    },
                    {"topics": ["computational pathology"]},
                ],
                next_hints=["https://abio.example.com"],
            ),
        ],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [],
        "iteration": 0,
        "max_iterations": 3,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "next_stage": "stage1",
        "queued_tasks": [],
        "related_entity_candidates": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    updated = planner_review_receipts(state)
    assert any(
        item["tool_name"] == "company_officer_search"
        and item["payload"] == {"runId": "run-1", "person_name": "Grace Hopper", "max_results": 8}
        for item in updated["queued_tasks"]
    )
    assert any(
        item["tool_name"] == "org_staff_page_search"
        and item["payload"] == {"runId": "run-1", "org_url": "https://abio.example.com", "org_name": "Analytical Bio Systems"}
        for item in updated["queued_tasks"]
    )
    assert any(
        item["tool_name"] == "arxiv_search_and_download"
        and item["payload"] == {"runId": "run-1", "author": "Ada Lovelace", "topic": "computational pathology", "max_results": 6}
        for item in updated["queued_tasks"]
    )


def test_planner_review_receipts_filters_low_signal_source_followups(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    planner_review_receipts = graph.nodes["planner_review_receipts"]

    state = {
        "run_id": "run-1",
        "prompt": "Investigate Frederick Xinyu Pi",
        "inputs": ["Frederick Xinyu Pi"],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [],
        "latest_tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="tavily_research",
                ok=True,
                summary="Returned a mixture of official and generic links.",
                arguments={"runId": "run-1", "input": "Frederick Xinyu Pi"},
                key_facts=[
                    {
                        "sourceUrls": [
                            "https://wordunscrambler.net/unscramble/notpi",
                            "https://github.com/USPS",
                            "https://www.usps.com/",
                            "https://example.edu/people/frederick-pi",
                        ]
                    }
                ],
                next_hints=[],
            )
        ],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [],
        "iteration": 0,
        "max_iterations": 3,
        "done": False,
        "enough_info": False,
        "noteboard": [],
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "next_stage": "stage1",
        "queued_tasks": [],
        "related_entity_candidates": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    updated = planner_review_receipts(state)
    fetch_payloads = [
        item["payload"]
        for item in updated["queued_tasks"]
        if item["tool_name"] == "fetch_url"
    ]

    assert {"runId": "run-1", "url": "https://example.edu/people/frederick-pi"} in fetch_payloads
    assert {"runId": "run-1", "url": "https://wordunscrambler.net/unscramble/notpi"} not in fetch_payloads
    assert {"runId": "run-1", "url": "https://github.com/USPS"} not in fetch_payloads
    assert {"runId": "run-1", "url": "https://www.usps.com/"} not in fetch_payloads


def test_plan_tools_uses_tavily_github_search_before_github_identity_search(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    monkeypatch.setattr(planner_graph, "emit_run_event", lambda *args, **kwargs: None)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    plan_tools = graph.nodes["plan_tools"]
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
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "next_stage": "stage1",
        "queued_tasks": [],
        "related_entity_candidates": [],
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    planned = plan_tools(state)
    tavily_items = [item for item in planned["tool_plan"] if item.tool == "tavily_person_search"]
    github_items = [item for item in planned["tool_plan"] if item.tool == "github_identity_search"]

    assert any(
        item.arguments.get("query") == "Find the public GitHub profile, account, or repositories associated with Ada Lovelace."
        for item in tavily_items
    )
    assert github_items == []
