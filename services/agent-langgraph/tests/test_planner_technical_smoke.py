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
    next_urls: List[str] = field(default_factory=list)
    next_people: List[str] = field(default_factory=list)
    next_orgs: List[str] = field(default_factory=list)
    next_topics: List[str] = field(default_factory=list)
    next_handles: List[str] = field(default_factory=list)
    next_queries: List[str] = field(default_factory=list)


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
    openrouter_module.invoke_complete_json = lambda *args, **kwargs: {}
    monkeypatch.setitem(sys.modules, "openrouter_llm", openrouter_module)

    logger_module = types.ModuleType("logger")
    logger_module.get_logger = lambda name: types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    monkeypatch.setitem(sys.modules, "logger", logger_module)

    pydantic_module = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **kwargs: Any) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        @classmethod
        def model_validate(cls, payload: Dict[str, Any] | None) -> "_BaseModel":
            return cls(**(payload or {}))

        def model_dump(self) -> Dict[str, Any]:
            return dict(self.__dict__)

        def model_copy(self, *, update: Dict[str, Any] | None = None, deep: bool = False) -> "_BaseModel":
            data = dict(self.__dict__)
            if update:
                data.update(update)
            return self.__class__(**data)

    def _field(*args: Any, default_factory=None, **kwargs: Any) -> Any:
        if default_factory is not None:
            return default_factory()
        return kwargs.get("default")

    pydantic_module.BaseModel = _BaseModel
    pydantic_module.Field = _field
    monkeypatch.setitem(sys.modules, "pydantic", pydantic_module)

    report_models_module = types.ModuleType("report_models")

    class _PrimaryTargetContractModel(_BaseModel):
        def __init__(
            self,
            target_type: str = "person",
            prompt_targets: List[str] | None = None,
            canonical_name: str = "",
            approved_aliases: List[str] | None = None,
            approved_handles: List[str] | None = None,
            approved_domains: List[str] | None = None,
            root_entity_id: str = "",
            anchor_receipt_ids: List[str] | None = None,
            locked_iteration: int = 0,
            lock_reason: str = "",
        ) -> None:
            super().__init__(
                target_type=target_type,
                prompt_targets=list(prompt_targets or []),
                canonical_name=canonical_name,
                approved_aliases=list(approved_aliases or []),
                approved_handles=list(approved_handles or []),
                approved_domains=list(approved_domains or []),
                root_entity_id=root_entity_id,
                anchor_receipt_ids=list(anchor_receipt_ids or []),
                locked_iteration=locked_iteration,
                lock_reason=lock_reason,
            )

    class _PrimaryGraphTemplateModel(_BaseModel):
        def __init__(
            self,
            root_entity_id: str = "",
            root_entity_type: str = "Person",
            root_canonical_name: str = "",
            root_aliases: List[str] | None = None,
            approved_handles: List[str] | None = None,
            approved_domains: List[str] | None = None,
            first_hop_entity_categories: List[str] | None = None,
            first_hop_relation_categories: List[str] | None = None,
            template_mode: str = "metadata_only",
        ) -> None:
            super().__init__(
                root_entity_id=root_entity_id,
                root_entity_type=root_entity_type,
                root_canonical_name=root_canonical_name,
                root_aliases=list(root_aliases or []),
                approved_handles=list(approved_handles or []),
                approved_domains=list(approved_domains or []),
                first_hop_entity_categories=list(first_hop_entity_categories or []),
                first_hop_relation_categories=list(first_hop_relation_categories or []),
                template_mode=template_mode,
            )

    report_models_module.PrimaryTargetContractModel = _PrimaryTargetContractModel
    report_models_module.PrimaryGraphTemplateModel = _PrimaryGraphTemplateModel
    monkeypatch.setitem(sys.modules, "report_models", report_models_module)

    run_store_module = types.ModuleType("run_store")
    run_store_module.persist_primary_target_contract = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "run_store", run_store_module)

    env_module = types.ModuleType("env")
    env_module.load_env = lambda: None
    monkeypatch.setitem(sys.modules, "env", env_module)

    target_norm_module = types.ModuleType("target_normalization")

    def _extract_person_targets(text: str) -> list[str]:
        matches: list[str] = []
        for name in (
            "Ada Lovelace",
            "Grace Hopper",
            "Alan Turing",
            "Xinyu Pi",
            "Frederick Xinyu Pi",
            "Frederick Pi",
            "Jingbin Lin",
            "Quanhui Jia",
            "Shuang Yang",
            "Justin Lin",
        ):
            if name in (text or ""):
                matches.append(name)
        return matches

    target_norm_module.extract_person_targets = _extract_person_targets
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


def test_extract_primary_person_targets_prefers_canonical_receipt_identity(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    state = {
        "prompt": "profile Xinyu Pi",
        "inputs": [],
        "tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="cross_platform_profile_resolver",
                ok=True,
                summary="Resolved canonical identity.",
                key_facts=[
                    {
                        "canonical_identity": {
                            "canonical_name": "Frederick Xinyu Pi",
                            "aliases": ["Xinyu Pi"],
                        }
                    }
                ],
            )
        ],
    }

    targets = planner_graph._extract_primary_person_targets(state)

    assert targets[0] == "Frederick Xinyu Pi"


def test_planner_graph_inserts_normalize_graph_structure_before_conflict_resolution(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)

    assert graph.edges["analyze_input"] == "bootstrap_primary_root"
    assert graph.edges["bootstrap_primary_root"] == "plan_tools"
    assert graph.edges["planner_review_receipts"] == "normalize_graph_structure"
    assert graph.edges["normalize_graph_structure"] == "resolve_graph_conflicts"


def test_conflict_solver_model_defaults_to_planner_model_and_allows_override(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    monkeypatch.setenv("OPENROUTER_MODEL", "base-model")
    monkeypatch.setenv("OPENROUTER_PLANNER_MODEL", "planner-model")
    monkeypatch.delenv("OPENROUTER_CONFLICT_SOLVER_MODEL", raising=False)

    assert planner_graph._planner_model_name() == "planner-model"
    assert planner_graph._conflict_solver_model_name("planner-model") == "planner-model"

    monkeypatch.setenv("OPENROUTER_CONFLICT_SOLVER_MODEL", "conflict-model")

    assert planner_graph._conflict_solver_model_name("planner-model") == "conflict-model"


def test_blueprint_contract_candidates_include_repo_schema_fallback(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    candidates = planner_graph._stage1_blueprint_contract_candidates(
        "/app/schemas/stage1_graph_blueprint_contract.v1.json"
    )

    assert any(path.name == "stage1_graph_blueprint_contract.v1.json" for path in candidates)
    assert any(path.is_file() for path in candidates)


def test_planner_smoke_runs_technical_followups(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any], **_kwargs: Any) -> ToolWorkerResultStub:
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
    tavily_research_receipts = [
        receipt
        for receipt in final_state["tool_receipts"]
        if receipt.tool_name == "tavily_research"
        and receipt.arguments.get("input") == "Find public information about Ada Lovelace, including biography, affiliations, publications, employment history, and online presence."
    ]
    assert tavily_research_receipts
    assert "github_identity_search" in tool_names
    assert final_state["coverage_ledger"]["identity"] is True
    assert final_state["next_stage"] == "stage2"


def test_execute_tools_runs_workers_in_parallel_with_max_worker_limit(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    current_running = 0
    max_running = 0
    lock = threading.Lock()

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any], **_kwargs: Any) -> ToolWorkerResultStub:
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


def test_execute_tools_returns_failed_receipt_when_worker_raises(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any], **_kwargs: Any) -> ToolWorkerResultStub:
        if tool_name == "tool_b":
            raise RuntimeError("transient worker failure")
        receipt = ReceiptStub(
            run_id=run_id,
            tool_name=tool_name,
            ok=True,
            summary=f"{tool_name} completed.",
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
        "depth_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    updated_state = execute_tools(state)
    receipts = updated_state["latest_tool_receipts"]

    assert [receipt.tool_name for receipt in receipts] == ["tool_a", "tool_b", "tool_c"]
    assert [receipt.ok for receipt in receipts] == [True, False, True]
    assert "transient worker failure" in receipts[1].summary


def test_planner_runs_at_least_two_iterations_before_stage2(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    call_count = {"count": 0}

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any], **_kwargs: Any) -> ToolWorkerResultStub:
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

    assert (
        "extract_webpage",
        {
            "runId": "run-1",
            "url": "https://www.example.edu/people/ada-lovelace",
            "query": (
                "Extract the sections of this page most relevant to Ada Lovelace, especially identity, "
                "biography, affiliation, relationship, contact, and timeline evidence."
            ),
            "chunks_per_source": 5,
            "extract_depth": "advanced",
            "format": "text",
        },
    ) in queued_tools
    assert (
        "arxiv_paper_ingest",
        {"runId": "run-1", "paper_url": "https://arxiv.org/abs/1234.5678", "author_hint": "Ada Lovelace"},
    ) in queued_tools
    assert not any(
        payload.get("url") == "https://en.wikipedia.org/wiki/Ada_Lovelace"
        for tool, payload in queued_tools
        if tool == "extract_webpage"
    )


def test_planner_review_receipts_adds_extracted_host_to_allowed_hosts(monkeypatch) -> None:
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
                tool_name="extract_webpage",
                ok=True,
                summary="Extracted URL.",
                arguments={"runId": "run-1", "url": "https://www.acme.com/about"},
                key_facts=[{"url": "https://www.acme.com/about"}],
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
                summary="Resolved company Analytical Bio Systems tied to Ada Lovelace with officer records.",
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
    extract_payloads = [
        item["payload"]
        for item in updated["queued_tasks"]
        if item["tool_name"] == "extract_webpage"
    ]

    expected_payload = next(
        payload
        for payload in extract_payloads
        if payload.get("url") == "https://example.edu/people/frederick-pi"
    )
    assert expected_payload["chunks_per_source"] == 5
    assert expected_payload["extract_depth"] == "advanced"
    assert expected_payload["format"] == "text"
    assert ("Frederick Xinyu Pi" in expected_payload["query"]) or ("Xinyu Pi" in expected_payload["query"])
    assert not any(payload.get("url") == "https://wordunscrambler.net/unscramble/notpi" for payload in extract_payloads)
    assert not any(payload.get("url") == "https://github.com/USPS" for payload in extract_payloads)
    assert not any(payload.get("url") == "https://www.usps.com/" for payload in extract_payloads)


def test_plan_tools_uses_tavily_extract_for_crawl_frontier(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    monkeypatch.setattr(planner_graph, "emit_run_event", lambda *args, **kwargs: None)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    plan_tools = graph.nodes["plan_tools"]
    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace",
        "inputs": ["Ada Lovelace"],
        "seed_urls": ["https://www.example.edu/people/ada-lovelace"],
        "pending_urls": ["https://www.example.edu/people/ada-lovelace"],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": ["example.edu"],
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

    assert any(
        item.tool == "extract_webpage"
        and item.arguments.get("url") == "https://www.example.edu/people/ada-lovelace"
        and item.arguments.get("chunks_per_source") == 5
        and item.arguments.get("extract_depth") == "advanced"
        and item.arguments.get("format") == "text"
        for item in planned["tool_plan"]
    )
    assert not any(item.tool == "fetch_url" for item in planned["tool_plan"])


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
    tavily_items = [item for item in planned["tool_plan"] if item.tool == "tavily_research"]
    github_items = [item for item in planned["tool_plan"] if item.tool == "github_identity_search"]

    assert any(
        item.arguments.get("input") == "Find public information about Ada Lovelace, including biography, affiliations, publications, employment history, and online presence."
        for item in tavily_items
    )
    assert github_items
    tavily_index = next(index for index, item in enumerate(planned["tool_plan"]) if item.tool == "tavily_research")
    github_index = next(index for index, item in enumerate(planned["tool_plan"]) if item.tool == "github_identity_search")
    assert tavily_index < github_index


def test_graph_planner_hints_cover_topic_and_timeline_mentions(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    hints = planner_graph._graph_planner_hints(
        "person",
        ["topic_surface", "timeline_mention_surface"],
    )
    joined = " ".join(hints).lower()
    assert "github_identity_search" in joined
    assert "x_get_user_posts_api" in joined
    assert "linkedin_download_html_ocr" in joined


def test_graph_stop_gate_blocks_when_balanced_required_slot_missing(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    ok, note = planner_graph._graph_stop_gate(
        {},
        {
            "enabled": True,
            "status": "ready",
            "query_terms": ["Ada Lovelace"],
            "missing_slots": ["time_node_surface", "topic_surface"],
            "blueprint_enabled": True,
            "blueprint_enforcement": "balanced",
            "blueprint_required_slots": ["time_node_surface", "topic_surface"],
        },
    )

    assert ok is False
    assert "time-node surface" in note


def test_graph_stop_gate_allows_fallback_when_graph_tools_unavailable(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    ok, note = planner_graph._graph_stop_gate(
        {},
        {
            "enabled": True,
            "status": "tool_unavailable",
            "query_terms": ["Ada Lovelace"],
            "missing_slots": ["time_node_surface"],
            "blueprint_enabled": True,
            "blueprint_enforcement": "balanced",
            "blueprint_required_slots": ["time_node_surface"],
        },
    )

    assert ok is True
    assert note == ""


def test_plan_item_priority_boosts_topic_surface_gap_tools(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    state = {
        "prompt": "Investigate Ada Lovelace",
        "inputs": ["Ada Lovelace"],
        "noteboard": [],
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "tool_receipts": [],
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
        "graph_state_snapshot": {
            "missing_slots": ["topic_surface"],
        },
        "queued_tasks": [],
    }
    high = planner_graph._plan_item_priority(
        state,
        planner_graph.ToolPlanItem(
            tool="github_identity_search",
            arguments={"runId": "run-1", "person_name": "Ada Lovelace"},
            rationale="",
        ),
    )
    low = planner_graph._plan_item_priority(
        state,
        planner_graph.ToolPlanItem(
            tool="sanctions_watchlist_search",
            arguments={"runId": "run-1", "person_name": "Ada Lovelace"},
            rationale="",
        ),
    )
    assert high > low


def test_plan_item_priority_prefers_username_permutation_when_code_gap_open(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    coverage = planner_graph.empty_coverage_ledger()
    for key in list(coverage.keys()):
        coverage[key] = True
    coverage["code_presence"] = False
    coverage["aliases"] = False

    state = {
        "prompt": "Investigate Ada Lovelace",
        "inputs": ["Ada Lovelace"],
        "noteboard": [],
        "noteboard_sections": planner_graph._empty_noteboard_sections(),
        "tool_receipts": [],
        "coverage_ledger": coverage,
        "graph_state_snapshot": {
            "missing_slots": [],
        },
        "queued_tasks": [],
    }

    username_priority = planner_graph._plan_item_priority(
        state,
        planner_graph.ToolPlanItem(
            tool="username_permutation_search",
            arguments={"runId": "run-1", "username": "xinyu.pi"},
            rationale="",
        ),
    )
    tavily_priority = planner_graph._plan_item_priority(
        state,
        planner_graph.ToolPlanItem(
            tool="tavily_research",
            arguments={"runId": "run-1", "input": "Ada Lovelace"},
            rationale="",
        ),
    )

    assert username_priority > tavily_priority


def test_social_timeline_retry_cap_blocks_further_scheduling(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    state = {
        "tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="x_get_user_posts_api",
                ok=False,
                summary="x_get_user_posts_api failed.",
                arguments={"runId": "run-1", "username": "ada", "max_results": 10},
                argument_signature=planner_graph.tool_argument_signature(
                    "x_get_user_posts_api",
                    {"runId": "run-1", "username": "ada", "max_results": 10},
                ),
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="x_get_user_posts_api",
                ok=False,
                summary="x_get_user_posts_api failed again.",
                arguments={"runId": "run-1", "username": "ada", "max_results": 10},
                argument_signature=planner_graph.tool_argument_signature(
                    "x_get_user_posts_api",
                    {"runId": "run-1", "username": "ada", "max_results": 10},
                ),
            ),
        ]
    }

    should_schedule = planner_graph._should_schedule_social_timeline_tool(
        state,
        "x_get_user_posts_api",
        {"runId": "run-1", "username": "ada", "max_results": 10},
    )
    assert should_schedule is False


def test_graph_stop_gate_waives_social_timeline_slots_after_retry_exhaustion(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    state = {
        "tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="x_get_user_posts_api",
                ok=False,
                summary="x_get_user_posts_api failed.",
                arguments={"runId": "run-1", "username": "ada", "max_results": 10},
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="x_get_user_posts_api",
                ok=False,
                summary="x_get_user_posts_api failed again.",
                arguments={"runId": "run-1", "username": "ada", "max_results": 10},
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="linkedin_download_html_ocr",
                ok=False,
                summary="linkedin_download_html_ocr failed.",
                arguments={"runId": "run-1", "profile": "https://linkedin.com/in/ada"},
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="linkedin_download_html_ocr",
                ok=False,
                summary="linkedin_download_html_ocr failed again.",
                arguments={"runId": "run-1", "profile": "https://linkedin.com/in/ada"},
            ),
        ]
    }

    ok, note = planner_graph._graph_stop_gate(
        state,
        {
            "enabled": True,
            "status": "ready",
            "query_terms": ["Ada Lovelace"],
            "missing_slots": ["timeline_mention_surface", "time_node_surface"],
            "blueprint_enabled": True,
            "blueprint_enforcement": "balanced",
            "blueprint_required_slots": ["timeline_mention_surface", "time_node_surface"],
        },
    )
    assert ok is True
    assert note == ""


def test_adjudicate_related_entity_candidates_uses_llm_override_for_anchored_person(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    class FakeLLM:
        def complete_json(self, system_prompt, user_payload, temperature=0.1, timeout=None, run_id=None, operation=None):
            assert operation == "planner.related_entity_adjudication"
            return {
                "candidates": [
                    {
                        "input_name": "Yan Gao",
                        "canonical_name": "Yan Gao",
                        "entity_type": "person",
                        "confidence": 0.92,
                        "expandable": True,
                        "reason": "Evidence indicates a specific coauthor tied to the primary target.",
                        "supporting_spans": ["coauthors: Xinyu Pi, Yan Gao"],
                    }
                ]
            }

    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="semantic_scholar_search",
            ok=True,
            summary="Semantic Scholar returned Xinyu Pi at UC San Diego.",
            key_facts=[
                {"sourceUrls": ["https://www.semanticscholar.org/author/123", "https://cse.ucsd.edu/people/xinyu-pi"]},
                {"organizations": ["University of California, San Diego"]},
            ],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="tavily_research",
            ok=True,
            summary="Publication snippet mentions Xinyu Pi and Yan Gao as coauthors.",
            key_facts=[
                {"relatedPeople": ["Yan Gao"]},
                {"coauthors": [{"name": "Yan Gao"}, {"name": "Xinyu Pi"}]},
                {"sourceUrls": ["https://example.edu/papers/xinyu-pi-yan-gao"]},
            ],
        ),
    ]

    candidates, _notes = planner_graph._adjudicate_related_entity_candidates(
        llm=FakeLLM(),
        run_id="run-1",
        receipts=receipts,
        primary_person_targets=["Xinyu Pi"],
        candidates=[
            {
                "entity_name": "Yan Gao",
                "entity_type": "person",
                "relationship_types": ["COAUTHORED_WITH"],
                "supporting_tools": ["tavily_research"],
                "domains": ["example.edu"],
                "urls": ["https://example.edu/papers/xinyu-pi-yan-gao"],
                "mention_count": 1,
                "score": 2,
            }
        ],
    )

    assert candidates[0]["entity_type"] == "person"
    assert candidates[0]["expandable"] is True
    assert candidates[0]["adjudication_source"] == "llm"


def test_adjudicate_related_entity_candidates_filters_generic_location_candidates(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    candidates, notes = planner_graph._adjudicate_related_entity_candidates(
        llm=None,
        run_id="run-1",
        receipts=[],
        primary_person_targets=["Frederick Xinyu Pi"],
        candidates=[
            {
                "entity_name": "United States",
                "entity_type": "person",
                "relationship_types": ["ASSOCIATE_OF"],
                "supporting_tools": ["tavily_research"],
                "domains": ["example.com"],
                "urls": ["https://example.com/frederick-pi"],
                "mention_count": 1,
                "score": 2,
            }
        ],
    )

    assert candidates == []
    assert any("classified as location" in note for note in notes)


def test_planner_has_sufficient_related_entity_depth_ignores_location_candidates(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    assert planner_graph._planner_has_sufficient_related_entity_depth(
        {
            "related_entity_candidates": [
                {
                    "entity_name": "United States",
                    "entity_type": "location",
                    "anchored": False,
                }
            ],
            "tool_receipts": [],
        }
    )


def test_planner_review_receipts_defers_unanchored_secondary_people_in_simple_scholar_mode(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    planner_review_receipts = graph.nodes["planner_review_receipts"]

    state = {
        "run_id": "run-1",
        "prompt": "Investigate Xinyu Pi",
        "inputs": ["Xinyu Pi"],
        "seed_urls": [],
        "pending_urls": [],
        "current_fetch_urls": [],
        "visited_urls": [],
        "allowed_hosts": [],
        "tool_plan": [],
        "latest_tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="semantic_scholar_search",
                ok=True,
                summary="Semantic Scholar resolved Xinyu Pi at UC San Diego.",
                key_facts=[
                    {
                        "candidates": [
                            {
                                "canonical_name": "Xinyu Pi",
                                "affiliations": ["University of California, San Diego"],
                                "evidence": [{"snippet": "Xinyu Pi, University of California, San Diego"}],
                            }
                        ]
                    },
                    {"sourceUrls": ["https://www.semanticscholar.org/author/123", "https://cse.ucsd.edu/people/xinyu-pi"]},
                    {"organizations": ["University of California, San Diego"]},
                ],
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="tavily_research",
                ok=True,
                summary="Search surfaced multiple Yan Gao profiles across unrelated institutions.",
                arguments={"runId": "run-1", "input": "Yan Gao"},
                key_facts=[
                    {"relatedPeople": ["Yan Gao"]},
                    {"coauthors": [{"name": "Yan Gao", "count": 1}]},
                    {
                        "sourceUrls": [
                            "https://www.mcw.edu/departments/biostatistics/faculty/yan-gao",
                            "https://theorg.com/org/flower-labs/org-chart/yan-gao",
                            "https://icmab.es/researchers/yan-gao",
                        ]
                    },
                ],
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

    assert not any(
        item["tool_name"] in {"tavily_research", "person_search", "github_identity_search", "tavily_person_search"}
        and (
            item["payload"].get("input") == "Yan Gao"
            or item["payload"].get("name") == "Yan Gao"
            or item["payload"].get("person_name") == "Yan Gao"
            or item["payload"].get("target_name") == "Yan Gao"
        )
        for item in updated["queued_tasks"]
    )
    assert any("simple scholar mode" in item.lower() for item in updated["noteboard_sections"]["depth_candidates"])


def test_related_entity_follow_up_tasks_use_budgeted_non_tavily_depth_tasks(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    tasks, _dedupe_store, _notes = planner_graph._derive_related_entity_expansion_follow_up_tasks(
        run_id="run-1",
        receipts=[],
        candidates=[
            {
                "entity_name": "Frederick Pi",
                "entity_type": "person",
                "expandable": True,
                "relationship_types": ["COAUTHORED_WITH"],
                "supporting_tools": ["semantic_scholar_search"],
                "domains": [],
                "urls": ["https://example.edu/people/frederick-pi"],
                "anchor_types": ["url"],
                "anchored": True,
                "anchor_score": 3.0,
                "anchor_reasons": ["shared_url"],
            },
            {
                "entity_name": "Scientific Reports",
                "entity_type": "topic",
                "expandable": True,
                "relationship_types": [],
                "supporting_tools": ["tavily_research"],
                "domains": [],
                "urls": [],
            },
            {
                "entity_name": "OpenAI",
                "entity_type": "organization",
                "expandable": True,
                "relationship_types": [],
                "supporting_tools": ["company_officer_search"],
                "domains": [],
                "urls": [],
            },
        ],
        primary_person_targets=["Frederick Xinyu Pi"],
        secondary_person_names=[],
        coauthor_person_names=[],
        iteration=1,
        dedupe_store={},
        allow_related_person_depth=True,
        allow_related_topic_depth=True,
    )

    tool_names = {item.tool_name for item in tasks}
    assert "tavily_research" not in tool_names
    assert {"person_search", "github_identity_search", "semantic_scholar_search"}.issubset(tool_names)
    assert "open_corporates_search" in tool_names
    assert "arxiv_search_and_download" in tool_names


def test_validate_llm_plan_items_drops_unanchored_person_like_tavily_research(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    plan = [
        planner_graph.ToolPlanItem(
            tool="tavily_research",
            arguments={"runId": "run-1", "input": "Scientific Reports", "timeout_seconds": 180},
            rationale="Expand related-person coverage for discovered person: Scientific Reports",
        )
    ]

    filtered, notes = planner_graph._validate_llm_plan_items(
        state={"run_id": "run-1", "tool_receipts": []},
        llm=None,
        plan=plan,
        primary_person_targets=["Xinyu Pi"],
    )

    assert filtered == []
    assert any("Scientific Reports" in note for note in notes)


def test_filter_tool_plan_for_budgets_dedupes_tavily_person_targets_and_caps_total(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    state = {
        "run_id": "run-1",
        "primary_target_contract": planner_graph.PrimaryTargetContractModel(
            canonical_name="Ada Lovelace",
            prompt_targets=["Ada Lovelace"],
            approved_aliases=["Ada Byron"],
        ),
        "secondary_person_names": [],
        "coauthor_person_names": [],
        "tavily_call_counter": 0,
        "tavily_person_counter": {},
        "tavily_crawl_counter": {},
        "related_entity_candidates": [],
    }
    plan = [
        planner_graph.ToolPlanItem(
            tool="tavily_person_search",
            arguments={"runId": "run-1", "target_name": "Ada Lovelace", "query": "Find Ada Lovelace", "max_results": 5},
            rationale="Search Ada once.",
        ),
        planner_graph.ToolPlanItem(
            tool="tavily_person_search",
            arguments={"runId": "run-1", "target_name": "Ada Lovelace", "query": "Find Ada Lovelace again", "max_results": 5},
            rationale="Duplicate Tavily search should be dropped.",
        ),
        planner_graph.ToolPlanItem(
            tool="tavily_research",
            arguments={"runId": "run-1", "target_name": "Ada Lovelace", "input": "Find Ada Lovelace", "timeout_seconds": 180},
            rationale="Same-person Tavily research should be dropped after one Tavily person call.",
        ),
        planner_graph.ToolPlanItem(
            tool="extract_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/1", "query": "Ada"},
            rationale="Extract 1.",
        ),
        planner_graph.ToolPlanItem(
            tool="crawl_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/2"},
            rationale="Crawl 2.",
        ),
        planner_graph.ToolPlanItem(
            tool="map_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/3"},
            rationale="Map 3.",
        ),
        planner_graph.ToolPlanItem(
            tool="extract_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/4", "query": "Ada"},
            rationale="Extract 4.",
        ),
        planner_graph.ToolPlanItem(
            tool="map_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/5"},
            rationale="Map 5 should exceed total Tavily budget.",
        ),
    ]

    filtered = planner_graph._filter_tool_plan_for_budgets(state, plan)
    tavily_tools = [
        item for item in filtered if item.tool in {"tavily_research", "tavily_person_search", "extract_webpage", "crawl_webpage", "map_webpage"}
    ]

    assert len(tavily_tools) == planner_graph.STAGE1_TAVILY_TOTAL_BUDGET
    assert sum(1 for item in filtered if item.tool == "tavily_person_search") == 1
    assert not any(item.tool == "tavily_research" for item in filtered)
    assert not any(item.arguments.get("url") == "https://example.com/5" for item in filtered)


def test_filter_tool_plan_for_budgets_skips_normalization_for_non_person_tools(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    def strict_normalize_person_candidate(value: str) -> str | None:
        normalized = " ".join(value.strip(" \t\r\n:;,.!?-").split())
        return normalized or None

    planner_graph.normalize_person_candidate = strict_normalize_person_candidate

    state = {
        "run_id": "run-1",
        "primary_target_contract": planner_graph.PrimaryTargetContractModel(canonical_name="Ada Lovelace"),
        "secondary_person_names": [],
        "coauthor_person_names": [],
        "tavily_call_counter": 0,
        "tavily_person_counter": {},
        "tavily_crawl_counter": {},
        "related_entity_candidates": [],
    }
    plan = [
        planner_graph.ToolPlanItem(
            tool="extract_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/profile"},
            rationale="Extract the page without a person target.",
        )
    ]

    filtered = planner_graph._filter_tool_plan_for_budgets(state, plan)

    assert [item.tool for item in filtered] == ["extract_webpage"]


def test_filter_tool_plan_for_budgets_drops_invalid_google_serp_person_targets(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    state = {
        "run_id": "run-1",
        "primary_target_contract": planner_graph.PrimaryTargetContractModel(canonical_name="Ada Lovelace"),
        "secondary_person_names": [],
        "coauthor_person_names": [],
        "tavily_call_counter": 0,
        "tavily_person_counter": {},
        "tavily_crawl_counter": {},
        "related_entity_candidates": [],
    }
    plan = [
        planner_graph.ToolPlanItem(
            tool="google_serp_person_search",
            arguments={
                "runId": "run-1",
                "target_name": 'site:scholar.google.com/citations "United States"',
                "max_results": 8,
            },
            rationale="Invalid scholar query should be dropped.",
        ),
        planner_graph.ToolPlanItem(
            tool="google_serp_person_search",
            arguments={
                "runId": "run-1",
                "target_name": 'site:scholar.google.com/citations "Ada Lovelace"',
                "max_results": 8,
            },
            rationale="Valid scholar query should remain.",
        ),
    ]

    filtered = planner_graph._filter_tool_plan_for_budgets(state, plan)

    assert len(filtered) == 1
    assert filtered[0].tool == "google_serp_person_search"
    assert filtered[0].arguments["target_name"] == 'site:scholar.google.com/citations "Ada Lovelace"'


def test_filter_tool_plan_for_budgets_drops_invalid_person_search_targets(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    state = {
        "run_id": "run-1",
        "primary_target_contract": planner_graph.PrimaryTargetContractModel(canonical_name="Ada Lovelace"),
        "secondary_person_names": [],
        "coauthor_person_names": [],
        "tavily_call_counter": 0,
        "tavily_person_counter": {},
        "tavily_crawl_counter": {},
        "related_entity_candidates": [],
    }
    plan = [
        planner_graph.ToolPlanItem(
            tool="person_search",
            arguments={
                "runId": "run-1",
                "name": "La Jolla Shores",
                "max_results": 8,
            },
            rationale="Invalid location-like person target should be dropped.",
        ),
        planner_graph.ToolPlanItem(
            tool="person_search",
            arguments={
                "runId": "run-1",
                "name": "Ada Lovelace",
                "max_results": 8,
            },
            rationale="Valid person target should remain.",
        ),
    ]

    filtered = planner_graph._filter_tool_plan_for_budgets(state, plan)

    assert len(filtered) == 1
    assert filtered[0].tool == "person_search"
    assert filtered[0].arguments["name"] == "Ada Lovelace"


def test_related_entity_follow_up_tasks_respect_secondary_and_coauthor_budgets(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    tasks, _dedupe_store, notes = planner_graph._derive_related_entity_expansion_follow_up_tasks(
        run_id="run-1",
        receipts=[],
        candidates=[
            {
                "entity_name": "Grace Hopper",
                "entity_type": "person",
                "expandable": True,
                "relationship_types": ["COAUTHORED_WITH"],
                "supporting_tools": ["semantic_scholar_search"],
                "domains": [],
                "urls": ["https://example.edu/people/grace-hopper"],
                "anchor_types": ["url"],
                "anchored": True,
                "anchor_score": 3.0,
                "anchor_reasons": ["shared_url"],
            }
        ],
        primary_person_targets=["Ada Lovelace"],
        secondary_person_names=["Frederick Pi", "Alan Turing"],
        coauthor_person_names=["Frederick Pi", "Alan Turing"],
        iteration=2,
        dedupe_store={},
        allow_related_person_depth=True,
        allow_related_topic_depth=True,
    )

    assert tasks == []
    assert any("secondary-person budget" in note or "coauthor budget" in note for note in notes)


def test_rewrite_crawl_plan_fetches_first_and_skips_after_meaningful_fetch(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    crawl_plan = [
        planner_graph.ToolPlanItem(
            tool="crawl_webpage",
            arguments={"runId": "run-1", "url": "https://example.com/profile"},
            rationale="Escalate to crawl.",
        )
    ]

    rewritten = planner_graph._rewrite_crawl_plan_with_fetch_fallback(
        {"run_id": "run-1", "tool_receipts": []},
        crawl_plan,
    )
    assert [item.tool for item in rewritten] == ["fetch_url"]
    assert rewritten[0].arguments["url"] == "https://example.com/profile"

    meaningful_fetch = ReceiptStub(
        run_id="run-1",
        tool_name="fetch_url",
        ok=True,
        summary="Fetched profile page.",
        arguments={"url": "https://example.com/profile"},
        document_ids=["doc-1"],
        key_facts=[
            {
                "url": "https://example.com/profile",
                "statusCode": 200,
                "title": "Ada Lovelace profile",
                "contentType": "text/html",
                "sizeBytes": 4096,
            }
        ],
    )
    skipped = planner_graph._rewrite_crawl_plan_with_fetch_fallback(
        {"run_id": "run-1", "tool_receipts": [meaningful_fetch]},
        crawl_plan,
    )
    assert skipped == []


def test_primary_target_contract_stays_locked_when_coauthor_cluster_is_noisy(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    initial_contract = planner_graph._initial_primary_target_contract(
        "Investigate Frederick Xinyu Pi",
        ["Frederick Xinyu Pi"],
    )
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="arxiv_search_and_download",
            ok=True,
            summary="Paper by Frederick Xinyu Pi with Ruolan Yang and Xinqi Huang.",
            key_facts=[{"authors": ["Frederick Xinyu Pi", "Ruolan Yang", "Xinqi Huang"]}],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="cross_platform_profile_resolver",
            ok=True,
            summary="Resolved canonical identity Ruolan Yang.",
            key_facts=[
                {
                    "canonical_identity": {
                        "canonical_name": "Ruolan Yang",
                        "aliases": ["Stone Tao", "Xinyu Fang"],
                    }
                }
            ],
        ),
    ]

    locked_contract = planner_graph._enrich_primary_target_contract(initial_contract, receipts, iteration=1)
    targets = planner_graph._extract_primary_person_targets(
        {
            "prompt": "Investigate Frederick Xinyu Pi",
            "inputs": ["Frederick Xinyu Pi"],
            "tool_receipts": receipts,
            "primary_target_contract": locked_contract,
        }
    )

    assert locked_contract.canonical_name in {"Frederick Xinyu Pi", "Xinyu Pi"}
    assert "Ruolan Yang" not in locked_contract.approved_aliases
    assert targets[0] in {"Frederick Xinyu Pi", "Xinyu Pi"}


def test_final_plan_rationale_prefers_final_tool_plan(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    rationale = planner_graph._final_plan_rationale(
        [
            planner_graph.ToolPlanItem(
                tool="github_identity_search",
                arguments={"runId": "run-1", "person_name": "Xinyu Pi"},
                rationale="Resolve public code identity anchors for Xinyu Pi.",
            ),
            planner_graph.ToolPlanItem(
                tool="extract_webpage",
                arguments={"runId": "run-1", "url": "https://example.edu/profile", "query": "Extract profile evidence."},
                rationale="Extract official profile evidence from example.edu.",
            ),
        ],
        "stale llm rationale",
    )

    assert "Resolve public code identity anchors for Xinyu Pi" in rationale
    assert "Extract official profile evidence from example.edu" in rationale
    assert "stale llm rationale" not in rationale


def test_rank_related_entity_candidates_ignores_off_target_mismatched_academic_coauthors(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="semantic_scholar_search",
            ok=True,
            summary="Candidate Quanhui Jia with unrelated coauthors.",
            arguments={"runId": "run-1", "person_name": "Jingbin Lin"},
            key_facts=[
                {
                    "canonical_name": "Quanhui Jia",
                    "coauthors": ["Shuang Yang", "Justin Lin"],
                }
            ],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="semantic_scholar_search",
            ok=True,
            summary="Publication by Jingbin Lin with Quanhui Jia.",
            arguments={"runId": "run-1", "person_name": "Jingbin Lin"},
            key_facts=[
                {
                    "authors": ["Jingbin Lin", "Quanhui Jia"],
                    "coauthors": ["Quanhui Jia"],
                    "title": "Anchored paper",
                }
            ],
        ),
    ]

    ranked = planner_graph._rank_related_entity_candidates(
        receipts=receipts,
        primary_person_targets=["Jingbin Lin"],
    )

    names = {item["entity_name"] for item in ranked if item["entity_type"] == "person"}

    assert "Quanhui Jia" in names
    assert "Shuang Yang" not in names
    assert "Justin Lin" not in names
