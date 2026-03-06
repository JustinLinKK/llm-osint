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
    logger_module.get_logger = lambda name: types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)
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
    spec = importlib.util.spec_from_file_location("planner_graph_archive_smoke", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "planner_graph_archive_smoke", module)
    spec.loader.exec_module(module)
    return module


def test_planner_smoke_runs_archive_identity_chain(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    def fake_run_tool_worker(_mcp_client, run_id: str, tool_name: str, arguments: Dict[str, Any]) -> ToolWorkerResultStub:
        if tool_name in {"person_search", "tavily_person_search"}:
            result = {
                "name": "Ada Lovelace",
                "count": 1,
                "results": [{"url": "https://example.edu/ada", "extracted_text": "Ada Lovelace profile"}],
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Searched public web sources for Ada Lovelace.",
                key_facts=[{"name": "Ada Lovelace"}, {"profileUrls": ["https://example.edu/ada"]}],
                next_hints=["https://example.edu/ada"],
            )
            return ToolWorkerResultStub(receipt=receipt, result=result)

        if tool_name == "wayback_fetch_url":
            url = str(arguments.get("url") or "")
            result = {
                "original_url": url,
                "snapshots": [
                    {
                        "timestamp": "20200101000000",
                        "original_url": url,
                        "archived_url": f"https://web.archive.org/web/20200101000000/{url}",
                        "mime_type": "text/html",
                    },
                    {
                        "timestamp": "20250101000000",
                        "original_url": url,
                        "archived_url": f"https://web.archive.org/web/20250101000000/{url}",
                        "mime_type": "text/html",
                    },
                ],
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Wayback returned snapshots.",
                key_facts=[
                    {"originalUrl": url},
                    {"earliestExtractedText": "Engineer at Acme. Based in London."},
                    {"latestExtractedText": "Director at Example. Based in New York."},
                    {"earliestArchivedUrl": f"https://web.archive.org/web/20200101000000/{url}"},
                    {"latestArchivedUrl": f"https://web.archive.org/web/20250101000000/{url}"},
                    {"firstArchivedAt": "20200101000000"},
                    {"lastArchivedAt": "20250101000000"},
                ],
                next_hints=[f"https://web.archive.org/web/20250101000000/{url}"],
            )
            return ToolWorkerResultStub(receipt=receipt, result=result)

        if tool_name == "historical_bio_diff":
            result = {
                "tool": "historical_bio_diff",
                "changes": [
                    {
                        "field": "employment",
                        "old": "Acme",
                        "new": "Example",
                        "timestamp_range": "2020..2025",
                    }
                ],
            }
            receipt = ReceiptStub(
                run_id=run_id,
                tool_name=tool_name,
                ok=True,
                summary="Historical bio diff identified 1 structured change.",
                key_facts=[{"changes": result["changes"]}],
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

        receipt = ReceiptStub(
            run_id=run_id,
            tool_name=tool_name,
            ok=False,
            summary=f"{tool_name} unavailable in smoke test.",
        )
        return ToolWorkerResultStub(receipt=receipt, result={})

    monkeypatch.setattr(planner_graph, "run_tool_worker", fake_run_tool_worker)
    monkeypatch.setattr(planner_graph, "emit_run_event", lambda *args, **kwargs: None)

    graph = planner_graph.build_planner_graph(mcp_client=object(), llm=None)
    state = {
        "run_id": "run-1",
        "prompt": "Investigate Ada Lovelace and preserve archived biography history",
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
        "academic_task_dedupe": {},
        "technical_task_dedupe": {},
        "business_task_dedupe": {},
        "archive_identity_task_dedupe": {},
        "relationship_task_dedupe": {},
        "coverage_ledger": planner_graph.empty_coverage_ledger(),
    }

    final_state = graph.compile().invoke(state)

    tool_names = [receipt.tool_name for receipt in final_state["tool_receipts"]]
    assert "tavily_person_search" in tool_names
    assert "wayback_fetch_url" in tool_names
    assert "historical_bio_diff" in tool_names
    assert "ingest_graph_entities" in tool_names
    assert final_state["coverage_ledger"]["identity"] is True
    assert final_state["coverage_ledger"]["archived_history"] is True
    assert final_state["next_stage"] == "stage2"


def test_extract_domains_from_state_filters_third_party_hosts(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
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
        "latest_tool_receipts": [],
        "rationale": "",
        "documents_created": [],
        "tool_receipts": [
            ReceiptStub(
                run_id="run-1",
                tool_name="google_serp_person_search",
                ok=True,
                summary="Search results included linkedin.com and scholar.google.com.",
                key_facts=[
                    {
                        "profileUrls": [
                            "https://www.linkedin.com/in/frederick-pi-40a668181",
                            "https://scholar.google.com/citations?user=UPtuhT4AAAAJ&hl=en",
                            "https://2024.emnlp.org/program/accepted_main_conference/",
                            "https://pubs.acs.org/doi/10.1021/acsaom.5c00115",
                        ]
                    },
                    {"emails": ["xpi@ucsd.edu"]},
                ],
                next_hints=[
                    "https://www.linkedin.com/in/frederick-pi-40a668181",
                    "https://scholar.google.com/citations?user=UPtuhT4AAAAJ&hl=en",
                    "https://2024.emnlp.org/program/accepted_main_conference/",
                    "https://pubs.acs.org/doi/10.1021/acsaom.5c00115",
                ],
            )
        ],
        "iteration": 1,
        "max_iterations": 3,
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

    domains = planner_graph._extract_domains_from_state(state)

    assert "ucsd.edu" in domains
    assert "linkedin.com" not in domains
    assert "scholar.google.com" not in domains
    assert "2024.emnlp.org" not in domains
    assert "pubs.acs.org" not in domains


def test_dedupe_tool_plan_merges_semantic_duplicates(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    plan = [
        planner_graph.ToolPlanItem(
            tool="github_identity_search",
            arguments={
                "runId": "run-1",
                "username": "frederickpi1969",
                "person_name": "Frederick Xinyu Pi",
                "profile_url": "https://github.com/frederickpi1969",
            },
            rationale="Resolve GitHub identity from Maigret username pivot.",
        ),
        planner_graph.ToolPlanItem(
            tool="github_identity_search",
            arguments={
                "runId": "run-1",
                "username": "frederickpi1969",
                "max_results": 5,
            },
            rationale="Resolve whether the discovered username pivot has a GitHub code identity.",
        ),
        planner_graph.ToolPlanItem(
            tool="google_serp_person_search",
            arguments={
                "runId": "run-1",
                "target_name": "Frederick Xinyu Pi",
                "max_results": 20,
            },
            rationale="Broad person search.",
        ),
        planner_graph.ToolPlanItem(
            tool="google_serp_person_search",
            arguments={
                "runId": "run-1",
                "target_name": "Frederick Xinyu Pi",
                "max_results": 10,
            },
            rationale="Repeat search with a smaller default result count.",
        ),
    ]

    deduped = planner_graph._dedupe_tool_plan(plan)

    assert len(deduped) == 2
    github_item = next(item for item in deduped if item.tool == "github_identity_search")
    google_item = next(item for item in deduped if item.tool == "google_serp_person_search")
    assert github_item.arguments["username"] == "frederickpi1969"
    assert github_item.arguments["person_name"] == "Frederick Xinyu Pi"
    assert github_item.arguments["profile_url"] == "https://github.com/frederickpi1969"
    assert github_item.arguments["max_results"] == 5
    assert google_item.arguments["target_name"] == "Frederick Xinyu Pi"
    assert google_item.arguments["max_results"] == 20


def test_extract_phone_numbers_ignores_date_like_values(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    numbers = planner_graph._extract_phone_numbers(
        "Reach me at 415-555-0123. Ignore dates 2019-09-04 and 09/04/2019."
    )

    assert "415-555-0123" in numbers
    assert "2019-09-04" not in numbers
    assert "09/04/2019" not in numbers


def test_extract_usernames_supports_dotted_handles_and_profile_urls(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    usernames = planner_graph._extract_usernames(
        "Mentions: @xinyu.pi and @xinyu-pi. Profiles: "
        "https://github.com/xinyu.pi https://gitlab.com/xinyu-pi "
        "https://www.reddit.com/user/xinyu_pi/"
    )

    assert "xinyu.pi" in usernames
    assert "xinyu-pi" in usernames
    assert "xinyu_pi" in usernames


def test_related_person_candidate_filter_rejects_none_publications(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    assert not planner_graph._is_related_person_candidate(
        "None Publications",
        source_key="relatedPeople",
        source_tool="tavily_research",
    )


def test_related_person_candidate_filter_rejects_noisy_suggest_phrase(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    assert not planner_graph._is_related_person_candidate(
        "Suggest Name Emails",
        source_key="relatedPeople",
        source_tool="tavily_person_search",
    )


def test_google_scholar_profile_query_is_site_constrained(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    assert planner_graph._google_scholar_profile_query("Ada Lovelace") == (
        'site:scholar.google.com/citations "Ada Lovelace"'
    )


def test_normalize_related_org_name_rejects_tool_provider_labels(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    assert planner_graph._normalize_related_org_name("Tavily research") is None
    assert planner_graph._normalize_related_org_name("Google SERP person search") is None


def test_filter_completed_tool_plan_skips_successful_semantic_repeats(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    prior_receipt = ReceiptStub(
        run_id="run-1",
        tool_name="google_serp_person_search",
        ok=True,
        summary="Searched Ada Lovelace across Google results.",
        arguments={
            "runId": "run-1",
            "target_name": "Ada Lovelace",
            "max_results": 10,
        },
        argument_signature="google_serp_person_search|[('max_results', 10), ('runId', 'run-1'), ('target_name', 'Ada Lovelace')]",
    )
    state = {
        "tool_receipts": [prior_receipt],
    }
    plan = [
        planner_graph.ToolPlanItem(
            tool="google_serp_person_search",
            arguments={
                "runId": "run-1",
                "target_name": "Ada Lovelace",
                "max_results": 20,
            },
            rationale="Repeat with a larger result count.",
        ),
        planner_graph.ToolPlanItem(
            tool="github_identity_search",
            arguments={
                "runId": "run-1",
                "username": "ada",
            },
            rationale="New pivot from prior search results.",
        ),
    ]

    filtered = planner_graph._filter_completed_tool_plan(state, plan)

    assert len(filtered) == 1
    assert filtered[0].tool == "github_identity_search"
