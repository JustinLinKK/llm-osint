from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
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
    openrouter_module.invoke_complete_json = lambda *args, **kwargs: {}
    monkeypatch.setitem(sys.modules, "openrouter_llm", openrouter_module)

    logger_module = types.ModuleType("logger")
    logger_module.get_logger = lambda name: types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
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


class _ConflictLookupClient:
    def __init__(self) -> None:
        self.calls: List[tuple[str, Dict[str, Any]]] = []

    def call_tool(self, tool_name: str, arguments: Dict[str, Any]):  # type: ignore[no-untyped-def]
        self.calls.append((tool_name, dict(arguments)))
        if tool_name == "graph_export_json":
            return SimpleNamespace(
                ok=True,
                content={
                    "runId": arguments.get("runId", "run-1"),
                    "scope": "run",
                    "nodeCount": 3,
                    "relationCount": 2,
                    "nodes": [
                        {
                            "entityId": "ent-primary",
                            "labels": ["Person", "Entity"],
                            "properties": {
                                "node_id": "ent-primary",
                                "canonical_name": "Ada Lovelace",
                                "type": "Person",
                            },
                        },
                        {
                            "entityId": "org-mit",
                            "labels": ["Institution", "Entity"],
                            "properties": {"node_id": "org-mit", "canonical_name": "MIT", "type": "Institution"},
                        },
                        {
                            "entityId": "org-stanford",
                            "labels": ["Institution", "Entity"],
                            "properties": {"node_id": "org-stanford", "canonical_name": "Stanford", "type": "Institution"},
                        },
                    ],
                    "relations": [
                        {
                            "edgeId": "rel-mit",
                            "relType": "AFFILIATED_WITH",
                            "srcEntityId": "ent-primary",
                            "dstEntityId": "org-mit",
                            "properties": {"edge_id": "rel-mit", "rel_type": "AFFILIATED_WITH", "canonical_name": "Affiliated With"},
                        },
                        {
                            "edgeId": "rel-stanford",
                            "relType": "AFFILIATED_WITH",
                            "srcEntityId": "ent-primary",
                            "dstEntityId": "org-stanford",
                            "properties": {"edge_id": "rel-stanford", "rel_type": "AFFILIATED_WITH", "canonical_name": "Affiliated With"},
                        },
                    ],
                },
            )
        if tool_name == "vector_lookup_refs":
            return SimpleNamespace(
                ok=True,
                content={
                    "results": [
                        {
                            "document_id": "doc-1",
                            "chunk_id": "chunk-1",
                            "snippet": "Structured evidence snippet",
                            "sourceUrl": "https://example.edu/profile",
                            "sourceDomain": "example.edu",
                            "objectRef": {"documentId": "doc-1", "chunkId": "chunk-1"},
                        }
                    ]
                },
            )
        if tool_name == "graph_apply_adjudications":
            return SimpleNamespace(ok=True, content={"appliedEntityIds": ["ent-primary"], "caseIds": ["case-1"]})
        raise AssertionError(f"Unexpected tool call: {tool_name}")


def test_graph_state_snapshot_prefers_run_graph_export(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    client = _ConflictLookupClient()

    snapshot = planner_graph._derive_graph_state_snapshot(
        client,
        {
            "run_id": "run-1",
            "prompt": "Investigate Ada Lovelace",
            "inputs": ["Ada Lovelace"],
            "tool_receipts": [],
            "related_entity_candidates": [],
        },
    )

    assert snapshot["status"] == "ready"
    assert snapshot["graph_export_status"] == "ready"
    assert snapshot["graph_export_node_count"] == 3
    assert snapshot["graph_export_relation_count"] == 2
    assert snapshot["resolved_entity_ids"] == ["ent-primary"]
    assert any(tool_name == "graph_export_json" for tool_name, _args in client.calls)
    assert not any(tool_name == "graph_search_entities" for tool_name, _args in client.calls)


def test_build_conflict_cases_resolves_primary_affiliation_with_exact_evidence(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    client = _ConflictLookupClient()
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="orcid_search",
            ok=True,
            summary="Ada Lovelace affiliation: MIT.",
            arguments={"person_name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["MIT"]}],
            graph_upserts={"entityIds": ["ent-primary"], "evidenceRefs": [{"documentId": "doc-1", "chunkId": "chunk-1", "sourceUrl": "https://example.edu/profile"}]},
            vector_upserts={"documentId": "doc-1", "chunkIds": ["chunk-1"]},
            document_ids=["doc-1"],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="institution_directory_search",
            ok=True,
            summary="Ada Lovelace is listed at MIT.",
            arguments={"person_name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["MIT"]}],
            graph_upserts={"entityIds": ["ent-primary"], "evidenceRefs": [{"documentId": "doc-2", "chunkId": "chunk-2", "sourceUrl": "https://mit.edu/people/ada"}]},
            vector_upserts={"documentId": "doc-2", "chunkIds": ["chunk-2"]},
            document_ids=["doc-2"],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Ada Lovelace biography mentions Stanford.",
            arguments={"name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["Stanford"]}],
            graph_upserts={"entityIds": ["ent-primary"], "evidenceRefs": [{"documentId": "doc-3", "chunkId": "chunk-3", "sourceUrl": "https://people.example.com/ada"}]},
            vector_upserts={"documentId": "doc-3", "chunkIds": ["chunk-3"]},
            document_ids=["doc-3"],
        ),
    ]

    cases = planner_graph._build_conflict_cases(
        mcp_client=client,
        llm=None,
        run_id="run-1",
        receipts=receipts,
        graph_state_snapshot={"resolved_entity_ids": ["ent-primary"]},
        primary_person_targets=["Ada Lovelace"],
    )

    assert len(cases) == 1
    case = cases[0]
    assert case.field_name == "affiliation"
    assert case.chosen_value == "MIT"
    assert case.status == "resolved"
    applied, notes = planner_graph._apply_resolved_conflicts(
        mcp_client=client,
        run_id="run-1",
        conflict_cases=cases,
    )

    assert applied[0].status == "applied"
    assert any(tool_name == "graph_apply_adjudications" for tool_name, _args in client.calls)
    assert any("Applied 1 resolved graph conflict" in note for note in notes)


def test_build_conflict_cases_uses_graph_first_detection_before_reference_lookup(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    client = _ConflictLookupClient()
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="orcid_search",
            ok=True,
            summary="Ada Lovelace affiliation: MIT.",
            arguments={"person_name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["MIT"]}],
            graph_upserts={"entityIds": ["ent-primary"], "evidenceRefs": [{"documentId": "doc-1", "chunkId": "chunk-1", "sourceUrl": "https://example.edu/profile"}]},
            vector_upserts={"documentId": "doc-1", "chunkIds": ["chunk-1"]},
            document_ids=["doc-1"],
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Ada Lovelace biography mentions Stanford.",
            arguments={"name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["Stanford"]}],
            graph_upserts={"entityIds": ["ent-primary"], "evidenceRefs": [{"documentId": "doc-3", "chunkId": "chunk-3", "sourceUrl": "https://people.example.com/ada"}]},
            vector_upserts={"documentId": "doc-3", "chunkIds": ["chunk-3"]},
            document_ids=["doc-3"],
        ),
    ]

    def fake_invoke_complete_json(_llm, _prompt, payload, **kwargs):  # type: ignore[no-untyped-def]
        operation = kwargs.get("operation")
        if operation == "planner.detect_graph_conflicts":
            assert payload["graph"]["nodeCount"] == 3
            return {
                "conflicts": [
                    {
                        "target_type": "entity",
                        "target_id": "ent-primary",
                        "field_name": "affiliation",
                        "scope": "primary",
                        "blocking": True,
                        "candidate_values": ["MIT", "Stanford"],
                        "reason": "Run graph shows conflicting affiliation surfaces for the primary target.",
                    }
                ]
            }
        if operation == "planner.resolve_graph_conflicts":
            return {
                "chosen_value": "MIT",
                "status": "resolved",
                "confidence": 0.91,
                "rationale": "MIT is better supported by the exported graph and exact reference evidence.",
                "evidence_ids": [],
            }
        return {}

    monkeypatch.setattr(planner_graph, "invoke_complete_json", fake_invoke_complete_json)

    cases = planner_graph._build_conflict_cases(
        mcp_client=client,
        llm=object(),
        run_id="run-1",
        receipts=receipts,
        graph_state_snapshot={"resolved_entity_ids": ["ent-primary"]},
        primary_person_targets=["Ada Lovelace"],
    )

    assert len(cases) == 1
    assert cases[0].field_name == "affiliation"
    assert cases[0].chosen_value == "MIT"
    assert cases[0].status == "resolved"
    assert any("Run graph shows conflicting affiliation surfaces" in note for note in cases[0].notes)
    call_names = [tool_name for tool_name, _args in client.calls]
    assert "graph_export_json" in call_names
    assert "vector_lookup_refs" in call_names
    assert call_names.index("graph_export_json") < call_names.index("vector_lookup_refs")


def test_build_conflict_cases_leaves_primary_conflict_unresolved_and_blocking(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    client = _ConflictLookupClient()
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Ada Lovelace affiliation appears to be MIT.",
            arguments={"name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["MIT"]}],
            graph_upserts={"entityIds": ["ent-primary"]},
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Ada Lovelace affiliation appears to be Stanford.",
            arguments={"name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["Stanford"]}],
            graph_upserts={"entityIds": ["ent-primary"]},
        ),
    ]

    cases = planner_graph._build_conflict_cases(
        mcp_client=client,
        llm=None,
        run_id="run-1",
        receipts=receipts,
        graph_state_snapshot={"resolved_entity_ids": ["ent-primary"]},
        primary_person_targets=["Ada Lovelace"],
    )

    assert len(cases) == 1
    assert cases[0].status == "unresolved"
    unresolved = [item for item in cases if item.status not in {"resolved", "applied"}]
    conflict_gate_ok = not any(item.blocking for item in unresolved)
    assert conflict_gate_ok is False


def test_secondary_unresolved_conflict_does_not_block_gate(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    secondary_case = planner_graph.GraphConflictCaseModel(
        case_id="case-secondary",
        run_id="run-1",
        target_type="entity",
        target_id="ent-secondary",
        field_name="organization",
        scope="secondary",
        status="unresolved",
        blocking=False,
    )

    unresolved = [secondary_case]
    conflict_gate_ok = not any(item.blocking for item in unresolved if item.status not in {"resolved", "applied"})

    assert conflict_gate_ok is True


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
        primary_person_targets=["Frederick Xinyu Pi", "Xinyu Pi"],
        primary_target_contract=planner_graph.PrimaryTargetContractModel(
            canonical_name="Frederick Xinyu Pi",
            prompt_targets=["Frederick Xinyu Pi", "Xinyu Pi"],
            approved_aliases=["Frederick Pi"],
        ),
        iteration=0,
        dedupe_store={},
    )
    assert any(task.tool_name == "cross_platform_profile_resolver" for task in tasks)
    assert any("identity resolution" in note.lower() for note in notes)


def test_profile_candidates_require_profile_identity_evidence_not_receipt_arguments(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    profiles = planner_graph._profile_candidates_from_receipts(
        [
            ReceiptStub(
                run_id="run-1",
                tool_name="github_identity_search",
                ok=True,
                summary="Returned a GitHub profile.",
                arguments={"person_name": "Frederick Xinyu Pi"},
                key_facts=[
                    {"profileUrl": "https://github.com/amycensys"},
                    {"username": "amycensys"},
                    {"displayName": "Amy Frederick"},
                ],
            )
        ],
        primary_person_targets=["Frederick Xinyu Pi", "Frederick Pi"],
    )

    assert profiles == []


def test_profile_candidates_accept_handle_or_path_evidence_for_primary_target(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    profiles = planner_graph._profile_candidates_from_receipts(
        [
            ReceiptStub(
                run_id="run-1",
                tool_name="github_identity_search",
                ok=True,
                summary="Returned a GitHub profile.",
                key_facts=[
                    {"profileUrl": "https://github.com/frederickpi1969"},
                    {"username": "frederickpi1969"},
                ],
            ),
            ReceiptStub(
                run_id="run-1",
                tool_name="linkedin_profile_lookup",
                ok=True,
                summary="Returned a LinkedIn profile.",
                key_facts=[
                    {"profileUrl": "https://www.linkedin.com/in/frederick-pi-40a668181"},
                ],
            ),
        ],
        primary_person_targets=["Frederick Xinyu Pi", "Frederick Pi"],
        approved_handles=["frederickpi1969"],
    )

    profile_urls = {item.get("profile_url") for item in profiles}
    assert "https://github.com/frederickpi1969" in profile_urls
    assert "https://www.linkedin.com/in/frederick-pi-40a668181" in profile_urls


def test_root_normalization_prefers_typo_tolerant_primary_root_without_merging_short_name_lookalikes(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    graph_export = {
        "generated": True,
        "nodes": [
            {
                "entityId": "ent-root",
                "labels": ["Person", "Entity"],
                "properties": {
                    "node_id": "ent-root",
                    "canonical_name": "Frederick Pi",
                    "alt_names": ["Federick Pi"],
                    "type": "Person",
                },
            },
            {
                "entityId": "ent-nearby",
                "labels": ["Person", "Entity"],
                "properties": {
                    "node_id": "ent-nearby",
                    "canonical_name": "Federick Qi",
                    "type": "Person",
                },
            },
        ],
        "relations": [],
    }
    contract = planner_graph.PrimaryTargetContractModel(
        canonical_name="Federick Pi",
        prompt_targets=["Federick Pi"],
        approved_aliases=["Frederick Pi"],
    )

    cases, actions, _notes, root_entity_id, _root_candidate_ids = planner_graph._build_graph_normalization_plan(
        run_id="run-1",
        graph_export=graph_export,
        contract=contract,
    )

    assert root_entity_id == "ent-root"
    assert any(case.case_type == "root_resolution" for case in cases)
    merge_sources = {
        action.source_entity_id
        for action in actions
        if action.action_type == "merge_entity_into"
    }
    assert "ent-nearby" not in merge_sources


def test_graph_normalization_suggestions_are_note_only(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    unresolved_case = planner_graph.GraphNormalizationCaseModel(
        case_id="gnorm-1",
        case_type="duplicate_cluster",
        summary="Ambiguous duplicate cluster.",
        deterministic_actions=[],
        notes=[],
    )

    monkeypatch.setattr(
        planner_graph,
        "_build_graph_normalization_plan",
        lambda **kwargs: ([unresolved_case], [], [], "ent-root", ["ent-root"]),
    )
    monkeypatch.setattr(
        planner_graph,
        "_llm_graph_normalization_suggestions",
        lambda **kwargs: {
            "gnorm-1": [
                planner_graph.GraphNormalizationActionModel(
                    action_id="gact-1",
                    action_type="merge_into_root",
                    source_entity_id="ent-dup",
                    target_entity_id="ent-root",
                    rationale="Looks like a duplicate root.",
                    deterministic=False,
                    applyable=False,
                )
            ]
        },
    )
    monkeypatch.setattr(
        planner_graph,
        "_derive_graph_state_snapshot",
        lambda *args, **kwargs: {
            "resolved_entity_ids": ["ent-root"],
            "status": "ready",
            "graph_export_status": "ready",
            "graph_export_node_count": 1,
            "graph_export_relation_count": 0,
        },
    )

    class _NormalizationClient:
        def __init__(self) -> None:
            self.calls: List[tuple[str, Dict[str, Any]]] = []

        def call_tool(self, tool_name: str, arguments: Dict[str, Any]):  # type: ignore[no-untyped-def]
            self.calls.append((tool_name, dict(arguments)))
            if tool_name == "graph_export_json":
                return SimpleNamespace(
                    ok=True,
                    content={
                        "generated": True,
                        "runId": arguments.get("runId", "run-1"),
                        "scope": "run",
                        "nodes": [
                            {
                                "entityId": "ent-root",
                                "labels": ["Person", "Entity"],
                                "properties": {
                                    "node_id": "ent-root",
                                    "canonical_name": "Frederick Pi",
                                    "type": "Person",
                                },
                            }
                        ],
                        "relations": [],
                    },
                )
            if tool_name == "graph_search_entities":
                return SimpleNamespace(
                    ok=True,
                    content={
                        "entities": [
                            {
                                "entityId": "ent-root",
                                "labels": ["Person", "Entity"],
                                "properties": {
                                    "node_id": "ent-root",
                                    "canonical_name": "Frederick Pi",
                                    "type": "Person",
                                },
                            }
                        ]
                    },
                )
            if tool_name == "graph_get_entity":
                return SimpleNamespace(
                    ok=True,
                    content={
                        "entity": {
                            "entityId": "ent-root",
                            "labels": ["Person", "Entity"],
                            "properties": {
                                "node_id": "ent-root",
                                "canonical_name": "Frederick Pi",
                                "type": "Person",
                            },
                        }
                    },
                )
            raise AssertionError(f"Unexpected tool call: {tool_name}")

    client = _NormalizationClient()

    updates = planner_graph._run_graph_normalization_pass(
        mcp_client=client,
        llm=object(),
        state={
            "run_id": "run-1",
            "primary_target_contract": planner_graph.PrimaryTargetContractModel(canonical_name="Frederick Pi"),
            "graph_export_json": {"generated": True, "nodes": [], "relations": []},
            "graph_state_snapshot": {},
        },
    )

    assert updates["graph_normalization_actions"] == []
    assert updates["graph_normalization_cases"][0].suggested_actions[0].action_type == "merge_into_root"
    assert any("note-only normalization action" in note for note in updates["graph_normalization_cases"][0].notes)
    assert not any(tool_name == "graph_apply_normalization_plan" for tool_name, _arguments in client.calls)


def test_primary_root_bootstrap_is_idempotent_and_reuses_root_id(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    class _BootstrapClient:
        def __init__(self) -> None:
            self.calls: List[tuple[str, Dict[str, Any]]] = []
            self.root_entity_id = ""
            self.canonical_name = ""
            self.aliases: List[str] = []

        def call_tool(self, tool_name: str, arguments: Dict[str, Any]):  # type: ignore[no-untyped-def]
            self.calls.append((tool_name, dict(arguments)))
            if tool_name == "graph_apply_normalization_plan":
                payload = json.loads(arguments["actionsJson"])
                action = payload[0]
                self.root_entity_id = str(action.get("targetEntityId") or self.root_entity_id or "ent-root")
                self.canonical_name = str(action.get("canonicalName") or self.canonical_name or "Federick Pi")
                self.aliases = [str(item).strip() for item in action.get("aliases", []) if str(item).strip()]
                return SimpleNamespace(ok=True, content={"appliedEntityIds": [self.root_entity_id]})
            if tool_name == "graph_export_json":
                return SimpleNamespace(
                    ok=True,
                    content={
                        "generated": True,
                        "runId": arguments.get("runId", "run-1"),
                        "scope": "run",
                        "nodes": [
                            {
                                "entityId": self.root_entity_id or "ent-root",
                                "labels": ["Person", "Entity"],
                                "properties": {
                                    "node_id": self.root_entity_id or "ent-root",
                                    "canonical_name": self.canonical_name or "Federick Pi",
                                    "alt_names": self.aliases,
                                    "type": "Person",
                                    "is_primary_root": True,
                                },
                            }
                        ],
                        "relations": [],
                    },
                )
            raise AssertionError(f"Unexpected tool call: {tool_name}")

    client = _BootstrapClient()
    first_contract = planner_graph.PrimaryTargetContractModel(
        canonical_name="Federick Pi",
        prompt_targets=["Federick Pi"],
        approved_aliases=["Federick Pi"],
    )
    ok_one, _notes_one, _export_one, root_id_one = planner_graph._sync_primary_root_contract_to_graph(
        mcp_client=client,
        run_id="run-1",
        contract=first_contract,
    )
    second_contract = first_contract.model_copy(update={"root_entity_id": root_id_one})
    ok_two, _notes_two, _export_two, root_id_two = planner_graph._sync_primary_root_contract_to_graph(
        mcp_client=client,
        run_id="run-1",
        contract=second_contract,
    )

    assert ok_one is True
    assert ok_two is True
    assert root_id_one == "ent-root"
    assert root_id_two == "ent-root"
    apply_calls = [arguments for tool_name, arguments in client.calls if tool_name == "graph_apply_normalization_plan"]
    assert len(apply_calls) == 2
    assert json.loads(apply_calls[1]["actionsJson"])[0]["targetEntityId"] == "ent-root"


def test_primary_root_bootstrap_noops_for_unnamed_target(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    class _NoopClient:
        def __init__(self) -> None:
            self.calls: List[tuple[str, Dict[str, Any]]] = []

        def call_tool(self, tool_name: str, arguments: Dict[str, Any]):  # type: ignore[no-untyped-def]
            self.calls.append((tool_name, dict(arguments)))
            raise AssertionError(f"Unexpected tool call: {tool_name}")

    ok, notes, graph_export, root_id = planner_graph._sync_primary_root_contract_to_graph(
        mcp_client=_NoopClient(),
        run_id="run-1",
        contract=planner_graph.PrimaryTargetContractModel(target_type="unknown", canonical_name=""),
    )

    assert ok is True
    assert root_id == ""
    assert graph_export["generated"] is False
    assert any("skipped" in note.lower() for note in notes)


def test_enrich_primary_target_contract_allows_guarded_canonical_upgrade_on_locked_root(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)

    initial = planner_graph.PrimaryTargetContractModel(
        canonical_name="Federick Pi",
        prompt_targets=["Federick Pi"],
        approved_aliases=["Federick Pi"],
        root_entity_id="ent-root",
    )
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="cross_platform_profile_resolver",
            ok=True,
            summary="Resolved canonical identity.",
            key_facts=[
                {
                    "canonical_identity": {
                        "canonical_name": "Frederick Xinyu Pi",
                        "aliases": ["Federick Pi", "Xinyu Pi"],
                    }
                }
            ],
        )
    ]

    enriched = planner_graph._enrich_primary_target_contract(initial, receipts, iteration=1)

    assert enriched.canonical_name == "Frederick Xinyu Pi"
    assert enriched.root_entity_id == "ent-root"
    assert "Xinyu Pi" in enriched.approved_aliases


def test_conflict_detection_rewrites_merged_entity_ids_before_grouping(monkeypatch) -> None:
    planner_graph = _load_planner_graph_module(monkeypatch)
    client = _ConflictLookupClient()
    receipts = [
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Duplicate entity says MIT.",
            arguments={"name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["MIT"]}],
            graph_upserts={"entityIds": ["ent-dup"]},
        ),
        ReceiptStub(
            run_id="run-1",
            tool_name="person_search",
            ok=True,
            summary="Canonical entity says Stanford.",
            arguments={"name": "Ada Lovelace"},
            key_facts=[{"affiliations": ["Stanford"]}],
            graph_upserts={"entityIds": ["ent-primary"]},
        ),
    ]

    cases = planner_graph._build_conflict_cases(
        mcp_client=client,
        llm=None,
        run_id="run-1",
        receipts=receipts,
        graph_state_snapshot={"resolved_entity_ids": ["ent-primary"]},
        primary_person_targets=["Ada Lovelace"],
        entity_rewrites={"ent-dup": "ent-primary"},
    )

    assert len(cases) == 1
    assert cases[0].target_id == "ent-primary"
