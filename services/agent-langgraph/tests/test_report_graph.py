import threading
import time

from conflict_models import GraphConflictCaseModel
from mcp_client import McpCallResult
from report_graph import build_report_graph, run_report_subgraph, verify_claims_for_task
from report_models import (
    ClaimModel,
    EvidenceRefModel,
    PrimaryTargetContractModel,
    SectionDraftModel,
    SectionTaskModel,
    make_initial_report_state,
)


class _FakeMcpClient:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_vector_calls = 0
        self.max_parallel_vector_calls = 0

    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def call_tool(self, name: str, arguments: dict) -> McpCallResult:
        if name == "vector_search":
            with self._lock:
                self._active_vector_calls += 1
                self.max_parallel_vector_calls = max(self.max_parallel_vector_calls, self._active_vector_calls)
            try:
                time.sleep(0.05)
                query = str(arguments.get("query") or "query").replace(" ", "_")
                return McpCallResult(
                    ok=True,
                    content={
                        "results": [
                            {
                                "document_id": f"doc-{query}",
                                "snippet": f"Stored evidence for {query}.",
                                "source_url": f"https://example.com/{query}",
                                "title": f"Evidence {query}",
                                "score": 0.9,
                            }
                        ]
                    },
                    raw={},
                )
            finally:
                with self._lock:
                    self._active_vector_calls -= 1
        if name in {"graph_get_entity", "graph_neighbors", "graph_search_entities"}:
            return McpCallResult(ok=True, content={}, raw={})
        raise AssertionError(f"Unexpected tool call: {name}")


def test_stage2_sections_run_in_parallel(monkeypatch) -> None:
    monkeypatch.setenv("STAGE2_MAX_SECTION_WORKERS", "4")
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)
    monkeypatch.setattr("report_graph.persist_report_snapshot", lambda **_: None)
    fake_client = _FakeMcpClient()
    graph = build_report_graph(fake_client, llm3=None).compile()
    state = make_initial_report_state(
        run_id="11111111-1111-1111-1111-111111111111",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=0,
    )

    final_state = graph.invoke(state)

    assert fake_client.max_parallel_vector_calls > 1
    assert len(final_state["section_drafts"]) >= 2
    assert final_state["done"] is True


def test_stage2_preserves_stage1_conflict_context(monkeypatch) -> None:
    monkeypatch.setenv("STAGE2_MAX_SECTION_WORKERS", "1")
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)
    monkeypatch.setattr("report_graph.persist_report_snapshot", lambda **_: None)

    fake_client = _FakeMcpClient()
    graph = build_report_graph(fake_client, llm3=None).compile()
    conflict = GraphConflictCaseModel(
        case_id="conflict-1",
        run_id="33333333-3333-3333-3333-333333333333",
        target_type="entity",
        target_id="ent-primary",
        field_name="affiliation",
        scope="primary",
        status="unresolved",
        blocking=True,
        rationale="Primary affiliation evidence remains contradictory.",
    )
    state = make_initial_report_state(
        run_id="33333333-3333-3333-3333-333333333333",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=0,
        stage1_conflict_cases=[conflict],
        stage1_unresolved_conflicts=[conflict],
    )

    final_state = graph.invoke(state)
    report_memory = final_state["report_memory"]

    assert [item.case_id for item in report_memory.stage1_conflict_cases] == ["conflict-1"]
    assert [item.case_id for item in report_memory.stage1_unresolved_conflicts] == ["conflict-1"]
    assert any(item.issue_id == "stage1_conflict_1" for item in final_state["consistency_issues"])
    assert "affiliation" in final_state["contradiction_query_hints"]
    assert any("graph conflict" in item.lower() for item in report_memory.limits)


class _FakeStage2LLM:
    def __init__(self, role: str) -> None:
        self.role = role
        self.calls: list[str] = []
        self.payloads: dict[str, list[dict]] = {}
        self.reflection_calls = 0

    def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
        operation = str(kwargs.get("operation") or "")
        self.calls.append(operation)
        self.payloads.setdefault(operation, []).append(payload)

        if self.role == "final":
            if operation == "stage2.outline":
                return {
                    "outline": [
                        {
                            "section_id": "identity_profile",
                            "title": "Identity profile",
                            "objective": "Establish the target's canonical identity and public-facing footprint.",
                            "required": True,
                            "entity_ids": [],
                            "query_hints": ["identity", "profile"],
                        }
                    ]
                }
            if operation == "stage2.final_reflection":
                self.reflection_calls += 1
                if self.reflection_calls == 1:
                    return {
                        "quality_ok": False,
                        "sections": [
                            {
                                "section_id": "identity_profile",
                                "status": "needs_revision",
                                "critique": "The section needs stronger chronology and should preserve the supported profile anchor.",
                                "next_step_suggestion": "Rewrite the section using the current draft, add chronology, and keep the cited profile anchor.",
                                "query_hints": ["timeline", "profile chronology"],
                            }
                        ],
                    }
                return {
                    "quality_ok": True,
                    "sections": [{"section_id": "identity_profile", "status": "ok"}],
                }
            if operation == "stage2.final_report":
                return {"report_text": "Final report\n\nRewritten identity profile [IDENTITY_PROFILE_1]"}
            return {}

        if operation == "stage2.query_variants":
            return {"queries": ["Ada Lovelace identity profile"]}
            if operation == "stage2.claim_extract":
                return {
                    "claims": [
                        {
                            "claim_id": "identity_profile_c1",
                            "text": "Ada Lovelace is tied to a stable public profile anchor.",
                            "subject_name": "Ada Lovelace",
                            "about_primary_subject": True,
                            "confidence": 0.8,
                            "impact": "medium",
                            "evidence_keys": ["IDENTITY_PROFILE_1"],
                            "conflict_flags": [],
                        }
                ]
            }
        if operation == "stage2.section_draft":
            section = payload.get("section", {})
            current_content = str(section.get("current_content") or "").strip()
            if current_content:
                return {"section_text": "Rewritten identity profile with chronology and preserved profile anchor [IDENTITY_PROFILE_1]"}
            return {"section_text": "Initial thin identity profile [IDENTITY_PROFILE_1]"}
        return {}


def test_stage2_final_reflection_can_trigger_worker_rewrite(monkeypatch) -> None:
    monkeypatch.setenv("STAGE2_MAX_SECTION_WORKERS", "1")
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)
    monkeypatch.setattr("report_graph.persist_report_snapshot", lambda **_: None)

    fake_client = _FakeMcpClient()
    final_llm = _FakeStage2LLM("final")
    section_llm = _FakeStage2LLM("section")
    graph = build_report_graph(fake_client, section_llm=section_llm, final_llm=final_llm).compile()
    state = make_initial_report_state(
        run_id="22222222-2222-2222-2222-222222222222",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=1,
    )

    final_state = graph.invoke(state)

    assert final_llm.calls.count("stage2.final_reflection") == 2
    assert "stage2.final_report" in final_llm.calls
    assert "stage2.section_draft" not in final_llm.calls
    assert section_llm.calls.count("stage2.section_draft") == 2
    assert "stage2.claim_extract" in section_llm.calls
    assert "stage2.final_report" not in section_llm.calls
    assert final_state["section_drafts"][0].content.startswith("Rewritten identity profile")
    rewrite_query_payload = section_llm.payloads["stage2.query_variants"][1]["section"]
    rewrite_draft_payload = section_llm.payloads["stage2.section_draft"][1]["section"]
    assert rewrite_query_payload["revision_query_hints"] == ["timeline", "profile chronology"]
    assert rewrite_draft_payload["reflection_source"] == "final_reflection_node"
    assert rewrite_draft_payload["revision_focus"].startswith("The section needs stronger chronology")
    assert rewrite_draft_payload["next_step_suggestion"].startswith("Rewrite the section using the current draft")
    assert rewrite_draft_payload["current_content"].startswith("Initial thin identity profile")


def test_stage2_role_specific_llms_handle_expected_operations(monkeypatch) -> None:
    monkeypatch.setenv("STAGE2_MAX_SECTION_WORKERS", "1")
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)
    monkeypatch.setattr("report_graph.persist_report_snapshot", lambda **_: None)

    class _RoutingLLM:
        def __init__(self, name: str, responses: dict[str, dict]) -> None:
            self.name = name
            self.responses = responses
            self.calls: list[str] = []

        def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
            operation = str(kwargs.get("operation") or "")
            self.calls.append(operation)
            return dict(self.responses.get(operation, {}))

    outline_llm = _RoutingLLM(
        "outline",
        {
            "stage2.outline": {
                "outline": [
                    {
                        "section_id": "identity_profile",
                        "title": "Identity profile",
                        "objective": "Establish the target's canonical identity and public anchor.",
                        "required": True,
                        "entity_ids": [],
                        "query_hints": ["identity", "profile"],
                    }
                ]
            }
        },
    )
    query_llm = _RoutingLLM("query", {"stage2.query_variants": {"queries": ["Ada Lovelace identity profile"]}})
    claim_llm = _RoutingLLM(
        "claim",
        {
            "stage2.claim_extract": {
                "claims": [
                    {
                        "claim_id": "identity_profile_c1",
                        "text": "Ada Lovelace is tied to a stable public profile anchor.",
                        "subject_name": "Ada Lovelace",
                        "about_primary_subject": True,
                        "confidence": 0.8,
                        "impact": "medium",
                        "evidence_keys": ["IDENTITY_PROFILE_1"],
                        "conflict_flags": [],
                    }
                ]
            }
        },
    )
    draft_llm = _RoutingLLM("draft", {"stage2.section_draft": {"section_text": "Ada profile [IDENTITY_PROFILE_1]"}})
    reflection_llm = _RoutingLLM(
        "reflection",
        {"stage2.final_reflection": {"quality_ok": True, "sections": [{"section_id": "identity_profile", "status": "ok"}]}},
    )
    final_report_llm = _RoutingLLM(
        "final_report",
        {"stage2.final_report": {"report_text": "Final report\n\nAda profile [IDENTITY_PROFILE_1]"}},
    )

    graph = build_report_graph(
        _FakeMcpClient(),
        role_llms={
            "outline": outline_llm,
            "section_query": query_llm,
            "section_claim": claim_llm,
            "section_draft": draft_llm,
            "final_reflection": reflection_llm,
            "final_report": final_report_llm,
        },
    ).compile()
    state = make_initial_report_state(
        run_id="77777777-7777-7777-7777-777777777777",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=0,
        primary_target_contract=PrimaryTargetContractModel(
            canonical_name="Ada Lovelace",
            prompt_targets=["Ada Lovelace"],
            approved_aliases=["Ada Byron"],
        ),
    )

    final_state = graph.invoke(state)

    assert outline_llm.calls == ["stage2.outline"]
    assert query_llm.calls == ["stage2.query_variants"]
    assert claim_llm.calls == ["stage2.claim_extract"]
    assert draft_llm.calls == ["stage2.section_draft"]
    assert reflection_llm.calls == ["stage2.final_reflection"]
    assert final_report_llm.calls == ["stage2.final_report"]
    assert final_state["primary_entities"][0] == "Ada Lovelace"


def test_stage2_template_fallback_sections_fail_quality_gate(monkeypatch) -> None:
    monkeypatch.setenv("STAGE2_MAX_SECTION_WORKERS", "1")
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)
    persisted: list[dict] = []
    monkeypatch.setattr("report_graph.persist_report_snapshot", lambda **kwargs: persisted.append(kwargs))

    graph = build_report_graph(_FakeMcpClient(), llm3=None).compile()
    state = make_initial_report_state(
        run_id="88888888-8888-8888-8888-888888888888",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=0,
    )

    final_state = graph.invoke(state)

    assert final_state["quality_ok"] is False
    assert any("template-style fallback content" in issue for issue in final_state["section_issues"])
    assert persisted[-1]["status"] == "failed"


def test_verify_claims_for_task_preserves_off_target_subject_in_conflict_section() -> None:
    task = SectionTaskModel(
        section_id="conflict_resolution",
        title="Conflict resolution",
        objective="Capture contradictory identity claims with citations.",
        entity_ids=["Jingbin Lin"],
    )
    evidence = [
        EvidenceRefModel(
            citation_key="CONFLICT_1",
            section_id="conflict_resolution",
            document_id="doc-1",
            snippet="Quanhui Jia appears in a conflicting author record unrelated to Jingbin Lin.",
            source_url="https://example.com/conflict",
            evidence_object_key="runs/run-1/conflict.json",
        )
    ]
    claims = [
        ClaimModel(
            claim_id="conflict_c1",
            section_id="conflict_resolution",
            text="Quanhui Jia appears in a conflicting author record.",
            subject_name="Quanhui Jia",
            about_primary_subject=False,
            evidence_keys=["CONFLICT_1"],
        )
    ]

    verified, issues = verify_claims_for_task(
        task,
        evidence,
        claims,
        primary_entities=["Jingbin Lin"],
    )

    assert issues == []
    assert len(verified) == 1
    assert verified[0].subject_entity_id == "Quanhui Jia"
    assert verified[0].subject_name == "Quanhui Jia"
    assert verified[0].about_primary_subject is False


class _AnchorDriftMcpClient:
    def start(self) -> None:
        return None

    def close(self) -> None:
        return None

    def call_tool(self, name: str, arguments: dict) -> McpCallResult:
        if name == "vector_search":
            query = str(arguments.get("query") or "query").replace(" ", "_")
            return McpCallResult(
                ok=True,
                content={
                    "results": [
                        {
                            "document_id": f"doc-{query}",
                            "snippet": "Jingbin Lin maintains a stable public profile anchor.",
                            "source_url": f"https://example.com/{query}",
                            "title": f"Evidence {query}",
                            "score": 0.9,
                            "evidence_object_key": f"runs/test/{query}.json",
                        }
                    ]
                },
                raw={},
            )
        if name in {"graph_get_entity", "graph_neighbors", "graph_search_entities"}:
            return McpCallResult(ok=True, content={}, raw={})
        raise AssertionError(f"Unexpected tool call: {name}")


class _AnchorDriftSectionLLM:
    def __init__(self) -> None:
        self.claim_extract_calls = 0
        self.section_draft_calls = 0

    def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
        operation = str(kwargs.get("operation") or "")
        if operation == "stage2.query_variants":
            return {"queries": ["Jingbin Lin identity profile"]}
        if operation == "stage2.claim_extract":
            self.claim_extract_calls += 1
            if self.claim_extract_calls == 1:
                return {
                    "claims": [
                        {
                            "claim_id": "identity_profile_c1",
                            "text": "Quanhui Jia is the subject of this identity profile.",
                            "subject_name": "Quanhui Jia",
                            "about_primary_subject": False,
                            "confidence": 0.8,
                            "impact": "medium",
                            "evidence_keys": ["IDENTITY_PROFILE_1"],
                            "conflict_flags": [],
                        }
                    ]
                }
            return {
                "claims": [
                    {
                        "claim_id": "identity_profile_c2",
                        "text": "Jingbin Lin is tied to a stable public profile anchor.",
                        "subject_name": "Jingbin Lin",
                        "about_primary_subject": True,
                        "confidence": 0.9,
                        "impact": "medium",
                        "evidence_keys": ["IDENTITY_PROFILE_1"],
                        "conflict_flags": [],
                    }
                ]
            }
        if operation == "stage2.section_draft":
            self.section_draft_calls += 1
            if self.section_draft_calls == 1:
                return {"section_text": "Quanhui Jia is the main subject of this section [IDENTITY_PROFILE_1]"}
            return {"section_text": "Jingbin Lin remains the primary subject in this section [IDENTITY_PROFILE_1]"}
        return {}


class _AnchorDriftFinalLLM:
    def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
        operation = str(kwargs.get("operation") or "")
        if operation == "stage2.outline":
            return {
                "outline": [
                    {
                        "section_id": "identity_profile",
                        "title": "Identity profile",
                        "objective": "Establish the target's canonical identity.",
                        "required": True,
                        "entity_ids": [],
                        "query_hints": ["identity", "profile"],
                    }
                ]
            }
        if operation == "stage2.final_reflection":
            return {
                "quality_ok": True,
                "sections": [{"section_id": "identity_profile", "status": "ok"}],
            }
        if operation == "stage2.final_report":
            return {
                "report_text": "# Quanhui Jia\n\n## Identity\nQuanhui Jia is treated as the main subject [IDENTITY_PROFILE_1]"
            }
        return {}


def test_stage2_anchor_guardrails_keep_report_centered_on_primary_subject(monkeypatch) -> None:
    monkeypatch.setenv("STAGE2_MAX_SECTION_WORKERS", "1")
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)
    monkeypatch.setattr("report_graph.persist_report_snapshot", lambda **_: None)

    graph = build_report_graph(
        _AnchorDriftMcpClient(),
        section_llm=_AnchorDriftSectionLLM(),
        final_llm=_AnchorDriftFinalLLM(),
    ).compile()
    state = make_initial_report_state(
        run_id="33333333-3333-3333-3333-333333333333",
        prompt="profile Jingbin Lin",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=1,
    )

    final_state = graph.invoke(state)

    assert final_state["section_drafts"][0].content.startswith("Jingbin Lin remains the primary subject")
    assert final_state["final_report"].startswith("Qwen Deep Research")
    assert "Quanhui Jia is treated as the main subject" not in final_state["final_report"]


def test_stage2_final_reflection_falls_back_to_heuristics_when_sections_missing(monkeypatch) -> None:
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)

    long_section_text = (
        "Ada Lovelace maintains a stable public identity profile with cited evidence and anchored chronology "
        "[IDENTITY_PROFILE_1]. "
        * 8
    ).strip()

    class _RoutingLLM:
        def __init__(self, responses: dict[str, dict]) -> None:
            self.responses = responses
            self.calls: list[str] = []

        def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
            operation = str(kwargs.get("operation") or "")
            self.calls.append(operation)
            return dict(self.responses.get(operation, {}))

    role_llms = {
        "final_reflection": _RoutingLLM({"stage2.final_reflection": {}}),
    }

    graph = build_report_graph(_FakeMcpClient(), role_llms=role_llms)
    final_reflection_node = graph.nodes["final_reflection_node"].runnable.invoke
    state = make_initial_report_state(
        run_id="44444444-4444-4444-4444-444444444444",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=0,
        primary_target_contract=PrimaryTargetContractModel(
            canonical_name="Ada Lovelace",
            prompt_targets=["Ada Lovelace"],
            approved_aliases=["Ada Byron"],
        ),
    )
    state["outline"] = [
        SectionTaskModel(
            section_id="identity_profile",
            title="Identity profile",
            objective="Establish the target's canonical identity and public anchor.",
            required=True,
            entity_ids=[],
            query_hints=["identity", "profile"],
        )
    ]
    state["section_drafts"] = [
        SectionDraftModel(
            section_id="identity_profile",
            title="Identity profile",
            content=long_section_text,
            citation_keys=["IDENTITY_PROFILE_1"],
        )
    ]
    state["primary_entities"] = ["Ada Lovelace"]
    state["claim_ledger"] = [
        ClaimModel(
            claim_id="identity_profile_c1",
            section_id="identity_profile",
            text="Ada Lovelace is tied to a stable public profile anchor.",
            subject_name="Ada Lovelace",
            about_primary_subject=True,
            confidence=0.9,
            impact="medium",
            evidence_keys=["IDENTITY_PROFILE_1"],
            conflict_flags=[],
        )
    ]

    result = final_reflection_node(state)

    assert result["quality_ok"] is True
    assert result["section_reflections"][0].status == "ok"
    assert role_llms["final_reflection"].calls == ["stage2.final_reflection"]


def test_stage2_final_reflection_does_not_trust_bare_quality_ok_without_sections(monkeypatch) -> None:
    monkeypatch.setattr("report_graph.emit_run_event", lambda *_, **__: None)

    class _RoutingLLM:
        def __init__(self, responses: dict[str, dict]) -> None:
            self.responses = responses
            self.calls: list[str] = []

        def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
            operation = str(kwargs.get("operation") or "")
            self.calls.append(operation)
            return dict(self.responses.get(operation, {}))

    role_llms = {
        "final_reflection": _RoutingLLM({"stage2.final_reflection": {"quality_ok": True, "sections": []}}),
    }

    graph = build_report_graph(_FakeMcpClient(), role_llms=role_llms)
    final_reflection_node = graph.nodes["final_reflection_node"].runnable.invoke
    state = make_initial_report_state(
        run_id="55555555-5555-5555-5555-555555555555",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
        max_refine_rounds=0,
    )
    state["outline"] = [
        SectionTaskModel(
            section_id="identity_profile",
            title="Identity profile",
            objective="Establish the target's canonical identity and public anchor.",
            required=True,
            entity_ids=[],
            query_hints=["identity", "profile"],
        )
    ]
    state["section_drafts"] = [
        SectionDraftModel(
            section_id="identity_profile",
            title="Identity profile",
            content="Thin draft [IDENTITY_PROFILE_1]",
            citation_keys=["IDENTITY_PROFILE_1"],
        )
    ]
    state["primary_entities"] = ["Ada Lovelace"]

    result = final_reflection_node(state)

    assert result["quality_ok"] is False
    assert result["section_reflections"][0].status == "needs_revision"
    assert role_llms["final_reflection"].calls == ["stage2.final_reflection"]


def test_run_report_subgraph_bootstraps_run_before_first_event(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setattr("report_graph.load_env", lambda: None)
    monkeypatch.setattr(
        "report_graph.ensure_run_exists",
        lambda run_id, prompt: calls.append(("ensure", f"{run_id}:{prompt}")),
    )
    monkeypatch.setattr(
        "report_graph.emit_run_event",
        lambda run_id, event_type, payload: calls.append(("event", event_type)),
    )
    monkeypatch.setattr(
        "report_graph.load_primary_target_contract",
        lambda run_id: PrimaryTargetContractModel(
            canonical_name="Ada Lovelace",
            prompt_targets=["Ada Lovelace"],
            approved_aliases=["Ada Byron"],
        ),
    )

    class _FakeCompiledGraph:
        def invoke(self, state: dict, config: dict | None = None) -> dict:
            return {
                "report_type": "person",
                "final_report": "Report",
                "evidence_appendix": "",
                "section_drafts": [],
                "claim_ledger": [],
                "evidence_refs": [],
                "quality_ok": True,
                "refine_round": 0,
                "report_memory": state["report_memory"],
                "primary_target_contract": state["primary_target_contract"],
            }

    class _FakeGraph:
        def compile(self, checkpointer=None) -> _FakeCompiledGraph:
            return _FakeCompiledGraph()

    class _FakeClient:
        def start(self) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr("report_graph.RoutedMcpClient", lambda: _FakeClient())
    monkeypatch.setattr("report_graph.build_report_graph", lambda *args, **kwargs: _FakeGraph())

    result = run_report_subgraph(
        run_id="66666666-6666-6666-6666-666666666666",
        prompt="profile Ada Lovelace",
        noteboard=[],
        stage1_receipts=[],
    )

    assert calls[0][0] == "ensure"
    assert calls[1] == ("event", "STAGE2_STARTED")
    assert result.quality_ok is True
