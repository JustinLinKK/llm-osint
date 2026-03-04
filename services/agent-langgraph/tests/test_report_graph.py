import threading
import time

from mcp_client import McpCallResult
from report_graph import build_report_graph
from report_models import make_initial_report_state


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


class _FakeStage2LLM:
    def __init__(self, role: str) -> None:
        self.role = role
        self.calls: list[str] = []
        self.reflection_calls = 0

    def complete_json(self, _system_prompt: str, payload: dict, temperature: float = 0.1, timeout: int = 30, **kwargs: object) -> dict:
        operation = str(kwargs.get("operation") or "")
        self.calls.append(operation)

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
