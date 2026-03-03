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
