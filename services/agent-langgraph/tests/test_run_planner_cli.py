from __future__ import annotations

import json
from types import SimpleNamespace

import run_planner as run_planner_module
from report_models import PrimaryTargetContractModel, ReportMemoryModel


class _Dumpable:
    def __init__(self, payload: dict) -> None:
        self._payload = dict(payload)

    def model_dump(self) -> dict:
        return dict(self._payload)


class _FakeMonitor:
    def __init__(self, run_id: str, emit_event) -> None:  # type: ignore[no-untyped-def]
        self.run_id = run_id
        self.emit_event = emit_event

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None


def test_run_planner_main_merges_stage2_model_overrides(monkeypatch, capsys) -> None:
    contract = PrimaryTargetContractModel(
        canonical_name="Frederick Xinyu Pi",
        prompt_targets=["Frederick Xinyu Pi", "Xinyu Pi"],
        approved_aliases=["Frederick Pi"],
    )
    resolved_conflict = _Dumpable(
        {
            "case_id": "conflict-1",
            "field_name": "affiliation",
            "status": "applied",
            "chosen_value": "UC San Diego",
        }
    )
    planner_result = SimpleNamespace(
        run_id="99999999-9999-9999-9999-999999999999",
        tool_plan=[_Dumpable({"tool": "person_search", "arguments": {"name": "Frederick Xinyu Pi"}, "rationale": "Resolve target."})],
        documents_created=[],
        rationale="Resolve the primary target.",
        tool_receipts=[_Dumpable({"tool_name": "person_search", "ok": True})],
        iterations=1,
        noteboard=["Primary target anchor: Frederick Xinyu Pi."],
        coverage_ledger={"identity": True},
        next_stage="stage2",
        primary_target_contract=contract,
        conflict_cases=[resolved_conflict],
        resolved_conflicts=[resolved_conflict],
        unresolved_conflicts=[],
        conflict_gate_ok=True,
    )
    report_result = SimpleNamespace(
        report_type="person",
        quality_ok=False,
        refine_round=1,
        final_report="Report text",
        evidence_appendix="Appendix",
        section_drafts=[_Dumpable({"section_id": "identity_profile", "title": "Identity", "content": "Draft"})],
        claim_ledger=[],
        evidence_refs=[],
        report_memory=ReportMemoryModel(question="profile Frederick Xinyu Pi", primary_target_contract=contract),
        primary_target_contract=contract,
    )
    captured_stage2_config: dict[str, str] = {}

    monkeypatch.setattr(run_planner_module, "RunMonitor", _FakeMonitor)
    monkeypatch.setattr(run_planner_module, "set_active_monitor", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_planner_module, "emit_run_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_planner_module, "load_run_constraints", lambda _run_id: {"stage2": {"models": {"outline_model": "model-outline", "section_query_model": "model-query"}}})
    monkeypatch.setattr(run_planner_module, "run_planner", lambda **_kwargs: planner_result)

    def _fake_run_report_subgraph(**kwargs):  # type: ignore[no-untyped-def]
        stage2_model_config = kwargs["stage2_model_config"]
        captured_stage2_config.update(stage2_model_config.model_dump())
        assert kwargs["primary_target_contract"].canonical_name == "Frederick Xinyu Pi"
        assert len(kwargs["stage1_conflict_cases"]) == 1
        assert kwargs["stage1_conflict_cases"][0].model_dump()["case_id"] == "conflict-1"
        assert len(kwargs["stage1_resolved_conflicts"]) == 1
        assert kwargs["stage1_unresolved_conflicts"] == []
        return report_result

    monkeypatch.setattr(run_planner_module, "run_report_subgraph", _fake_run_report_subgraph)
    monkeypatch.setattr(
        run_planner_module.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            run_id="99999999-9999-9999-9999-999999999999",
            prompt="profile Frederick Xinyu Pi",
            input=[],
            max_iterations=2,
            max_worker=1,
            run_stage2=True,
            stage2_model_config_json=json.dumps(
                {
                    "section_draft_model": "model-draft-override",
                    "final_report_model": "model-final-report",
                }
            ),
        ),
    )

    run_planner_module.main()
    output = json.loads(capsys.readouterr().out)

    assert captured_stage2_config["outline_model"] == "model-outline"
    assert captured_stage2_config["section_query_model"] == "model-query"
    assert captured_stage2_config["section_draft_model"] == "model-draft-override"
    assert captured_stage2_config["final_report_model"] == "model-final-report"
    assert output["primaryTargetContract"]["canonical_name"] == "Frederick Xinyu Pi"
    assert output["conflictCases"][0]["case_id"] == "conflict-1"
    assert output["resolvedConflicts"][0]["chosen_value"] == "UC San Diego"
    assert output["unresolvedConflicts"] == []
    assert output["conflictGateOk"] is True
    assert output["stage2"]["primaryTargetContract"]["canonical_name"] == "Frederick Xinyu Pi"


def test_run_planner_main_bootstraps_run_before_monitor_start(monkeypatch, capsys) -> None:
    order: list[tuple[str, str]] = []
    planner_result = SimpleNamespace(
        run_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        tool_plan=[],
        documents_created=[],
        rationale="",
        tool_receipts=[],
        iterations=1,
        noteboard=[],
        coverage_ledger={},
        next_stage="done",
        primary_target_contract=PrimaryTargetContractModel(canonical_name="Ada Lovelace"),
        conflict_cases=[],
        resolved_conflicts=[],
        unresolved_conflicts=[],
        conflict_gate_ok=True,
    )

    class _OrderedMonitor(_FakeMonitor):
        def start(self) -> None:
            order.append(("monitor", "start"))

    monkeypatch.setattr(run_planner_module, "ensure_run_exists", lambda run_id, prompt: order.append(("ensure", f"{run_id}:{prompt}")))
    monkeypatch.setattr(run_planner_module, "RunMonitor", _OrderedMonitor)
    monkeypatch.setattr(run_planner_module, "set_active_monitor", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_planner_module, "emit_run_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_planner_module, "load_run_constraints", lambda _run_id: {})
    monkeypatch.setattr(run_planner_module, "run_planner", lambda **_kwargs: planner_result)
    monkeypatch.setattr(
        run_planner_module.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            run_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            prompt="profile Ada Lovelace",
            input=[],
            max_iterations=1,
            max_worker=1,
            run_stage2=False,
            stage2_model_config_json="",
        ),
    )

    run_planner_module.main()
    output = json.loads(capsys.readouterr().out)

    assert order[:2] == [
        ("ensure", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:profile Ada Lovelace"),
        ("monitor", "start"),
    ]
    assert output["runId"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
