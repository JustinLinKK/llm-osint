from __future__ import annotations

import argparse
import json
import os
import signal

from planner_graph import run_planner
from report_graph import run_report_subgraph
from logger import get_logger
from run_events import emit_run_event
from run_monitor import RunMonitor, set_active_monitor

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the LangGraph planner")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--input", action="append", default=[])
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=max(1, int(os.getenv("LANGGRAPH_MAX_ITERATIONS", "7"))),
    )
    parser.add_argument(
        "--max-worker",
        type=int,
        default=max(1, int(os.getenv("LANGGRAPH_MAX_WORKER", os.getenv("LANGGRAPH_MAX_WORKERS", "5")))),
    )
    parser.add_argument("--run-stage2", action="store_true")

    args = parser.parse_args()

    monitor = RunMonitor(
        run_id=args.run_id,
        emit_event=lambda event_type, payload: emit_run_event(args.run_id, event_type, payload),
    )
    set_active_monitor(monitor)
    monitor.start()

    def _handle_signal(signum: int, _frame: object) -> None:
        emit_run_event(
            args.run_id,
            "RUN_ABORTED",
            {"signal": signum, "signalName": signal.Signals(signum).name},
        )
        raise SystemExit(128 + signum)

    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        result = run_planner(
            run_id=args.run_id,
            prompt=args.prompt,
            inputs=args.input,
            max_iterations=args.max_iterations,
            max_worker=args.max_worker,
        )

        logger.info("Planner CLI finished", extra={"run_id": result.run_id, "iterations": result.iterations})

        output: dict[str, object] = {
            "runId": result.run_id,
            "toolPlan": [item.model_dump() for item in result.tool_plan],
            "documentsCreated": result.documents_created,
            "rationale": result.rationale,
            "toolReceipts": [receipt.model_dump() for receipt in result.tool_receipts],
            "iterations": result.iterations,
            "noteboard": result.noteboard,
            "coverageLedger": result.coverage_ledger,
            "nextStage": result.next_stage,
        }

        if args.run_stage2 and result.next_stage == "stage2":
            report = run_report_subgraph(
                run_id=result.run_id,
                prompt=args.prompt,
                noteboard=result.noteboard,
                stage1_receipts=result.tool_receipts,
            )
            output["stage2"] = {
                "reportType": report.report_type,
                "qualityOk": report.quality_ok,
                "refineRound": report.refine_round,
                "finalReport": report.final_report,
                "evidenceAppendix": report.evidence_appendix,
                "sectionDrafts": [item.model_dump() for item in report.section_drafts],
                "claimLedger": [item.model_dump() for item in report.claim_ledger],
                "evidenceRefs": [item.model_dump() for item in report.evidence_refs],
                "reportMemory": report.report_memory.model_dump(),
            }

        print(json.dumps(output, indent=2))
    finally:
        monitor.stop()
        set_active_monitor(None)
        signal.signal(signal.SIGTERM, previous_sigterm)


if __name__ == "__main__":
    main()
