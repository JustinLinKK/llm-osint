from __future__ import annotations

import argparse
import json
import os

from planner_graph import run_planner
from report_graph import run_report_subgraph
from logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the LangGraph planner")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--input", action="append", default=[])
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=max(1, int(os.getenv("LANGGRAPH_MAX_ITERATIONS", "3"))),
    )
    parser.add_argument(
        "--max-worker",
        type=int,
        default=max(1, int(os.getenv("LANGGRAPH_MAX_WORKER", os.getenv("LANGGRAPH_MAX_WORKERS", "5")))),
    )
    parser.add_argument("--run-stage2", action="store_true")

    args = parser.parse_args()

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


if __name__ == "__main__":
    main()
