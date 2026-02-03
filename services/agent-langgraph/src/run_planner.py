from __future__ import annotations

import argparse
import json

from planner_graph import run_planner


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the LangGraph planner")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--input", action="append", default=[])
    parser.add_argument("--max-iterations", type=int, default=1)

    args = parser.parse_args()

    result = run_planner(
        run_id=args.run_id,
        prompt=args.prompt,
        inputs=args.input,
        max_iterations=args.max_iterations,
    )

    print(
        json.dumps(
            {
                "runId": result.run_id,
                "toolPlan": [item.model_dump() for item in result.tool_plan],
                "documentsCreated": result.documents_created,
                "rationale": result.rationale,
                "toolResults": result.tool_results,
                "iterations": result.iterations,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
