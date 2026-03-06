# Pipeline Vs Benchmark Report

Date of comparison artifact: **March 3, 2026**

This file remains a point-in-time gap analysis between:

- `pipeline_result.md`
- `Benchmark.txt`

## Why it still matters

The benchmark report is still useful because the current repo contains code aimed at the exact gaps identified there:

- stronger Stage 1 follow-up logic in `services/agent-langgraph/src/planner_graph.py`
- tool-result normalization in `services/agent-langgraph/src/tool_worker_graph.py`
- report citation filtering and quality gates in `services/agent-langgraph/src/report_graph.py` and `report_helpers.py`

## Current interpretation

The comparison should be read as a quality target, not as a statement that the repo is still missing Stage 2 or report retrieval. Those parts now exist and are wired into API-launched runs by default.

The biggest remaining benchmark-alignment gaps are:

- collecting more diverse hard-anchor sources during Stage 1
- improving evidence hygiene before Stage 2 assembly
- increasing claim-level source specificity in final reports

## Related files

- `pipeline_result.md`
- `pipeline_structure.md`
- `scripts/benchmark_compare_run.py`
- `pipeline_gap_closure_plan.txt`
