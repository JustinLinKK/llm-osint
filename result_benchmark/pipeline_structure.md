# Pipeline Structure

This file describes the current runtime path in the repo.

## Entry point

The planner CLI is `services/agent-langgraph/src/run_planner.py`.

When runs are created through the API:

1. `apps/api` spawns `run_planner.py`
2. the API passes `--run-stage2`
3. successful Stage 1 runs continue into the Stage 2 report graph

## Stage 1

Defined in `services/agent-langgraph/src/planner_graph.py`.

Main loop:

1. `analyze_input`
2. `plan_tools`
3. `explain_plan`
4. `execute_tools`
5. `planner_review_receipts`
6. `decide_stop_or_refine`

Stage 1 responsibilities:

- derive seeds, pivots, and follow-up tasks
- choose MCP tools
- execute tool-worker subgraphs
- merge receipts and noteboard updates
- decide whether coverage is sufficient to stop

## Tool-worker subgraph

Defined in `services/agent-langgraph/src/tool_worker_graph.py`.

Per-tool flow:

1. `execute_tool`
2. `summarize_tool_result`
3. `vector_ingest_worker`
4. `graph_ingest_worker`
5. `receipt_summarize_worker`
6. `persist_receipt`

Purpose:

- normalize raw tool output
- ingest evidence into vector and graph stores
- produce planner-facing receipts instead of raw tool payloads

## Stage 2

Defined in `services/agent-langgraph/src/report_graph.py`.

Main path:

1. `report_init_node`
2. `build_outline_node`
3. `section_router_node`
4. `process_sections_node`
5. `reduce_sections_node`
6. `final_reflection_node`
7. `quality_gate_node`
8. `finalize_report_node`

Stage 2 writes report state to:

- `report_runs`
- `section_drafts`
- `claim_ledger`
- `evidence_refs`

The API report endpoint prefers these Stage 2 tables.

## Service boundaries

- external I/O: MCP tools
- run creation and status surface: API
- evidence stores: Postgres, MinIO, Qdrant, Neo4j
- orchestration today: API-spawned planner process
- orchestration target: Temporal, still not primary
