# Current Checkpoint

Last reviewed against the current worktree on **March 6, 2026**.

## Repository state

The repo now has a working prompt-to-report path driven by the API:

- `POST /runs` creates a run and emits `RUN_CREATED`
- the API spawns `services/agent-langgraph/src/run_planner.py`
- the API passes `--run-stage2`, so Stage 2 runs by default for successful API-launched runs
- the web UI can create runs, stream events, inspect evidence, and fetch the latest report

## Implemented components

### API

- health endpoint
- run creation, listing, single-run lookup, title update
- SSE event stream
- file listing and deletion flows
- graph projection endpoint
- report retrieval endpoint with Stage 2-first lookup

### MCP server

- Streamable HTTP transport at `/mcp`
- deterministic ingest tools:
  - `fetch_url`
  - `ingest_text`
  - `ingest_graph_entity`
  - `ingest_graph_entities`
  - `ingest_graph_relations`
- report query tools
- Python bridge for research/OSINT tools
- dedicated Kali/preset tool image on port `3002`

### LangGraph

- Stage 1 planner graph
- tool-worker graph with normalization, vector ingest, graph ingest, and receipt persistence
- Stage 2 report graph with outline, retrieval, drafting, reflection, quality gate, and Postgres snapshot persistence

### Web UI

- prompt composer
- run list
- live event timeline
- files view
- graph view
- report view backed by `GET /runs/:runId/report`

## Gaps that still matter

- Temporal worker exists but is not the primary orchestrator
- `services/worker-python` is helper code, not a standalone worker process in the run lifecycle
- the legacy `reports` + MinIO final artifact path still exists, but Stage 2 snapshot tables are the main report source today
- safety controls, budgets, CI, and broader end-to-end verification still need work
