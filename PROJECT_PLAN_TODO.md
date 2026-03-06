# Project Plan And TODO

This file reflects the current implementation state rather than the original greenfield plan.

## Done

- local Docker Compose stack for data stores, API, MCP servers, and embedding worker
- Fastify API run lifecycle and SSE event streaming
- Streamable HTTP MCP server with ingest tools and Python bridge
- LangGraph Stage 1 planner and tool-worker graphs
- LangGraph Stage 2 report graph
- Web UI for run creation, events, evidence, graph, and report viewing
- Postgres tables for run events, receipts, and Stage 2 report snapshots

## In progress

- stronger Stage 1 evidence quality and coverage gating
- cleaner citation hygiene in final reports
- better graph-backed synthesis quality
- richer frontend report UX

## Remaining high-value work

### Orchestration

- make Temporal the real orchestrator instead of a skeleton worker
- define durable retry boundaries across planner, tool execution, and reporting

### Evidence processing

- wire `services/worker-python` into the real runtime as a background worker or explicit processing stage
- improve deterministic chunking, embedding retries, and graph extraction handoff

### Reporting

- consolidate legacy `reports` artifact flow with the newer Stage 2 tables
- publish final report artifacts to a stable download/export path
- tighten claim-level citation requirements and evidence filtering

### Safety and controls

- add rate limits, budgets, and tool allowlists
- harden external-tool failure handling
- expand provider-specific guardrails

### Quality

- add broader end-to-end tests
- add CI for TypeScript builds, Python tests, and doc drift checks
- add smoke tests for dockerized run creation and report retrieval
