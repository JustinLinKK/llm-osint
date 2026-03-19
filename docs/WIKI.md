# LLM-OSINT Wiki

This wiki is the release-stage source of truth for how the repository is organized, how the runtime works, and which documents still matter.

## Overview

LLM-OSINT is a local-first OSINT research stack built around:

- a Fastify API for run lifecycle, reports, files, and graph views
- a Streamable HTTP MCP server for ingest and research tools
- Python LangGraph workflows for Stage 1 collection and Stage 2 reporting
- a React web UI for analysts
- local infrastructure for Postgres, MinIO, Qdrant, Neo4j, Redis, Temporal, and embeddings

## Repository Map

| Path | Purpose |
| --- | --- |
| `apps/api` | Fastify API for runs, events, files, graph views, and reports |
| `apps/mcp-server` | Streamable HTTP MCP server plus Python-backed tool bridge |
| `apps/web` | React + Vite analyst UI |
| `services/agent-langgraph` | Stage 1 planner/tool-worker graphs and Stage 2 report graph |
| `services/worker-embedding` | Local vLLM embedding service |
| `services/worker-python` | Deterministic helper code for chunking and embeddings |
| `services/worker-temporal` | Temporal worker skeleton |
| `infra/docker` | Local Docker Compose stack |
| `infra/db/migrations` | Postgres schema migrations |
| `schemas` | Runtime contracts, including the Stage 1 blueprint contract |
| `result_benchmark` | Benchmark reference assets and comparison baseline |

## Runtime Architecture

### Entry Point

API-created runs start at `POST /runs`. The API spawns `services/agent-langgraph/src/run_planner.py` and passes `--run-stage2`, so successful API-launched runs execute Stage 1 and Stage 2 in sequence.

### Stage 1: Planner And Tool Execution

The Stage 1 planner lives in `services/agent-langgraph/src/planner_graph.py`.

High-level loop:

1. Analyze the request and derive seeds/pivots
2. Plan MCP tool usage
3. Execute tool-worker subgraphs
4. Merge receipts and update planner state
5. Decide whether coverage is sufficient or another pass is required

The tool-worker graph lives in `services/agent-langgraph/src/tool_worker_graph.py` and is responsible for:

- raw tool execution
- result normalization
- vector ingest
- graph ingest
- receipt generation and persistence

### Stage 2: Report Synthesis

The Stage 2 report flow lives in `services/agent-langgraph/src/report_graph.py`.

Main path:

1. Initialize report state
2. Build the outline
3. Route section work
4. Draft sections with retrieval
5. Reduce and reflect
6. Run the quality gate
7. Finalize report state

Stage 2 persists report data to:

- `report_runs`
- `section_drafts`
- `claim_ledger`
- `evidence_refs`

`GET /runs/:runId/report` prefers these Stage 2 tables and falls back to the legacy report path only when needed.

## Core Services

| Service | Default local URL |
| --- | --- |
| API | `http://localhost:3000` |
| MCP server | `http://localhost:3001/mcp` |
| Kali/preset MCP server | `http://localhost:3002/mcp` |
| Web UI | `http://localhost:5173` |
| MinIO console | `http://localhost:9001` |
| Neo4j browser | `http://localhost:7474` |
| Temporal UI | `http://localhost:8233` |

Primary backing systems:

- Postgres for runs, receipts, and report state
- MinIO for stored raw objects
- Qdrant for vector search
- Neo4j for graph storage
- Redis and Temporal for orchestration-related plumbing
- vLLM embedding worker for local embedding generation

## Setup

### Prerequisites

- Docker + Docker Compose
- Node.js 20+
- Yarn 4
- Python 3.11+

### Base Setup

```bash
cp .env.example .env
cp infra/docker/.env.example infra/docker/.env
yarn install
python3 -m venv .venv-agent
. .venv-agent/bin/activate
pip install -r services/agent-langgraph/requirements.txt
```

If you are inside the VS Code dev container, connect it to the compose network once:

```bash
docker network connect docker_default $(hostname) || true
```

### Start Infra And App

```bash
yarn infra:up
yarn db:migrate
yarn dev:web
```

If you want the API outside Docker in watch mode:

```bash
yarn dev:api
```

## Environment Rules

### Dev Container Or Codespace

Use `.env.example` values with Docker service names such as:

- `postgres`
- `minio`
- `qdrant`
- `neo4j`
- `temporal`
- `redis`
- `mcp-server`

### Native Host

Copy `.env.example` and replace service hostnames with `localhost`.

Important examples:

- `DATABASE_URL=postgresql://osint:osint@localhost:5432/osint`
- `MINIO_ENDPOINT=http://localhost:9000`
- `QDRANT_URL=http://localhost:6333`
- `NEO4J_URI=bolt://localhost:7687`
- `TEMPORAL_ADDRESS=localhost:7233`
- `MCP_SERVER_URL=http://localhost:3001/mcp`

### Required Runtime Notes

- `LANGGRAPH_AUTOSTART=true` causes the API to spawn the planner on `POST /runs`
- API-launched runs include Stage 2 by default
- `MCP_PYTHON_TOOLS` controls Python tool bridge registration
- embedding behavior is driven by `EMBEDDING_PROVIDER`, `EMBEDDING_API_URL`, and model settings

## Verification

Health check:

```bash
curl http://localhost:3000/health
```

Create a run:

```bash
curl -X POST http://localhost:3000/runs \
  -H "Content-Type: application/json" \
  -d '{"prompt":"Investigate example.com and related accounts"}'
```

Stream events:

```bash
curl -N http://localhost:3000/runs/<RUN_ID>/events
```

Fetch the latest report:

```bash
curl http://localhost:3000/runs/<RUN_ID>/report
```

Exercise the MCP example client:

```bash
cd apps/mcp-server && yarn example
```

## MCP And Tooling Notes

The MCP server uses JSON-RPC 2.0 over Streamable HTTP and is session-based. The main server lives at `apps/mcp-server`, and the Python tool bridge lives under `apps/mcp-server/src/tools/tools_python`.

Core tool categories:

- ingest tools such as `fetch_url`, `ingest_text`, and graph ingest helpers
- retrieval tools for vector and graph access
- Python-backed research tools such as person search, LinkedIn HTML capture, Google SERP wrappers, arXiv search, and Tavily-backed collection
- curated OSINT/Kali wrappers on the `3002` preset server

For detailed usage:

- `apps/mcp-server/README.md`
- `apps/mcp-server/MCP_CLIENT_GUIDE.md`
- `apps/mcp-server/src/tools/tools_python/README.md`

## Benchmarks And Quality Assets

Benchmark reference assets now live under `result_benchmark/`.

Important files:

- `result_benchmark/Benchmark.txt`
- `result_benchmark/PIPELINE_VS_BENCHMARK_REPORT.md`
- `result_benchmark/pipeline_structure.md`
- `scripts/benchmark_compare_run.py`

Generated per-run benchmark reports are written to `reports/benchmark_comparison/` and should be treated as disposable artifacts, not checked-in documentation.

## Current Release Status

Stable and wired today:

- prompt-to-report flow from API through Stage 1 and Stage 2
- HTTP MCP transport
- deterministic ingest, vector ingest, graph ingest, and retrieval flows
- web UI for run creation, events, evidence, graph, and report viewing
- Stage 2 report snapshots in Postgres

Known limits:

- Temporal is still a skeleton worker, not the primary orchestrator
- `services/worker-python` is helper code rather than a first-class runtime worker
- final export/download paths still need consolidation beyond Stage 2 snapshots
- safety controls, budgets, CI, and broader end-to-end verification can still be expanded

## Documentation Map

Release-ready entry points:

- `README.md` for repo overview
- `SETUP.md` for the shortest setup path
- `docs/ENVIRONMENT.md` for deeper environment troubleshooting
- `docs/STAGE1_BLUEPRINT_RUNTIME_MAPPING.md` for blueprint/runtime mapping

Component docs:

- `apps/mcp-server/README.md`
- `apps/mcp-server/MCP_CLIENT_GUIDE.md`
- `services/worker-embedding/README.md`
- `graph_construction_sample/README.md`
