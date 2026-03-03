# Current Checkpoint

Last reviewed against the current worktree on **February 27, 2026**.

## Repository Progress

The repository is beyond "infra setup only". It now has a working Stage 1 path:

- Docker Compose stack for Postgres, MinIO, Redis, Qdrant, Neo4j, Temporal, API, and MCP servers
- Fastify API for creating runs, streaming run events, listing runs, listing stored files, reading graph snapshots, and fetching the latest report artifact
- MCP server over **Streamable HTTP** with deterministic ingest/query tools and Python-backed research/OSINT tools
- LangGraph planner and tool-worker graphs with receipts, run notes, vector ingest, and graph ingest
- Web UI that can start runs, follow live events, and inspect file/graph evidence after a run finishes

## What Exists But Is Not Fully Wired

- `services/agent-langgraph/src/report_graph.py` implements a Stage 2 report subgraph
- `infra/db/migrations/0007_stage2_reports.sql` adds Stage 2 persistence tables
- `services/agent-langgraph/src/run_planner.py` can invoke Stage 2 with `--run-stage2`

Current limitation:
- API autostart only launches Stage 1 today; Stage 2 is not part of the default run lifecycle yet
- Temporal worker code is still a placeholder and does not orchestrate the real pipeline

## Component Snapshot

### API
- Working:
  - `POST /runs`
  - `GET /runs`
  - `GET /runs/:runId`
  - `GET /runs/:runId/events`
  - `GET /runs/:runId/files`
  - `GET /runs/:runId/graph`
  - `GET /runs/:runId/report`
  - `PATCH /runs/:runId/title`
- Behavior:
  - creating a run writes `RUN_CREATED`
  - API spawns the LangGraph planner in the background
  - successful planner exit marks the run `done`

### MCP Server
- Transport: Streamable HTTP at `/mcp`
- Core tools:
  - `fetch_url`
  - `ingest_text`
  - `ingest_graph_entity`
  - `ingest_graph_entities`
  - `ingest_graph_relations`
- Retrieval/query tools:
  - `vector_search`
  - `vector_get_document`
  - `graph_get_entity`
  - `graph_neighbors`
  - `graph_search_entities`
- Python bridge:
  - research integration tools are available by default
  - curated Kali/preset tools are exposed on the dedicated OSINT image

### LangGraph
- Stage 1:
  - planner extracts URLs/domains/usernames and chooses tools
  - tool worker normalizes tool output, optionally ingests vector/graph data, and persists receipts
- Stage 2:
  - outline generation
  - vector retrieval + graph context
  - claim extraction/verification
  - section drafting
  - refinement rounds
  - persistence to `report_runs`, `section_drafts`, `claim_ledger`, `evidence_refs`

### Web UI
- Implemented:
  - run list
  - prompt composer
  - live event timeline
  - file evidence panel
  - simple graph evidence visualization
- Not implemented:
  - file upload flow
  - report reader with citations

### Workers
- `services/worker-python` contains chunk/embed logic
- `services/worker-temporal` still emits placeholder events rather than running the real system

## Validation Run

The following checks passed during this review:

- `pytest -q` in `services/agent-langgraph` -> `1 passed`
- `yarn workspace @osint/api build`
- `yarn workspace @osint/mcp-server build`
- `yarn workspace @osint/web build`

Not covered by this checkpoint:

- full end-to-end Docker integration run
- actual external tool execution against live providers
- Temporal workflow execution
