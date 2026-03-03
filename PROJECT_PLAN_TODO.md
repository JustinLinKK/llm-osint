# LLM-OSINT Project Plan (LangGraph + MCP) + ToDo

This document is the **detailed plan** for the OSINT pipeline, using:
- **Python LangGraph** for agent reasoning (planning + synthesis)
- **MCP Server** for tool execution (I/O boundary)
- **Temporal** for orchestration (retries, state machine, durability)
- **MinIO + Postgres + Qdrant + Neo4j** as the evidence and derived stores
- **Web UI** for user input + live progress + final report display

---

## 0) Current State (Actual)

### ✅ Infrastructure (Docker Compose)
Running and verified:
- Postgres, MinIO (erasure + versioning), Redis, Qdrant, Neo4j, Temporal, Temporal UI
- API container (Node 20, Yarn 4, Fastify) on port 3000
- MCP server on port 3001 and Kali/preset MCP server on port 3002

### ✅ Postgres Provenance Schema
Tables exist:
- `runs`, `documents`, `document_objects`, `chunks`, `tool_calls`, `run_events`, `reports`
- `artifacts`, `artifact_summaries`, `tool_call_receipts`, `run_notes`
- `report_runs`, `section_drafts`, `claim_ledger`, `evidence_refs`

### ✅ Working Application Surfaces
- API endpoints for runs, SSE events, files, graph snapshots, and latest report retrieval
- Web UI can create runs, stream events, and inspect file + graph evidence for completed runs
- LangGraph planner + tool-worker path is launched by the API on run creation
- Stage 2 report subgraph exists in `services/agent-langgraph/src/report_graph.py`

### ⚠️ Important Gaps
- Temporal worker is still emitting placeholder events, not orchestrating the real planner/collection/report flow
- Stage 2 report generation is implemented but not yet triggered by default from API autostart
- `reports` table / MinIO-backed final report persistence exists separately from the newer Stage 2 snapshot tables

---

## 1) Target End-to-End Workflow (Product Behavior)

### User-visible flow
1. **User submits** prompt and/or files on the **Web UI**
2. **Planner agent (LangGraph)** analyzes input and decides which tools to call (MCP)
3. UI shows **what tools are used and why**, and live tool progress
4. Tools collect data and store **raw evidence** to MinIO + metadata to Postgres
5. Processing pipeline runs: **normalize → chunk → embed → graph extract**
6. **Synthesizer agent (LangGraph)** performs evidence-backed summary + reasoning + reflection
7. UI displays final report with **citations** and downloadable artifacts

### Non-negotiable correctness properties
- **Evidence-first:** raw bytes always stored in MinIO; Postgres stores pointers
- **Traceability:** every claim cites `document_id` + `chunk_id` (+ minio version id when available)
- **Deterministic I/O boundary:** only MCP tools do external I/O; agents call tools via MCP
- **Observability:** every tool call and workflow step emits run events

---

## 2) Architecture Overview (Services + Responsibilities)

### `apps/web` (Frontend, TS)
- Upload text/docs
- Live status dashboard (tool progress)
- Report viewer with citations and evidence drill-down

### `apps/api` (Gateway, TS/Fastify)
- Run creation + session endpoints
- File upload → MinIO → Postgres insert
- Event streaming to UI (SSE)
- Report retrieval endpoints

### `services/worker-temporal` (TS)
- Runs `RunWorkflow(run_id)`
- Calls LangGraph Agent service + MCP tools + processing workers
- Emits run events for UI

### `services/agent-langgraph` (Python)
- LangGraph graphs:
  - **Planner graph:** plan tools, explain rationale, call MCP
  - **Synthesizer graph:** retrieve evidence, draft, resolve conflicts, reflect, finalize report
- Produces structured artifacts and citations

### `apps/mcp-server` (TS)
- Tool registry (fetch/search/parse/OCR/ASR, etc.)
- Enforcement: allowlists, rate limits, budgets
- Writes evidence to MinIO + Postgres, logs tool_calls, emits run events

### `services/worker-python` (Python)
- Deterministic data processing:
  - normalization/parsing
  - chunking
  - embedding + Qdrant upsert
  - entity/claim extraction → Neo4j

---

## 3) Data Model (What goes where)

### MinIO (bytes)
- `runs/<run_id>/raw/...` (HTML/PDF/images/audio/video)
- `runs/<run_id>/derived/normalized_text/...`
- `runs/<run_id>/outputs/report.json|report.md`

### Postgres (ledger + indexing)
Already: `runs, documents, document_objects, chunks, tool_calls, run_events, reports`
Add next: `tasks` (optional)

### Qdrant
- Embeddings for `chunks`
- Payload includes: `run_id`, `document_id`, `chunk_id`, offsets, content_type, source_url

### Neo4j
- Nodes: `Person`, `Org`, `Handle`, `Location`, etc.
- Edges/Claims must reference evidence: `document_id`, `chunk_id`, `confidence`

---

## 4) Contracts (Interfaces between services)

### Run events (for UI)
Event types (minimum):
- `RUN_CREATED`
- `PLANNER_STARTED`, `TOOLS_SELECTED`
- `TOOL_CALL_STARTED`, `TOOL_CALL_FINISHED`
- `PROCESSING_STARTED`, `CHUNKING_FINISHED`, `EMBEDDING_FINISHED`, `GRAPH_FINISHED`
- `SYNTHESIS_STARTED`, `REPORT_READY`, `RUN_FAILED`

### Tool plan (Planner output)
- List of tools + parameters + rationale + stop conditions
- Stored as JSON artifact under run outputs

### Tool result (MCP output)
- Structured results + a list of created `document_id`s and object pointers

### Report (Synthesizer output)
- Markdown + JSON
- Claims section includes evidence citations

---

## 5) Implementation Plan (Milestones)

### Milestone A — Observability + UI progress (foundation)
Goal: user can start a run and watch progress *even if tools are stubbed*

Deliverables:
- `run_events` table
- API SSE endpoint: `GET /runs/:runId/events`
- Helper: `emit_event(run_id, type, payload)`
- UI page: run dashboard subscribes to SSE and renders events

### Milestone B — MCP server skeleton (2 tools)
Goal: tool execution boundary exists and is auditable

Deliverables:
- MCP server service with tool registry
- Tools:
  - `fetch_url` (basic HTTP GET + store raw)
  - `upload_user_file` (already in API; optionally mirror as tool)
- Each tool:
  - writes to MinIO + Postgres
  - writes to `tool_calls`
  - emits events

### Milestone C — LangGraph Planner (Python)
Goal: Planner graph produces plan + calls MCP tools

Deliverables:
- `planner_graph.py`
- Nodes:
  1. `analyze_input`
  2. `plan_tools`
  3. `explain_plan` (for UI)
  4. `execute_tools` (tool-worker subgraph)
  5. `decide_stop_or_refine`
- Emits `TOOLS_SELECTED` with rationale
- Returns `documents_created[]`

### Milestone D — Processing pipeline (chunk + embed)
Goal: pipeline can build vector store for retrieval

Deliverables:
- `chunk_documents(run_id)` writes to `chunks`
- `embed_chunks(run_id)` upserts to Qdrant (store `vector_id` back to Postgres)
- events: `CHUNKING_FINISHED`, `EMBEDDING_FINISHED`

### Milestone E — Graph extraction (Neo4j)
Goal: entity/claim graph exists for session

Deliverables:
- simple NER-based entity extraction (spaCy or transformer) OR heuristic
- write entities/relations to Neo4j
- store evidence refs on edges

### Milestone F — LangGraph Synthesizer / Stage 2 Report (Python)
Goal: evidence-backed report + reflection

Deliverables:
- `report_graph.py` (current implementation path)
- Nodes:
  1. `retrieve_evidence` (Qdrant + Neo4j)
  2. `draft_summary`
  3. `resolve_conflicts`
  4. `reflection`
  5. `finalize_report`
- Current status:
  - graph/report state persistence exists in Stage 2 tables
  - automatic final artifact publication to `reports` / MinIO is still pending wiring

### Milestone G — Hardening for production
Goal: safer and scalable deployment

Deliverables:
- tool allowlists, budgets, timeouts
- MinIO `version_id` capture reliably (AWS S3 SDK)
- rate limits + retry policy
- better error taxonomy + run failure handling
- CI checks + formatting + basic tests

---

# ✅ ToDo Checklist (Actionable)

## A) Repository structure
- [x] Create folders: `apps/web`, `apps/mcp-server`, `services/agent-langgraph`, `services/worker-temporal`, `services/worker-python`, `packages/shared`
- [ ] Add shared types package (zod schemas for ToolPlan, ToolResult, RunEvent, Report)

## B) Postgres schema upgrades
- [x] Add migration `0002_run_events.sql` for `run_events` table
- [x] Add migration `0003_reports.sql` for `reports` table (report pointers + status)
- [x] Add indexes: `run_events(run_id, ts)` and `reports(run_id)`
- [x] Add migration `0004_micro_agent_schema.sql` for artifacts + receipts

## C) API upgrades (Fastify)
- [x] SSE endpoint: `GET /runs/:runId/events`
- [x] Emit events on run creation and ingest endpoints
- [x] Endpoint: `GET /runs/:runId` (status + latest report pointer)
- [x] Endpoint: `GET /runs/:runId/report` (render markdown + citations)

## D) Temporal workflow skeleton (TS)
- [ ] Define `RunWorkflow(run_id)` with steps:
  - plan (LangGraph Planner)
  - collect (MCP tools)
  - process (chunk + embed + graph)
  - synthesize (LangGraph Synthesizer)
- [x] Placeholder activities emit `run_events`
- [ ] Add retry + timeout per activity

## E) MCP server (TS) — minimal viable tools
- [x] MCP server scaffolding + tool registry
- [x] Streamable HTTP transport (network MCP server on port 3001)
- [x] Tool: `fetch_url` (HTTP GET + store raw to MinIO)
- [x] Tool: `ingest_text` (chunk + embed + Qdrant upsert)
- [x] Tool: `ingest_graph_entity` (Neo4j ingest + normalization + location merge threshold)
- [x] Graph query tools (`graph_get_entity`, `graph_neighbors`, `graph_search_entities`)
- [x] Vector query tools (`vector_search`, `vector_get_document`)
- [x] Python bridge with research-integration preset tools
- [x] Python bridge with curated Kali/preset OSINT tools
- [ ] Tool: `web_search` (generic search provider abstraction)
- [x] Write tool call logs into Postgres `tool_calls`
- [x] Emit events `TOOL_CALL_STARTED/FINISHED`

## F) LangGraph Agent Service (Python)
- [x] `planner_graph.py` (tool planning + rationale + execute via MCP client)
- [x] OpenRouter LLM planning integration (direct HTTP)
- [x] Tool worker subgraph (execute tool, store artifacts, write receipts)
- [x] Load root `.env` for agent + pipeline test runs
- [x] `report_graph.py` Stage 2 subgraph (outline, retrieval, claims, drafting, refinement, finalize)
- [x] Stage 2 report state/models (`report_models.py`)
- [ ] Unify planner + Stage 2 states/contracts across the whole service
- [ ] Enforce stronger claim-to-chunk/document citation policy in final outputs

## G) Processing workers (Python)
- [x] Ingestion/chunk/embed helper implemented in `services/worker-python/src/ingest_text.py`
- [x] Chunker writes `chunks`
- [x] Embedder writes Qdrant
- [x] Graph ingest path exists through MCP tools
- [ ] Standalone background worker orchestration
- [ ] Automated entity/claim extraction pipeline from stored evidence

## H) UI (Web)
- [ ] Upload form (text + file)
- [x] Prompt composer for starting runs
- [x] Run dashboard page: SSE stream shows event timeline
- [x] Evidence views for file artifacts and graph nodes/edges
- [ ] Report view: markdown render + citation links to evidence

## I) Production hardening
- [ ] Replace MinIO SDK with AWS S3 SDK v3 to capture `version_id` reliably
- [ ] Tool allowlists + rate limiting
- [x] Secrets handling via env + `.env.example`
- [x] Add basic import/regression test for agent service
- [ ] Add CI: lint, typecheck, unit tests

---

## Current Demo Path

What works today:
1. Create a run from the API or web UI
2. API autostarts the LangGraph planner
3. Planner selects MCP tools and executes them over HTTP
4. Tool worker stores artifacts, receipts, notes, vector data, and graph data
5. UI streams run events and can inspect stored files/graph evidence after completion

What still needs wiring for the intended end-to-end product:
1. Temporal owning the real workflow
2. Stage 2 report generation being triggered automatically
3. Report rendering in the web UI

---

## Suggested Build Order (fastest path to a demo)
1. `run_events` + SSE → UI progress works early  
2. MCP server with `fetch_url` → real collection begins  
3. Planner LangGraph calls MCP → visible “tool reasoning”  
4. Chunk + embed → retrieval works  
5. Synthesizer LangGraph → final report  

---

## Notes / Decisions (locked in)
- LangGraph will be implemented as **Python service**
- MCP server will be **TypeScript service**
- Temporal orchestrates the whole run lifecycle
- Evidence + provenance is the foundation; graph/vector are derived
