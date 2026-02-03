# LLM-OSINT Project Plan (LangGraph + MCP) + ToDo

This document is the **detailed plan** for the OSINT pipeline, using:
- **Python LangGraph** for agent reasoning (planning + synthesis)
- **MCP Server** for tool execution (I/O boundary)
- **Temporal** for orchestration (retries, state machine, durability)
- **MinIO + Postgres + Qdrant + Neo4j** as the evidence and derived stores
- **Web UI** for user input + live progress + final report display

---

## 0) Current State (Done)

### ✅ Infrastructure (Docker Compose)
Running and verified:
- Postgres, MinIO (erasure + versioning), Redis, Qdrant, Neo4j, Temporal, Temporal UI
- API container (Node 20, Yarn 4, Fastify) running on port 3000

### ✅ Postgres Provenance Schema
Tables exist:
- `runs`, `documents`, `document_objects`, `chunks`, `tool_calls`, `run_events`, `reports`

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
  4. `execute_tools` (MCP client calls)
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

### Milestone F — LangGraph Synthesizer (Python)
Goal: evidence-backed report + reflection

Deliverables:
- `synth_graph.py`
- Nodes:
  1. `retrieve_evidence` (Qdrant + Neo4j)
  2. `draft_summary`
  3. `resolve_conflicts`
  4. `reflection`
  5. `finalize_report`
- Writes report artifacts to MinIO + `reports` table
- emits `REPORT_READY`

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
- [ ] Each activity emits `run_events`
- [ ] Add retry + timeout per activity

## E) MCP server (TS) — minimal viable tools
- [x] MCP server scaffolding + tool registry
- [x] Tool: `fetch_url` (HTTP GET + store raw to MinIO)
- [ ] Tool: `web_search` (stub initially; later real provider)
- [x] Write tool call logs into Postgres `tool_calls`
- [x] Emit events `TOOL_CALL_STARTED/FINISHED`

## F) LangGraph Agent Service (Python)
- [x] `planner_graph.py` (tool planning + rationale + execute via MCP client)
- [x] OpenRouter LLM planning integration (direct HTTP)
- [ ] `synth_graph.py` (retrieve via Qdrant/Neo4j + write report)
- [ ] Define consistent state objects for graphs (pydantic models)
- [ ] Add “evidence policy”: output must cite chunk/document IDs

## G) Processing workers (Python)
- [ ] Parser/normalizer reading from MinIO based on `document_objects(kind='raw')`
- [ ] Chunker writes `chunks`
- [ ] Embedder writes Qdrant and stores `vector_id` back to Postgres
- [ ] (Optional) entity/claim extractor writes to Neo4j

## H) UI (Web)
- [ ] Upload form (text + file)
- [ ] Run dashboard page: SSE stream shows event timeline + tool rationale
- [ ] Report view: markdown render + citation links to evidence

## I) Production hardening
- [ ] Replace MinIO SDK with AWS S3 SDK v3 to capture `version_id` reliably
- [ ] Tool allowlists + rate limiting
- [ ] Secrets handling via env + `.env.example`
- [ ] Add basic tests for schema + ingest
- [ ] Add CI: lint, typecheck, unit tests

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
