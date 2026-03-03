# LLM-OSINT: Agentic OSINT Person Profiling Pipeline

LLM-OSINT is an **open-source, scalable OSINT research pipeline** that combines:

- **Agentic data collection** (via MCP tool servers)
- **Multimodal ingestion** (text, images, audio/video → text)
- **Graph databases** for structured facts and provenance
- **Vector databases** for semantic retrieval
- **LLMs** for evidence-grounded summarization, reasoning, and reflection

The system is designed to be **auditable, explainable, and research-friendly**, avoiding “black-box” OSINT or ungrounded LLM hallucination.

---

## 🚀 Project Goals

This project aims to:

- Build a **reproducible OSINT pipeline** from prompt → collection → analysis → report
- Separate **semantic recall (Vector DB)** from **structural truth (Graph DB)**
- Use LLMs only for **grounded synthesis**, never raw discovery
- Support **human-in-the-loop investigation** via a local web UI
- Stay **open-source, modular, and scalable**

---

## 🧠 High-Level Pipeline
![LLM-OSINT Pipeline Flow](public/Flow.png)

---

## 🧱 Core Architecture

### Agentic Collection
- Planner/executor loop controlled by an **MCP server**
- Strict tool boundaries, allowlists, rate limits, and stop rules
- Full audit log of *what was accessed and why*

### Storage Model
- **Object Storage (MinIO)**  
  Raw artifacts: HTML, PDFs, images, audio/video, transcripts
- **Vector DB (Qdrant)**  
  Semantic retrieval of evidence chunks
- **Graph DB (Neo4j)**  
  Entities, claims, relationships, provenance, confidence
- **Postgres**  
  Metadata, runs, logs, reproducibility

### LLM Usage (Constrained)
LLMs are used only after evidence is assembled:
1. Evidence-grounded summary (with citations)
2. Structured reasoning (confidence tiers, alternatives)
3. Reflection / critique (unsupported claims, gaps)

---

## 🖥️ Local Web UI (Analyst-Facing)

The frontend (localhost) allows users to:
- Start investigations with scoped prompts
- Monitor agentic collection in real time
- Browse documents and extracted evidence
- Explore entity graphs interactively
- Read/export final OSINT reports

---

## 🛠️ Tech Stack

### Languages
- **TypeScript** — APIs, MCP server, orchestration, frontend
- **Python** — parsing, extraction, embeddings, graph mining


### Infrastructure
- Docker + Docker Compose (local dev)
- Kubernetes + Helm (deployment, later)
- Temporal (workflow orchestration)

### Datastores
- MinIO (object storage)
- PostgreSQL (metadata, provenance)
- Qdrant (vector database)
- Neo4j Community (graph database)
- Redis (cache)

### LLM Integration
- OpenRouter-backed planning, embeddings, and Stage 2 drafting/retrieval helpers
- Heuristic fallbacks where LLM calls are unavailable

---

## ⚖️ Ethics & Scope

This project is intended for **legitimate OSINT use**, such as:

- Research and education
- Security analysis
- Fraud/impersonation detection
- Investigating *your own* digital footprint
- Consent-based or authorized analysis

**Non-goals:**
- Doxxing
- Harassment
- Circumventing paywalls or authentication
- Accessing private or non-public data

All agent actions are:
- Logged
- Scope-restricted
- Auditable

---

## 🧑‍💻 Development Setup (Quick Start)

### Prerequisites
- Windows / macOS / Linux
- Docker Desktop (WSL2 on Windows)
- VS Code + Dev Containers extension

### Start Developing
1. Open repo in VS Code
2. **“Reopen in Container”** (clone into container volume is supported)
3. Inside the container:
   ```bash
   yarn dev
   ```

This brings up:
- Infrastructure services (Postgres, MinIO, Neo4j, Qdrant, Temporal)
- Backend API
- Frontend web UI (localhost)

---

## 📂 Repository Structure (Current)

```
apps/
  web/            # Analyst UI (React + Vite)
  api/            # Fastify API + run/event endpoints + LangGraph launcher
  mcp-server/     # MCP tool server (HTTP) + Python tool bridge
services/
  agent-langgraph/ # Planner graph, tool worker graph, Stage 2 report graph
  worker-embedding/ # vLLM OpenAI-compatible embedding service
  worker-python/   # Deterministic ingestion/chunk/embed helpers
  worker-temporal/ # Temporal worker skeleton
packages/
  shared/          # Shared package placeholder
infra/
  docker/          # Docker Compose (local dev)
  db/              # Postgres + Neo4j schema migrations
.devcontainer/     # VS Code Dev Container config
```

---

## 🧪 Project Status

**Current stage:**  
🟡 Foundation complete, Stage 1 pipeline working, Stage 2 reporting prototyped

**Implemented now:**
- Docker-based local stack for Postgres, MinIO, Qdrant, Neo4j, Redis, Temporal, API, and MCP servers
- Fastify API with run creation, run listing, SSE events, file listing, graph listing, and report retrieval endpoints
- MCP server over **Streamable HTTP** with working `fetch_url`, `ingest_text`, graph ingest, report-query, and Python-bridge tools
- LangGraph **planner graph** plus **tool worker graph** with receipts, noteboard notes, vector ingest, and graph ingest hooks
- LangGraph **Stage 2 report graph** with outline/draft/claim/evidence persistence to Stage 2 tables
- Web UI for starting runs, monitoring live events, and inspecting file/graph evidence after completion

**Still incomplete or only partially wired:**
- Temporal workflow is still a stub; it is not orchestrating the real pipeline yet
- `worker-python` exists for ingestion logic, but there is no standalone background processing service wired into runs
- Stage 2 reporting exists in Python, but API autostart currently launches only Stage 1 unless Stage 2 is invoked explicitly
- No dedicated analyst report viewer in the web app yet
- CI, broader tests, rate limiting, and stronger safety controls are still pending

---

## 🤝 Contributions

Contributions are welcome once the core pipeline stabilizes.

Planned contribution guidelines:
- Clear provenance for all extracted data
- No LLM-only facts
- Reproducible runs

---

## 📜 License

MIT

---

## 📌 Disclaimer

This software is provided **as-is** for research and educational purposes.  
Users are responsible for complying with applicable laws, platform terms of service, and ethical standards.
