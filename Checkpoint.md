
## ✅ Current Project Status

Successfully built a **production‑grade OSINT ingestion foundation**.

### Infrastructure (Docker Compose)
Running services on a shared Docker network:

- **PostgreSQL 16** – authoritative metadata & provenance ledger  
- **MinIO (Erasure Mode + Versioning)** – immutable evidence storage  
- **Qdrant** – vector database (wired via MCP ingest_text)  
- **Neo4j** – graph database (wired via MCP graph ingest tool)  
- **Redis** – cache / coordination  
- **Temporal + Temporal UI** – workflow orchestration  
- **API (Node 20, Yarn 4, Fastify)** – containerized, production‑style

All services communicate via **container DNS** (no `localhost` misuse).

---

### Data Model (Postgres – Live)

Verified tables:

- `runs` – investigation sessions  
- `documents` – logical artifacts  
- `document_objects` – MinIO object pointers (bucket/key/version/etag)  
- `chunks` – text chunks (for vector DB)  
- `tool_calls` – agentic audit log  
- `run_events` – observability events (SSE source)
- `reports` – report pointers + status

This schema is:
- Auditable  
- Evidence‑anchored  
- Production‑appropriate  

---

### API Capabilities (Working)

- `GET /health`
- `POST /runs`
- `POST /runs/:runId/ingest-text`
- `GET /runs/:runId/events` (SSE)
- `GET /runs/:runId` (status + latest report pointer)
- `GET /runs/:runId/report` (render report + citations)

End‑to‑end flow works:
**API → MinIO (raw bytes) → Postgres (provenance metadata)**

---

### MCP + LangGraph (Working)

- MCP server `fetch_url` tool (HTTP GET → MinIO + Postgres + run_events)
- MCP server `ingest_text` tool (chunk → embed via OpenRouter → Qdrant + Postgres)
- MCP server `ingest_graph_entity` tool (Neo4j ingest with normalization + location merge by lat/lon threshold)
- LangGraph Planner (Python) with tool planning + MCP execution
- OpenRouter LLM integration for planning (fallback to heuristic URL extraction)

Planner test: OpenRouter → `fetch_url` → document stored in MinIO and logged in Postgres.
Vector test: MCP `ingest_text` → chunks stored in Postgres + vectors upserted to Qdrant.
Graph test: MCP `ingest_graph_entity` → nodes + evidence links in Neo4j.

---

## 🧠 What Is Intentionally Not Done Yet

- Automated processing pipeline (chunking/embedding worker orchestration)
- Graph extraction pipeline (automated entity/claim extraction)
- LLM summarization & reasoning
- Temporal orchestration of the LangGraph planner

The foundation is now stable enough to add these without refactors.

---

# ⚡ Quick Start (After a Break)

## 1. Open in Dev Container

In VS Code:

```
Command Palette → Dev Containers: Open Folder in Container
```

Choose **“Clone repository in container volume”**.

---

## 2. Start All Services

From repo root **inside the devcontainer**:

```bash
docker compose -f infra/docker/docker-compose.yml up -d
```

Check status:

```bash
docker compose -f infra/docker/docker-compose.yml ps
```

---

## 2.1 Apply Database Migrations (Required)

Run all SQL files in order from `infra/db/migrations`:

```bash
PG_CID=$(docker compose -f infra/docker/docker-compose.yml ps -q postgres)
NET=$(docker inspect "$PG_CID" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')

for f in infra/db/migrations/*.sql; do
  docker run --rm -i --network "$NET" -e PGPASSWORD=osint postgres:16 \
    psql -h postgres -U osint -d osint -v ON_ERROR_STOP=1 < "$f"
done
```

Re-run this after adding new migrations.

## 3. Verify Core Services

### API
```bash
curl http://localhost:3000/health
# Expected: {"ok":true}
```

### MinIO Console
- URL: http://localhost:9001  
- User: `minio`  
- Password: `minio12345`  
- Bucket: `osint-raw` (versioning enabled)

### Temporal UI
- http://localhost:8233

### Neo4j Browser
- http://localhost:7474  
- User: `neo4j`  
- Password: `neo4jpassword`

---

## 4. Create a Run (Investigation Session)

```bash
curl -X POST http://localhost:3000/runs \
  -H 'content-type: application/json' \
  -d '{"prompt":"sanity test"}'
```

Response:
```json
{ "runId": "UUID" }
```

Save the `runId`.

---

## 5. Ingest Raw Text (End‑to‑End Proof)

```bash
curl -X POST http://localhost:3000/runs/<RUN_ID>/ingest-text \
  -H 'content-type: application/json' \
  -d '{
    "text": "hello osint",
    "sourceUrl": "https://example.com",
    "title": "Example Document"
  }'
```

This will:
- Upload raw bytes to **MinIO**
- Insert metadata into **Postgres**
- Link evidence immutably

---

## 6. Verify Data in Postgres (Optional)

```bash
PG_CID=$(docker compose -f infra/docker/docker-compose.yml ps -q postgres)
NET=$(docker inspect "$PG_CID" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')

docker run --rm --network "$NET" -e PGPASSWORD=osint postgres:16 \
  psql -h postgres -U osint -d osint \
  -c "SELECT document_id, source_url, sha256 FROM documents;"
```

---

## 7. Stop Everything

```bash
docker compose -f infra/docker/docker-compose.yml down
```

---

# 🧭 Recommended Next Steps

Correct technical progression:

1. Reliable **MinIO `version_id` capture** (AWS S3 SDK v3)  
2. **Temporal wiring for LangGraph planner**  
3. **Processing pipeline for chunking + Qdrant embeddings**  
4. **Automated Neo4j extraction pipeline (entities + claims)**  
5. **LLM evidence‑backed summarization & reflection**

---

**You now have a solid, production‑grade OSINT ingestion backbone.**
