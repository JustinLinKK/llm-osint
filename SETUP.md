# Setup Guide

This guide matches the current repo behavior as of March 6, 2026.

## Prerequisites

- Docker + Docker Compose
- Node.js 20+
- Yarn 4
- Python 3.11+

VS Code dev container is recommended but not required.

## 1. Create `.env`

```bash
cp .env.example .env
```

Required for normal LangGraph planning:

```bash
OPENROUTER_API_KEY=your_key_here
```

Common optional credentials:

```bash
TAVILY_API_KEY=
HIBP_API_KEY=
SHODAN_API_KEY=
LINKEDIN_EMAIL=
LINKEDIN_PASSWORD=
BROWSERBASE_API_KEY=
BROWSERBASE_PROJECT_ID=
X_BEARER_TOKEN=
```

Notes:

- Inside the dev container, keep service hostnames like `postgres`, `minio`, `qdrant`, `neo4j`, `mcp-server`.
- On a native host, copy `.env.example` and replace those hostnames with `localhost`.
- The API defaults `LANGGRAPH_AUTOSTART=true`, and API-launched runs include Stage 2 automatically.

## 2. Install dependencies

```bash
yarn install
python3 -m venv .venv-agent
. .venv-agent/bin/activate
pip install -r services/agent-langgraph/requirements.txt
```

## 3. Start infrastructure

```bash
yarn infra:up
```

Useful variants:

```bash
yarn infra:ps
yarn infra:up:build
yarn infra:restart:all-lite
yarn infra:restart:api
yarn infra:restart:embedding
yarn infra:restart:kali
```

When to rebuild instead of restart:

- Dockerfile changed
- Python or Node dependencies changed inside a container image
- base image changed

## 4. Dev container network hookup

If you are inside the dev container:

```bash
docker network connect docker_default $(hostname) || true
```

This is required so the dev container can resolve compose service names like `postgres` and `minio`.

## 5. Apply database migrations

```bash
PG_CID=$(docker compose -f infra/docker/docker-compose.yml ps -q postgres)
for f in infra/db/migrations/*.sql; do
  echo "Applying $f"
  docker exec -i "$PG_CID" psql -U osint -d osint < "$f"
done
```

Current migration set:

- `0001_init.sql`
- `0002_run_events.sql`
- `0003_reports.sql`
- `0004_micro_agent_schema.sql`
- `0005_evidence_links.sql`
- `0006_run_titles.sql`
- `0007_stage2_reports.sql`

## 6. Start the web app

```bash
yarn dev:web
```

Open `http://localhost:5173`.

The Vite app proxies API requests to `http://localhost:3000`.

## 7. Verify services

Health check:

```bash
curl http://localhost:3000/health
```

Expected:

```json
{"ok":true}
```

Create a run:

```bash
curl -X POST http://localhost:3000/runs \
  -H "Content-Type: application/json" \
  -d '{"prompt":"Investigate example.com and related accounts"}'
```

Follow live events:

```bash
curl -N http://localhost:3000/runs/<RUN_ID>/events
```

Fetch the latest report:

```bash
curl http://localhost:3000/runs/<RUN_ID>/report
```

## Current service ports

- API: `3000`
- MCP server: `3001`
- Kali MCP server: `3002`
- Web UI: `5173`
- Postgres: `5432`
- Redis: `6379`
- Qdrant: `6333`
- Neo4j browser / bolt: `7474` / `7687`
- Temporal / UI: `7233` / `8233`
- MinIO API / console: `9000` / `9001`
- Embedding worker: host `8008` -> container `8000`

## What is real today

- `POST /runs` starts the LangGraph planner from the API
- the API passes `--run-stage2`
- Stage 2 report snapshots are stored in `report_runs`, `section_drafts`, `claim_ledger`, and `evidence_refs`
- `GET /runs/:runId/report` prefers the Stage 2 tables, then falls back to the legacy `reports` path

## What is not fully wired

- Temporal is still a skeleton worker, not the primary orchestrator
- `services/worker-python` is not running as an independent background service
- upload-centric workflows are still limited compared with prompt-driven runs
