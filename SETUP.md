# LLM-OSINT Setup Guide

Run the full local pipeline:
- Infra: Postgres, MinIO, Neo4j, Qdrant, Redis, Temporal
- API: Fastify backend
- MCP servers: normal + kali-osint toolset
- Agent: LangGraph planner/tool worker
- Web UI: Tailwind + HeroUI frontend

---

## Prerequisites

- Docker + Docker Compose
- Node.js 20+
- Yarn 4
- Python 3.11+
- VS Code Dev Container (recommended)

---

## 1) Environment Setup

From repo root:

```bash
cp .env.example .env
```

Minimum required for full pipeline behavior:

```bash
# Required for ingest_text embeddings (OpenRouter)
OPENROUTER_API_KEY=your_key_here

# Optional (used by selected OSINT tools)
HIBP_API_KEY=
SHODAN_API_KEY=
```

If running inside Dev Container, keep service hostnames like `postgres`, `minio`, `mcp-server`.
If running on native host, use `localhost` endpoints as needed.

---

## 2) Install JS + Python Dependencies

From repo root:

```bash
yarn install
```

For agent-langgraph:

```bash
python3 -m venv .venv-agent
. .venv-agent/bin/activate
pip install -r services/agent-langgraph/requirements.txt
```

---

## 3) Start Infra + MCP/API Containers

Bring up everything needed by agent + UI:

```bash
docker compose -f infra/docker/docker-compose.yml up -d --build \
  postgres redis minio qdrant neo4j temporal temporal-ui api mcp-server mcp-server-kali
```

Check status:

```bash
docker compose -f infra/docker/docker-compose.yml ps
```

If using Dev Container, connect it to compose network once:

```bash
docker network connect docker_default $(hostname) || true
```

---

## 4) Apply Database Migrations

Run all migrations in order (includes run titles + micro-agent tables):

```bash
PG_CID=$(docker compose -f infra/docker/docker-compose.yml ps -q postgres)
for f in infra/db/migrations/*.sql; do
  echo "Applying $f"
  docker exec -i "$PG_CID" psql -U osint -d osint < "$f"
done
```

---

## 5) Start Web UI (Local Dev)

```bash
yarn workspace @osint/web dev
```

Open: http://localhost:5173

The web app proxies API requests to `http://localhost:3000`.

---

## 6) Verify Core Services

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

With `LANGGRAPH_AUTOSTART=true` (default), this now launches
`services/agent-langgraph/src/run_planner.py` in the background and emits
`RUN_STARTED`/`RUN_FINISHED` or `RUN_FAILED` events to `run_events`.

List runs (paged):

```bash
curl "http://localhost:3000/runs?limit=20&offset=0"
```

---

## 7) Run Agent Pipeline Test

With virtualenv active:

```bash
. .venv-agent/bin/activate
python services/agent-langgraph/src/run_pipeline_test.py \
  --url https://en.wikipedia.org/wiki/Joe_Biden \
  --max-chars 40000
```

What this test does:
- ensures run exists
- invokes planner/tool worker
- routes tools to MCP servers (`mcp-server` / `mcp-server-kali`)
- ingests text + graph entities

Agent uses:
- `MCP_SERVER_URL` default: `http://mcp-server:3001/mcp`
- `MCP_SERVER_KALI_URL` default: `http://mcp-server-kali:3002/mcp`

If running outside container network, export:

```bash
export MCP_SERVER_URL=http://localhost:3001/mcp
export MCP_SERVER_KALI_URL=http://localhost:3002/mcp
```

---

## 8) Access UIs

- Web UI: http://localhost:5173
- MinIO Console: http://localhost:9001 (`minio` / `minio12345`)
- Neo4j Browser: http://localhost:7474 (`neo4j` / `neo4jpassword`)
- Temporal UI: http://localhost:8233
- Qdrant Dashboard: http://localhost:6333/dashboard

---

## 9) Common Operations

Tail logs:

```bash
docker compose -f infra/docker/docker-compose.yml logs -f --tail=200 api
docker compose -f infra/docker/docker-compose.yml logs -f --tail=200 mcp-server
docker compose -f infra/docker/docker-compose.yml logs -f --tail=200 mcp-server-kali
```

Stop everything:

```bash
docker compose -f infra/docker/docker-compose.yml down
```

Hard reset (deletes DB/object/vector/graph data):

```bash
docker compose -f infra/docker/docker-compose.yml down -v
```

---

## 10) Troubleshooting

- `externally-managed-environment` during OSINT image build:
  - fixed by installing pip packages into `/opt/osint-venv` in `install-osint-tools.sh`.

- Agent cannot reach MCP or Postgres from Dev Container:
  - run `docker network connect docker_default $(hostname)` once.

- `ingest_text` fails with OpenRouter error:
  - set `OPENROUTER_API_KEY` in `.env`.

- Some OSINT packages unavailable (`spiderfoot`, `recon-ng`, apt `amass`):
  - currently non-fatal warnings in image build.
