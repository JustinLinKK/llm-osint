# LLM-OSINT Setup Guide

Run the current local stack:
- Infra: Postgres, MinIO, Neo4j, Qdrant, Redis, Temporal
- API: Fastify backend
- MCP servers: normal + kali-osint toolset
- Agent: LangGraph planner/tool worker
- Web UI: React + HeroUI frontend

Current behavior:
- API autostart runs the LangGraph planner by default
- Stage 2 report generation is now part of the default run lifecycle
- The web UI can load the generated report from the API when Stage 2 finishes

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

Stage 1 blueprint runtime toggles:

```bash
STAGE1_BLUEPRINT_ENABLED=true
STAGE1_BLUEPRINT_CONTRACT_PATH=/workspaces/llm-osint/schemas/stage1_graph_blueprint_contract.v1.json
STAGE1_BLUEPRINT_ENFORCEMENT=balanced
STAGE1_SOCIAL_TIMELINE_MAX_FAILURES=2
```

Notes:
- `STAGE1_BLUEPRINT_ENFORCEMENT=balanced` blocks Stage 1 stop when required blueprint slots are missing.
- `STAGE1_SOCIAL_TIMELINE_MAX_FAILURES` caps retries for `x_get_user_posts_api` and `linkedin_download_html_ocr` to prevent repeated failure loops.
- Visual files (`graph_blueprint_sample.json/png`) are design aids only; runtime enforcement uses the contract JSON above.

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

Normal startup should not rebuild images. This matters for `worker-embedding` and `mcp-server-kali`, which are heavier and usually do not change often.

```bash
yarn infra:up
```

Check status:

```bash
yarn infra:ps
```

Use a rebuild only when Dockerfiles, Python dependencies, Node dependencies, or base images changed:

```bash
yarn infra:up:build
```

Targeted rebuilds for the heavier services:

```bash
yarn infra:rebuild:embedding
yarn infra:rebuild:kali
```

Simple restarts when only env/runtime state changed:

```bash
yarn infra:restart:all-lite
yarn infra:restart:api
yarn infra:restart:embedding
yarn infra:restart:kali
```

Use `yarn infra:restart:all-lite` when you want to restart the main infra stack after code changes without touching `mcp-server-kali`. If `EMBEDDING_PROVIDER=vllm`, it will also auto-start `worker-embedding` when missing.

Code update workflow without touching the two large images:

- For most TypeScript/Python code changes, run `yarn infra:up` and restart only the service you changed.
- Do not rebuild `worker-embedding` or `mcp-server-kali` unless you changed their Dockerfiles, dependencies, or base image requirements.
- Examples:

```bash
yarn infra:restart:all-lite
yarn infra:restart:api
docker compose -f infra/docker/docker-compose.yml restart mcp-server
```

- If you changed only frontend code, run:

```bash
yarn workspace @osint/web dev
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

If you only need the Stage 2 reporting tables:

```bash
docker compose --env-file infra/docker/.env -f infra/docker/docker-compose.yml exec -T postgres \
  psql -U osint -d osint < infra/db/migrations/0007_stage2_reports.sql
```

---

## 5) Start Web UI (Local Dev)

```bash
yarn workspace @osint/web dev
```

Open: http://localhost:5173

The web app proxies API requests to `http://localhost:3000`.

Restart the web UI after frontend code changes:

```bash
yarn dev:web
```

If it is already running in a terminal, stop it with `Ctrl+C` and start it again.

If you need both backend and frontend refreshed after code changes:

```bash
yarn restart:api-web
```

`yarn restart:api-web` restarts the Docker `api` service, then starts the web dev server in the current terminal.
If a previous web dev server is still running, stop it with `Ctrl+C` first.

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

In Docker Compose, the `api` image now includes the LangGraph Python runtime,
so frontend `Start Run` works without running API locally.

List runs (paged):

```bash
curl "http://localhost:3000/runs?limit=20&offset=0"
```

Verify the local embedder:

```bash
docker exec worker-embedding nvidia-smi
docker exec worker-embedding /bin/sh -lc "curl -sS http://127.0.0.1:8000/v1/models"
docker exec worker-embedding /bin/sh -lc "curl -sS http://127.0.0.1:8000/v1/embeddings -H 'Content-Type: application/json' -d '{\"model\":\"Qwen/Qwen3-Embedding-0.6B\",\"input\":[\"hello world\"]}' | head -c 400"
docker logs --tail 50 worker-embedding
```

What to check:

- `nvidia-smi` inside the container shows your GPU and driver.
- `/v1/models` returns `Qwen/Qwen3-Embedding-0.6B`.
- `/v1/embeddings` returns `200 OK` with an embedding array.
- `worker-embedding` logs show `device_config=cuda` and `POST /v1/embeddings HTTP/1.1" 200 OK`.

Verify backend containers can reach the embedder:

```bash
docker exec docker-mcp-server-1 /bin/sh -lc 'echo $EMBEDDING_API_URL && echo $EMBEDDING_MODEL && getent hosts worker-embedding'
```

End-to-end ingest + vector retrieval check:

```bash
RUN_ID=$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)

MCP_TEST_TOOL=ingest_text \
RUN_ID=$RUN_ID \
MCP_TEST_ARGS="{\"runId\":\"$RUN_ID\",\"text\":\"## Profile\nAda Lovelace wrote notes on the Analytical Engine.\n\n## Interests\nCharles Babbage collaborated with Ada Lovelace on early computing ideas.\",\"sourceUrl\":\"https://example.com/ada-lovelace\",\"title\":\"Ada Lovelace Test\"}" \
yarn tsx apps/mcp-server/scripts/test-python-tools.ts

MCP_TEST_TOOL=vector_search \
RUN_ID=$RUN_ID \
MCP_TEST_ARGS="{\"runId\":\"$RUN_ID\",\"query\":\"Who collaborated with Ada Lovelace on the Analytical Engine?\",\"k\":3}" \
yarn tsx apps/mcp-server/scripts/test-python-tools.ts
```

Expected result:

- `ingest_text` returns a `documentId`, `chunkCount`, and `vectorCount`.
- `vector_search` returns a ranked result containing the ingested Ada/Charles snippet.
- If you switched embedding models and hit a Qdrant vector dimension mismatch, set a new `QDRANT_COLLECTION` in `.env` and restart `mcp-server` plus `mcp-server-kali` so a fresh collection is created for the new embedding size.

---

## 7) Run Agent Pipeline Test

With virtualenv active, run the integration script explicitly:

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

## 8) Run Automated Tests

Agent-langgraph unit/regression tests:

```bash
. .venv-agent/bin/activate
cd services/agent-langgraph
pytest -q
```

Stage 1 blueprint alignment focused tests:

```bash
. .venv-agent/bin/activate
cd services/agent-langgraph
pytest -q tests/test_planner_technical_smoke.py -k "graph_slot or blueprint or stop_gate"
pytest -q tests/test_tool_worker_normalization.py -k "topic or timeline or time_node"
```

Optional MCP graph ingest compatibility smoke:

```bash
yarn tsx apps/mcp-server/scripts/test-ingest-graph.ts
```

Notes:
- `src/run_pipeline_test.py` is an integration runner script and is intentionally excluded from `pytest` collection.
- Use section 7 to run that script directly when you want end-to-end validation.

---

## 9) Access UIs

- Web UI: http://localhost:5173
- MinIO Console: http://localhost:9001 (`minio` / `minio12345`)
- Neo4j Browser: http://localhost:7474 (`neo4j` / `neo4jpassword`)
- Temporal UI: http://localhost:8233
- Qdrant Dashboard: http://localhost:6333/dashboard

---

## 10) Common Operations

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

## 11) Troubleshooting

- `externally-managed-environment` during OSINT image build:
  - fixed by installing pip packages into `/opt/osint-venv` in `install-osint-tools.sh`.

- Agent cannot reach MCP or Postgres from Dev Container:
  - run `docker network connect docker_default $(hostname)` once.

- `ingest_text` fails with OpenRouter error:
  - if `EMBEDDING_PROVIDER=openrouter`, set `OPENROUTER_API_KEY` in `.env`.
  - if `EMBEDDING_PROVIDER=vllm` or `custom`, verify `EMBEDDING_API_URL`.

- The `mcp-server-kali` image is a curated Debian-based OSINT image (not full Kali):
  - tool installs are best-effort in `infra/docker/install-osint-tools.sh`; check build logs for warnings.
  - if wrappers return passive fallback warnings, rebuild `mcp-server-kali`.

- `worker-embedding` first boot is slow:
  - the first pull of `vllm/vllm-openai:latest` is large.
  - after that, prefer `yarn infra:restart:embedding` over full rebuilds.
