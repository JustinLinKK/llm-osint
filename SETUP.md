# LLM OSINT Pipeline - Setup Guide

Complete guide for new developers to get the project running from scratch.

---

## Prerequisites

- **VS Code** with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- **Docker Desktop** (or Docker Engine + Docker Compose)
- **Git**

---

## 1. Clone Repository in Dev Container Volume

This approach gives you better I/O performance by using a Docker volume instead of mounting from host filesystem.

### Steps:

1. Open VS Code
2. Press `F1` or `Cmd/Ctrl+Shift+P` to open Command Palette
3. Search for: **"Dev Containers: Clone Repository in Container Volume"**
4. Enter repository URL:
   ```
   https://github.com/JustinLinKK/llm-osint.git
   ```
5. Select branch: `main`
6. Wait for the container to build and VS Code to connect (first time takes ~5 minutes)

The dev container includes:
- Ubuntu 22.04 LTS
- Node.js 20 + Yarn 4
- Python 3 + pip
- Docker CLI (for managing compose services)
- Git, curl, wget, and common CLI tools

---

## 2. Configure Environment Variables

Copy the appropriate environment template based on your setup:

**Option A: Dev Container (Recommended)**
```bash
# Uses Docker service names (postgres, minio, etc.)
cp .env.example .env
```

**Option B: Native Linux Host**
```bash
# Uses localhost for all services
cp .env.linux .env
```

**Option C: Windows Host**
```powershell
# Uses localhost for all services
copy .env.windows .env
```

**Option D: macOS Host**
```bash
# Uses localhost for all services
cp .env.macos .env
```

**Important differences:**
- **Dev container**: Uses service names, requires network connection (step 3)
- **Native hosts**: Uses localhost, no network connection needed

Review the configuration:
```bash
cat .env
```

For detailed information about environment configuration, see [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md).

---

## 3. Start Infrastructure Services

From the **terminal inside the dev container**:

```bash
# Start all services (PostgreSQL, MinIO, Redis, Qdrant, Neo4j, Temporal)
docker compose -f infra/docker/docker-compose.yml up -d

# Verify all services are healthy
docker compose -f infra/docker/docker-compose.yml ps

# CRITICAL: Connect dev container to the same Docker network
# This allows MCP server (spawned from dev container) to reach services
docker network connect docker_default $(hostname)

# Verify service name resolution works
getent hosts postgres minio
# Should show: 172.18.0.x postgres / 172.18.0.x minio
```

**Why the network connection is needed (Dev Container only):**
- The dev container runs on the default `bridge` network
- Docker Compose services run on `docker_default` network  
- Connecting to both networks allows DNS resolution of service names
- Without this, you'll get "ENOTFOUND postgres" errors

**Skip this step if using native host** (Linux/Windows/macOS with localhost config)

---

## 4. Apply Database Migrations

The database schema is defined in migration files that must be run in order.

```bash
# Get the postgres container ID
PG_CID=$(docker compose -f infra/docker/docker-compose.yml ps -q postgres)

# Run all migrations in order
docker exec -i $PG_CID psql -U osint -d osint < infra/db/migrations/0001_init.sql
docker exec -i $PG_CID psql -U osint -d osint < infra/db/migrations/0002_run_events.sql
docker exec -i $PG_CID psql -U osint -d osint < infra/db/migrations/0003_reports.sql
```

### What Gets Created:

**0001_init.sql** - Core schema:
- `runs` - Investigation sessions
- `documents` - Logical artifacts (URLs, files)
- `document_objects` - MinIO object pointers (bucket/key/etag)
- `chunks` - Text chunks for embedding
- `tool_calls` - Agent audit log

**0002_run_events.sql** - Observability:
- `run_events` - Event stream for real-time UI updates (SSE source)

**0003_reports.sql** - Report outputs:
- `reports` - Pointers to LLM-generated reports (markdown/JSON in MinIO)

---

## 5. Install Dependencies

Install all workspace dependencies (runs for all packages):

```bash
yarn install
```

This installs dependencies for:
- `packages/shared` - Zod schemas for types
- `apps/api` - Fastify API server
- `apps/mcp-server` - MCP tool server
- `apps/web` - Web UI (not yet implemented)
- `services/worker-temporal` - Temporal workflow worker

---

## 5. Install Dependencies

Install all workspace dependencies (runs for all packages):

```bash
yarn install
```

This installs dependencies for:
- `packages/shared` - Zod schemas for types
- `apps/api` - Fastify API server
- `apps/mcp-server` - MCP tool server (includes dotenv for config)
- `apps/web` - Web UI (not yet implemented)
- `services/worker-temporal` - Temporal workflow worker

---

## 6. Build All Projects

Compile TypeScript for all services:

```bash
# Build shared package first (other packages depend on it)
yarn workspace @osint/shared build

# Build API
yarn workspace @osint/api build

# Build MCP server
yarn workspace @osint/mcp-server build

# Build Temporal worker
yarn workspace @osint/worker-temporal build
```

Or build everything at once:

```bash
# From root
yarn workspaces foreach -A run build
```

---

## 6. Run Services in Development Mode

Now run the services that are ready:

### Terminal 1: API Server

```bash
cd apps/api
yarn dev
```

API will start on **http://localhost:3000** with hot reload.

Available endpoints:
- `GET /health` - Health check
- `POST /runs` - Create investigation run
- `POST /runs/:runId/ingest-text` - Ingest raw text
- `GET /runs/:runId/events` - Stream events (SSE)
- `GET /runs/:runId` - Get run status + report pointer
- `GET /runs/:runId/report` - Get rendered report with citations

### Terminal 2: MCP Server

```bash
cd apps/mcp-server
yarn dev
```

MCP server runs on **stdio transport** (not HTTP). It's designed to be spawned by an MCP client like Claude Desktop or the LangGraph agent.

Available tools:
- `fetch_url` - Fetch URL via HTTP GET and store to MinIO + Postgres

### Terminal 3: Temporal Worker (Optional - Not Yet Fully Wired)

```bash
cd services/worker-temporal
yarn dev
```

Temporal worker connects to Temporal server and polls for workflows. The skeleton is defined but not yet fully integrated.

---

## 7. Verify Everything Works

### Test API Health:

```bash
curl http://localhost:3000/health
```

Expected response:
```json
{
  "status": "healthy",
  "timestamp": "2026-01-30T..."
}
```

### Create a Test Run:

```bash
curl -X POST http://localhost:3000/runs \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Investigate example.com",
    "seeds": [{"type": "url", "value": "https://example.com"}]
  }'
```

Returns:
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Ingest Text:

```bash
curl -X POST http://localhost:3000/runs/<RUN_ID>/ingest-text \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Sample evidence document",
    "sourceLabel": "test-source"
  }'
```

### Stream Events (SSE):

```bash
curl -N http://localhost:3000/runs/<RUN_ID>/events
```

You should see SSE events in real-time as operations occur.

---

## 8. Access Web UIs

While developing, you can access these web interfaces:

- **MinIO Console**: http://localhost:9001
  - Username: `minio`
  - Password: `minio12345`
  - Browse raw documents, check bucket contents

- **Temporal UI**: http://localhost:8233
  - View workflow executions, task queues, namespaces

- **Neo4j Browser**: http://localhost:7474
  - Username: `neo4j`
  - Password: `neo4jpassword`
  - (Not yet wired to pipeline)

- **Qdrant Dashboard**: http://localhost:6333/dashboard
  - (Not yet wired to pipeline)

---

## 9. Project Structure

```
llm-osint/
├── apps/
│   ├── api/              # Fastify API server (SSE, runs, reports)
│   ├── mcp-server/       # MCP tool server (fetch_url via stdio)
│   └── web/              # Web UI (TODO)
├── packages/
│   └── shared/           # Zod schemas for cross-service types
├── services/
│   ├── agent-langgraph/  # Python LangGraph agent (TODO)
│   ├── worker-python/    # Processing workers (TODO)
│   └── worker-temporal/  # Temporal workflow orchestrator (skeleton)
├── infra/
│   ├── db/migrations/    # PostgreSQL schema migrations
│   └── docker/
│       └── docker-compose.yml  # All infrastructure services
└── scripts/
    └── bootstrap-dev.sh  # Future: one-command setup
```

---

## 10. Common Commands (Cheat Sheet)

```bash
# Infrastructure management
yarn infra:up                              # Start all docker services
yarn infra:down                            # Stop all services
docker compose -f infra/docker/docker-compose.yml logs -f api  # Tail API logs

# Development
yarn dev:api                               # Run API in dev mode
cd apps/mcp-server && yarn dev            # Run MCP server
cd services/worker-temporal && yarn dev    # Run Temporal worker

# Building
yarn workspace @osint/shared build
yarn workspace @osint/api build
yarn workspace @osint/mcp-server build
yarn workspaces foreach -A run build      # Build all at once

# Database
docker compose -f infra/docker/docker-compose.yml exec postgres psql -U osint -d osint
# Then run SQL queries or check tables

# Reset database (nuclear option)
docker compose -f infra/docker/docker-compose.yml down -v  # Delete volumes
docker compose -f infra/docker/docker-compose.yml up -d postgres
# Re-run migrations from step 3
```

---

## 11. What's Working vs. Not Yet Implemented

### ✅ Working:
- PostgreSQL schema (runs, documents, chunks, tool_calls, run_events, reports)
- MinIO object storage with versioning
- API server with SSE streaming
- MCP server with fetch_url tool (stdio transport)
- Temporal workflow skeleton
- Shared type definitions (Zod schemas)

### 🚧 Not Yet Implemented:
- LangGraph Python agent to call MCP tools
- Chunking + embedding to Qdrant
- Graph extraction to Neo4j
- Web UI for viewing results
- Additional MCP tools (web_search, parse_pdf, OCR, ASR)
- LLM report synthesis
- Docker deployment of API/MCP server (currently dev mode only)

---

## 12. Troubleshooting

### Services won't start:

```bash
# Check if ports are already in use
lsof -i :5432,9000,6379,6333,7474,7233,8233

# Reset everything
docker compose -f infra/docker/docker-compose.yml down -v
docker compose -f infra/docker/docker-compose.yml up -d
```

### Database connection errors:

```bash
# Ensure postgres is healthy
docker compose -f infra/docker/docker-compose.yml ps postgres

# Check logs
docker compose -f infra/docker/docker-compose.yml logs postgres

# Test connection
docker compose -f infra/docker/docker-compose.yml exec postgres psql -U osint -d osint -c "SELECT 1;"
```

### MCP server not responding:

The MCP server uses stdio transport, so you can't curl it like an HTTP server. It needs to be spawned by an MCP client that communicates via stdin/stdout.

### Build errors:

```bash
# Clean and reinstall
rm -rf node_modules
yarn install

# Rebuild
yarn workspaces foreach -A run build
```

---

## 13. Next Steps After Setup

Once everything is running:

1. **Test the API**: Create runs, ingest text, stream events
2. **Explore the database**: Connect via psql and examine tables
3. **Check MinIO**: Verify documents are being stored
4. **Read the code**: Start with [apps/api/src/index.ts](apps/api/src/index.ts) and [apps/mcp-server/src/index.ts](apps/mcp-server/src/index.ts)
5. **Build the agent**: Next logical step is the Python LangGraph agent that calls MCP tools
6. **Add processing**: Implement chunking, embedding, and graph extraction workers

---

## Questions?

Check the following files for more context:
- [Checkpoint.md](Checkpoint.md) - Current project status
- [PROJECT_PLAN_TODO.md](PROJECT_PLAN_TODO.md) - Roadmap and TODOs
- [README.md](README.md) - Project overview

Happy hacking! 🚀
