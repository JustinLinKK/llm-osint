# Environment Files

Root-level environment docs have been simplified to match the current repo workflow.

## Use this by default

```bash
cp .env.example .env
```

`.env.example` is the primary template.

## Choose hostnames based on where your code runs

Use Docker service names when your shell is inside the dev container:

- `postgres`
- `minio`
- `qdrant`
- `neo4j`
- `temporal`
- `redis`
- `mcp-server`

Use `localhost` when your code runs on the host OS.

## Template roles

| File | Role |
|------|------|
| `.env.example` | main template |
| `.env.linux` | localhost reference |
| `.env.windows` | localhost reference |
| `.env.macos` | localhost reference |
| `infra/docker/.env.example` | optional compose-specific override |

## Current repo behavior tied to env

- `LANGGRAPH_AUTOSTART=true` causes the API to spawn LangGraph on `POST /runs`
- the API also passes `--run-stage2`
- `EMBEDDING_PROVIDER`, `EMBEDDING_API_URL`, and model settings control vector ingest behavior
- `MCP_PYTHON_TOOLS` controls Python tool bridge registration

## Common failure cases

Dev container:

```bash
docker network connect docker_default $(hostname) || true
getent hosts postgres minio qdrant neo4j
```

Native host:

```bash
docker compose -f infra/docker/docker-compose.yml ps
curl http://localhost:3000/health
```

## Quick verification

```bash
./test-env-setup.sh
cd apps/mcp-server && yarn example
```
