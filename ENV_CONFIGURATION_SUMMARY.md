# Environment Configuration Summary

This repo now uses a single root `.env` as the main configuration file for both local tooling and Docker Compose-based development.

## Recommended approach

Use `.env.example` for all setups:

```bash
cp .env.example .env
```

Then:

- keep Docker service names if you run code inside the dev container
- replace service names with `localhost` if you run code directly on the host OS

## Current templates

| File | Status | Intended use |
|------|--------|--------------|
| `.env.example` | primary | repo root runtime config |
| `.env.linux` | reference only | localhost example values |
| `.env.windows` | reference only | localhost example values |
| `.env.macos` | reference only | localhost example values |
| `infra/docker/.env.example` | optional | compose-only override when launching from `infra/docker` |

## Current defaults in `.env.example`

- Postgres: `postgresql://osint:osint@postgres:5432/osint`
- MinIO: `http://minio:9000`
- Qdrant: `http://qdrant:6333`
- Neo4j: `bolt://neo4j:7687`
- Temporal: `temporal:7233`
- Redis: `redis://redis:6379`
- MCP server: `http://mcp-server:3001/mcp`
- Kali MCP server: `http://mcp-server-kali:3002/mcp`

## Important runtime flags

- `LANGGRAPH_AUTOSTART=true` by default
- API-launched runs execute `run_planner.py` with `--run-stage2`
- `EMBEDDING_PROVIDER=vllm` by default in `.env.example`
- Stage 1 blueprint enforcement is enabled by default

## Dev container rule

If your shell is running inside the dev container, connect it to the compose network once:

```bash
docker network connect docker_default $(hostname) || true
```

Without that, names like `postgres` and `minio` will not resolve.

## Native host rule

If you run API, web, or helper scripts directly on Linux, macOS, or Windows, change service hosts in `.env` from compose names to `localhost`.

Examples:

- `postgres` -> `localhost`
- `minio` -> `localhost`
- `qdrant` -> `localhost`
- `neo4j` -> `localhost`
- `temporal` -> `localhost`
- `redis` -> `localhost`

## Validation

Useful checks:

```bash
./test-env-setup.sh
curl http://localhost:3000/health
cd apps/mcp-server && yarn example
```

## Related docs

- `ENV_FILES_README.md`
- `PLATFORM_COMPARISON.md`
- `SETUP.md`
