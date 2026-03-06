# Platform Configuration Comparison

This is a quick reference for how the repo should be configured depending on where commands are executed.

## Matrix

| Runtime location | Start from | Hostnames in `.env` | Extra network step |
|------------------|------------|---------------------|--------------------|
| VS Code dev container | `.env.example` | compose service names | yes |
| GitHub Codespaces / containerized shell | `.env.example` | compose service names | usually yes |
| Native Linux | `.env.example` | `localhost` | no |
| Native macOS | `.env.example` | `localhost` | no |
| Native Windows | `.env.example` | `localhost` | no |

## Dev container values

```bash
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
QDRANT_URL=http://qdrant:6333
NEO4J_URI=bolt://neo4j:7687
TEMPORAL_ADDRESS=temporal:7233
REDIS_URL=redis://redis:6379
MCP_SERVER_URL=http://mcp-server:3001/mcp
MCP_SERVER_KALI_URL=http://mcp-server-kali:3002/mcp
```

Required once:

```bash
docker network connect docker_default $(hostname) || true
```

## Native host values

```bash
DATABASE_URL=postgresql://osint:osint@localhost:5432/osint
MINIO_ENDPOINT=http://localhost:9000
QDRANT_URL=http://localhost:6333
NEO4J_URI=bolt://localhost:7687
TEMPORAL_ADDRESS=localhost:7233
REDIS_URL=redis://localhost:6379
MCP_SERVER_URL=http://localhost:3001/mcp
MCP_SERVER_KALI_URL=http://localhost:3002/mcp
```

## Operational difference

- Inside the dev container you are another container on Docker’s network.
- On the native host you access ports published by Docker Compose.

## Sanity checks

Dev container:

```bash
getent hosts postgres minio qdrant neo4j
```

Native host:

```bash
docker compose -f infra/docker/docker-compose.yml ps
curl http://localhost:3000/health
```
