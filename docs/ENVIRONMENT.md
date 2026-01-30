# Environment Configuration Guide

This document explains how environment variables are configured across the project to ensure proper connectivity between services.

## Problem We Solve

When running in a dev container, spawned processes (like the MCP server) need to connect to Docker Compose services. This requires:

1. **Network connectivity**: Dev container must be on the same Docker network as compose services
2. **Service discovery**: Using correct hostnames (service names vs localhost)
3. **Consistent configuration**: Environment variables must match the deployment context

## File Structure

```
llm-osint/
├── .env                          # Active environment (gitignored)
├── .env.example                  # Template for dev container (Docker service names)
├── .env.linux                    # Template for native Linux host (localhost)
├── .env.windows                  # Template for Windows host (localhost)
├── .env.macos                    # Template for macOS host (localhost)
└── infra/docker/
    ├── .env                      # Docker Compose environment
    └── .env.example              # Template for Docker Compose env
```

## Environment Files

### Root `.env` (Dev Container)

Used by:
- Node.js/TypeScript services running directly in dev container
- MCP server when spawned by clients in dev container
- API server in dev mode (`yarn dev:api`)
- Example clients and test scripts

**Key characteristics:**
- Uses **service names** for Docker services: `postgres`, `minio`, `qdrant`, etc.
- Assumes dev container is connected to `docker_default` network
- Loaded automatically by tools like dotenv

**Setup:**
```bash
# For dev container (default)
cp .env.example .env

# For native Linux host
cp .env.linux .env

# For Windows host
copy .env.windows .env

# For macOS host
cp .env.macos .env
```

### `infra/docker/.env` (Docker Compose)

Used by:
- Docker Compose service definitions
- Services running inside containers
- Environment variables passed to containerized apps

**Key characteristics:**
- Uses **service names** for inter-service communication
- Network: `docker_default` (defined in docker-compose.yml)
- Maps ports to host for external access

**Setup:**
```bash
cp infra/docker/.env.example infra/docker/.env
# Usually no changes needed
```

## Network Setup (Critical!)

For services in the dev container to reach Docker Compose services:

```bash
# Connect dev container to the compose network (run once after container start)
docker network connect docker_default $(hostname)

# Verify connectivity
getent hosts postgres minio
# Should output: 172.18.0.x postgres / 172.18.0.x minio
```

This command is **required** in the dev container and should be run:
- After the dev container first starts
- After restarting Docker
- If you see "ENOTFOUND postgres" or similar errors

## Service Name Resolution

### When to use service names vs localhost

| Context | Postgres | MinIO | Reason |
|---------|----------|-------|--------|
| **Dev container (after network connect)** | `postgres:5432` | `minio:9000` | Resolved via Docker DNS |
| **Native Linux host** | `localhost:5432` | `localhost:9000` | Ports exposed to host |
| **Windows host** | `localhost:5432` | `localhost:9000` | Ports exposed to host |
| **macOS host** | `localhost:5432` | `localhost:9000` | Ports exposed to host |
| **Docker Compose services** | `postgres:5432` | `minio:9000` | Same network |
| **MCP server spawned in dev container** | `postgres:5432` | `minio:9000` | Inherits parent network |

### Quick Selection Guide

**Choose `.env.example`** (service names) if:
- ✅ Running in VS Code dev container
- ✅ Dev container connected to docker_default network
- ✅ All development happens inside the container

**Choose `.env.linux`** (localhost) if:
- ✅ Running natively on Linux host machine
- ✅ Docker services accessed from outside containers
- ✅ Node.js/Python installed directly on host

**Choose `.env.windows`** (localhost) if:
- ✅ Running on Windows with Docker Desktop
- ✅ Using WSL 2 backend
- ✅ Accessing services from Windows terminal/PowerShell

**Choose `.env.macos`** (localhost) if:
- ✅ Running on macOS with Docker Desktop
- ✅ Accessing services from macOS terminal
- ✅ Apple Silicon or Intel Mac

## Configuration by Service

### MCP Server

**File:** `apps/mcp-server/src/config.ts`

Reads from environment:
```typescript
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
```

**Example client:** `apps/mcp-server/src/example-client.ts`
- Loads `.env` from workspace root
- Passes environment to spawned server process
- Demonstrates proper env variable usage

### API Server

**File:** `apps/api/src/config.ts`

Same configuration as MCP server. When running:
```bash
yarn dev:api  # Uses .env from root
```

### Temporal Worker

**File:** `services/worker-temporal/src/config.ts`

Reads `TEMPORAL_ADDRESS` and database config from environment.

## Troubleshooting

### "ENOTFOUND postgres" or "ENOTFOUND minio"

**Problem:** Dev container can't resolve Docker service names

**Solution:**
```bash
# Connect to docker_default network
docker network connect docker_default $(hostname)

# Verify
getent hosts postgres minio
```

### "ECONNREFUSED 127.0.0.1:5432"

**Problem:** Using `localhost` when service names should be used

**Solution:** Check your `.env` file uses service names:
```bash
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint  # ✓ Correct
# Not: postgresql://osint:osint@localhost:5432/osint      # ✗ Wrong in dev container
```

### Services can't talk to each other in Docker Compose

**Problem:** Using `localhost` in docker-compose.yml or service configs

**Solution:** Use service names everywhere in containerized environments:
```yaml
environment:
  DATABASE_URL: postgresql://osint:osint@postgres:5432/osint
  MINIO_ENDPOINT: http://minio:9000
```

### MCP server spawned by client can't connect

**Problem:** Client passes wrong environment or network not connected

**Solution:**
1. Ensure client loads `.env` properly
2. Ensure client passes all required env vars to spawned process
3. Check dev container is on `docker_default` network

Example from `example-client.ts`:
```typescript
import dotenv from "dotenv";
dotenv.config({ path: resolve(rootDir, ".env") });

const transport = new StdioClientTransport({
  command: "yarn",
  args: ["tsx", "src/index.ts"],
  env: {
    ...process.env,  // Pass all environment
    DATABASE_URL: process.env.DATABASE_URL,
    MINIO_ENDPOINT: process.env.MINIO_ENDPOINT,
    // ... etc
  },
});
```

## Best Practices

1. **Always use `.env` files** - Don't hardcode connection strings
2. **Use service names in dev container** - After connecting to docker_default network
3. **Never commit `.env`** - Only commit `.env.example` templates
4. **Document network requirements** - Make network setup explicit in docs
5. **Test with example client** - Verify connectivity before building new features

## Quick Reference

```bash
# Initial setup
cp .env.example .env
cp infra/docker/.env.example infra/docker/.env
docker network connect docker_default $(hostname)

# Start infrastructure
docker compose -f infra/docker/docker-compose.yml up -d

# Test connectivity
cd apps/mcp-server && yarn example

# View current environment
cat .env

# Check network connectivity
getent hosts postgres minio qdrant neo4j redis temporal
```

## Additional Resources

- [SETUP.md](../SETUP.md) - Complete setup guide
- [MCP_CLIENT_GUIDE.md](../apps/mcp-server/MCP_CLIENT_GUIDE.md) - MCP client patterns
- [Checkpoint.md](../Checkpoint.md) - Current project status
