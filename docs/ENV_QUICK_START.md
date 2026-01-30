# Environment Configuration - Quick Summary

## What We Fixed

Previously, MCP server couldn't connect to Docker services when spawned from the dev container. Now it works by:

1. **Using proper environment files** with Docker service names
2. **Connecting dev container to docker_default network**
3. **Loading config from .env** instead of hardcoding

## Files Created/Modified

### Created:
- `.env.example` - Template for dev container (Docker service names)
- `.env.linux` - Template for native Linux host (localhost)
- `.env.windows` - Template for Windows host (localhost)
- `.env.macos` - Template for macOS host (localhost)
- `.env` - Active dev container configuration (gitignored)
- `docs/ENVIRONMENT.md` - Comprehensive environment guide

### Updated:
- `infra/docker/.env` - Cleaned up and documented
- `infra/docker/.env.example` - Better documentation
- `apps/mcp-server/src/example-client.ts` - Now loads from .env
- `apps/mcp-server/package.json` - Added dotenv dependency
- `SETUP.md` - Added environment configuration step

## Quick Start

**For Dev Container (default):**
```bash
# 1. Copy environment template (uses Docker service names)
cp .env.example .env

# 2. Start infrastructure
docker compose -f infra/docker/docker-compose.yml up -d

# 3. Connect dev container to network (CRITICAL!)
docker network connect docker_default $(hostname)

# 4. Verify it works
cd apps/mcp-server && yarn example
```

**For Native Linux Host:**
```bash
# 1. Copy Linux template (uses localhost)
cp .env.linux .env

# 2. Start infrastructure
docker compose -f infra/docker/docker-compose.yml up -d

# 3. No network connection needed (you're on the host)

# 4. Verify it works
cd apps/mcp-server && yarn example
```

**For Windows Host:**
```powershell
# 1. Copy Windows template (uses localhost)
copy .env.windows .env

# 2. Start infrastructure
docker compose -f infra/docker/docker-compose.yml up -d

# 3. Verify it works
cd apps/mcp-server
yarn example
```

**For macOS Host:**
```bash
# 1. Copy macOS template (uses localhost)
cp .env.macos .env

# 2. Start infrastructure
docker compose -f infra/docker/docker-compose.yml up -d

# 3. Verify it works
cd apps/mcp-server && yarn example
```

## Key Configuration

### Dev Container (`.env.example`)

Uses **Docker service names** (requires network connection):

```bash
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
```

This works because the dev container is connected to the `docker_default` network where these service names are resolvable via Docker DNS.

### Native Host (`.env.linux`, `.env.windows`, `.env.macos`)

Uses **localhost** (no network connection needed):

```bash
DATABASE_URL=postgresql://osint:osint@localhost:5432/osint
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio12345
MINIO_BUCKET=osint-raw
```

This works because Docker Compose exposes ports to the host machine, accessible via localhost.

## Testing

```bash
# Verify service resolution
getent hosts postgres minio
# Should show: 172.18.0.x postgres / 172.18.0.x minio

# Test MCP client (loads .env automatically)
cd apps/mcp-server && yarn example
# Should successfully fetch URL and store to MinIO+Postgres
```

## For Future Developers

1. **Always run** `docker network connect docker_default $(hostname)` after dev container starts
2. **Use .env files** - never hardcode connection strings
3. **Service names in dev container** - postgres, minio, etc. (not localhost)
4. **localhost on host machine** - when accessing from outside Docker

See [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) for complete details.
