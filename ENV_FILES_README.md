# Environment Configuration Files

This directory contains environment configuration templates for different deployment scenarios.

## Available Templates

| File | Use Case | Service URLs |
|------|----------|--------------|
| `.env.example` | **VS Code Dev Container** (default) | `postgres:5432`, `minio:9000` |
| `.env.linux` | Native Linux host | `localhost:5432`, `localhost:9000` |
| `.env.windows` | Windows + Docker Desktop | `localhost:5432`, `localhost:9000` |
| `.env.macos` | macOS + Docker Desktop | `localhost:5432`, `localhost:9000` |

## Quick Setup

### 1. Choose Your Template

**Dev Container (Recommended):**
```bash
cp .env.example .env
docker network connect docker_default $(hostname)
```

**Native Linux:**
```bash
cp .env.linux .env
```

**Windows:**
```powershell
copy .env.windows .env
```

**macOS:**
```bash
cp .env.macos .env
```

### 2. Start Services

```bash
docker compose -f infra/docker/docker-compose.yml up -d
```

### 3. Test Connection

```bash
cd apps/mcp-server && yarn example
```

## Key Differences

### Service Names vs Localhost

**Dev Container** (`.env.example`):
- Uses Docker service names: `postgres`, `minio`, etc.
- Requires dev container to be connected to `docker_default` network
- Services resolved via Docker DNS
- Example: `DATABASE_URL=postgresql://osint:osint@postgres:5432/osint`

**Native Host** (`.env.linux`, `.env.windows`, `.env.macos`):
- Uses `localhost` for all services
- No network connection required
- Services accessed via exposed ports on host
- Example: `DATABASE_URL=postgresql://osint:osint@localhost:5432/osint`

## When Things Don't Work

### Dev Container Issues

**Error: "ENOTFOUND postgres"**
```bash
# Solution: Connect to docker network
docker network connect docker_default $(hostname)

# Verify
getent hosts postgres minio
```

**Error: "network already connected"**
```bash
# Already connected, you're good!
# Just verify service resolution:
getent hosts postgres
```

### Native Host Issues

**Error: "Connection refused to localhost:5432"**
```bash
# Check if services are running
docker compose -f infra/docker/docker-compose.yml ps

# Check if ports are exposed
docker compose -f infra/docker/docker-compose.yml ps postgres
# Should show: 0.0.0.0:5432->5432/tcp
```

**Windows: localhost not working**
```bash
# Try 127.0.0.1 instead
DATABASE_URL=postgresql://osint:osint@127.0.0.1:5432/osint

# Or use host.docker.internal (Docker Desktop 18.03+)
DATABASE_URL=postgresql://osint:osint@host.docker.internal:5432/osint
```

## File Locations

```
llm-osint/
├── .env              # Active configuration (gitignored)
├── .env.example      # Dev container template
├── .env.linux        # Linux host template
├── .env.windows      # Windows host template
├── .env.macos        # macOS host template
└── infra/docker/
    └── .env          # Docker Compose internal config
```

## What Gets Configured

All templates configure:
- **PostgreSQL** - Main database for metadata
- **MinIO** - Object storage for raw documents
- **Qdrant** - Vector database for embeddings
- **Neo4j** - Graph database for entity relationships
- **Temporal** - Workflow orchestration
- **Redis** - Cache and coordination

## Security Notes

1. **Never commit `.env`** - It's in .gitignore
2. **Change passwords in production** - These are development defaults
3. **Use secrets management** - For production deployments, use proper secrets
4. **Rotate credentials** - After sharing code or screenshots

## Additional Resources

- [Complete Environment Guide](docs/ENVIRONMENT.md)
- [Quick Start Guide](docs/ENV_QUICK_START.md)
- [Setup Instructions](SETUP.md)
- [MCP Client Guide](apps/mcp-server/MCP_CLIENT_GUIDE.md)

## Validation

Run the validation script to check your setup:

```bash
./test-env-setup.sh
```

This checks:
- ✅ `.env` file exists
- ✅ Network connectivity (if dev container)
- ✅ Service name resolution
- ✅ Services are running
- ✅ Ports are accessible
