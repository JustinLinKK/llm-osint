# Environment Configuration Summary

## ✅ What Was Created

### Environment Templates (All Scenarios)

| File | Purpose | URLs Used | Network Setup Required |
|------|---------|-----------|------------------------|
| `.env.example` | **Default template** (dev container) | Docker service names (`postgres:5432`) | ✅ Yes - `docker network connect` |
| `.env.linux` | Deprecated (Linux host reference) | localhost (`localhost:5432`) | ❌ No |
| `.env.windows` | Deprecated (Windows host reference) | localhost (`localhost:5432`) | ❌ No |
| `.env.macos` | Deprecated (macOS host reference) | localhost (`localhost:5432`) | ❌ No |

### Documentation Files

- **[ENV_FILES_README.md](ENV_FILES_README.md)** - Quick reference for choosing the right template
- **[docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)** - Comprehensive environment guide with troubleshooting
- **[docs/ENV_QUICK_START.md](docs/ENV_QUICK_START.md)** - Quick setup for each platform

### Testing Scripts

- **[test-env-setup.sh](test-env-setup.sh)** - Validates environment configuration
- **[test-mcp-connection.sh](test-mcp-connection.sh)** - Tests MCP client connectivity

## 🎯 How to Use

### Dev Container (VS Code)

```bash
# 1. Copy template
cp .env.example .env

# 2. Start services
docker compose -f infra/docker/docker-compose.yml up -d

# 3. Connect to network
docker network connect docker_default $(hostname)

# 4. Verify
./test-env-setup.sh
cd apps/mcp-server && yarn example
```

### Native Host (Linux/Windows/macOS)

```bash
# 1. Copy default template
cp .env.example .env

# 2. Update service hosts to localhost
# Example: DATABASE_URL=postgresql://osint:osint@localhost:5432/osint

# 3. Start services
docker compose -f infra/docker/docker-compose.yml up -d

# 4. Verify
./test-env-setup.sh
cd apps/mcp-server && yarn example
```

## 🔑 Key Differences Explained

### Why Different Templates?

**Dev Container Problem:**
- Dev container runs in its own Docker container
- Can't access host's localhost
- Needs Docker DNS to resolve service names
- Requires being on the same network as Compose services

**Solution:** Use service names (`postgres`) + connect to `docker_default` network

**Native Host Problem:**
- Node.js runs directly on host OS (not in container)
- Docker services only expose ports to host
- Can't resolve Docker service names from host
- Host doesn't have access to internal Docker DNS

**Solution:** Use localhost with exposed ports

### Environment Variable Comparison

| Variable | Dev Container Value | Native Host Value |
|----------|-------------------|------------------|
| DATABASE_URL | `postgresql://...@postgres:5432/osint` | `postgresql://...@localhost:5432/osint` |
| MINIO_ENDPOINT | `http://minio:9000` | `http://localhost:9000` |
| QDRANT_URL | `http://qdrant:6333` | `http://localhost:6333` |
| NEO4J_URI | `bolt://neo4j:7687` | `bolt://localhost:7687` |
| TEMPORAL_ADDRESS | `temporal:7233` | `localhost:7233` |
| REDIS_URL | `redis://redis:6379` | `redis://localhost:6379` |

## 🐛 Troubleshooting

### "ENOTFOUND postgres" (Dev Container)

```bash
# You forgot to connect to the network
docker network connect docker_default $(hostname)
getent hosts postgres  # Should now work
```

### "ECONNREFUSED localhost:5432" (Native Host)

```bash
# Services not running
docker compose -f infra/docker/docker-compose.yml up -d
docker compose -f infra/docker/docker-compose.yml ps
```

### "Wrong .env file for my setup"

```bash
# Dev container using localhost values by mistake?
rm .env
cp .env.example .env

# Native host using service names by mistake?
rm .env
cp .env.example .env
# Then update service hosts to localhost
```

### MCP Client Can't Connect

```bash
# 1. Check which .env is active
head -3 .env

# 2. Ensure dotenv is loading correctly
cd apps/mcp-server
yarn example
# Should show: "Loading config from: /workspaces/llm-osint/.env"

# 3. Check if service names resolve (dev container only)
getent hosts postgres minio
```

## 📝 Best Practices

1. **Choose the right template first** - Don't mix localhost and service names
2. **Never commit `.env`** - It's in .gitignore for a reason
3. **Use validation script** - Run `./test-env-setup.sh` before developing
4. **Update templates together** - If adding new services, update all templates
5. **Document changes** - Update ENV_FILES_README.md when adding services

## 🚀 Integration with Project

### MCP Server

The example client automatically loads `.env`:

```typescript
import dotenv from "dotenv";
dotenv.config({ path: resolve(rootDir, ".env") });
```

### API Server

Add to `apps/api/src/index.ts`:

```typescript
import dotenv from "dotenv";
dotenv.config();
```

### Python Services (Future)

```python
from dotenv import load_dotenv
load_dotenv()
```

## 📦 What's Committed to Git

✅ **Committed** (templates):
- `.env.example`
- `.env.linux`
- `.env.windows`
- `.env.macos`
- `infra/docker/.env.example`

❌ **Not Committed** (gitignored):
- `.env` (your active config)
- `.env.local`
- `.env.*.local`

## Single Root .env Rule

By default, the repo uses a single root `.env`. The Docker Compose file reads the `.env`
from the current working directory. Use `infra/docker/.env.example` only if you run
compose from `infra/docker`.

## 🎓 Further Reading

- [Complete Setup Guide](SETUP.md)
- [Environment Deep Dive](docs/ENVIRONMENT.md)
- [MCP Client Integration](apps/mcp-server/MCP_CLIENT_GUIDE.md)
- [Project Status](Checkpoint.md)
