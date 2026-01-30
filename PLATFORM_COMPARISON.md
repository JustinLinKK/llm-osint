# Platform-Specific Configuration Comparison

Quick reference table showing the differences between each environment template.

## Template Selection Matrix

| Your Setup | Template File | Database URL | MinIO URL | Network Command Required? |
|------------|---------------|--------------|-----------|---------------------------|
| **VS Code Dev Container** | `.env.example` | `postgres:5432` | `minio:9000` | ✅ Yes |
| **Linux (Ubuntu/Debian/Fedora)** | `.env.linux` | `localhost:5432` | `localhost:9000` | ❌ No |
| **Windows + Docker Desktop** | `.env.windows` | `localhost:5432` | `localhost:9000` | ❌ No |
| **macOS + Docker Desktop** | `.env.macos` | `localhost:5432` | `localhost:9000` | ❌ No |
| **WSL2 (Windows Subsystem)** | `.env.linux` | `localhost:5432` | `localhost:9000` | ❌ No |
| **GitHub Codespaces** | `.env.example` | `postgres:5432` | `minio:9000` | ✅ Yes* |
| **Docker in Docker (DinD)** | `.env.example` | `postgres:5432` | `minio:9000` | ✅ Yes |

*For GitHub Codespaces, check if `docker_default` network exists first.

## Complete URL Comparison

### Dev Container (`.env.example`)

```bash
DATABASE_URL=postgresql://osint:osint@postgres:5432/osint
MINIO_ENDPOINT=http://minio:9000
QDRANT_URL=http://qdrant:6333
NEO4J_URI=bolt://neo4j:7687
TEMPORAL_ADDRESS=temporal:7233
REDIS_URL=redis://redis:6379
```

**Network Setup:**
```bash
docker network connect docker_default $(hostname)
```

### Native Linux (`.env.linux`)

```bash
DATABASE_URL=postgresql://osint:osint@localhost:5432/osint
MINIO_ENDPOINT=http://localhost:9000
QDRANT_URL=http://localhost:6333
NEO4J_URI=bolt://localhost:7687
TEMPORAL_ADDRESS=localhost:7233
REDIS_URL=redis://localhost:6379
```

**Network Setup:**
```bash
# None required
```

### Windows (`.env.windows`)

```powershell
DATABASE_URL=postgresql://osint:osint@localhost:5432/osint
MINIO_ENDPOINT=http://localhost:9000
QDRANT_URL=http://localhost:6333
NEO4J_URI=bolt://localhost:7687
TEMPORAL_ADDRESS=localhost:7233
REDIS_URL=redis://localhost:6379
```

**Alternative (if localhost doesn't work):**
```powershell
# Try 127.0.0.1
DATABASE_URL=postgresql://osint:osint@127.0.0.1:5432/osint

# Or host.docker.internal (Docker Desktop 18.03+)
DATABASE_URL=postgresql://osint:osint@host.docker.internal:5432/osint
```

**Network Setup:**
```powershell
# None required
```

### macOS (`.env.macos`)

```bash
DATABASE_URL=postgresql://osint:osint@localhost:5432/osint
MINIO_ENDPOINT=http://localhost:9000
QDRANT_URL=http://localhost:6333
NEO4J_URI=bolt://localhost:7687
TEMPORAL_ADDRESS=localhost:7233
REDIS_URL=redis://localhost:6379
```

**Network Setup:**
```bash
# None required
```

## Platform-Specific Considerations

### Dev Container (VS Code)

✅ **Advantages:**
- Consistent environment across team
- Isolated from host system
- Pre-configured with all tools
- Easy to reset/rebuild

⚠️ **Requirements:**
- Must connect to docker_default network
- Service names must be used
- Slightly slower I/O on macOS/Windows

### Native Linux

✅ **Advantages:**
- Best performance
- Direct access to all tools
- No container overhead
- Simpler networking

⚠️ **Requirements:**
- Node.js 20+ installed
- Yarn installed
- Direct access to Docker socket

### Windows + Docker Desktop

✅ **Advantages:**
- Works on Windows 10/11
- WSL 2 backend for good performance
- Easy setup with Docker Desktop

⚠️ **Considerations:**
- Requires WSL 2 backend
- Some path differences (use `/` not `\`)
- Firewall may need configuration
- `localhost` might need to be `127.0.0.1`

### macOS + Docker Desktop

✅ **Advantages:**
- Native Docker Desktop support
- Works on Intel and Apple Silicon
- Good performance with VirtioFS

⚠️ **Considerations:**
- File I/O slower than Linux
- Use VirtioFS for better performance
- Apple Silicon may need Rosetta for some images

## Setup Commands by Platform

### Dev Container

```bash
# 1. Setup
cp .env.example .env
docker compose -f infra/docker/docker-compose.yml up -d
docker network connect docker_default $(hostname)

# 2. Verify
getent hosts postgres minio
./test-env-setup.sh

# 3. Test
cd apps/mcp-server && yarn example
```

### Native Linux

```bash
# 1. Setup
cp .env.linux .env
docker compose -f infra/docker/docker-compose.yml up -d

# 2. Verify
./test-env-setup.sh

# 3. Test
cd apps/mcp-server && yarn example
```

### Windows (PowerShell)

```powershell
# 1. Setup
copy .env.windows .env
docker compose -f infra\docker\docker-compose.yml up -d

# 2. Verify (if bash available)
bash test-env-setup.sh

# 3. Test
cd apps\mcp-server
yarn example
```

### macOS

```bash
# 1. Setup
cp .env.macos .env
docker compose -f infra/docker/docker-compose.yml up -d

# 2. Verify
./test-env-setup.sh

# 3. Test
cd apps/mcp-server && yarn example
```

## Troubleshooting Decision Tree

```
Can't connect to services?
│
├─ Using Dev Container?
│  │
│  ├─ Error: ENOTFOUND postgres
│  │  └─ Run: docker network connect docker_default $(hostname)
│  │
│  └─ Error: Already connected to network
│     └─ Check .env uses service names, not localhost
│
└─ Using Native Host?
   │
   ├─ Error: ECONNREFUSED localhost:5432
   │  └─ Check: docker compose ps (services running?)
   │
   └─ Error: ENOTFOUND postgres
      └─ Wrong .env! Use .env.linux/.windows/.macos (localhost)
```

## Performance Comparison

| Platform | Startup Time | I/O Performance | Network Performance |
|----------|--------------|-----------------|---------------------|
| **Linux Native** | ⚡⚡⚡ Fastest | ⚡⚡⚡ Best | ⚡⚡⚡ Best |
| **Dev Container (Linux)** | ⚡⚡ Fast | ⚡⚡⚡ Best | ⚡⚡ Good |
| **macOS Native** | ⚡⚡ Fast | ⚡⚡ Good | ⚡⚡ Good |
| **Windows WSL2** | ⚡⚡ Fast | ⚡⚡ Good | ⚡⚡ Good |
| **macOS Dev Container** | ⚡ Slower | ⚡ Slower | ⚡⚡ Good |
| **Windows Dev Container** | ⚡ Slower | ⚡ Slower | ⚡⚡ Good |

## Migration Between Platforms

### Moving from Dev Container to Native Linux

```bash
# 1. Backup current .env
cp .env .env.devcontainer.backup

# 2. Switch to Linux template
cp .env.linux .env

# 3. No network command needed
# Services already accessible via localhost

# 4. Test
cd apps/mcp-server && yarn example
```

### Moving from Native to Dev Container

```bash
# 1. Backup current .env
cp .env .env.native.backup

# 2. Switch to dev container template
cp .env.example .env

# 3. Connect to network
docker network connect docker_default $(hostname)

# 4. Test
cd apps/mcp-server && yarn example
```

## Quick Reference

| Question | Answer |
|----------|--------|
| **I'm in VS Code with Remote Containers** | Use `.env.example` |
| **I'm running Node.js directly on my laptop** | Use `.env.linux` / `.env.windows` / `.env.macos` |
| **Services work in Docker but not from my code** | Check you're using the right template |
| **ENOTFOUND postgres** | Either wrong template, or forgot network connect |
| **ECONNREFUSED localhost** | Either services not running, or wrong template |
| **How do I know which template I'm using?** | `head -3 .env` shows a comment |
| **Can I switch templates?** | Yes! Just copy the right one and restart |

---

**Still confused?** See [ENV_FILES_README.md](ENV_FILES_README.md) or [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md)
