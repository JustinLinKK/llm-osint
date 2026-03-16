#!/usr/bin/env bash
set -euo pipefail

# Rebuild and force-recreate the local Docker Compose infra WITHOUT touching:
# - worker-embedding (GPU-heavy / managed separately)
# - mcp-server-kali (often heavy, optional for many dev flows)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/infra/docker/.env"
COMPOSE_FILE="$ROOT_DIR/infra/docker/docker-compose.yml"

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

echo "[infra:rebuild:all-lite] Building app images (excluding worker-embedding and mcp-server-kali)..."
compose build api mcp-server

echo "[infra:rebuild:all-lite] Recreating core services..."
compose rm -sf postgres redis minio qdrant neo4j temporal temporal-ui >/dev/null 2>&1 || true
compose up -d --force-recreate postgres redis minio qdrant neo4j temporal temporal-ui

echo "[infra:rebuild:all-lite] Applying DB migrations..."
bash "$ROOT_DIR/scripts/db-migrate.sh"

echo "[infra:rebuild:all-lite] Recreating mcp-server (no deps; leaves worker-embedding and mcp-server-kali untouched)..."
compose rm -sf mcp-server >/dev/null 2>&1 || true
compose up -d --force-recreate --no-deps mcp-server

echo "[infra:rebuild:all-lite] Recreating API (no deps)..."
compose rm -sf api >/dev/null 2>&1 || true
compose up -d --force-recreate --no-deps api

echo "[infra:rebuild:all-lite] Done."
