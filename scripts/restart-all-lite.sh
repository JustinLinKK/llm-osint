#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/infra/docker/.env"
COMPOSE_FILE="$ROOT_DIR/infra/docker/docker-compose.yml"

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

wait_for_worker_embedding() {
  local attempts=90
  local delay_seconds=2

  for ((i = 1; i <= attempts; i += 1)); do
    if compose exec -T mcp-server sh -lc 'node -e "fetch(\"http://worker-embedding:8000/v1/models\").then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"' >/dev/null 2>&1; then
      echo "worker-embedding is ready."
      return 0
    fi
    sleep "$delay_seconds"
  done

  echo "worker-embedding did not become ready in time; embedding calls may fail until startup finishes." >&2
}

compose restart api mcp-server minio neo4j postgres qdrant redis temporal temporal-ui

provider=""
if [[ -f "$ENV_FILE" ]]; then
  provider="$(sed -nE 's/^[[:space:]]*EMBEDDING_PROVIDER[[:space:]]*=[[:space:]]*([^[:space:]#]+).*/\1/p' "$ENV_FILE" | tail -n1 | tr -d '"' | tr '[:upper:]' '[:lower:]')"
fi

if [[ "$provider" == "vllm" ]]; then
  if compose ps --services --status running | grep -qx "worker-embedding"; then
    echo "worker-embedding already running."
  else
    echo "EMBEDDING_PROVIDER=vllm; starting worker-embedding to avoid embedding fetch failures..."
    compose up -d worker-embedding
    wait_for_worker_embedding
  fi
fi
