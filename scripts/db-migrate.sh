#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/infra/docker/.env"
COMPOSE_FILE="$ROOT_DIR/infra/docker/docker-compose.yml"
MIGRATIONS_DIR="$ROOT_DIR/infra/db/migrations"

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

wait_for_postgres() {
  local attempts=60
  local delay_seconds=2
  local pg_cid=""

  for ((i = 1; i <= attempts; i += 1)); do
    pg_cid="$(compose ps -q postgres 2>/dev/null || true)"
    if [[ -n "$pg_cid" ]] && docker exec "$pg_cid" pg_isready -U osint -d osint >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay_seconds"
  done

  echo "Postgres did not become ready in time." >&2
  return 1
}

wait_for_postgres

PG_CID="$(compose ps -q postgres)"
if [[ -z "$PG_CID" ]]; then
  echo "Could not find running postgres container." >&2
  exit 1
fi

shopt -s nullglob
migration_files=("$MIGRATIONS_DIR"/*.sql)
shopt -u nullglob

if [[ ${#migration_files[@]} -eq 0 ]]; then
  echo "No migrations found in $MIGRATIONS_DIR" >&2
  exit 1
fi

for f in "${migration_files[@]}"; do
  echo "[db:migrate] Applying $(basename "$f")"
  docker exec -i "$PG_CID" psql -v ON_ERROR_STOP=1 -U osint -d osint < "$f"
done

echo "[db:migrate] Done."

