#!/usr/bin/env bash
set -euo pipefail

echo "[bootstrap] Ensuring env file exists..."
if [ ! -f infra/docker/.env ] && [ -f infra/docker/.env.example ]; then
  cp infra/docker/.env.example infra/docker/.env
  echo "[bootstrap] Created infra/docker/.env"
fi

echo "[bootstrap] Installing Yarn deps..."
yarn install

echo "[bootstrap] Done."
