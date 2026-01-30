#!/bin/bash
# Test script to verify environment configuration is correct

set -e

echo "🔍 Testing Environment Configuration..."
echo ""

# Check .env file exists
if [ ! -f ".env" ]; then
    echo "❌ .env file not found"
    echo "   Run: cp .env.example .env"
    exit 1
fi
echo "✓ .env file exists"

# Check docker_default network connection
if docker network inspect docker_default | grep -q $(hostname); then
    echo "✓ Dev container connected to docker_default network"
else
    echo "⚠️  Dev container NOT connected to docker_default network"
    echo "   Run: docker network connect docker_default \$(hostname)"
    exit 1
fi

# Check service resolution
if getent hosts postgres >/dev/null 2>&1 && getent hosts minio >/dev/null 2>&1; then
    echo "✓ Docker services resolvable (postgres, minio)"
else
    echo "❌ Cannot resolve Docker service names"
    echo "   Ensure docker network connect command was run"
    exit 1
fi

# Check services are running
if docker compose -f infra/docker/docker-compose.yml ps postgres | grep -q "Up"; then
    echo "✓ PostgreSQL is running"
else
    echo "❌ PostgreSQL not running"
    echo "   Run: docker compose -f infra/docker/docker-compose.yml up -d"
    exit 1
fi

if docker compose -f infra/docker/docker-compose.yml ps minio | grep -q "Up"; then
    echo "✓ MinIO is running"
else
    echo "❌ MinIO not running"
    exit 1
fi

echo ""
echo "✅ All environment checks passed!"
echo ""
echo "You can now:"
echo "  • Run API server: yarn dev:api"
echo "  • Test MCP server: cd apps/mcp-server && yarn example"
echo "  • Build project: yarn workspaces foreach -A run build"
