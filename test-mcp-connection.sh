#!/bin/bash
# Quick test to verify MCP server works in dev container

echo "🔧 Starting infrastructure..."
docker compose -f infra/docker/docker-compose.yml up -d postgres minio 2>&1 | grep -E "(Started|Running|Healthy)" || true

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 3

echo ""
echo "🚀 Running MCP client example (spawns server in same container)..."
cd apps/mcp-server && yarn example

echo ""
echo "✅ Test complete! The client and server ran in the same dev container."
