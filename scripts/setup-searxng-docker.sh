#!/bin/bash
# SearxNG Docker Setup for unlimited niche+location discovery
#
# SearxNG is a self-hosted metasearch engine that aggregates results from
# 70+ engines (Google, Bing, DuckDuckGo, Brave, etc.) behind your own IP.
# This is the audit-recommended primary discovery layer for find-leads.js.
#
# Run ONCE to start SearxNG, then export SEARXNG_URL in your shell.

set -e

echo "=== SearxNG Docker Setup ==="

if ! command -v docker &>/dev/null; then
  echo "ERROR: Docker is not installed. Install Docker Desktop first:"
  echo "  brew install --cask docker"
  echo "  # or download: https://www.docker.com/products/docker-desktop"
  exit 1
fi

CONTAINER_NAME="searxng-mortar"
PORT="${SEARXNG_PORT:-8888}"

# Stop + remove old container if exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "Removing existing container..."
  docker stop "$CONTAINER_NAME" 2>/dev/null || true
  docker rm "$CONTAINER_NAME" 2>/dev/null || true
fi

# Pull + start
echo "Starting SearxNG on port $PORT..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -p "${PORT}:8080" \
  -e "INSTANCE_NAME=Mortar-Internal" \
  -e "BASE_URL=http://localhost:${PORT}/" \
  searxng/searxng

echo ""
echo "Waiting 10s for SearxNG to boot..."
sleep 10

# Test it
echo "Testing connection..."
if curl -s "http://localhost:${PORT}/search?q=test&format=json&engines=duckduckgo" --max-time 15 | grep -q '"results"'; then
  echo "✓ SearxNG is running!"
  echo ""
  echo "To use with find-leads.js, export:"
  echo "  export SEARXNG_URL=http://localhost:${PORT}"
  echo ""
  echo "Then run:"
  echo "  node scripts/find-leads.js \"immigration consultant\" \"Miami FL\" --count 50"
  echo ""
  echo "To stop SearxNG later:"
  echo "  docker stop $CONTAINER_NAME"
else
  echo "✗ SearxNG not responding. Check logs:"
  echo "  docker logs $CONTAINER_NAME"
  exit 1
fi
