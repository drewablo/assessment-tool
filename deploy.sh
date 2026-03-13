#!/usr/bin/env bash
# deploy.sh — First-time or re-deploy of the Ministry Assessment Tool
# Usage: ./deploy.sh
#
# Prerequisites:
#   - Docker + Docker Compose installed on the server
#   - .env.prod file created from .env.prod.example and filled in
#   - (Optional) A domain pointed at this server's IP
#
set -euo pipefail

ENV_FILE=".env.prod"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found."
  echo "Copy .env.prod.example to .env.prod and fill in all values, then re-run."
  exit 1
fi

# Validate that placeholder values have been replaced
if grep -q "CHANGE_ME" "$ENV_FILE"; then
  echo "ERROR: $ENV_FILE still contains CHANGE_ME placeholder values."
  echo "Fill in all required values before deploying."
  exit 1
fi

echo "==> Pulling latest code..."
git pull

echo "==> Building and starting services..."
BUILD_LOG=$(mktemp)
set +e
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" up -d --build 2>&1 | tee "$BUILD_LOG"
STATUS=${PIPESTATUS[0]}
set -e

if [ "$STATUS" -ne 0 ]; then
  if grep -Eq "parent snapshot .* does not exist|failed to prepare extraction snapshot" "$BUILD_LOG"; then
    echo "==> Detected Docker BuildKit cache corruption. Pruning builder cache and retrying once..."
    docker builder prune -f || true
    docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" up -d --build
  else
    rm -f "$BUILD_LOG"
    exit "$STATUS"
  fi
fi
rm -f "$BUILD_LOG"

echo "==> Waiting for services to be healthy..."
sleep 5
docker compose -f docker-compose.prod.yml ps

echo ""
echo "==> Deploy complete."
# Print the URL from the env file
FRONTEND_URL=$(grep '^FRONTEND_URL=' "$ENV_FILE" | cut -d= -f2-)
echo "    App is running at: $FRONTEND_URL"
echo ""
echo "Useful commands:"
echo "  View logs:    docker compose -f docker-compose.prod.yml logs -f"
echo "  Stop:         docker compose -f docker-compose.prod.yml down"
echo "  Restart api:  docker compose -f docker-compose.prod.yml restart api"
