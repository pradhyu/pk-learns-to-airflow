#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker-compose"
else
  echo "Error: docker compose or docker-compose is required but not installed." >&2
  exit 1
fi

echo "Stopping Apache Airflow containers..."
$DOCKER_COMPOSE down "$@"
echo "Airflow cluster stopped."
