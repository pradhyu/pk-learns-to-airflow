#!/usr/bin/env bash
set -e

# Change directory to the root of the project
cd "$(dirname "$0")"

echo "=================================================="
echo " 🌪️ Starting Apache Airflow with Docker Compose"
echo "=================================================="

# Check if .env exists, if not copy from .env.example
if [ ! -f .env ]; then
  echo "Creating .env from .env.example..."
  cp .env.example .env
fi

# Ensure necessary host directories exist
mkdir -p dags logs plugins data

# Determine docker compose command
if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker-compose"
else
  echo "Error: docker compose or docker-compose is required but not installed." >&2
  exit 1
fi

echo "Building custom Airflow image and launching containers..."
$DOCKER_COMPOSE up --build -d

echo ""
echo "=================================================="
echo " ✅ Airflow is starting in the background!"
echo "=================================================="
echo " 🌐 Web UI:       http://localhost:8080"
echo " 👤 Username:     admin"
echo " 🔑 Password:     admin"
echo "=================================================="
echo " 📜 View logs:    $DOCKER_COMPOSE logs -f"
echo " 🛑 Stop system:  ./stop.sh or $DOCKER_COMPOSE down"
echo "=================================================="
