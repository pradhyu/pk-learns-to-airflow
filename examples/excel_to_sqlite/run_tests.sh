#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "======================================================================="
echo "🧪 RUNNING EXCEL TO SQLITE TEST SUITE (PYTEST)"
echo "======================================================================="

if command -v uv &> /dev/null; then
    echo "⚡ Running pytest with uv..."
    PYTHONPATH="$ROOT_DIR" uv run pytest -v "$SCRIPT_DIR/tests"
elif [ -f "$ROOT_DIR/.venv/bin/activate" ]; then
    echo "🐍 Running pytest with virtualenv..."
    source "$ROOT_DIR/.venv/bin/activate"
    PYTHONPATH="$ROOT_DIR" pytest -v "$SCRIPT_DIR/tests"
else
    PYTHONPATH="$ROOT_DIR" pytest -v "$SCRIPT_DIR/tests"
fi
