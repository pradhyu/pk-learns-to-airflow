#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "======================================================================="
echo "📊 EXCEL TO SQLITE ETL PIPELINE RUNNER (SELF-CONTAINED)"
echo "======================================================================="

# Determine runner: uv vs local .venv vs root .venv vs python3
if command -v uv &> /dev/null; then
    echo "⚡ Using 'uv' package manager (PEP 723 isolated execution)..."
    RUNNER="uv run"
elif [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    echo "🐍 Using local virtualenv at $SCRIPT_DIR/.venv..."
    source "$SCRIPT_DIR/.venv/bin/activate"
    RUNNER="python3"
elif [ -f "$ROOT_DIR/.venv/bin/activate" ]; then
    echo "🐍 Using root virtualenv at $ROOT_DIR/.venv..."
    source "$ROOT_DIR/.venv/bin/activate"
    RUNNER="python3"
else
    RUNNER="python3"
fi

echo -e "\n[1/2] Generating messy Excel data..."
$RUNNER "$SCRIPT_DIR/01_generate_sample_excel.py"

echo -e "\n[2/2] Running ETL Pipeline (Extract -> Transform -> Load -> Verify)..."
$RUNNER "$SCRIPT_DIR/02_excel_to_sqlite_pipeline.py"

echo -e "\n✨ All steps finished successfully!"
