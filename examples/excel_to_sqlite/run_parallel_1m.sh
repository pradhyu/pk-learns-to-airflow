#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "======================================================================="
echo "⚡ 1 MILLION ROWS PARALLEL EXCEL TO SQLITE PIPELINE"
echo "======================================================================="

if command -v uv &> /dev/null; then
    echo "⚡ Using 'uv' package manager..."
    RUNNER="uv run"
elif [ -f "$ROOT_DIR/.venv/bin/activate" ]; then
    echo "🐍 Using virtualenv at $ROOT_DIR/.venv..."
    source "$ROOT_DIR/.venv/bin/activate"
    RUNNER="python3"
else
    RUNNER="python3"
fi

if [ ! -f "$SCRIPT_DIR/data/sales_1m_rows.xlsx" ]; then
    echo -e "\n[1/2] Generating 1,000,000 rows of messy Excel data (XlsxWriter streaming)..."
    $RUNNER "$SCRIPT_DIR/01_generate_1m_excel.py"
else
    echo -e "\n[1/2] Reusing existing 1,000,000 row Excel file ($SCRIPT_DIR/data/sales_1m_rows.xlsx)..."
fi

echo -e "\n[2/2] Running Parallel Multi-Core ETL (Extract -> Transform -> Load -> Verify)..."
$RUNNER "$SCRIPT_DIR/02_parallel_excel_to_sqlite.py"

echo -e "\n✨ Benchmark & Verification completed successfully!"
