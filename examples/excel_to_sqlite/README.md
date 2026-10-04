# 📊 High-Performance Parallel Excel to SQLite ETL Pipeline (1 Million Rows)

This directory is a **completely standalone module**. It demonstrates how to ingest, transform in parallel across multi-core CPUs, load, and verify **1,000,000 rows** of messy Excel spreadsheets into an indexed SQLite warehouse at high throughput.

---

## 📁 Directory Layout

```
examples/excel_to_sqlite/
├── pyproject.toml                     # Dedicated uv project & dependency definitions
├── requirements.txt                   # Standard pip requirements
├── 01_generate_sample_excel.py        # Small sample generator (14 rows, quick testing)
├── 01_generate_1m_excel.py            # High-speed 1,000,000 row generator (XlsxWriter streaming)
├── 02_excel_to_sqlite_pipeline.py     # Standard ETL pipeline + verification
├── 02_parallel_excel_to_sqlite.py     # ⚡ Multi-Core Parallel ETL pipeline (1 Million Rows)
├── 03_airflow_dag_excel_to_sqlite.py  # Airflow TaskFlow DAG version
├── run_pipeline.sh                    # Standard pipeline runner
├── run_parallel_1m.sh                 # ⚡ 1 Million Rows Parallel runner
├── run_tests.sh                       # 🧪 Pytest test suite runner
├── README.md                          # Architecture & Benchmark documentation
├── tests/
│   └── test_pipeline.py               # Unit & Integration test suite
└── data/
    ├── sales_1m_rows.xlsx             # 1,000,000 row Excel file (~42MB)
    └── sales_warehouse_1m.db          # High-performance SQLite warehouse
```

---

## 🧪 Running the Test Suite (Pytest)

```bash
cd ~/git/airflow/examples/excel_to_sqlite

# One-click test runner:
./run_tests.sh

# Or directly with uv:
uv run pytest -v tests/
```

---

## ⚡ 1-Million Row Parallel Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. RUST-POWERED INGESTION (calamine engine)                                 │
│    Reads 1,000,000 rows from XML zip streams into memory in ~25 seconds.    │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 2. MULTI-CORE PARALLEL TRANSFORMATION (ProcessPoolExecutor)                 │
│    Splits 1M rows across all 8 CPU cores:                                    │
│    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ │
│    │ Worker Core 1 │ │ Worker Core 2 │ │ Worker Core 3 │ │ Worker Core 4 │ │
│    │ (125k chunks) │ │ (125k chunks) │ │ (125k chunks) │ │ (125k chunks) │ │
│    └───────┬───────┘ └───────┬───────┘ └───────┬───────┘ └───────┬───────┘ │
│            │                 │                 │                 │          │
│            ▼                 ▼                 ▼                 ▼          │
│    • Vectorized Currency Parsing: "$1,249.99" -> 1249.99 (float)            │
│    • Vectorized Discount Normalization: "15%" -> 0.15 (float)               │
│    • Vectorized Date Standardization: Mixed dates -> ISO YYYY-MM-DD         │
│    • Vectorized Financial Calculations: Net = Gross - Discount              │
│    • Vectorized Data Validation & Quarantine Filter                         │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3. TUNED HIGH-THROUGHPUT SQLITE LOADING                                     │
│    • PRAGMA journal_mode = WAL;                                             │
│    • PRAGMA synchronous = NORMAL;                                           │
│    • PRAGMA cache_size = -64000; (64MB In-Memory Cache)                     │
│    • Streaming batch inserts in 50,000-row transactions                     │
│    • B-Tree Indexes created post-bulk-load for maximum speed                │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 4. COMPLETE DATA QUALITY VERIFICATION                                       │
│    • Row Count Parity: DB Count == 1,000,000                                │
│    • Financial Parity: DB Net Revenue == In-Memory Checksum                 │
│    • Primary Key Integrity: 0 NULL keys                                     │
│    • Analytical SQL Summary Reports (Aggregations by Category & Customer)   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 How to Run the 1 Million Row Parallel Pipeline

### Option 1: One-Click Runner
```bash
cd ~/git/airflow/examples/excel_to_sqlite
./run_parallel_1m.sh
```

### Option 2: Standalone with `uv` (Zero Setup)
```bash
cd ~/git/airflow/examples/excel_to_sqlite

# 1. Generate 1,000,000 rows of messy Excel data
uv run 01_generate_1m_excel.py

# 2. Run Parallel Multi-Core ETL & Verification
uv run 02_parallel_excel_to_sqlite.py
```
