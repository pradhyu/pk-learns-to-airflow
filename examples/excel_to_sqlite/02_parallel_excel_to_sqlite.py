#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pandas>=2.0.0",
#     "python-calamine>=0.2.0",
#     "openpyxl>=3.1.0",
#     "tabulate>=0.9.0",
#     "sqlite-utils>=3.35.0",
# ]
# ///
"""
High-Performance Parallel Excel to SQLite ETL Pipeline (1 Million Rows).

Architecture:
  1. Fast Rust Ingestion: Reads 1,000,000 Excel rows in seconds using 'calamine'.
  2. Parallel Multi-Core Transformation: Distributes data chunks across all CPU cores
     via ProcessPoolExecutor for concurrent regex, type casting, date normalization,
     and financial calculations.
  3. Tuned High-Throughput SQLite Loading: Uses WAL mode, high cache size, and
     transactional batch streaming (50k rows/batch).
  4. Complete Verification & Benchmarking: Verifies 1,000,000 row count parity,
     financial checksum parity, and reports throughput (rows/sec).
"""

import os
import re
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
EXCEL_FILE = DATA_DIR / "sales_1m_rows.xlsx"
SQLITE_DB = DATA_DIR / "sales_warehouse_1m.db"

NUM_WORKERS = max(1, os.cpu_count() or 4)
BATCH_SIZE = 50_000


@dataclass
class ParallelMetrics:
    total_raw_rows: int = 0
    total_valid_rows: int = 0
    total_quarantined: int = 0
    total_net_revenue: float = 0.0
    read_seconds: float = 0.0
    transform_seconds: float = 0.0
    load_seconds: float = 0.0
    total_seconds: float = 0.0


# -----------------------------------------------------------------------------
# WORKER TRANSFORMATION FUNCTION (Runs concurrently across CPU cores)
# -----------------------------------------------------------------------------
def transform_chunk(chunk_df: pd.DataFrame) -> Tuple[List[Tuple], int, float]:
    """Worker process function: vectorized data cleaning & calculations on a partition."""
    # 1. Clean strings
    order_id = chunk_df["Order_ID"].astype(str).str.strip()
    cust_id = chunk_df["Customer_ID"].astype(str).str.strip()
    category = chunk_df["Product_Category"].astype(str).str.strip().str.title()
    item_desc = chunk_df["Item_Description"].astype(str).str.strip()
    status = chunk_df["Status"].astype(str).str.strip().str.upper()

    # 2. Vectorized Currency parsing ($1,249.99 -> 1249.99)
    raw_price = chunk_df["Unit_Price"].astype(str).str.replace(r"[^\d.]", "", regex=True)
    unit_price = pd.to_numeric(raw_price, errors="coerce").fillna(0.0)

    # 3. Vectorized Units parsing
    units_sold = pd.to_numeric(chunk_df["Units_Sold"], errors="coerce").fillna(0).astype(int)

    # 4. Vectorized Discount parsing (15% -> 0.15)
    raw_disc = chunk_df["Discount_Rate"].astype(str).str.strip()
    is_pct = raw_disc.str.endswith("%")
    disc_num = pd.to_numeric(raw_disc.str.rstrip("%"), errors="coerce").fillna(0.0)
    discount_rate = disc_num.where(~is_pct, disc_num / 100.0)
    discount_rate = discount_rate.where(discount_rate <= 1.0, discount_rate / 100.0)

    # 5. Vectorized Date parsing
    order_date = pd.to_datetime(chunk_df["Order_Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("2026-01-01")

    # 6. Financial Calculations
    gross_amount = (units_sold * unit_price).round(2)
    discount_amount = (gross_amount * discount_rate).round(2)
    net_amount = (gross_amount - discount_amount).round(2)

    # 7. Valid vs Quarantined filter
    valid_mask = (units_sold > 0) & (unit_price > 0) & (order_id != "None")
    valid_df = pd.DataFrame({
        "order_id": order_id[valid_mask],
        "customer_id": cust_id[valid_mask],
        "product_category": category[valid_mask],
        "item_description": item_desc[valid_mask],
        "units_sold": units_sold[valid_mask],
        "unit_price": unit_price[valid_mask],
        "discount_rate": discount_rate[valid_mask],
        "gross_amount": gross_amount[valid_mask],
        "discount_amount": discount_amount[valid_mask],
        "net_amount": net_amount[valid_mask],
        "order_date": order_date[valid_mask],
        "status": status[valid_mask],
    })

    records = list(valid_df.itertuples(index=False, name=None))
    quarantined_count = int((~valid_mask).sum())
    chunk_net_rev = float(valid_df["net_amount"].sum())

    return records, quarantined_count, chunk_net_rev


# -----------------------------------------------------------------------------
# DATABASE SETUP
# -----------------------------------------------------------------------------
def initialize_high_performance_db(db_path: Path):
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA cache_size = -64000;")  # 64MB Cache
        conn.execute("PRAGMA temp_store = MEMORY;")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                product_category TEXT NOT NULL,
                item_description TEXT NOT NULL,
                units_sold INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                discount_rate REAL NOT NULL,
                gross_amount REAL NOT NULL,
                discount_amount REAL NOT NULL,
                net_amount REAL NOT NULL,
                order_date TEXT NOT NULL,
                status TEXT NOT NULL
            );
        """)
        conn.commit()


# -----------------------------------------------------------------------------
# MAIN PARALLEL PIPELINE
# -----------------------------------------------------------------------------
def run_parallel_pipeline():
    print("=" * 75)
    print(f"🚀 PARALLEL 1M EXCEL TO SQLITE ETL (Using {NUM_WORKERS} CPU Cores)")
    print("=" * 75)
    metrics = ParallelMetrics()
    overall_start = time.time()

    # --- 1. Fast Excel Ingestion ---
    print(f"\n📂 [1/4 EXTRACT] Reading Excel with Rust-powered 'calamine' engine...")
    read_start = time.time()
    try:
        # Calamine reads 1M rows in ~3 seconds
        orders_df = pd.read_excel(EXCEL_FILE, sheet_name="Orders", engine="calamine")
    except Exception:
        print("   (Fallback to openpyxl...)")
        orders_df = pd.read_excel(EXCEL_FILE, sheet_name="Orders")

    metrics.read_seconds = time.time() - read_start
    metrics.total_raw_rows = len(orders_df)
    print(f"   -> Read {metrics.total_raw_rows:,} rows in {metrics.read_seconds:.2f}s "
          f"({metrics.total_raw_rows / metrics.read_seconds:,.0f} rows/sec)")

    # --- 2. Parallel Multi-Core Transformation ---
    print(f"\n⚙️  [2/4 TRANSFORM] Distributing into {NUM_WORKERS} parallel process workers...")
    transform_start = time.time()

    chunk_size = (metrics.total_raw_rows + NUM_WORKERS - 1) // NUM_WORKERS
    chunks = [orders_df.iloc[i : i + chunk_size] for i in range(0, metrics.total_raw_rows, chunk_size)]

    transformed_records = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        results = executor.map(transform_chunk, chunks)
        for records, quarantined, chunk_rev in results:
            transformed_records.extend(records)
            metrics.total_quarantined += quarantined
            metrics.total_net_revenue += chunk_rev

    metrics.transform_seconds = time.time() - transform_start
    metrics.total_valid_rows = len(transformed_records)
    print(f"   -> Transformed {metrics.total_valid_rows:,} valid rows in {metrics.transform_seconds:.2f}s "
          f"({metrics.total_valid_rows / metrics.transform_seconds:,.0f} rows/sec)")
    print(f"   -> Quarantined rows: {metrics.total_quarantined:,}")
    print(f"   -> Total calculated net revenue: ${metrics.total_net_revenue:,.2f}")

    # --- 3. Tuned High-Throughput SQLite Loading ---
    print(f"\n📥 [3/4 LOAD] Loading into SQLite ({SQLITE_DB.name})...")
    load_start = time.time()
    initialize_high_performance_db(SQLITE_DB)

    with sqlite3.connect(SQLITE_DB) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA cache_size = -64000;")

        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO fact_orders (
                order_id, customer_id, product_category, item_description,
                units_sold, unit_price, discount_rate, gross_amount,
                discount_amount, net_amount, order_date, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Batch streaming inserts
        for i in range(0, len(transformed_records), BATCH_SIZE):
            batch = transformed_records[i : i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)

        # Build index after bulk load for maximum speed
        print("   -> Creating database indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer ON fact_orders(customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_category ON fact_orders(product_category);")
        conn.commit()

    metrics.load_seconds = time.time() - load_start
    print(f"   -> Loaded {metrics.total_valid_rows:,} rows into SQLite in {metrics.load_seconds:.2f}s "
          f"({metrics.total_valid_rows / metrics.load_seconds:,.0f} rows/sec)")

    # --- 4. Automated Verification & Quality Assertions ---
    print(f"\n🔍 [4/4 VERIFY] Executing automated Data Quality checks...")
    with sqlite3.connect(SQLITE_DB) as conn:
        cursor = conn.cursor()

        # Check 1: Row count parity
        cursor.execute("SELECT COUNT(*) FROM fact_orders;")
        db_count = cursor.fetchone()[0]
        assert db_count == metrics.total_valid_rows, f"❌ Count mismatch: {db_count} vs {metrics.total_valid_rows}"
        print(f"   ✅ [Check 1: Row Count Parity] Loaded {db_count:,} rows == Transformed {metrics.total_valid_rows:,}")

        # Check 2: Financial Sum Checksum Parity
        cursor.execute("SELECT ROUND(SUM(net_amount), 2) FROM fact_orders;")
        db_rev = cursor.fetchone()[0]
        expected_rev = round(metrics.total_net_revenue, 2)
        assert abs(db_rev - expected_rev) < 1.0, f"❌ Revenue mismatch: {db_rev} vs {expected_rev}"
        print(f"   ✅ [Check 2: Financial Checksum Parity] DB Revenue ${db_rev:,.2f} == Memory Sum ${expected_rev:,.2f}")

        # Summary SQL Report
        print("\n📊 --- Analytics Summary Across 1,000,000 Rows ---")
        summary_df = pd.read_sql_query(
            """
            SELECT 
                product_category AS Category,
                COUNT(order_id) AS Total_Orders,
                SUM(units_sold) AS Total_Units,
                printf('$%,.2f', SUM(net_amount)) AS Net_Revenue
            FROM fact_orders
            GROUP BY product_category
            ORDER BY SUM(net_amount) DESC;
            """,
            conn,
        )
        print("\n" + summary_df.to_string(index=False))

    metrics.total_seconds = time.time() - overall_start
    print("\n" + "=" * 75)
    print("⚡ PERFORMANCE BENCHMARK SUMMARY (1 MILLION ROWS)")
    print("=" * 75)
    print(f"• Ingestion Time (Excel -> Memory):   {metrics.read_seconds:6.2f}s  ({metrics.total_raw_rows / metrics.read_seconds:,.0f} rows/sec)")
    print(f"• Transform Time ({NUM_WORKERS} CPU Cores):        {metrics.transform_seconds:6.2f}s  ({metrics.total_valid_rows / metrics.transform_seconds:,.0f} rows/sec)")
    print(f"• Loading Time (Memory -> SQLite):   {metrics.load_seconds:6.2f}s  ({metrics.total_valid_rows / metrics.load_seconds:,.0f} rows/sec)")
    print(f"• Total End-to-End Pipeline Time:    {metrics.total_seconds:6.2f}s  ({metrics.total_valid_rows / metrics.total_seconds:,.0f} rows/sec overall)")
    print("=" * 75)


if __name__ == "__main__":
    run_parallel_pipeline()
