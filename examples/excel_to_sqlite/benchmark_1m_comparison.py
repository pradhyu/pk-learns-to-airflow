#!/usr/bin/env python3
"""
Comprehensive Benchmark: 1 Million Rows Parallel vs Non-Parallel (Single Core) ETL.
"""

import os
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List, Tuple, Dict, Any
import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
EXCEL_FILE = DATA_DIR / "sales_1m_rows.xlsx"
DB_SINGLE = DATA_DIR / "sales_warehouse_1m_single.db"
DB_PARALLEL = DATA_DIR / "sales_warehouse_1m_parallel.db"

NUM_WORKERS = max(1, os.cpu_count() or 8)
BATCH_SIZE = 50_000


def transform_chunk(chunk_df: pd.DataFrame) -> Tuple[List[Tuple], int, float]:
    """Vectorized cleaning & transformation on a DataFrame partition."""
    order_id = chunk_df["Order_ID"].astype(str).str.strip()
    cust_id = chunk_df["Customer_ID"].astype(str).str.strip()
    category = chunk_df["Product_Category"].astype(str).str.strip().str.title()
    item_desc = chunk_df["Item_Description"].astype(str).str.strip()
    status = chunk_df["Status"].astype(str).str.strip().str.upper()

    raw_price = chunk_df["Unit_Price"].astype(str).str.replace(r"[^\d.]", "", regex=True)
    unit_price = pd.to_numeric(raw_price, errors="coerce").fillna(0.0)

    units_sold = pd.to_numeric(chunk_df["Units_Sold"], errors="coerce").fillna(0).astype(int)

    raw_disc = chunk_df["Discount_Rate"].astype(str).str.strip()
    is_pct = raw_disc.str.endswith("%")
    disc_num = pd.to_numeric(raw_disc.str.rstrip("%"), errors="coerce").fillna(0.0)
    discount_rate = disc_num.where(~is_pct, disc_num / 100.0)
    discount_rate = discount_rate.where(discount_rate <= 1.0, discount_rate / 100.0)

    order_date = pd.to_datetime(chunk_df["Order_Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("2026-01-01")

    gross_amount = (units_sold * unit_price).round(2)
    discount_amount = (gross_amount * discount_rate).round(2)
    net_amount = (gross_amount - discount_amount).round(2)

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


def init_db(db_path: Path):
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA cache_size = -64000;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("""
            CREATE TABLE fact_orders (
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


def run_benchmark(mode: str, workers: int, db_path: Path) -> Dict[str, Any]:
    print(f"\n{'='*70}")
    print(f"▶ RUNNING BENCHMARK: {mode.upper()} ({workers} Worker{'s' if workers > 1 else ''})")
    print(f"{'='*70}")

    total_start = time.perf_counter()

    # 1. Ingestion
    read_start = time.perf_counter()
    orders_df = pd.read_excel(EXCEL_FILE, sheet_name="Orders", engine="calamine")
    read_time = time.perf_counter() - read_start
    total_raw_rows = len(orders_df)
    print(f"  [1/3 Extract] Read {total_raw_rows:,} rows in {read_time:.3f}s ({total_raw_rows / read_time:,.0f} rows/s)")

    # 2. Transformation
    transform_start = time.perf_counter()
    transformed_records = []
    quarantined_total = 0
    total_revenue = 0.0

    if workers == 1:
        # Non-parallel sequential execution
        records, quarantined, rev = transform_chunk(orders_df)
        transformed_records.extend(records)
        quarantined_total = quarantined
        total_revenue = rev
    else:
        # Multi-core process pool execution
        chunk_size = (total_raw_rows + workers - 1) // workers
        chunks = [orders_df.iloc[i : i + chunk_size] for i in range(0, total_raw_rows, chunk_size)]
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = executor.map(transform_chunk, chunks)
            for records, quarantined, rev in results:
                transformed_records.extend(records)
                quarantined_total += quarantined
                total_revenue += rev

    transform_time = time.perf_counter() - transform_start
    valid_rows = len(transformed_records)
    print(f"  [2/3 Transform] Transformed {valid_rows:,} valid rows in {transform_time:.3f}s ({valid_rows / transform_time:,.0f} rows/s)")

    # 3. Load
    load_start = time.perf_counter()
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
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
        for i in range(0, len(transformed_records), BATCH_SIZE):
            cursor.executemany(insert_sql, transformed_records[i : i + BATCH_SIZE])

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer ON fact_orders(customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_category ON fact_orders(product_category);")
        conn.commit()

        # Verification
        cursor.execute("SELECT COUNT(*), ROUND(SUM(net_amount), 2) FROM fact_orders;")
        row_count, db_revenue = cursor.fetchone()
        assert row_count == valid_rows
        assert abs(db_revenue - round(total_revenue, 2)) < 1.0

    load_time = time.perf_counter() - load_start
    print(f"  [3/3 Load] Loaded {valid_rows:,} rows into SQLite in {load_time:.3f}s ({valid_rows / load_time:,.0f} rows/s)")

    total_time = time.perf_counter() - total_start
    print(f"  ✔ Complete End-to-End: {total_time:.3f}s ({valid_rows / total_time:,.0f} total rows/s)")

    return {
        "mode": mode,
        "workers": workers,
        "read_time": read_time,
        "transform_time": transform_time,
        "load_time": load_time,
        "total_time": total_time,
        "raw_rows": total_raw_rows,
        "valid_rows": valid_rows,
        "quarantined": quarantined_total,
        "revenue": total_revenue,
    }


def main():
    print("=" * 70)
    print(f"🏁 1,000,000 ROWS PIPELINE BENCHMARK: NON-PARALLEL VS PARALLEL")
    print(f"Host: {NUM_WORKERS} CPU Cores | Dataset: 1,000,000 Excel Rows")
    print("=" * 70)

    # 1. Non-Parallel Benchmark (1 Worker)
    single_res = run_benchmark("Non-Parallel (Single Core)", 1, DB_SINGLE)

    # 2. Parallel Benchmark (All Workers)
    parallel_res = run_benchmark(f"Parallel ({NUM_WORKERS} Cores)", NUM_WORKERS, DB_PARALLEL)

    # Output Comparative Summary
    print("\n" + "=" * 70)
    print("📊 FINAL SIDE-BY-SIDE BENCHMARK COMPARISON (1,000,000 ROWS)")
    print("=" * 70)

    t_speedup = single_res["transform_time"] / parallel_res["transform_time"]
    e2e_speedup = single_res["total_time"] / parallel_res["total_time"]

    print(f"{'Phase':<24} | {'Non-Parallel (1 Core)':<20} | {'Parallel (' + str(NUM_WORKERS) + ' Cores)':<20} | {'Speedup':<10}")
    print("-" * 80)
    print(f"{'1. Extract (Calamine)':<24} | {single_res['read_time']:>18.2f}s | {parallel_res['read_time']:>18.2f}s | {single_res['read_time']/parallel_res['read_time']:>8.2f}x")
    print(f"{'2. Transform (Cleaning)':<24} | {single_res['transform_time']:>18.2f}s | {parallel_res['transform_time']:>18.2f}s | {t_speedup:>8.2f}x 🚀")
    print(f"{'3. Load & Index (SQLite)':<24} | {single_res['load_time']:>18.2f}s | {parallel_res['load_time']:>18.2f}s | {single_res['load_time']/parallel_res['load_time']:>8.2f}x")
    print("-" * 80)
    print(f"{'TOTAL END-TO-END':<24} | {single_res['total_time']:>18.2f}s | {parallel_res['total_time']:>18.2f}s | {e2e_speedup:>8.2f}x ⚡")
    print("-" * 80)
    print(f"{'Throughput (rows/sec)':<24} | {single_res['valid_rows']/single_res['total_time']:>18,.0f} | {parallel_res['valid_rows']/parallel_res['total_time']:>18,.0f} | {e2e_speedup:>8.2f}x")
    print("=" * 70)


if __name__ == "__main__":
    main()
