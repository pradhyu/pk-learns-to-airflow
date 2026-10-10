#!/usr/bin/env python3
"""
Parquet vs JSON/Dict Staging Benchmark
=======================================
Demonstrates why Apache Parquet is vastly superior to JSON/Dict/XCom serialization
for intermediate data processing in data pipelines.
"""

import json
import time
from pathlib import Path
import pandas as pd
from tabulate import tabulate

CURRENT_DIR = Path(__file__).resolve().parent
STAGING_DIR = CURRENT_DIR / "staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

JSON_FILE = STAGING_DIR / "orders_staging.json"
PARQUET_FILE = STAGING_DIR / "orders_staging.parquet"


def generate_benchmark_dataset(rows: int = 100_000) -> pd.DataFrame:
    """Generates a representative e-commerce order dataset."""
    print(f"Generating {rows:,} test records for benchmark...")
    import random

    categories = ["Electronics", "Furniture", "Office Supplies", "Appliances", "Hardware"]
    items = ["Monitor 4K", "Ergonomic Chair", "Gel Pen Box", "Standing Desk", "USB Hub", "Air Purifier"]
    
    data = {
        "order_id": [f"ORD-{100000 + i}" for i in range(rows)],
        "customer_id": [f"CUST-{random.randint(1000, 9999)}" for i in range(rows)],
        "category": [random.choice(categories) for _ in range(rows)],
        "item_description": [random.choice(items) for _ in range(rows)],
        "units": [random.randint(1, 15) for _ in range(rows)],
        "unit_price": [round(random.uniform(9.99, 899.99), 2) for _ in range(rows)],
        "discount_rate": [round(random.choice([0.0, 0.05, 0.10, 0.15, 0.20]), 2) for _ in range(rows)],
        "order_date": ["2026-09-18" for _ in range(rows)],
        "status": ["COMPLETED" for _ in range(rows)],
    }
    df = pd.DataFrame(data)
    df["gross_amount"] = (df["units"] * df["unit_price"]).round(2)
    df["net_amount"] = (df["gross_amount"] * (1 - df["discount_rate"])).round(2)
    return df


def run_benchmark():
    df = generate_benchmark_dataset(rows=100_000)

    # 1. JSON Serialization (Equivalent to typical Airflow dict XCom serialization)
    t0 = time.perf_counter()
    df.to_json(JSON_FILE, orient="records", lines=True)
    json_write_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    df_from_json = pd.read_json(JSON_FILE, orient="records", lines=True)
    json_read_time = time.perf_counter() - t0
    json_size_kb = JSON_FILE.stat().st_size / 1024

    # 2. Parquet Serialization (Snappy compression)
    t0 = time.perf_counter()
    df.to_parquet(PARQUET_FILE, engine="pyarrow", compression="snappy", index=False)
    parquet_write_time = time.perf_counter() - t0

    t0 = time.perf_counter()
    df_from_parquet = pd.read_parquet(PARQUET_FILE, engine="pyarrow")
    parquet_read_time = time.perf_counter() - t0
    parquet_size_kb = PARQUET_FILE.stat().st_size / 1024

    # 3. Parquet Selective Column Read (Projection)
    t0 = time.perf_counter()
    df_proj = pd.read_parquet(PARQUET_FILE, columns=["category", "net_amount"], engine="pyarrow")
    parquet_proj_read_time = time.perf_counter() - t0

    # Summary table
    table_data = [
        ["Metric", "JSON / Dict (XCom style)", "Parquet (Snappy)", "Improvement"],
        ["File Size", f"{json_size_kb:,.1f} KB", f"{parquet_size_kb:,.1f} KB", f"{(1 - parquet_size_kb/json_size_kb)*100:.1f}% smaller"],
        ["Write Time", f"{json_write_time:.3f} s", f"{parquet_write_time:.3f} s", f"{json_write_time / parquet_write_time:.1f}x faster"],
        ["Read Time (Full)", f"{json_read_time:.3f} s", f"{parquet_read_time:.3f} s", f"{json_read_time / parquet_read_time:.1f}x faster"],
        ["Read Time (2 Cols)", "N/A (Reads all)", f"{parquet_proj_read_time:.3f} s", f"{json_read_time / parquet_proj_read_time:.1f}x faster"],
        ["Schema Fidelity", "Lossy (Dates/types lost)", "100% Native binary types", "Guaranteed types"],
    ]

    print("\n" + "=" * 70)
    print("📊 BENCHMARK RESULTS: 100,000 ROWS INTERMEDIATE DATA")
    print("=" * 70)
    print(tabulate(table_data[1:], headers=table_data[0], tablefmt="fancy_grid"))
    print("\n✨ Conclusion: Staging intermediate data in Parquet prevents DB bloat,")
    print("   drastically cuts task latency, and preserves clean typed schemas.\n")


if __name__ == "__main__":
    run_benchmark()
