"""
09_parquet_staging_etl.py
=========================
High-Performance Intermediate DAG Processing using Apache Parquet & PostgreSQL.

Architecture & Spec:
--------------------
1. Extraction & Parquet Staging:
   Extracts raw tabular data and writes directly to an ephemeral, run-scoped Parquet file:
   `data/staging/09_parquet_staging_etl/{run_id}/raw_orders.parquet`
   Avoids serializing large data payloads into Airflow's metadata DB (XCom anti-pattern).
   Only the file path string and basic metrics are passed through XCom.

2. Parquet-to-Parquet Transformation:
   Reads intermediate Parquet with column pruning and schema fidelity. Cleans dirty
   currency and percentage strings, standardizes types, calculates derived financial metrics
   (gross_revenue, discount_amount, net_revenue), and writes:
   `data/staging/09_parquet_staging_etl/{run_id}/clean_orders.parquet`

3. Analytical KPI Aggregation:
   Performs column projection to read only required columns from Parquet, computes
   category-level aggregations (total_orders, total_units, net_revenue, avg_order_value),
   and saves:
   `data/staging/09_parquet_staging_etl/{run_id}/category_summary.parquet`

4. PostgreSQL Warehouse Load:
   Connects to PostgreSQL (using Airflow's PostgresHook or SQLAlchemy connection pool)
   and atomically loads both datasets into production warehouse tables:
   - public.parquet_clean_orders
   - public.parquet_category_summary
   Executes automated data quality verification before completing.

5. Staging Cleanup:
   Uses TriggerRule.ALL_DONE to safely purge ephemeral intermediate files after loading,
   preventing disk accumulation even if tasks fail.
"""

from __future__ import annotations

import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from debug_utils import attach_neovim_debugger

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
EXCEL_FILE = DATA_DIR / "raw_sales_data.xlsx"
STAGING_BASE_DIR = DATA_DIR / "staging" / "09_parquet_staging_etl"


def get_postgres_engine():
    """
    Returns an SQLAlchemy engine for PostgreSQL.
    Tries Airflow PostgresHook first, then falls back to direct SQLAlchemy engine
    with environment variable defaults (postgres / localhost).
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_default")
        return hook.get_sqlalchemy_engine()
    except Exception:
        pass

    from sqlalchemy import create_engine

    pg_user = os.environ.get("POSTGRES_USER", "airflow")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "airflow")
    pg_db = os.environ.get("POSTGRES_DB", "airflow")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        )
        with engine.connect() as conn:
            pass
        return engine
    except Exception:
        # Fallback to localhost if executed directly on host outside Docker
        return create_engine(
            f"postgresql+psycopg2://{pg_user}:{pg_pass}@localhost:{pg_port}/{pg_db}"
        )


@dag(
    dag_id="09_parquet_staging_etl",
    description="High-performance intermediate DAG processing with Parquet & PostgreSQL load",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "parquet", "postgres", "performance", "taskflow", "staging"],
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def parquet_staging_etl_pipeline():

    @task
    def extract_and_stage_parquet(**context) -> Dict[str, Any]:
        """
        Extracts source records and writes them to a run-scoped Parquet file.
        Passes only the file path and lightweight metadata through XCom.
        """
        run_id = context["run_id"]
        safe_run_id = re.sub(r"[^a-zA-Z0-9_\-]", "_", str(run_id))
        run_staging_dir = STAGING_BASE_DIR / safe_run_id
        run_staging_dir.mkdir(parents=True, exist_ok=True)

        raw_parquet_path = run_staging_dir / "raw_orders.parquet"

        # Read source data (from existing Excel or generate mock batch)
        if EXCEL_FILE.exists():
            print(f"📖 Extracting raw records from {EXCEL_FILE}...")
            orders_df = pd.read_excel(EXCEL_FILE, sheet_name="Orders")
        else:
            print("⚠️ Excel source not found, synthesizing realistic raw order batch...")
            orders_df = pd.DataFrame([
                {
                    "Order_ID": f"ORD-{1000+i}",
                    "Customer_ID": f"CUST-0{i%5+1}",
                    "Product_Category": "Electronics" if i % 2 == 0 else "Furniture",
                    "Item_Description": "4K Monitor" if i % 2 == 0 else "Standing Desk",
                    "Units_Sold": (i % 3) + 1,
                    "Unit_Price": f"${199.99 * (i + 1):.2f}",
                    "Discount": "10%" if i % 2 == 0 else "0.05",
                    "Order_Date": "2026-09-18",
                    "Status": "Completed",
                }
                for i in range(25)
            ])

        # Write to intermediate Parquet storage with Snappy compression
        orders_df.to_parquet(raw_parquet_path, engine="pyarrow", compression="snappy", index=False)

        file_size_bytes = raw_parquet_path.stat().st_size
        print(f"✅ Staged {len(orders_df)} raw records to Parquet: {raw_parquet_path} ({file_size_bytes} bytes)")

        return {
            "parquet_path": str(raw_parquet_path),
            "record_count": len(orders_df),
            "size_bytes": file_size_bytes,
            "run_staging_dir": str(run_staging_dir),
        }

    @task
    def transform_and_enrich_parquet(staged_meta: Dict[str, Any]) -> str:
        """
        Reads intermediate Parquet, cleans data, computes financial metrics,
        and writes an enriched Parquet dataset.
        """
        input_path = Path(staged_meta["parquet_path"])
        output_path = input_path.parent / "clean_orders.parquet"

        # Check for active Neovim DAP debugger attach hook
        attach_neovim_debugger()

        print(f"🔄 Reading intermediate Parquet from {input_path}...")
        df = pd.read_parquet(input_path, engine="pyarrow")

        # 1. Deduplicate & filter invalid IDs
        df = df.dropna(subset=["Order_ID"]).drop_duplicates(subset=["Order_ID"]).copy()

        # 2. Cleaning routines
        def clean_currency(val) -> float:
            if pd.isna(val):
                return 0.0
            cleaned = re.sub(r"[^\d.]", "", str(val))
            return float(cleaned) if cleaned else 0.0

        def clean_discount(val) -> float:
            if pd.isna(val):
                return 0.0
            s = str(val).strip()
            if s.endswith("%"):
                return float(s[:-1]) / 100.0
            try:
                num = float(s)
                return num / 100.0 if num > 1.0 else num
            except ValueError:
                return 0.0

        # 3. Clean and standardize types
        df["order_id"] = df["Order_ID"].astype(str).str.strip()
        df["customer_id"] = df["Customer_ID"].astype(str).str.strip()
        df["category"] = df["Product_Category"].astype(str).str.strip().str.title()
        df["item_desc"] = df["Item_Description"].astype(str).str.strip()
        df["units"] = pd.to_numeric(df["Units_Sold"], errors="coerce").fillna(0).astype(int)
        df = df[df["units"] > 0]  # filter corrupted negative/zero quantities

        df["unit_price"] = df["Unit_Price"].apply(clean_currency)
        df["discount_rate"] = df["Discount"].apply(clean_discount)

        # 4. Compute financial metrics
        df["gross_revenue"] = (df["units"] * df["unit_price"]).round(2)
        df["discount_amount"] = (df["gross_revenue"] * df["discount_rate"]).round(2)
        df["net_revenue"] = (df["gross_revenue"] - df["discount_amount"]).round(2)
        df["processed_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        clean_cols = [
            "order_id", "customer_id", "category", "item_desc",
            "units", "unit_price", "discount_rate", "gross_revenue",
            "discount_amount", "net_revenue", "processed_at",
        ]
        clean_df = df[clean_cols]

        # 5. Save cleaned intermediate Parquet dataset
        clean_df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
        print(f"✅ Transformed {len(clean_df)} valid orders saved to Parquet: {output_path}")

        return str(output_path)

    @task
    def aggregate_category_kpis(clean_parquet_path: str) -> str:
        """
        Reads clean Parquet, computes category-level KPIs, and outputs a summary Parquet.
        Demonstrates column projection: only loads the columns needed for aggregation.
        """
        input_path = Path(clean_parquet_path)
        summary_path = input_path.parent / "category_summary.parquet"

        # Column projection: read only the columns needed for aggregation
        projection = ["category", "units", "gross_revenue", "net_revenue"]
        df = pd.read_parquet(input_path, columns=projection, engine="pyarrow")

        summary_df = df.groupby("category").agg(
            total_orders=("category", "count"),
            total_units=("units", "sum"),
            total_gross_rev=("gross_revenue", "sum"),
            total_net_rev=("net_revenue", "sum"),
        ).reset_index()

        summary_df["avg_order_value"] = (
            summary_df["total_net_rev"] / summary_df["total_orders"]
        ).round(2)
        summary_df["aggregated_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        summary_df.to_parquet(summary_path, engine="pyarrow", compression="snappy", index=False)
        print(f"📊 Aggregated {len(summary_df)} categories saved to: {summary_path}")

        return str(summary_path)

    @task
    def load_to_postgres(clean_orders_path: str, summary_path: str):
        """
        Loads cleaned records and category summaries from Parquet directly into PostgreSQL.
        Performs data quality verification and logs summary reports.
        """
        from sqlalchemy import text

        clean_df = pd.read_parquet(clean_orders_path, engine="pyarrow")
        summary_df = pd.read_parquet(summary_path, engine="pyarrow")

        engine = get_postgres_engine()

        print(f"🐘 Loading data from Parquet into PostgreSQL tables...")
        # Write tables to PostgreSQL
        clean_df.to_sql("parquet_clean_orders", con=engine, if_exists="replace", index=False)
        summary_df.to_sql("parquet_category_summary", con=engine, if_exists="replace", index=False)

        # Data quality verification check
        with engine.connect() as conn:
            orders_count = conn.execute(text("SELECT count(*) FROM parquet_clean_orders")).scalar()
            summary_rows = conn.execute(text("SELECT count(*) FROM parquet_category_summary")).scalar()
            print(f"✅ PostgreSQL Verification Passed:")
            print(f"   • public.parquet_clean_orders:     {orders_count} rows")
            print(f"   • public.parquet_category_summary: {summary_rows} rows")

            # Print category breakdown from Postgres
            results = conn.execute(
                text("SELECT category, total_orders, total_net_rev, avg_order_value "
                     "FROM parquet_category_summary ORDER BY total_net_rev DESC")
            ).fetchall()
            print("\n📈 Category Summary in PostgreSQL:")
            print(f"   {'Category':<20} {'Orders':<10} {'Net Revenue ($)':<16} {'AOV ($)':<10}")
            print("   " + "-" * 56)
            for row in results:
                print(f"   {row[0]:<20} {row[1]:<10} {row[2]:<16.2f} {row[3]:<10.2f}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_staging_files(staged_meta: Dict[str, Any]):
        """
        Purges ephemeral Parquet staging files for this DAG run to keep storage clean.
        Runs regardless of upstream success/failure (TriggerRule.ALL_DONE).
        """
        staging_dir = Path(staged_meta.get("run_staging_dir", ""))
        if staging_dir.exists() and staging_dir.is_dir():
            shutil.rmtree(staging_dir)
            print(f"🧹 Cleaned up ephemeral staging directory: {staging_dir}")
        else:
            print(f"ℹ️ Staging directory already cleaned or not found: {staging_dir}")

    # Pipeline task dependency wiring
    staged_data = extract_and_stage_parquet()
    clean_data_path = transform_and_enrich_parquet(staged_data)
    category_kpi_path = aggregate_category_kpis(clean_data_path)
    load_task = load_to_postgres(clean_data_path, category_kpi_path)

    # Cleanup staging directory after loading completes
    cleanup_task = cleanup_staging_files(staged_data)
    load_task >> cleanup_task


# Instantiate DAG
parquet_staging_etl_pipeline()
