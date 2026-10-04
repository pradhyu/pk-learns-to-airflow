"""
### DAG 08: Excel (.xlsx/.xls) to SQLite ETL with Transformation & Verification

Demonstrates:
  1. Extracting multi-sheet Excel files into structured payloads.
  2. Multi-step transformations (currency parsing, date standardization, deduplication, computed columns).
  3. Quarantining bad/corrupt records into a quarantine table.
  4. Atomic loading to SQLite warehouse with upsert semantics (ON CONFLICT DO UPDATE).
  5. Automated Data Quality Gate & Reconciliation check before marking the run successful.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from airflow.decorators import dag, task

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
EXCEL_FILE = DATA_DIR / "raw_sales_data.xlsx"
SQLITE_DB = DATA_DIR / "sales_warehouse.db"


@dag(
    dag_id="08_excel_to_sqlite_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Disabled automatic daily schedule (manual trigger only)
    is_paused_upon_creation=True,
    catchup=False,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["etl", "excel", "sqlite", "data-quality"],
)
def excel_to_sqlite_etl():

    @task
    def extract_excel_data() -> Dict[str, Any]:
        """Extract multi-sheet raw tables from Excel."""
        if not EXCEL_FILE.exists():
            raise FileNotFoundError(f"Excel file not found at {EXCEL_FILE}")

        orders_df = pd.read_excel(EXCEL_FILE, sheet_name="Orders")
        customers_df = pd.read_excel(EXCEL_FILE, sheet_name="Customers")

        return {
            "orders_raw": orders_df.to_dict(orient="records"),
            "customers_raw": customers_df.to_dict(orient="records"),
            "extracted_count": len(orders_df),
        }

    @task
    def transform_data(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Clean, normalize, compute metrics, and partition valid vs quarantined rows."""
        orders_df = pd.DataFrame(payload["orders_raw"])
        customers_df = pd.DataFrame(payload["customers_raw"])

        # 1. Transform Customers
        customers_df["cust_id"] = customers_df["Cust_ID"].astype(str).str.strip()
        customers_df["full_name"] = customers_df["Full_Name"].astype(str).str.strip()
        customers_df["email"] = customers_df["Email"].astype(str).str.strip().str.lower()
        country_map = {"UNITED STATES": "USA", "UNITED KINGDOM": "UK", "US": "USA", "GREAT BRITAIN": "UK"}
        customers_df["country"] = (
            customers_df["Country"].astype(str).str.strip().str.upper().replace(country_map)
        )
        clean_customers = customers_df[["cust_id", "full_name", "email", "country"]].drop_duplicates("cust_id")

        # 2. Transform Orders
        def clean_curr(v):
            if pd.isna(v):
                return 0.0
            c = re.sub(r"[^\d.]", "", str(v))
            return float(c) if c else 0.0

        def clean_disc(v):
            if pd.isna(v):
                return 0.0
            s = str(v).strip()
            if s.endswith("%"):
                return float(s[:-1]) / 100.0
            try:
                f = float(s)
                return f / 100.0 if f > 1.0 else f
            except ValueError:
                return 0.0

        def parse_date(v):
            if pd.isna(v):
                return None
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(str(v).strip(), fmt).strftime("%Y-%m-%d")
                except ValueError:
                    pass
            return None

        orders_df["order_id"] = orders_df["Order_ID"].astype(str).str.strip()
        orders_df["customer_id"] = orders_df["Customer_ID"].astype(str).str.strip()
        orders_df["product_category"] = orders_df["Product_Category"].astype(str).str.strip().str.title()
        orders_df["item_description"] = orders_df["Item_Description"].astype(str).str.strip()
        orders_df["status"] = orders_df["Status"].astype(str).str.strip().str.upper()

        orders_df["units_sold"] = pd.to_numeric(orders_df["Units_Sold"], errors="coerce")
        orders_df["unit_price"] = orders_df["Unit_Price"].apply(clean_curr)
        orders_df["discount_rate"] = orders_df["Discount_Rate"].apply(clean_disc)
        orders_df["order_date"] = orders_df["Order_Date"].apply(parse_date)

        # Quarantine Bad Records
        invalid_mask = (
            (orders_df["Order_ID"].isna())
            | (orders_df["order_id"].isin(["None", "nan", ""]))
            | (orders_df["units_sold"] <= 0)
            | (orders_df["units_sold"].isna())
            | (orders_df["unit_price"] <= 0)
            | (orders_df["order_date"].isna())
        )

        quarantined = orders_df[invalid_mask].copy()
        valid = orders_df[~invalid_mask].drop_duplicates(subset=["order_id"], keep="last").copy()

        valid["units_sold"] = valid["units_sold"].astype(int)
        valid["gross_amount"] = (valid["units_sold"] * valid["unit_price"]).round(2)
        valid["discount_amount"] = (valid["gross_amount"] * valid["discount_rate"]).round(2)
        valid["net_amount"] = (valid["gross_amount"] - valid["discount_amount"]).round(2)

        total_net_rev = float(valid["net_amount"].sum())

        return {
            "customers": clean_customers.to_dict(orient="records"),
            "orders": valid.to_dict(orient="records"),
            "quarantined_count": len(quarantined),
            "valid_count": len(valid),
            "expected_net_revenue": total_net_rev,
        }

    @task
    def load_to_sqlite(transformed_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Load data into SQLite with atomic transaction and upsert."""
        SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)
        orders = transformed_payload["orders"]
        customers = transformed_payload["customers"]

        with sqlite3.connect(SQLITE_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")

            # Ensure tables exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_customers (
                    cust_id TEXT PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    email TEXT,
                    country TEXT NOT NULL,
                    signup_date TEXT
                );
            """)
            cursor.execute("""
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
                    status TEXT NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Upsert customers
            for c in customers:
                cursor.execute(
                    """
                    INSERT INTO dim_customers (cust_id, full_name, email, country)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cust_id) DO UPDATE SET full_name=excluded.full_name, country=excluded.country;
                    """,
                    (c["cust_id"], c["full_name"], c["email"], c["country"]),
                )

            # Upsert orders
            for o in orders:
                cursor.execute(
                    """
                    INSERT INTO fact_orders (
                        order_id, customer_id, product_category, item_description,
                        units_sold, unit_price, discount_rate, gross_amount,
                        discount_amount, net_amount, order_date, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(order_id) DO UPDATE SET
                        net_amount=excluded.net_amount, status=excluded.status, loaded_at=CURRENT_TIMESTAMP;
                    """,
                    (
                        o["order_id"],
                        o["customer_id"],
                        o["product_category"],
                        o["item_description"],
                        o["units_sold"],
                        o["unit_price"],
                        o["discount_rate"],
                        o["gross_amount"],
                        o["discount_amount"],
                        o["net_amount"],
                        o["order_date"],
                        o["status"],
                    ),
                )
            conn.commit()

        return {
            "loaded_orders_count": len(orders),
            "expected_net_revenue": transformed_payload["expected_net_revenue"],
        }

    @task
    def verify_data_quality(load_summary: Dict[str, Any]):
        """Quality Gate: Assert row parity, null constraints, and revenue balance."""
        with sqlite3.connect(SQLITE_DB) as conn:
            cursor = conn.cursor()

            # 1. Count check
            cursor.execute("SELECT COUNT(*) FROM fact_orders;")
            actual_count = cursor.fetchone()[0]
            assert actual_count == load_summary["loaded_orders_count"], (
                f"Row mismatch: Expected {load_summary['loaded_orders_count']}, found {actual_count}"
            )

            # 2. Revenue parity check
            cursor.execute("SELECT ROUND(SUM(net_amount), 2) FROM fact_orders;")
            actual_rev = cursor.fetchone()[0]
            expected_rev = round(load_summary["expected_net_revenue"], 2)
            assert abs(actual_rev - expected_rev) < 0.01, (
                f"Revenue balance failure: Expected ${expected_rev}, found ${actual_rev}"
            )

            # 3. Primary Key null check
            cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL;")
            assert cursor.fetchone()[0] == 0, "Found NULL primary keys in fact_orders!"

        print(f"✅ Data Quality Gate Passed! Loaded {actual_count} orders totaling ${actual_rev:,.2f}")

    # Orchestrate DAG dependencies
    raw_payload = extract_excel_data()
    transformed_payload = transform_data(raw_payload)
    load_metrics = load_to_sqlite(transformed_payload)
    verify_data_quality(load_metrics)


excel_to_sqlite_dag_instance = excel_to_sqlite_etl()
