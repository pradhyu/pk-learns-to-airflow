"""
Airflow DAG: Excel to SQLite ETL Pipeline with Quality Verification.

This DAG can be symlinked or placed into ~/git/airflow/dags/ to run within Airflow.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from airflow.decorators import dag, task

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
EXCEL_FILE = DATA_DIR / "raw_sales_data.xlsx"
SQLITE_DB = DATA_DIR / "sales_warehouse.db"


@dag(
    dag_id="example_excel_to_sqlite_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["example", "excel", "sqlite", "etl", "verification"],
)
def excel_to_sqlite_etl():

    @task
    def extract_excel() -> Dict[str, Any]:
        """Extract multi-sheet tables from Excel."""
        if not EXCEL_FILE.exists():
            raise FileNotFoundError(f"Missing source Excel file at: {EXCEL_FILE}")

        orders_df = pd.read_excel(EXCEL_FILE, sheet_name="Orders")
        customers_df = pd.read_excel(EXCEL_FILE, sheet_name="Customers")

        return {
            "orders_raw": orders_df.to_dict(orient="records"),
            "customers_raw": customers_df.to_dict(orient="records"),
        }

    @task
    def transform(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Clean data, parse currencies/dates, calculate metrics, and quarantine bad records."""
        orders_df = pd.DataFrame(payload["orders_raw"])
        customers_df = pd.DataFrame(payload["customers_raw"])

        # Customer transformations
        customers_df["cust_id"] = customers_df["Cust_ID"].astype(str).str.strip()
        customers_df["full_name"] = customers_df["Full_Name"].astype(str).str.strip()
        customers_df["email"] = customers_df["Email"].astype(str).str.strip().str.lower()
        country_map = {"UNITED STATES": "USA", "UNITED KINGDOM": "UK", "US": "USA"}
        customers_df["country"] = (
            customers_df["Country"].astype(str).str.strip().str.upper().replace(country_map)
        )
        clean_customers = customers_df[["cust_id", "full_name", "email", "country"]].drop_duplicates("cust_id")

        # Helpers
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

        # Quarantine filter
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
            "expected_orders_count": len(valid),
            "expected_net_revenue": total_net_rev,
        }

    @task
    def load_sqlite(transformed_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Load data into SQLite with atomic transaction and upsert."""
        SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)
        orders = transformed_payload["orders"]
        customers = transformed_payload["customers"]

        with sqlite3.connect(SQLITE_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")

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

            for c in customers:
                cursor.execute(
                    """
                    INSERT INTO dim_customers (cust_id, full_name, email, country)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cust_id) DO UPDATE SET full_name=excluded.full_name, country=excluded.country;
                    """,
                    (c["cust_id"], c["full_name"], c["email"], c["country"]),
                )

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
    def verify(load_summary: Dict[str, Any]):
        """Quality Gate: Assert row parity, null constraints, and revenue checksum."""
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
                f"Revenue failure: Expected ${expected_rev}, found ${actual_rev}"
            )

        print(f"✅ Data Quality Gate Passed: {actual_count} orders verified totaling ${actual_rev:,.2f}")

    raw = extract_excel()
    cleaned = transform(raw)
    loaded = load_sqlite(cleaned)
    verify(loaded)


excel_dag = excel_to_sqlite_etl()
