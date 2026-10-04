#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pandas>=2.0.0",
#     "openpyxl>=3.1.0",
#     "tabulate>=0.9.0",
#     "sqlite-utils>=3.35.0",
# ]
# ///
"""
Step 2: Excel to SQLite ETL Pipeline with Transformation & Verification.

Steps:
  1. Extract: Multi-sheet Excel workbook (.xlsx / .xls).
  2. Transform:
     - Trims and normalizes string casing.
     - Parses currency strings ($1,249.99 -> 1249.99).
     - Parses discount percentage strings (15% -> 0.15).
     - Standardizes mixed date formats to ISO (YYYY-MM-DD).
     - Computes gross_amount, discount_amount, and net_amount.
     - Quarantines bad/corrupted rows (invalid IDs, negative units).
     - Deduplicates order records.
  3. Load:
     - SQLite transactional upsert (ON CONFLICT DO UPDATE).
     - Primary Keys, Foreign Keys, and Check constraints.
  4. Verify:
     - Row count parity check.
     - Financial checksum parity check.
     - Null and orphan checks.
     - Analytical SQL summary reports.
"""

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Tuple
import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
EXCEL_FILE = DATA_DIR / "raw_sales_data.xlsx"
SQLITE_DB = DATA_DIR / "sales_warehouse.db"


@dataclass
class ETLMetrics:
    raw_orders: int = 0
    quarantined_orders: int = 0
    transformed_orders: int = 0
    loaded_orders: int = 0
    loaded_customers: int = 0
    total_net_revenue: float = 0.0


def clean_currency(value) -> float:
    """Convert currency strings like ' $1,249.99 ' to float."""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[^\d.]", "", str(value))
    return float(cleaned) if cleaned else 0.0


def clean_discount(value) -> float:
    """Convert discount strings like '15%' or '0.05' to float 0.0 - 1.0."""
    if pd.isna(value):
        return 0.0
    val_str = str(value).strip()
    if val_str.endswith("%"):
        return float(val_str[:-1].strip()) / 100.0
    try:
        val_float = float(val_str)
        return val_float / 100.0 if val_float > 1.0 else val_float
    except ValueError:
        return 0.0


def parse_date_to_iso(value) -> str:
    """Parse mixed date formats into ISO YYYY-MM-DD."""
    if pd.isna(value):
        return None
    val_str = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def normalize_country(value) -> str:
    """Standardize country names."""
    if pd.isna(value):
        return "UNKNOWN"
    cleaned = str(value).strip().upper()
    mapping = {
        "UNITED STATES": "USA",
        "UNITED STATES OF AMERICA": "USA",
        "US": "USA",
        "UNITED KINGDOM": "UK",
        "GREAT BRITAIN": "UK",
    }
    return mapping.get(cleaned, cleaned)


# --- 1. EXTRACT ---
def extract_excel_sheets(filepath: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    print(f"\n📂 [EXTRACT] Reading Excel file: {filepath}")
    if not filepath.exists():
        raise FileNotFoundError(f"Source file not found at: {filepath}. Run Step 1 first!")

    orders_df = pd.read_excel(filepath, sheet_name="Orders")
    customers_df = pd.read_excel(filepath, sheet_name="Customers")

    print(f"   -> Orders raw rows: {len(orders_df)}")
    print(f"   -> Customers raw rows: {len(customers_df)}")
    return orders_df, customers_df


# --- 2. TRANSFORM ---
def transform_data(
    raw_orders_df: pd.DataFrame, raw_customers_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, ETLMetrics]:
    print("\n⚙️  [TRANSFORM] Cleaning, normalizing, and calculating metrics...")
    metrics = ETLMetrics(raw_orders=len(raw_orders_df))

    # Customers
    customers = raw_customers_df.copy()
    customers["cust_id"] = customers["Cust_ID"].astype(str).str.strip()
    customers["full_name"] = customers["Full_Name"].astype(str).str.strip()
    customers["email"] = customers["Email"].astype(str).str.strip().str.lower()
    customers["country"] = customers["Country"].apply(normalize_country)
    customers["signup_date"] = customers["Signup_Date"].apply(parse_date_to_iso)
    customers = customers[["cust_id", "full_name", "email", "country", "signup_date"]]
    customers = customers.drop_duplicates(subset=["cust_id"], keep="last")

    # Orders
    orders = raw_orders_df.copy()
    orders["order_id"] = orders["Order_ID"].astype(str).str.strip()
    orders["customer_id"] = orders["Customer_ID"].astype(str).str.strip()
    orders["product_category"] = orders["Product_Category"].astype(str).str.strip().str.title()
    orders["item_description"] = orders["Item_Description"].astype(str).str.strip()
    orders["status"] = orders["Status"].astype(str).str.strip().str.upper()

    orders["units_sold"] = pd.to_numeric(orders["Units_Sold"], errors="coerce")
    orders["unit_price"] = orders["Unit_Price"].apply(clean_currency)
    orders["discount_rate"] = orders["Discount_Rate"].apply(clean_discount)
    orders["order_date"] = orders["Order_Date"].apply(parse_date_to_iso)

    # Quarantine invalid records
    invalid_mask = (
        (orders["Order_ID"].isna())
        | (orders["order_id"].isin(["None", "nan", ""]))
        | (orders["units_sold"] <= 0)
        | (orders["units_sold"].isna())
        | (orders["unit_price"] <= 0)
        | (orders["order_date"].isna())
    )

    quarantined = orders[invalid_mask].copy()
    quarantined["rejection_reason"] = "Missing ID, non-positive units/price, or unparseable date"
    metrics.quarantined_orders = len(quarantined)

    # Deduplicate valid records
    valid_orders = orders[~invalid_mask].drop_duplicates(subset=["order_id"], keep="last").copy()

    # Derived calculations
    valid_orders["units_sold"] = valid_orders["units_sold"].astype(int)
    valid_orders["gross_amount"] = (valid_orders["units_sold"] * valid_orders["unit_price"]).round(2)
    valid_orders["discount_amount"] = (valid_orders["gross_amount"] * valid_orders["discount_rate"]).round(2)
    valid_orders["net_amount"] = (valid_orders["gross_amount"] - valid_orders["discount_amount"]).round(2)

    clean_orders = valid_orders[
        [
            "order_id",
            "customer_id",
            "product_category",
            "item_description",
            "units_sold",
            "unit_price",
            "discount_rate",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "order_date",
            "status",
        ]
    ]

    metrics.transformed_orders = len(clean_orders)
    metrics.total_net_revenue = float(clean_orders["net_amount"].sum())

    print(f"   -> Valid cleaned orders: {len(clean_orders)}")
    print(f"   -> Quarantined corrupt rows: {len(quarantined)}")
    print(f"   -> Total net revenue: ${metrics.total_net_revenue:,.2f}")

    return clean_orders, customers, quarantined, metrics


# --- 3. LOAD ---
def load_into_sqlite(
    db_path: Path,
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    quarantined_df: pd.DataFrame,
    metrics: ETLMetrics,
):
    print(f"\n📥 [LOAD] Initializing schema and loading into SQLite: {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        # DDL
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
                units_sold INTEGER NOT NULL CHECK (units_sold > 0),
                unit_price REAL NOT NULL CHECK (unit_price > 0),
                discount_rate REAL NOT NULL CHECK (discount_rate >= 0 AND discount_rate <= 1.0),
                gross_amount REAL NOT NULL,
                discount_amount REAL NOT NULL,
                net_amount REAL NOT NULL CHECK (net_amount >= 0),
                order_date TEXT NOT NULL,
                status TEXT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES dim_customers (cust_id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_orders (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_order_id TEXT,
                raw_customer_id TEXT,
                raw_category TEXT,
                rejection_reason TEXT,
                quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer ON fact_orders(customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_category ON fact_orders(product_category);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON fact_orders(order_date);")

        # Upsert Customers
        cursor.executemany(
            """
            INSERT INTO dim_customers (cust_id, full_name, email, country, signup_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cust_id) DO UPDATE SET
                full_name = excluded.full_name,
                email = excluded.email,
                country = excluded.country,
                signup_date = excluded.signup_date;
            """,
            customers_df.values.tolist(),
        )

        # Upsert Orders
        cursor.executemany(
            """
            INSERT INTO fact_orders (
                order_id, customer_id, product_category, item_description,
                units_sold, unit_price, discount_rate, gross_amount,
                discount_amount, net_amount, order_date, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                product_category = excluded.product_category,
                item_description = excluded.item_description,
                units_sold = excluded.units_sold,
                unit_price = excluded.unit_price,
                discount_rate = excluded.discount_rate,
                gross_amount = excluded.gross_amount,
                discount_amount = excluded.discount_amount,
                net_amount = excluded.net_amount,
                order_date = excluded.order_date,
                status = excluded.status,
                loaded_at = CURRENT_TIMESTAMP;
            """,
            orders_df.values.tolist(),
        )

        # Quarantine
        if not quarantined_df.empty:
            quarantine_records = [
                (
                    str(row.get("Order_ID", "")),
                    str(row.get("Customer_ID", "")),
                    str(row.get("Product_Category", "")),
                    str(row.get("rejection_reason", "Validation error")),
                )
                for _, row in quarantined_df.iterrows()
            ]
            cursor.executemany(
                "INSERT INTO quarantine_orders (raw_order_id, raw_customer_id, raw_category, rejection_reason) VALUES (?, ?, ?, ?);",
                quarantine_records,
            )

        conn.commit()
    print("   -> Loaded into SQLite with atomic transaction.")


# --- 4. VERIFY ---
def verify_pipeline(db_path: Path, metrics: ETLMetrics):
    print("\n🔍 [VERIFY] Running automated Data Quality checks...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Check 1: Row count parity
        cursor.execute("SELECT COUNT(*) FROM fact_orders;")
        db_order_count = cursor.fetchone()[0]
        assert db_order_count == metrics.transformed_orders, (
            f"Row count mismatch: {db_order_count} vs {metrics.transformed_orders}"
        )
        print(f"   ✅ [Check 1: Row Parity] {db_order_count} rows in fact_orders == transformed.")

        # Check 2: Financial sum parity
        cursor.execute("SELECT ROUND(SUM(net_amount), 2) FROM fact_orders;")
        db_net_total = cursor.fetchone()[0] or 0.0
        assert abs(db_net_total - round(metrics.total_net_revenue, 2)) < 0.01, (
            f"Financial mismatch: {db_net_total} vs {metrics.total_net_revenue}"
        )
        print(f"   ✅ [Check 2: Financial Sum Parity] DB Total ${db_net_total:,.2f} == Transformed Sum.")

        # Check 3: Null Key check
        cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL OR customer_id IS NULL;")
        assert cursor.fetchone()[0] == 0, "Found NULL primary/foreign keys!"
        print("   ✅ [Check 3: Null Keys] 0 NULL keys found.")

        # Analytics Report
        print("\n📊 --- Analytical Summary by Category ---")
        category_report = pd.read_sql_query(
            """
            SELECT 
                product_category AS Category,
                COUNT(order_id) AS Orders_Count,
                SUM(units_sold) AS Total_Units,
                printf('$%.2f', SUM(gross_amount)) AS Gross_Revenue,
                printf('$%.2f', SUM(discount_amount)) AS Total_Discount,
                printf('$%.2f', SUM(net_amount)) AS Net_Revenue
            FROM fact_orders
            GROUP BY product_category
            ORDER BY SUM(net_amount) DESC;
            """,
            conn,
        )
        print("\n" + category_report.to_string(index=False))


def run_pipeline():
    print("=" * 70)
    print("🚀 EXCEL TO SQLITE ETL PIPELINE (WITH TRANSFORMATIONS & VERIFICATION)")
    print("=" * 70)

    orders_raw, customers_raw = extract_excel_sheets(EXCEL_FILE)
    clean_orders, clean_customers, quarantined, metrics = transform_data(orders_raw, customers_raw)
    load_into_sqlite(SQLITE_DB, clean_customers, clean_orders, quarantined, metrics)
    verify_pipeline(SQLITE_DB, metrics)

    print("\n" + "=" * 70)
    print("🎉 PIPELINE RUN & VERIFICATION SUCCEEDED!")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()
