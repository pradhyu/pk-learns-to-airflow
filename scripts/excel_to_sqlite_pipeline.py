#!/usr/bin/env python3
"""
Production-grade Excel to SQLite ETL Pipeline with Transformation & Verification.

Steps:
  1. Extract: Load multi-sheet Excel file (.xlsx / .xls).
  2. Transform:
     - Clean strings (trim, uppercase/lowercase standardization).
     - Parse messy currency ($1,249.99 -> 1249.99).
     - Parse discount percentages (15% -> 0.15).
     - Parse and standardize mixed date formats -> ISO-8601 (YYYY-MM-DD).
     - Derive computed fields (gross_amount, discount_amount, net_amount).
     - Deduplicate & validate data (quarantine invalid records).
  3. Load:
     - Atomic transaction into SQLite database with DDL, Primary Keys, Foreign Keys, Indexes.
     - Upsert support (ON CONFLICT DO UPDATE).
  4. Verify:
     - Row count reconciliation.
     - Checksum / Financial aggregate parity.
     - Referential integrity & constraint checks.
     - Sample analytic SQL reports.
"""

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
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


# -----------------------------------------------------------------------------
# 1. HELPER FUNCTIONS FOR DATA TRANSFORMATION
# -----------------------------------------------------------------------------
def clean_currency(value) -> float:
    """Convert currency strings like ' $1,249.99 ' or numeric floats to float."""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # Strip $, commas, spaces
    cleaned = re.sub(r"[^\d.]", "", str(value))
    return float(cleaned) if cleaned else 0.0


def clean_discount(value) -> float:
    """Convert discount strings like '15%', ' 0.05 ', or floats to decimal 0.0 - 1.0."""
    if pd.isna(value):
        return 0.0
    val_str = str(value).strip()
    if val_str.endswith("%"):
        return float(val_str[:-1].strip()) / 100.0
    try:
        val_float = float(val_str)
        # If entered as whole percentage like 15 instead of 0.15
        if val_float > 1.0:
            return val_float / 100.0
        return val_float
    except ValueError:
        return 0.0


def parse_date_to_iso(value) -> str:
    """Parse mixed date formats (YYYY-MM-DD, MM/DD/YYYY, YYYY/MM/DD) into ISO YYYY-MM-DD."""
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
    """Standardize messy country names."""
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


# -----------------------------------------------------------------------------
# 2. EXTRACT
# -----------------------------------------------------------------------------
def extract_excel_sheets(filepath: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read Orders and Customers sheets from Excel workbook."""
    print(f"\n📂 [EXTRACT] Reading Excel workbook: {filepath}")
    if not filepath.exists():
        raise FileNotFoundError(f"Source file not found: {filepath}")

    orders_df = pd.read_excel(filepath, sheet_name="Orders")
    customers_df = pd.read_excel(filepath, sheet_name="Customers")

    print(f"   -> Read {len(orders_df)} raw rows from 'Orders' sheet.")
    print(f"   -> Read {len(customers_df)} raw rows from 'Customers' sheet.")
    return orders_df, customers_df


# -----------------------------------------------------------------------------
# 3. TRANSFORM
# -----------------------------------------------------------------------------
def transform_data(
    raw_orders_df: pd.DataFrame, raw_customers_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, ETLMetrics]:
    """Clean, transform, calculate derived columns, and quarantine bad rows."""
    print("\n⚙️  [TRANSFORM] Executing data transformations...")
    metrics = ETLMetrics(raw_orders=len(raw_orders_df))

    # --- A. Transform Customers ---
    customers = raw_customers_df.copy()
    customers["cust_id"] = customers["Cust_ID"].astype(str).str.strip()
    customers["full_name"] = customers["Full_Name"].astype(str).str.strip()
    customers["email"] = customers["Email"].astype(str).str.strip().str.lower()
    customers["country"] = customers["Country"].apply(normalize_country)
    customers["signup_date"] = customers["Signup_Date"].apply(parse_date_to_iso)
    customers = customers[["cust_id", "full_name", "email", "country", "signup_date"]]
    customers = customers.drop_duplicates(subset=["cust_id"], keep="last")

    # --- B. Transform Orders ---
    orders = raw_orders_df.copy()

    # 1. Clean string fields
    orders["order_id"] = orders["Order_ID"].astype(str).str.strip()
    orders["customer_id"] = orders["Customer_ID"].astype(str).str.strip()
    orders["product_category"] = orders["Product_Category"].astype(str).str.strip().str.title()
    orders["item_description"] = orders["Item_Description"].astype(str).str.strip()
    orders["status"] = orders["Status"].astype(str).str.strip().str.upper()

    # 2. Clean numeric and date fields
    orders["units_sold"] = pd.to_numeric(orders["Units_Sold"], errors="coerce")
    orders["unit_price"] = orders["Unit_Price"].apply(clean_currency)
    orders["discount_rate"] = orders["Discount_Rate"].apply(clean_discount)
    orders["order_date"] = orders["Order_Date"].apply(parse_date_to_iso)

    # 3. Identify and Quarantine invalid / corrupted records
    invalid_mask = (
        (orders["Order_ID"].isna())
        | (orders["order_id"] == "None")
        | (orders["order_id"] == "nan")
        | (orders["units_sold"] <= 0)
        | (orders["units_sold"].isna())
        | (orders["unit_price"] <= 0)
        | (orders["order_date"].isna())
    )

    quarantined = orders[invalid_mask].copy()
    quarantined["rejection_reason"] = "Invalid Order ID, non-positive units/price, or unparseable date"
    metrics.quarantined_orders = len(quarantined)

    valid_orders = orders[~invalid_mask].copy()

    # 4. Deduplicate valid orders by Order ID (keep latest)
    valid_orders = valid_orders.drop_duplicates(subset=["order_id"], keep="last")

    # 5. Derived financial calculations
    valid_orders["units_sold"] = valid_orders["units_sold"].astype(int)
    valid_orders["gross_amount"] = (valid_orders["units_sold"] * valid_orders["unit_price"]).round(2)
    valid_orders["discount_amount"] = (valid_orders["gross_amount"] * valid_orders["discount_rate"]).round(2)
    valid_orders["net_amount"] = (valid_orders["gross_amount"] - valid_orders["discount_amount"]).round(2)

    # Final column ordering
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

    print(f"   -> Valid transformed orders: {len(clean_orders)}")
    print(f"   -> Quarantined bad records: {len(quarantined)}")
    print(f"   -> Calculated total net revenue: ${metrics.total_net_revenue:,.2f}")

    return clean_orders, customers, quarantined, metrics


# -----------------------------------------------------------------------------
# 4. LOAD (SQLITE)
# -----------------------------------------------------------------------------
def initialize_database(db_path: Path):
    """Create schema, tables, constraints, and indexes in SQLite."""
    print(f"\n🏗️  [LOAD: DDL] Initializing SQLite database schema at: {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Table: Customers
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                cust_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                email TEXT,
                country TEXT NOT NULL,
                signup_date TEXT
            );
        """)

        # Table: Orders
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

        # Table: Quarantine Log
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

        # Indexes for query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer ON fact_orders(customer_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_category ON fact_orders(product_category);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON fact_orders(order_date);")
        conn.commit()


def load_into_sqlite(
    db_path: Path,
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    quarantined_df: pd.DataFrame,
    metrics: ETLMetrics,
):
    """Load transformed datasets transactionally with Upsert support."""
    print(f"📥 [LOAD: DML] Loading records into SQLite...")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()

        # 1. Upsert Customers
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
        metrics.loaded_customers = cursor.rowcount

        # 2. Upsert Orders
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
        metrics.loaded_orders = cursor.rowcount

        # 3. Log Quarantined Records
        if not quarantined_df.empty:
            quarantine_records = [
                (
                    str(row.get("Order_ID", "")),
                    str(row.get("Customer_ID", "")),
                    str(row.get("Product_Category", "")),
                    str(row.get("rejection_reason", "Data validation failed")),
                )
                for _, row in quarantined_df.iterrows()
            ]
            cursor.executemany(
                """
                INSERT INTO quarantine_orders (raw_order_id, raw_customer_id, raw_category, rejection_reason)
                VALUES (?, ?, ?, ?);
                """,
                quarantine_records,
            )

        conn.commit()
    print("   -> Transaction committed successfully.")


# -----------------------------------------------------------------------------
# 5. VERIFICATION & DATA QUALITY CHECKS
# -----------------------------------------------------------------------------
def verify_pipeline(db_path: Path, metrics: ETLMetrics):
    """Run automated verification checks and query analytical summaries."""
    print("\n🔍 [VERIFY] Running Data Quality & Verification Checks...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Check 1: Record Count Integrity
        cursor.execute("SELECT COUNT(*) FROM fact_orders;")
        db_order_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM dim_customers;")
        db_customer_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM quarantine_orders;")
        db_quarantine_count = cursor.fetchone()[0]

        print(f"   [Check 1: Row Parity] Transformed={metrics.transformed_orders} | DB Fact={db_order_count} | DB Dim={db_customer_count}")
        assert db_order_count == metrics.transformed_orders, (
            f"❌ Row count mismatch: expected {metrics.transformed_orders}, got {db_order_count}"
        )
        print("   ✅ Row count check PASSED.")

        # Check 2: Financial Checksum / Aggregate Parity
        cursor.execute("SELECT ROUND(SUM(net_amount), 2) FROM fact_orders;")
        db_net_total = cursor.fetchone()[0] or 0.0

        print(f"   [Check 2: Financial Parity] In-Memory Sum=${metrics.total_net_revenue:,.2f} | DB Sum=${db_net_total:,.2f}")
        assert abs(db_net_total - round(metrics.total_net_revenue, 2)) < 0.01, (
            f"❌ Financial mismatch: in-memory {metrics.total_net_revenue} vs DB {db_net_total}"
        )
        print("   ✅ Financial reconciliation check PASSED.")

        # Check 3: Primary Key & Null Checks
        cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL OR customer_id IS NULL;")
        null_keys = cursor.fetchone()[0]
        assert null_keys == 0, f"❌ Found {null_keys} rows with null keys!"
        print("   ✅ Primary/Foreign Key null check PASSED.")

        # Check 4: Foreign Key Orphan Check
        cursor.execute("""
            SELECT COUNT(*) FROM fact_orders o
            LEFT JOIN dim_customers c ON o.customer_id = c.cust_id
            WHERE c.cust_id IS NULL;
        """)
        orphaned_orders = cursor.fetchone()[0]
        assert orphaned_orders == 0, f"❌ Found {orphaned_orders} orphaned orders without valid customer!"
        print("   ✅ Referential integrity check PASSED.")

        # Analytics Report: Revenue by Category
        print("\n📊 --- Analytical Summary Report ---")
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

        # Analytics Report: Customer Purchase Summary
        print("\n👥 --- Top Customer Summary ---")
        cust_report = pd.read_sql_query(
            """
            SELECT 
                c.cust_id AS Cust_ID,
                c.full_name AS Name,
                c.country AS Country,
                COUNT(o.order_id) AS Total_Orders,
                printf('$%.2f', SUM(o.net_amount)) AS Total_Spend
            FROM dim_customers c
            JOIN fact_orders o ON c.cust_id = o.customer_id
            GROUP BY c.cust_id, c.full_name, c.country
            ORDER BY SUM(o.net_amount) DESC;
            """,
            conn,
        )
        print("\n" + cust_report.to_string(index=False))


# -----------------------------------------------------------------------------
# MAIN PIPELINE EXECUTION
# -----------------------------------------------------------------------------
def run_pipeline():
    print("=" * 70)
    print("🚀 STARTING EXCEL TO SQLITE ETL PIPELINE")
    print("=" * 70)

    # 1. Extract
    orders_raw, customers_raw = extract_excel_sheets(EXCEL_FILE)

    # 2. Transform
    clean_orders, clean_customers, quarantined, metrics = transform_data(orders_raw, customers_raw)

    # 3. Load
    initialize_database(SQLITE_DB)
    load_into_sqlite(SQLITE_DB, clean_customers, clean_orders, quarantined, metrics)

    # 4. Verify
    verify_pipeline(SQLITE_DB, metrics)

    print("\n" + "=" * 70)
    print("🎉 ETL PIPELINE & VERIFICATION COMPLETED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()
