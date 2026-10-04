"""
Unit & Integration Tests for Excel to SQLite ETL Pipeline.
"""

import sqlite3
import tempfile
from pathlib import Path
import pandas as pd
import pytest

from examples.excel_to_sqlite import (
    clean_currency,
    clean_discount,
    normalize_country,
    parse_date_to_iso,
    transform_chunk,
    transform_data,
    load_into_sqlite,
    verify_pipeline,
)


# -----------------------------------------------------------------------------
# 1. UNIT TESTS: Transformation Helpers
# -----------------------------------------------------------------------------
class TestTransformationHelpers:

    def test_clean_currency(self):
        assert clean_currency("$1,249.99") == 1249.99
        assert clean_currency(" $350.00 ") == 350.00
        assert clean_currency(499.5) == 499.50
        assert clean_currency(None) == 0.0
        assert clean_currency("invalid") == 0.0

    def test_clean_discount(self):
        assert clean_discount("15%") == 0.15
        assert clean_discount("0.05") == 0.05
        assert clean_discount(0.20) == 0.20
        assert clean_discount(" 10% ") == 0.10
        assert clean_discount("0%") == 0.0
        assert clean_discount(None) == 0.0

    def test_parse_date_to_iso(self):
        assert parse_date_to_iso("2026-01-15") == "2026-01-15"
        assert parse_date_to_iso("01/18/2026") == "2026-01-18"
        assert parse_date_to_iso("2026/02/01") == "2026-02-01"
        assert parse_date_to_iso("invalid_date") is None
        assert parse_date_to_iso(None) is None

    def test_normalize_country(self):
        assert normalize_country(" united states ") == "USA"
        assert normalize_country("US") == "USA"
        assert normalize_country("UNITED KINGDOM") == "UK"
        assert normalize_country("Canada") == "CANADA"
        assert normalize_country(None) == "UNKNOWN"


# -----------------------------------------------------------------------------
# 2. INTEGRATION TESTS: Transformation & Business Logic
# -----------------------------------------------------------------------------
class TestPipelineTransformation:

    @pytest.fixture
    def sample_raw_data(self):
        orders_df = pd.DataFrame({
            "Order_ID": ["ORD-1", "ORD-2", "ORD-2", None, "ORD-3"],
            "Customer_ID": ["CUST-1", "CUST-2", "CUST-2", "CUST-3", "CUST-1"],
            "Product_Category": ["Electronics", " Furniture ", "Furniture", "Hardware", "Appliances"],
            "Item_Description": ["Headphones", "Desk", "Desk", "Tool", "Air Purifier"],
            "Units_Sold": [2, 1, 1, 0, -5],
            "Unit_Price": ["$100.00", "$200.00", "$200.00", "$50.00", "$150.00"],
            "Discount_Rate": ["10%", "5%", "5%", "0%", "10%"],
            "Order_Date": ["2026-01-15", "01/18/2026", "01/18/2026", "2026-02-01", "invalid"],
            "Status": ["Delivered", "Shipped", "Shipped", "Draft", "Cancelled"],
        })
        customers_df = pd.DataFrame({
            "Cust_ID": ["CUST-1", "CUST-2"],
            "Full_Name": ["Alice", "Bob"],
            "Email": ["alice@test.com", "bob@test.com"],
            "Country": ["USA", "UK"],
            "Signup_Date": ["2025-01-01", "2025-02-01"],
        })
        return orders_df, customers_df

    def test_transformation_and_quarantine(self, sample_raw_data):
        orders_raw, custs_raw = sample_raw_data
        clean_orders, clean_custs, quarantined, metrics = transform_data(orders_raw, custs_raw)

        # ORD-1 & ORD-2 are valid (ORD-2 duplicate deduplicated)
        assert len(clean_orders) == 2
        # None ID, Units <= 0, Invalid Date should be quarantined
        assert len(quarantined) == 2
        assert metrics.raw_orders == 5
        assert metrics.transformed_orders == 2

        # Check financial calculation for ORD-1 (Units: 2, Price: 100, Disc: 10%)
        ord1 = clean_orders[clean_orders["order_id"] == "ORD-1"].iloc[0]
        assert ord1["gross_amount"] == 200.0
        assert ord1["discount_amount"] == 20.0
        assert ord1["net_amount"] == 180.0


# -----------------------------------------------------------------------------
# 3. DATABASE LOADING & VERIFICATION TESTS
# -----------------------------------------------------------------------------
class TestDatabaseLoadAndVerify:

    def test_end_to_end_sqlite_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_warehouse.db"

            orders_df = pd.DataFrame({
                "Order_ID": ["ORD-101", "ORD-102"],
                "Customer_ID": ["C-1", "C-2"],
                "Product_Category": ["Electronics", "Furniture"],
                "Item_Description": ["Monitor", "Chair"],
                "Units_Sold": [1, 2],
                "Unit_Price": ["$300.00", "$150.00"],
                "Discount_Rate": ["10%", "0%"],
                "Order_Date": ["2026-01-10", "2026-01-12"],
                "Status": ["DELIVERED", "SHIPPED"],
            })
            customers_df = pd.DataFrame({
                "Cust_ID": ["C-1", "C-2"],
                "Full_Name": ["Test User 1", "Test User 2"],
                "Email": ["u1@test.com", "u2@test.com"],
                "Country": ["USA", "UK"],
                "Signup_Date": ["2025-01-01", "2025-01-02"],
            })

            clean_orders, clean_custs, quarantined, metrics = transform_data(orders_df, customers_df)
            load_into_sqlite(db_path, clean_custs, clean_orders, quarantined, metrics)

            # Verification assertions
            verify_pipeline(db_path, metrics)

            # Direct SQLite sanity check
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM fact_orders;")
                assert cursor.fetchone()[0] == 2
                cursor.execute("SELECT COUNT(*) FROM dim_customers;")
                assert cursor.fetchone()[0] == 2


# -----------------------------------------------------------------------------
# 4. PARALLEL WORKER TEST
# -----------------------------------------------------------------------------
class TestParallelWorker:

    def test_parallel_chunk_worker(self):
        chunk_df = pd.DataFrame({
            "Order_ID": ["ORD-501", "ORD-502"],
            "Customer_ID": ["CUST-1", "CUST-2"],
            "Product_Category": ["electronics", "furniture"],
            "Item_Description": ["Laptop", "Desk"],
            "Units_Sold": ["2", "3"],
            "Unit_Price": ["$1,000.00", "$200.00"],
            "Discount_Rate": ["10%", "5%"],
            "Order_Date": ["2026-01-01", "2026-01-02"],
            "Status": ["delivered", "shipped"],
        })

        records, quarantined, chunk_rev = transform_chunk(chunk_df)
        assert len(records) == 2
        assert quarantined == 0
        # ORD-501: 2 * 1000 - 10% = 1800
        # ORD-502: 3 * 200 - 5% = 570
        # Total = 2370.00
        assert chunk_rev == 2370.0
