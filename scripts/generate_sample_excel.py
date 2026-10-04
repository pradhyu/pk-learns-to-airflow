#!/usr/bin/env python3
"""
Generate realistic, messy sample Excel data for ETL demonstration.
Contains:
  1. 'Orders' sheet: dirty currency strings, inconsistent dates, whitespace, missing values, duplicates.
  2. 'Customers' sheet: customer demographics with messy email casing and inconsistent country names.
"""
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = DATA_DIR / "raw_sales_data.xlsx"

def generate_sample_workbook():
    print(f"Generating sample messy Excel file at: {OUTPUT_FILE}")

    # Sheet 1: Orders (Messy raw data)
    orders_data = {
        "Order_ID": [
            "ORD-1001", "ORD-1002", "ORD-1003", "ORD-1004", "ORD-1005",
            "ORD-1006", "ORD-1007", "ORD-1008", "ORD-1008",  # Duplicate row
            "ORD-1009", "ORD-1010", None,                     # Null Order_ID
            "ORD-1011", "ORD-1012"
        ],
        "Customer_ID": [
            "CUST-01", "CUST-02", "CUST-01", "CUST-03", "CUST-04",
            "CUST-02", "CUST-05", "CUST-01", "CUST-01",
            "CUST-06", "CUST-03", "CUST-99",
            "CUST-04", "CUST-05"
        ],
        "Product_Category": [
            "  Electronics ", "furniture", "ELECTRONICS", " Office Supplies ", "Appliances",
            "Furniture  ", "Electronics", "Office Supplies", "Office Supplies",
            "Hardware", "  furniture ", "Unknown",
            "Electronics", "Appliances "
        ],
        "Item_Description": [
            "Noise Cancelling Headphones", "Ergonomic Desk Chair", "4K Ultra HD Monitor",
            "Standing Desk Mat", "Air Purifier Pro", "Executive Office Desk",
            "Wireless Mechanical Keyboard", "Gel Pen Box (50pk)", "Gel Pen Box (50pk)",
            "Heavy Duty Tool Set", "Lumbar Support Cushion", "Corrupted Item",
            "USB-C Docking Station", "Smart Espresso Machine"
        ],
        "Units_Sold": [
            "2", "1", "3", "5", "1",
            "2", "4", "10", "10",
            "-1",  # Corrupted negative quantity
            "3", "0",
            "2", "1"
        ],
        "Unit_Price": [
            "$249.99", " $350.00 ", "$499.50", " $45.00", "$180.00",
            "$750.00", "$120.00", "$18.50", "$18.50",
            "$89.99", " $65.00 ", "$0.00",
            "$189.99", "$599.00"
        ],
        "Discount_Rate": [
            "10%", "0.05", "15%", "0%", "20%",
            "5%", "0.0", "12%", "12%",
            "0%", "10%", "0%",
            "15%", "8%"
        ],
        "Order_Date": [
            "2026-01-15", "01/18/2026", "2026/01/20", "2026-02-01", "02/05/2026",
            "2026-02-10", "2026-02-14", "2026-02-20", "2026-02-20",
            "2026-02-22", "02/25/2026", "invalid_date",
            "2026-03-01", "03/05/2026"
        ],
        "Status": [
            " Delivered ", "SHIPPED", "pending", "Delivered", "CANCELLED",
            "Delivered", "pending", "Delivered", "Delivered",
            "Refunded", "SHIPPED", "draft",
            "Delivered", "Pending"
        ],
    }

    # Sheet 2: Customers
    customers_data = {
        "Cust_ID": ["CUST-01", "CUST-02", "CUST-03", "CUST-04", "CUST-05", "CUST-06"],
        "Full_Name": ["  Alice Walker ", "Bob Smith", "Charlie Brown  ", "Diana Prince", "Evan Wright", "Fiona Gallagher"],
        "Email": ["ALICE@Example.COM", "bob.smith@work.org ", "charlie.b@gmail.com", "diana@amazon.com", "evan.w@tech.io", "fiona@gallagher.co"],
        "Country": ["USA", " united states ", "Canada", "USA", "UK", " united kingdom "],
        "Signup_Date": ["2025-05-12", "2025-08-20", "2025-11-01", "2026-01-03", "2026-01-10", "2026-02-01"]
    }

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        pd.DataFrame(orders_data).to_excel(writer, sheet_name="Orders", index=False)
        pd.DataFrame(customers_data).to_excel(writer, sheet_name="Customers", index=False)

    print(f"✅ Successfully created sample messy Excel file: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_sample_workbook()
