#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "xlsxwriter>=3.1.0",
#     "numpy>=1.24.0",
# ]
# ///
"""
High-speed generator for 1,000,000 rows of realistic messy Excel data.
Uses XlsxWriter with constant_memory=True for fast, low-RAM streaming.
"""

import time
from pathlib import Path
import numpy as np
import xlsxwriter

CURRENT_DIR = Path(__file__).resolve().parent
DATA_DIR = CURRENT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = DATA_DIR / "sales_1m_rows.xlsx"

TOTAL_ROWS = 1_000_000
CHUNK_SIZE = 50_000


def generate_1m_excel(num_rows: int = TOTAL_ROWS):
    print(f"🚀 Generating {num_rows:,} rows of messy Excel data...")
    print(f"   Target file: {OUTPUT_FILE}")
    start_time = time.time()

    workbook = xlsxwriter.Workbook(str(OUTPUT_FILE), {"constant_memory": True})
    worksheet = workbook.add_worksheet("Orders")

    # Header row
    headers = [
        "Order_ID",
        "Customer_ID",
        "Product_Category",
        "Item_Description",
        "Units_Sold",
        "Unit_Price",
        "Discount_Rate",
        "Order_Date",
        "Status",
    ]
    for col_idx, header in enumerate(headers):
        worksheet.write(0, col_idx, header)

    categories = np.array(["  Electronics ", "furniture", "ELECTRONICS", " Office Supplies ", "Appliances", "Hardware"])
    items = np.array([
        "Noise Cancelling Headphones", "Ergonomic Desk Chair", "4K Ultra HD Monitor",
        "Standing Desk Mat", "Air Purifier Pro", "Executive Office Desk",
        "Wireless Mechanical Keyboard", "Gel Pen Box (50pk)", "USB-C Docking Station"
    ])
    prices = np.array(["$249.99", " $350.00 ", "$499.50", " $45.00", "$180.00", "$750.00", "$120.00", "$18.50"])
    discounts = np.array(["10%", "0.05", "15%", "0%", "20%", "5%", "0.0", "12%"])
    dates = np.array(["2026-01-15", "01/18/2026", "2026/01/20", "2026-02-01", "02/05/2026", "2026-02-20"])
    statuses = np.array([" Delivered ", "SHIPPED", "pending", "Delivered", "CANCELLED"])

    cust_ids = [f"CUST-{i:04d}" for i in range(1, 5001)]
    cust_ids_arr = np.array(cust_ids)

    row_idx = 1
    num_chunks = (num_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk in range(num_chunks):
        current_chunk_size = min(CHUNK_SIZE, num_rows - (chunk * CHUNK_SIZE))

        # Generate vectorized synthetic columns
        rand_cats = np.random.choice(categories, size=current_chunk_size)
        rand_items = np.random.choice(items, size=current_chunk_size)
        rand_prices = np.random.choice(prices, size=current_chunk_size)
        rand_discounts = np.random.choice(discounts, size=current_chunk_size)
        rand_dates = np.random.choice(dates, size=current_chunk_size)
        rand_statuses = np.random.choice(statuses, size=current_chunk_size)
        rand_custs = np.random.choice(cust_ids_arr, size=current_chunk_size)
        rand_units = np.random.randint(1, 15, size=current_chunk_size)

        for i in range(current_chunk_size):
            order_num = chunk * CHUNK_SIZE + i + 1
            worksheet.write(row_idx, 0, f"ORD-{order_num:08d}")
            worksheet.write(row_idx, 1, rand_custs[i])
            worksheet.write(row_idx, 2, rand_cats[i])
            worksheet.write(row_idx, 3, rand_items[i])
            worksheet.write(row_idx, 4, int(rand_units[i]))
            worksheet.write(row_idx, 5, rand_prices[i])
            worksheet.write(row_idx, 6, rand_discounts[i])
            worksheet.write(row_idx, 7, rand_dates[i])
            worksheet.write(row_idx, 8, rand_statuses[i])
            row_idx += 1

        print(f"   Writing progress: {row_idx - 1:,} / {num_rows:,} rows ({(row_idx - 1) / num_rows * 100:.1f}%)")

    # Add Customers sheet
    cust_sheet = workbook.add_worksheet("Customers")
    cust_sheet.write_row(0, 0, ["Cust_ID", "Full_Name", "Email", "Country", "Signup_Date"])
    for idx, cid in enumerate(cust_ids, start=1):
        cust_sheet.write_row(idx, 0, [
            cid,
            f"Customer {idx}",
            f"USER_{idx}@EXAMPLE.COM ",
            "united states" if idx % 2 == 0 else "UK",
            "2025-01-01"
        ])

    workbook.close()
    elapsed = time.time() - start_time
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"\n✅ Completed in {elapsed:.2f}s! File size: {file_size_mb:.2f} MB ({OUTPUT_FILE})")


if __name__ == "__main__":
    generate_1m_excel(1_000_000)
