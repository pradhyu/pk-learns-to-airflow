# Excel (.xlsx / .xls) to SQLite ETL Guide

A complete reference and runnable example demonstrating how to extract messy Excel spreadsheets, apply data transformations, load into SQLite with constraints and upsert support, and perform automated verification.

---

## 📁 Project Structure

```
~/git/airflow/
├── data/
│   ├── raw_sales_data.xlsx       # Multi-sheet Excel workbook (generated)
│   └── sales_warehouse.db       # Target SQLite Database
├── scripts/
│   ├── generate_sample_excel.py  # Generates realistic messy Excel workbook
│   └── excel_to_sqlite_pipeline.py # Standalone Python ETL + Verification script
└── dags/
    └── 08_excel_to_sqlite_dag.py # Airflow TaskFlow DAG with quality gates
```

---

## ⚡ Quick Start (Try It in 5 Seconds)

```bash
cd ~/git/airflow
source .venv/bin/activate

# 1. Generate realistic messy Excel workbook
python scripts/generate_sample_excel.py

# 2. Run ETL Pipeline + Automated Verification
python scripts/excel_to_sqlite_pipeline.py
```

---

## 🛠️ Step-by-Step Architecture

### 1. Extraction
- Reads multi-tab Excel files (`Orders`, `Customers`) with `pandas` / `openpyxl`.
- Supports both `.xlsx` (modern XML) and `.xls` (via `xlrd`).

### 2. Transformation Pipeline
- **String Sanitization:** Trims whitespace, standardizes case (e.g. `" united states "` $\rightarrow$ `"USA"`, `"electronics"` $\rightarrow$ `"Electronics"`).
- **Currency Parsing:** Converts dirty formatted currency strings (`"$1,249.99"`) into standard numeric floats (`1249.99`).
- **Percentage Normalization:** Handles both string percentages (`"15%"`) and decimals (`0.15`) into uniform decimal floats.
- **Date Standardization:** Parses mixed date strings (`YYYY-MM-DD`, `MM/DD/YYYY`, `YYYY/MM/DD`) into ISO-8601 strings (`YYYY-MM-DD`).
- **Derived Financial Metrics:**
  $$\text{gross\_amount} = \text{units\_sold} \times \text{unit\_price}$$
  $$\text{discount\_amount} = \text{gross\_amount} \times \text{discount\_rate}$$
  $$\text{net\_amount} = \text{gross\_amount} - \text{discount\_amount}$$
- **Data Validation & Quarantining:** Corrupt rows (negative units, missing IDs, unparseable dates) are filtered out and routed to a `quarantine_orders` table instead of crashing the pipeline.

### 3. Loading (SQLite Warehouse)
- Uses strict DDL schema with `PRIMARY KEY`, `FOREIGN KEY`, and `CHECK` constraints.
- Employs **Upsert** semantics (`INSERT ... ON CONFLICT(order_id) DO UPDATE ...`).
- Creates secondary indexes on `customer_id`, `product_category`, and `order_date`.

### 4. Automated Verification & Quality Gates
- **Row Count Parity:** Ensures DB fact row count matches transformed clean row count.
- **Financial Parity:** Ensures `SUM(net_amount)` in database exactly matches memory checksum.
- **Primary/Foreign Key Constraints:** Confirms zero null keys and zero orphaned orders.
- **Analytical Reporting:** Executes SQL aggregations by Product Category and Customer.
