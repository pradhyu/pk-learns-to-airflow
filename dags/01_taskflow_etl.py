"""
01_taskflow_etl.py
===================
Demonstrates the modern Apache Airflow 2.x TaskFlow API.
Uses Python decorators (@dag, @task) to build clean ETL pipelines
with automatic XCom data passing between tasks.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="01_taskflow_etl",
    description="Modern Airflow TaskFlow API: Extract, Transform, and Load User Orders",
    schedule="0 6 * * *",  # Runs daily at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "taskflow", "etl"],
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def taskflow_etl_pipeline():
    @task
    def extract_orders() -> dict:
        """Simulate extracting raw order records from an API or database."""
        raw_data = {
            "batch_id": "BATCH-20260918",
            "orders": [
                {"id": 101, "customer": "Alice", "amount": 149.50, "currency": "USD"},
                {"id": 102, "customer": "Bob", "amount": 89.99, "currency": "USD"},
                {"id": 103, "customer": "Charlie", "amount": 420.00, "currency": "USD"},
                {"id": 104, "customer": "Dana", "amount": 15.00, "currency": "USD"},
            ],
        }
        print(f"Extracted {len(raw_data['orders'])} orders for batch {raw_data['batch_id']}.")
        return raw_data

    @task
    def transform_orders(order_data: dict) -> dict:
        """Clean and aggregate order metrics."""
        orders = order_data.get("orders", [])
        total_revenue = sum(o["amount"] for o in orders)
        vip_customers = [o["customer"] for o in orders if o["amount"] > 100.0]

        summary = {
            "batch_id": order_data["batch_id"],
            "total_orders": len(orders),
            "total_revenue": round(total_revenue, 2),
            "vip_customers": vip_customers,
            "processed_at": datetime.utcnow().isoformat(),
        }
        print(f"Summary computed: Revenue=${summary['total_revenue']}, VIPs={vip_customers}")
        return summary

    @task
    def load_and_report(summary: dict):
        """Simulate loading summary into a data warehouse and publishing a report."""
        print("=" * 50)
        print(f"📊 ETL Report for Batch: {summary['batch_id']}")
        print(f" • Total Orders:  {summary['total_orders']}")
        print(f" • Total Revenue: ${summary['total_revenue']}")
        print(f" • VIP Customers: {', '.join(summary['vip_customers'])}")
        print(f" • Processed At:  {summary['processed_at']}")
        print("=" * 50)

    # Define task dependencies cleanly via function invocations (TaskFlow API)
    raw_orders = extract_orders()
    transformed_summary = transform_orders(raw_orders)
    load_and_report(transformed_summary)


# Instantiate the DAG
taskflow_etl_pipeline()
