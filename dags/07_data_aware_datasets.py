"""
07_data_aware_datasets.py
=========================
Demonstrates Data-Aware Scheduling with Airflow Datasets.
A Producer DAG updates a Dataset upon completion, which immediately
triggers the downstream Consumer DAG without relying on cron timers.
"""

from __future__ import annotations

from datetime import datetime
from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Define dataset URI
RAW_ORDERS_DATASET = Dataset("s3://datalake-bucket/raw/orders_feed.parquet")
AGGREGATED_METRICS_DATASET = Dataset("s3://datalake-bucket/curated/daily_metrics.parquet")


# ==========================================
# 1. PRODUCER DAG
# ==========================================
@dag(
    dag_id="07_dataset_producer_ingest",
    description="Producer DAG: Ingests raw data and updates RAW_ORDERS_DATASET",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "datasets", "producer"],
    default_args={"owner": "ingestion-team"},
)
def dataset_producer_dag():
    @task(outlets=[RAW_ORDERS_DATASET])
    def ingest_from_upstream_feed():
        print(f"📥 Ingesting orders feed...")
        print(f"💾 Saved payload to {RAW_ORDERS_DATASET.uri}")
        print("✅ RAW_ORDERS_DATASET marked as updated!")

    ingest_from_upstream_feed()


# ==========================================
# 2. CONSUMER DAG (Triggered by RAW_ORDERS_DATASET)
# ==========================================
@dag(
    dag_id="07_dataset_consumer_analytics",
    description="Consumer DAG: Triggered automatically when RAW_ORDERS_DATASET is updated",
    schedule=[RAW_ORDERS_DATASET],  # Listens for updates to this dataset!
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "datasets", "consumer"],
    default_args={"owner": "analytics-team"},
)
def dataset_consumer_dag():
    @task(outlets=[AGGREGATED_METRICS_DATASET])
    def transform_analytics():
        print(f"⚡ Triggered by update event from {RAW_ORDERS_DATASET.uri}")
        print("📊 Running rollups and aggregation queries...")
        print(f"💾 Published curated metrics to {AGGREGATED_METRICS_DATASET.uri}")

    transform_analytics()


# Instantiate DAGs
dataset_producer_dag()
dataset_consumer_dag()
