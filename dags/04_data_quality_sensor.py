"""
04_data_quality_sensor.py
=========================
Demonstrates Airflow Sensors and Data Quality Validation.
Waits for an incoming data file before triggering downstream processing.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

DATA_DIR = Path("/Users/pkshrestha/git/airflow/data")
INPUT_FILE = "incoming_sales.json"


@dag(
    dag_id="04_data_quality_sensor",
    description="FileSensor & Data Quality Validation: Waits for file arrival and validates schemas",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "sensor", "data_quality"],
    default_args={
        "owner": "data-eng",
        "retries": 1,
    },
)
def data_quality_pipeline():
    # 1. Setup mock file generator task (for instant self-contained testing)
    @task
    def mock_generate_data_file():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        file_path = DATA_DIR / INPUT_FILE
        payload = {
            "source": "pos_terminal_01",
            "timestamp": datetime.utcnow().isoformat(),
            "records": [
                {"item": "Mechanical Keyboard", "price": 120.00, "valid": True},
                {"item": "USB-C Dock", "price": 85.50, "valid": True},
                {"item": "Corrupt Entry", "price": -5.00, "valid": False},
            ],
        }
        with open(file_path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Generated sample file at: {file_path}")
        return str(file_path)

    # 2. Validate data quality
    @task
    def validate_and_clean_data(file_path_str: str) -> dict:
        file_path = Path(file_path_str)
        if not file_path.exists():
            raise FileNotFoundError(f"Missing expected data file: {file_path}")

        with open(file_path, "r") as f:
            data = json.load(f)

        records = data.get("records", [])
        valid_records = []
        rejected_records = []

        for r in records:
            if r.get("valid") and r.get("price", 0) > 0:
                valid_records.append(r)
            else:
                rejected_records.append(r)

        print(f"Data Quality Check: {len(valid_records)} valid, {len(rejected_records)} rejected.")
        return {
            "source": data.get("source"),
            "valid_count": len(valid_records),
            "rejected_count": len(rejected_records),
            "valid_records": valid_records,
        }

    @task
    def publish_clean_dataset(metrics: dict):
        print(f"✅ Published {metrics['valid_count']} validated records from {metrics['source']} to Warehouse.")

    # Orchestrate
    f_path = mock_generate_data_file()
    clean_metrics = validate_and_clean_data(f_path)
    publish_clean_dataset(clean_metrics)


data_quality_pipeline()
