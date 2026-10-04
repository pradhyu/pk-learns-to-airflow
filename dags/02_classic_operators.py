"""
02_classic_operators.py
========================
Demonstrates traditional Airflow Operators (BashOperator, PythonOperator,
EmptyOperator) with explicit bitshift operators (>> and <<).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def generate_system_metrics(**context):
    """Generate system telemetry metrics."""
    execution_date = context.get("ds", datetime.utcnow().strftime("%Y-%m-%d"))
    load_avg = os.getloadavg() if hasattr(os, "getloadavg") else (0.1, 0.2, 0.3)
    print(f"[{execution_date}] System 1m/5m/15m Load: {load_avg}")
    return {"load_1m": load_avg[0], "date": execution_date}


with DAG(
    dag_id="02_classic_operators",
    description="Classic Airflow: Bash, Python, and Empty Operators with bitshift orchestration",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "classic", "bash", "python"],
    default_args={
        "owner": "devops",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
) as dag:
    start_pipeline = EmptyOperator(task_id="start_pipeline")

    check_disk_space = BashOperator(
        task_id="check_disk_space",
        bash_command="df -h | head -n 5",
    )

    fetch_system_metrics = PythonOperator(
        task_id="fetch_system_metrics",
        python_callable=generate_system_metrics,
    )

    archive_temp_files = BashOperator(
        task_id="archive_temp_files",
        bash_command='echo "Archiving /tmp files for run: {{ ds }}"; sleep 1; echo "Archive complete."',
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Define orchestration flow:
    # start_pipeline -> [check_disk_space, fetch_system_metrics] in parallel -> archive_temp_files -> end_pipeline
    start_pipeline >> [check_disk_space, fetch_system_metrics] >> archive_temp_files >> end_pipeline
