"""
03_conditional_branching.py
===========================
Demonstrates dynamic decision making in Airflow with @task.branch
and trigger rules to join branches cleanly.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="03_conditional_branching",
    description="Conditional Branching: Dynamic routing based on payload size or metric thresholds",
    schedule=None,  # Triggered manually or via API
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "branching", "dynamic"],
    default_args={
        "owner": "analytics",
    },
)
def branching_pipeline():
    @task
    def evaluate_traffic_volume() -> int:
        """Simulate evaluating real-time transaction count."""
        volume = random.randint(50, 5000)
        print(f"Current transaction volume: {volume} req/sec")
        return volume

    @task.branch
    def choose_processing_tier(volume: int) -> str:
        """Decide whether to route to high-capacity cluster or standard batch worker."""
        if volume > 1000:
            print(f"High traffic ({volume} > 1000). Routing to -> high_capacity_cluster")
            return "high_capacity_cluster"
        else:
            print(f"Normal traffic ({volume} <= 1000). Routing to -> standard_batch_worker")
            return "standard_batch_worker"

    @task(task_id="high_capacity_cluster")
    def process_high_capacity():
        print("⚡ Spinning up Spark / Distributed worker pool for heavy workload...")
        print("⚡ Heavy computation completed successfully.")

    @task(task_id="standard_batch_worker")
    def process_standard():
        print("🌱 Processing on standard single-node container...")
        print("🌱 Standard computation completed successfully.")

    # Join task with TriggerRule to proceed regardless of which branch executed
    finalize_report = EmptyOperator(
        task_id="finalize_report",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    volume = evaluate_traffic_volume()
    branch_decision = choose_processing_tier(volume)

    p_high = process_high_capacity()
    p_std = process_standard()

    branch_decision >> [p_high, p_std] >> finalize_report


branching_pipeline()
