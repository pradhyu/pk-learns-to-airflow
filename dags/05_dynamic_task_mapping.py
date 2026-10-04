"""
05_dynamic_task_mapping.py
==========================
Demonstrates Dynamic Task Mapping (.expand()) in Airflow 2.3+.
Spawns N parallel task instances at runtime based on upstream data.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="05_dynamic_task_mapping",
    description="Dynamic Task Mapping: Map-Reduce pattern across dynamic partition batches",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "dynamic_mapping", "map_reduce"],
    default_args={"owner": "data-eng"},
)
def dynamic_mapping_pipeline():
    @task
    def discover_customer_segments() -> list[str]:
        """Discovers active customer segments to process in parallel."""
        segments = ["enterprise", "smb", "consumer", "education", "government"]
        print(f"Discovered {len(segments)} customer segments: {segments}")
        return segments

    @task
    def process_segment(segment_name: str) -> dict:
        """Processes an individual segment. Airflow dynamically spawns 1 task instance per segment!"""
        print(f"⚡ Processing segment: {segment_name}")
        # Simulate processing work
        record_count = len(segment_name) * 150
        revenue = round(len(segment_name) * 1234.50, 2)
        return {
            "segment": segment_name,
            "record_count": record_count,
            "revenue": revenue,
        }

    @task
    def aggregate_all_segments(results: list[dict]):
        """Reduces / aggregates results from all parallel mapped tasks."""
        total_revenue = sum(r["revenue"] for r in results)
        total_records = sum(r["record_count"] for r in results)
        print("=" * 55)
        print("📊 Mapped Aggregation Complete:")
        for r in results:
            print(f" • Segment [{r['segment'].upper()}]: {r['record_count']} records, ${r['revenue']}")
        print(f"💰 Grand Total: {total_records} records, ${total_revenue:.2f}")
        print("=" * 55)

    # Orchestrate with .expand()
    segments = discover_customer_segments()
    segment_summaries = process_segment.expand(segment_name=segments)
    aggregate_all_segments(segment_summaries)


dynamic_mapping_pipeline()
