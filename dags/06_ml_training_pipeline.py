"""
06_ml_training_pipeline.py
==========================
Demonstrates an end-to-end Machine Learning Lifecycle:
1. Feature Extraction & Dataset Splitting
2. Model Training & Hyperparameter Evaluation
3. Performance Gate (Accuracy Threshold)
4. Conditional Promotion to Model Registry or Alerting
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="06_ml_training_pipeline",
    description="MLOps: Automated training, evaluation, and conditional production deployment",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tutorial", "mlops", "ai", "training"],
    default_args={
        "owner": "ml-engineering",
        "retries": 1,
    },
)
def ml_training_pipeline():
    @task
    def prepare_features() -> dict:
        """Simulate feature extraction from warehouse."""
        sample_count = 50000
        print(f"Prepared {sample_count} training samples with 48 features.")
        return {"sample_count": sample_count, "feature_version": "v2.4"}

    @task
    def train_model(feature_meta: dict) -> dict:
        """Simulate training a gradient boosted / neural model."""
        model_id = f"churn-predictor-{datetime.utcnow().strftime('%Y%m%d%H%M')}"
        # Simulate accuracy score (between 0.85 and 0.98)
        accuracy = round(random.uniform(0.88, 0.97), 4)
        loss = round(1.0 - accuracy, 4)
        print(f"Model [{model_id}] trained. Accuracy={accuracy}, Loss={loss}")
        return {
            "model_id": model_id,
            "accuracy": accuracy,
            "loss": loss,
            "feature_version": feature_meta["feature_version"],
        }

    @task.branch
    def evaluate_model_quality(metrics: dict) -> str:
        """Evaluate if model meets production quality bar (>= 92% accuracy)."""
        threshold = 0.92
        if metrics["accuracy"] >= threshold:
            print(f"✅ Accuracy ({metrics['accuracy']}) meets threshold ({threshold}). Deploying.")
            return "promote_model_to_registry"
        else:
            print(f"❌ Accuracy ({metrics['accuracy']}) below threshold ({threshold}). Alerting.")
            return "notify_model_rejection"

    @task(task_id="promote_model_to_registry")
    def promote_model(metrics: dict):
        print(f"🚀 Promoting model [{metrics['model_id']}] to Production Model Registry.")
        print(f"🏷️ Tagged as 'production-latest' (Accuracy: {metrics['accuracy'] * 100:.2f}%)")

    @task(task_id="notify_model_rejection")
    def notify_rejection(metrics: dict):
        print(f"⚠️ Model [{metrics['model_id']}] rejected. Accuracy was only {metrics['accuracy'] * 100:.2f}%.")

    pipeline_completed = EmptyOperator(
        task_id="pipeline_completed",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    features = prepare_features()
    model_metrics = train_model(features)
    decision = evaluate_model_quality(model_metrics)

    promoted = promote_model(model_metrics)
    rejected = notify_rejection(model_metrics)

    decision >> [promoted, rejected] >> pipeline_completed


ml_training_pipeline()
