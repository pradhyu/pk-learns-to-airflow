# 🌪️ Apache Airflow Engineering Specification (`AIRFLOW-SPEC.md`)

> **Comprehensive Architecture, Real-World Use Cases, Design Patterns, and Implementation Reference for Apache Airflow 2.x.**

---

## 📑 Table of Contents

- [1. Executive Summary & Design Philosophy](#1-executive-summary--design-philosophy)
- [2. Core Architecture & Component Lifecycle](#2-core-architecture--component-lifecycle)
- [3. Industry Use Cases & When to Use Airflow](#3-industry-use-cases--when-to-use-airflow)
- [4. Architectural Patterns & Implementation Standards](#4-architectural-patterns--implementation-standards)
  - [Pattern A: Modern TaskFlow API (Recommended)](#pattern-a-modern-taskflow-api-recommended)
  - [Pattern B: Dynamic Task Mapping (`.expand()`)](#pattern-b-dynamic-task-mapping-expand)
  - [Pattern C: Data-Aware Scheduling (Airflow Datasets)](#pattern-c-data-aware-scheduling-airflow-datasets)
  - [Pattern D: Machine Learning / MLOps Orchestration](#pattern-d-machine-learning--mlops-orchestration)
  - [Pattern E: API Ingestion & Warehouse Sync (ELT)](#pattern-e-api-ingestion--warehouse-sync-elt)
  - [Pattern F: Deferrable Sensors & Async Polling](#pattern-f-deferrable-sensors--async-polling)
  - [Pattern G: Failure Callbacks & Slack/PagerDuty Alerts](#pattern-g-failure-callbacks--slackpagerduty-alerts)
- [5. Production Quality & Golden Rules](#5-production-quality--golden-rules)
- [6. Local Development, Testing & Verification Matrix](#6-local-development-testing--verification-matrix)

---

## 1. Executive Summary & Design Philosophy

Apache Airflow is a **workflow orchestrator**, not an execution engine:
* **Airflow's Job:** Coordinate **when**, **in what order**, and **under what conditions** tasks execute, managing retries, SLAs, alerts, and backfilling.
* **The Worker's Job:** Heavy computations should be offloaded to specialized compute layers (Snowflake, Spark, BigQuery, AWS EMR, Kubernetes, Docker, DuckDB) rather than running massive computations in the Airflow scheduler/webserver process.

### The 4 Pillars of Airflow 2.x:
1. **Dynamic Python Code:** Pipelines are defined as code; you can dynamically instantiate tasks from configs, database tables, or APIs.
2. **TaskFlow & Type-Safety:** Native `@dag` and `@task` decorators eliminate boilerplate and handle XCom data passing seamlessly.
3. **Data-Awareness (Datasets):** Trigger pipelines immediately upon data arrival rather than guessing arbitrary cron intervals.
4. **Scale & Elasticity:** From single-process `standalone` for local dev to `CeleryExecutor` and `KubernetesExecutor` in production.

---

## 2. Core Architecture & Component Lifecycle

```
                                    ┌────────────────────────┐
                                    │     Airflow Web UI     │ (Port 8080: Monitoring, Logs, Manual Triggers)
                                    └───────────┬────────────┘
                                                │
┌─────────────────────────┐   DAG Parsing       ▼      Reads & Updates State  ┌─────────────────────────┐
│ DAGs Directory          │ ──────────────► ┌───────────────┐ ◄──────────────┤ Metadata Database       │
│ (~/git/airflow/dags)    │ (Every 30s)     │   Scheduler   │                │ (PostgreSQL / SQLite)   │
└─────────────────────────┘                 └───────┬───────┘                └─────────────────────────┘
                                                    │ Dispatches task instances
                                                    ▼
                                            ┌───────────────┐
                                            │   Executor    │ (Local / Celery / Kubernetes)
                                            └───────┬───────┘
                                                    │
                      ┌─────────────────────────────┼─────────────────────────────┐
                      ▼                             ▼                             ▼
              ┌───────────────┐             ┌───────────────┐             ┌───────────────┐
              │ Worker Task 1 │             │ Worker Task 2 │             │ Worker Task 3 │
              │ (Extract API) │             │ (Transform)   │             │ (Warehouse)   │
              └───────────────┘             └───────────────┘             └───────────────┘
```

---

## 3. Industry Use Cases & When to Use Airflow

| Use Case | Description | Typical Tech Stack |
| :--- | :--- | :--- |
| **1. Batch ETL / ELT Pipelines** | Ingest data from relational DBs, SaaS APIs, and event streams; load into data lake/warehouse. | Postgres, S3, Snowflake, BigQuery, dbt |
| **2. MLOps & AI Model Pipelines** | Feature extraction, training jobs, model evaluation, and automated batch inference. | PyTorch, Scikit-learn, MLflow, S3 |
| **3. Event & Sensor Ingestion** | Wait for external batch drops (S3 files, partner SFTP drops) before processing. | `S3KeySensor`, `FileSensor`, Kafka |
| **4. Multi-System API Sync** | Synchronize CRM, billing, and marketing data across third-party tools. | Stripe, Salesforce, HubSpot, Slack |
| **5. Infrastructure & DB Maintenance** | Automated nightly backups, table vacuums, index rebuilds, and disk cleanups. | Bash, Docker, PostgreSQL, Kubernetes |

---

## 4. Architectural Patterns & Implementation Standards

---

### Pattern A: Modern TaskFlow API (Recommended)

The **TaskFlow API** (`@dag`, `@task`) is the standard for Airflow 2.0+. It eliminates manual `xcom_push` and `xcom_pull` boilerplate.

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="taskflow_etl_standard",
    schedule="0 4 * * *",  # Daily at 04:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def taskflow_etl():
    @task
    def extract() -> list[dict]:
        return [{"id": 1, "price": 100}, {"id": 2, "price": 250}]

    @task
    def transform(items: list[dict]) -> dict:
        total = sum(x["price"] for x in items)
        return {"count": len(items), "total_revenue": total}

    @task
    def load(metrics: dict):
        print(f"Loaded metrics: {metrics}")

    raw_items = extract()
    summary = transform(raw_items)
    load(summary)

taskflow_etl()
```

---

### Pattern B: Dynamic Task Mapping (`.expand()`)

Introduced in Airflow 2.3+, **Dynamic Task Mapping** allows a single task definition to spawn $N$ parallel worker tasks at runtime based on the output of an upstream task (Map-Reduce pattern).

```python
from datetime import datetime
from airflow.decorators import dag, task

@dag(dag_id="dynamic_task_mapping_demo", start_date=datetime(2026, 1, 1), schedule=None)
def dynamic_mapping_dag():
    @task
    def get_file_partitions() -> list[str]:
        # Returns a dynamic list of files to process
        return ["partition_2026_01.csv", "partition_2026_02.csv", "partition_2026_03.csv"]

    @task
    def process_partition(filename: str) -> dict:
        # Runs in parallel for each partition!
        print(f"Processing partition: {filename}")
        return {"file": filename, "status": "COMPLETED"}

    @task
    def aggregate_results(results: list[dict]):
        print(f"Aggregated {len(results)} partitions successfully.")

    files = get_file_partitions()
    # .expand() dynamically creates 3 parallel task instances
    processed = process_partition.expand(filename=files)
    aggregate_results(processed)

dynamic_mapping_dag()
```

---

### Pattern C: Data-Aware Scheduling (Airflow Datasets)

Instead of scheduling DAGs on a fixed cron clock (e.g. `0 2 * * *`), DAGs can declare that they **produce** or **consume** specific **Datasets**. Downstream DAGs trigger automatically when all upstream datasets are updated.

```python
from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Define dataset URIs
RAW_SALES_DATA = Dataset("s3://company-datalake/raw/sales_data.parquet")

# --- PRODUCER DAG ---
@dag(dag_id="dataset_producer_dag", schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=False)
def producer():
    @task(outlets=[RAW_SALES_DATA])  # Marks dataset as updated upon completion
    def ingest_sales():
        print("Ingesting sales data and writing to S3...")

    ingest_sales()

# --- CONSUMER DAG (Triggers automatically when RAW_SALES_DATA is updated!) ---
@dag(dag_id="dataset_consumer_dag", schedule=[RAW_SALES_DATA], start_date=datetime(2026, 1, 1), catchup=False)
def consumer():
    @task
    def transform_analytics():
        print("Triggered immediately because RAW_SALES_DATA was updated!")

    transform_analytics()

producer()
consumer()
```

---

### Pattern D: Machine Learning / MLOps Orchestration

Pipelines that ingest raw data, train a model, evaluate metrics against a threshold, and conditionally register/deploy the model.

```python
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

@dag(dag_id="ml_training_pipeline", start_date=datetime(2026, 1, 1), schedule="@weekly", catchup=False)
def ml_pipeline():
    @task
    def train_model() -> dict:
        # Train model and calculate accuracy
        accuracy = 0.942
        return {"model_id": "MOD-2026-09", "accuracy": accuracy}

    @task.branch
    def evaluate_model(metrics: dict) -> str:
        if metrics["accuracy"] >= 0.90:
            print(f"Accuracy {metrics['accuracy']} >= 0.90. Deploying model...")
            return "deploy_to_production"
        else:
            print(f"Accuracy {metrics['accuracy']} < 0.90. Model rejected.")
            return "send_rejection_alert"

    @task(task_id="deploy_to_production")
    def deploy():
        print("🚀 Model promoted to Production Model Registry.")

    @task(task_id="send_rejection_alert")
    def alert():
        print("⚠️ Model accuracy below threshold. Training aborted.")

    join = EmptyOperator(task_id="pipeline_complete", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    metrics = train_model()
    eval_branch = evaluate_model(metrics)
    eval_branch >> [deploy(), alert()] >> join

ml_pipeline()
```

---

### Pattern E: API Ingestion & Warehouse Sync (ELT)

Fetches paginated data from third-party APIs (Stripe, GitHub, Salesforce), saves to local/S3 staging, and loads into database.

```python
from datetime import datetime
from airflow.decorators import dag, task
import json, requests

@dag(dag_id="api_warehouse_sync", start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
def api_sync():
    @task
    def fetch_api_page(endpoint: str) -> list[dict]:
        # Fetch data with retry capability
        print(f"Fetching {endpoint}...")
        return [{"id": "evt_01", "amount": 99.00}, {"id": "evt_02", "amount": 150.00}]

    @task
    def stage_to_parquet(records: list[dict]) -> str:
        staging_path = "/tmp/staging_events.json"
        with open(staging_path, "w") as f:
            json.dump(records, f)
        return staging_path

    @task
    def load_warehouse(staging_path: str):
        print(f"COPY INTO warehouse.events FROM '{staging_path}'")

    data = fetch_api_page("https://api.example.com/v1/events")
    staged = stage_to_parquet(data)
    load_warehouse(staged)

api_sync()
```

---

### Pattern F: Deferrable Sensors & Async Polling

Standard sensors tie up worker CPU slots while waiting. **Deferrable Sensors** release their worker thread to the `Triggerer` daemon and wake up only when the condition is met.

```python
from datetime import datetime
from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor

@dag(dag_id="sensor_pipeline_demo", start_date=datetime(2026, 1, 1), schedule="@hourly", catchup=False)
def sensor_dag():
    # mode="reschedule" or deferrable=True frees worker slots while polling
    wait_for_file = FileSensor(
        task_id="wait_for_partner_data",
        filepath="/Users/pkshrestha/git/airflow/data/incoming_sales.json",
        poke_interval=30,      # Check every 30 seconds
        timeout=60 * 60,       # 1 hour timeout
        mode="reschedule",     # Releases worker thread between pokes!
    )

sensor_dag()
```

---

### Pattern G: Failure Callbacks & Slack/PagerDuty Alerts

Define universal failure callbacks to automatically capture error logs and send structured alerts.

```python
def on_task_failure_alert(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    log_url = context['task_instance'].log_url
    error = context.get('exception')
    print(f"🚨 CRITICAL ALERT: DAG '{dag_id}' Task '{task_id}' failed!")
    print(f"Logs: {log_url}")
    print(f"Error: {error}")
    # In production: Send webhook payload to Slack / PagerDuty / Microsoft Teams

default_args = {
    "owner": "data_engineering",
    "on_failure_callback": on_task_failure_alert,
}
```

---

## 5. Production Quality & Golden Rules

1. ⚡ **Top-Level Code Performance:** The Airflow Scheduler parses every DAG file in `dags/` every 30 seconds. **Never** run heavy database queries, HTTP requests, or imports of heavy packages (`torch`, `pandas`) at the root level of a DAG file. Keep imports inside `@task` functions.
2. 🔄 **Idempotency:** A DAG run on date `2026-09-18` must produce the exact same outcome whether executed once or retried 5 times.
3. 📦 **XCom Size Limit:** Default XComs are stored in the Airflow metadata database (Postgres/SQLite) and have a size limit (usually ~48KB - 1GB depending on DB). **Never pass large DataFrames or raw files through XComs**. Instead, write the file to S3/GCS/Disk and pass the **file path URI string** via XCom.
4. ⏱️ **Set Realistic Timeouts & Retries:** Every task should define `retries = 2`, `retry_delay = timedelta(minutes=1)`, and `execution_timeout = timedelta(hours=1)` to prevent rogue tasks from hanging forever.

---

## 6. Local Development, Testing & Verification Matrix

### CLI Commands for Testing DAGs

```bash
cd /Users/pkshrestha/git/airflow
export AIRFLOW_HOME=/Users/pkshrestha/git/airflow

# 1. Syntax & Import Validation (Parses all DAGs without executing)
airflow dags list

# 2. Test execution of a full DAG locally (Simulates scheduler & worker)
airflow dags test 01_taskflow_etl

# 3. Test execution of a single task
airflow tasks test 01_taskflow_etl extract_orders 2026-09-18

# 4. Show task dependency graph in ASCII
airflow dags show 01_taskflow_etl --tree
```
