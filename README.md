# 🌪️ Apache Airflow Starter & Tutorial Suite

> **Comprehensive guide, architecture breakdown, ready-to-run sample DAGs, self-contained ETL examples, and full Docker deployment for Apache Airflow.**
>
> 📖 **Full Deployment Guide:** See [**`DEPLOYMENT.md`**](file:///Users/pkshrestha/git/airflow/DEPLOYMENT.md) for production hardening, worker scaling, and disaster recovery procedures.

---

## 📑 Table of Contents
- [1. Quickstart with Docker Compose (Recommended)](#1-quickstart-with-docker-compose-recommended)
- [2. Detailed Deployment Guide (`DEPLOYMENT.md`)](file:///Users/pkshrestha/git/airflow/DEPLOYMENT.md)
- [3. What is Apache Airflow?](#3-what-is-apache-airflow)
- [4. Core Concepts & Architecture](#4-core-concepts--architecture)
- [5. Standalone ETL Examples (`examples/`)](#5-standalone-etl-examples-examples)
- [6. Sample DAGs in this Repository (`dags/`)](#6-sample-dags-in-this-repository-dags)
- [7. Running Airflow Locally with `uv`](#7-running-airflow-locally-with-uv)
- [8. Testing DAGs via CLI](#8-testing-dags-via-cli)
- [9. Airflow Best Practices](#9-airflow-best-practices)

---

## 1. Quickstart with Docker Compose (Recommended)

Run the entire Apache Airflow system (**Web UI, Scheduler, Celery Worker, Triggerer, Redis, and Postgres**) with a single command:

### 🚀 Start the Cluster
```bash
# Using the helper script
./start.sh

# Or using Make
make up

# Or directly with Docker Compose
docker compose up --build -d
```

### 🌐 Access Web UI
- **URL:** [http://localhost:8080](http://localhost:8080)
- **Username:** `admin`
- **Password:** `admin`

### 🛠️ Common Operations
```bash
# View live logs across all services
docker compose logs -f

# View logs for a specific service (worker, scheduler, webserver)
docker compose logs -f airflow-worker
docker compose logs -f airflow-scheduler

# Check service status
docker compose ps

# Run Airflow CLI commands inside the container
docker compose run --rm airflow-cli airflow dags list
docker compose run --rm airflow-cli airflow dags test 01_taskflow_etl

# Stop the cluster
./stop.sh        # or: make down or docker compose down

# Stop and wipe database volumes (clean reset)
make clean       # or: docker compose down -v
```

---

## 3. What is Apache Airflow?

**Apache Airflow** is an open-source platform used to author, schedule, and monitor workflows programmatically as **Directed Acyclic Graphs (DAGs)**.

### Why use Airflow?
* **Code as Configuration:** Workflows are standard Python code—enabling version control, unit testing, dynamic generation, and modular design.
* **Resilient Scheduling:** Built-in automatic retries, backfilling, SLA alerts, and dependency graphs.
* **Rich Web UI:** Real-time visibility into task runtimes, logs, Gantt charts, dependency trees, and manual trigger/retry controls.
* **Extensible Ecosystem:** Pre-built connectors for AWS (S3, Redshift), GCP (BigQuery, GCS), Snowflake, Postgres, Spark, Kubernetes, Slack, and Docker.

---

## 4. Core Concepts & Architecture

```
                      ┌─────────────────────────┐
                      │    Airflow Web UI       │ (Inspect logs, trigger DAGs, monitor)
                      └───────────┬─────────────┘
                                  │
┌──────────────────┐  Reads DAGs  ▼  Writes State ┌─────────────────────────┐
│ DAGs Directory   │ ──────────► ┌───────────────┐ ◄──────────┤ Metadata Database       │
│ (~/git/airflow)  │             │   Scheduler   │            │ (SQLite / Postgres)     │
└──────────────────┘             └───────┬───────┘            └─────────────────────────┘
                                         │ Schedules tasks
                                         ▼
                                 ┌───────────────┐
                                 │   Executor    │ (Sequential / Local / Celery / K8s)
                                 └───────┬───────┘
                                         │ Executes worker
                                         ▼
                                 ┌───────────────┐
                                 │ Worker Tasks  │
                                 └───────────────┘
```

| Component | Role |
| :--- | :--- |
| **DAG (Directed Acyclic Graph)** | The pipeline definition. A collection of tasks with directional execution dependencies. |
| **Task / Operator** | A single unit of work (e.g., `PythonOperator`, `BashOperator`, `@task`). |
| **Sensor** | A special task that polls until an external event occurs (e.g., file arrival, database row exists). |
| **XCom (Cross-Communication)** | Lightweight message passing mechanism between tasks in a DAG. |
| **Scheduler** | Daemon that evaluates DAG schedules and triggers task instances when upstream dependencies are met. |
| **Webserver** | Flask-based UI at `http://localhost:8080` for monitoring and managing runs. |

---

## 5. Standalone ETL Examples (`examples/`)

### 📊 Excel to SQLite Pipeline: [`examples/excel_to_sqlite/`](file:///Users/pkshrestha/git/airflow/examples/excel_to_sqlite)
A full production-pattern pipeline that extracts multi-sheet Excel files, performs cleanups, currency/date parsing, and data calculations, loads to SQLite with upsert support, and verifies integrity.

```bash
# One-click execution with auto-detected uv / venv
./examples/excel_to_sqlite/run_pipeline.sh
```

| Script | Purpose |
| :--- | :--- |
| [`01_generate_sample_excel.py`](file:///Users/pkshrestha/git/airflow/examples/excel_to_sqlite/01_generate_sample_excel.py) | Generates messy multi-tab Excel workbook (`raw_sales_data.xlsx`). |
| [`02_excel_to_sqlite_pipeline.py`](file:///Users/pkshrestha/git/airflow/examples/excel_to_sqlite/02_excel_to_sqlite_pipeline.py) | Standalone ETL & Data Quality verification suite. |
| [`03_airflow_dag_excel_to_sqlite.py`](file:///Users/pkshrestha/git/airflow/examples/excel_to_sqlite/03_airflow_dag_excel_to_sqlite.py) | Orchestrated Airflow DAG version with TaskFlow API. |

---

## 6. Sample DAGs in this Repository (`dags/`)

All sample pipelines are located in [`dags/`](file:///Users/pkshrestha/git/airflow/dags):

1. [**`01_taskflow_etl.py`**](file:///Users/pkshrestha/git/airflow/dags/01_taskflow_etl.py): Modern TaskFlow API (`@dag`, `@task`, automatic XComs).
2. [**`02_classic_operators.py`**](file:///Users/pkshrestha/git/airflow/dags/02_classic_operators.py): Classic Operators (`BashOperator`, `PythonOperator`, `EmptyOperator`, Jinja templating).
3. [**`03_conditional_branching.py`**](file:///Users/pkshrestha/git/airflow/dags/03_conditional_branching.py): Dynamic Branching (`@task.branch`, TriggerRules).
4. [**`04_data_quality_sensor.py`**](file:///Users/pkshrestha/git/airflow/dags/04_data_quality_sensor.py): Sensors & schema quality validation.
5. [**`08_excel_to_sqlite_dag.py`**](file:///Users/pkshrestha/git/airflow/dags/08_excel_to_sqlite_dag.py): Multi-sheet Excel extraction, data cleaning, and SQLite loading with quality assertions.

---

## 7. Running Airflow Locally with `uv`

[`uv`](https://github.com/astral-sh/uv) provides instant virtualenv creation and package management if running without Docker.

### Option A: Direct Run with `uv run` (No manual activation required)
```bash
cd ~/git/airflow

# Run generator and ETL pipeline
uv run python examples/excel_to_sqlite/01_generate_sample_excel.py
uv run python examples/excel_to_sqlite/02_excel_to_sqlite_pipeline.py
```

### Option B: Local Standalone Airflow
```bash
# 1. Install dependencies
uv sync # or: uv pip install -r requirements.txt

# 2. Start standalone Airflow
export AIRFLOW_HOME=$(pwd)
airflow standalone
```

---

## 8. Testing DAGs via CLI (Without Web UI)

You can instantly test and debug your DAGs from the command line without waiting for the scheduler or starting a webserver:

```bash
cd /Users/pkshrestha/git/airflow
export AIRFLOW_HOME=/Users/pkshrestha/git/airflow

# 1. Verify DAG parsing (checks for syntax/import errors)
airflow dags list

# 2. Test run an entire DAG end-to-end
airflow dags test 01_taskflow_etl

# 3. Test dynamic branching DAG
airflow dags test 03_conditional_branching

# 4. Test a specific single task inside a DAG
airflow tasks test 01_taskflow_etl extract_orders 2026-09-18
```

---

## 9. Airflow Best Practices

1. **Idempotency:** Designing tasks so that running them multiple times with the same execution date produces the exact same result (critical for retries).
2. **Use the TaskFlow API for Python:** Prefer `@task` and `@dag` over classic `PythonOperator` for cleaner syntax and automatic XCom serialization.
3. **Keep Tasks Atomic:** A task should do one discrete job (e.g. Extract, Transform, or Load). If a task fails midway, only that task needs to retry.
4. **Avoid Heavy Computations in Top-Level Code:** The Airflow scheduler continuously parses DAG files every few seconds. Heavy DB queries or API calls outside of `@task` functions will slow down the scheduler.
5. **Use Sensors with `mode="reschedule"`:** For long-waiting sensors, use `reschedule` mode to release worker slots while waiting.
