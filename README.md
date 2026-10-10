# 🌪️ Apache Airflow Starter & Tutorial Suite

> **Comprehensive guide, architecture breakdown, ready-to-run sample DAGs, self-contained ETL examples, host CLI tools (`./airflow`, `airflowctl`, `astro`), and full Docker deployment for Apache Airflow.**
>
> 📖 **Full Deployment Guide:** See [**`DEPLOYMENT.md`**](file:///home/pkshrestha/git/pk-learns-to-airflow/DEPLOYMENT.md) for production hardening, worker scaling, and disaster recovery procedures.
> 🐞 **Neovim Live Debugging:** See [**`NEOVIM-SETUP.md`**](file:///home/pkshrestha/git/pk-learns-to-airflow/NEOVIM-SETUP.md) for step-by-step instructions on attaching Neovim (`nvim-dap`) to DAG tasks running inside Docker.

---

## 📑 Table of Contents
- [1. Quickstart with Docker Compose (Recommended)](#1-quickstart-with-docker-compose-recommended)
- [2. CLI Tools & Remote Management (`./airflow`, `airflowctl`, `astro`)](#2-cli-tools--remote-management-airflow-airflowctl-astro)
- [3. What is Apache Airflow?](#3-what-is-apache-airflow)
- [4. Core Concepts & Architecture](#4-core-concepts--architecture)
- [5. Web UI Features & Native Dark Mode](#5-web-ui-features--native-dark-mode)
- [6. Standalone ETL Examples (`examples/`)](#6-standalone-etl-examples-examples)
- [7. Sample DAGs in this Repository (`dags/`)](#7-sample-dags-in-this-repository-dags)
- [8. Running Airflow Locally with `uv`](#8-running-airflow-locally-with-uv)
- [9. Testing DAGs via CLI](#9-testing-dags-via-cli)
- [10. Airflow Best Practices](#10-airflow-best-practices)
- [11. Neovim Python Debugger Integration (`NEOVIM-SETUP.md`)](#neovim-python-debugger-integration)

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

# Stop the cluster
./stop.sh        # or: make down or docker compose down

# Stop and wipe database volumes (clean reset)
make clean       # or: docker compose down -v
```

---

## 2. CLI Tools & Remote Management (`./airflow`, `airflowctl`, `astro`)

Instead of attaching into Docker containers manually (`docker compose exec airflow-webserver bash`), you have multiple streamlined ways to interact with Airflow directly from your host terminal:

### Option A: Native Host Wrapper (`./airflow`) — *Fastest for this repository*
An executable wrapper located at [`./airflow`](file:///Users/pkshrestha/git/airflow/airflow) that transparently routes any Airflow CLI command into the running webserver container:

```bash
# List all registered DAGs
./airflow dags list

# Trigger a DAG run
./airflow dags trigger 01_taskflow_etl

# Unpause / pause a DAG
./airflow dags unpause 08_excel_to_sqlite_pipeline
./airflow dags pause 08_excel_to_sqlite_pipeline

# Test a single task instance directly
./airflow tasks test 01_taskflow_etl extract_orders 2026-09-18

# Inspect database connectivity & version
./airflow db check
./airflow version
```

### Option B: `airflowctl` (Official Airflow CLI Client)
`airflowctl` is the official client utility installed on your system (`/usr/local/bin/airflowctl`) designed to interact with local and remote Airflow environments via REST API and configuration profiles:

```bash
# Verify installation
airflowctl --version

# View available commands
airflowctl --help
```

### Option C: Astronomer CLI (`astro`) — *The Industry Standard for Airflow Dev*
The **Astronomer CLI** (`astro` `v1.46.0` installed at `/usr/local/bin/astro`) is widely considered the best developer experience tool for Airflow:

```bash
# Verify installation
astro version
```

#### Why use `astro`?
| Feature | Custom Docker Compose | Astronomer CLI (`astro`) |
| :--- | :--- | :--- |
| **Project Bootstrapping** | Manual `docker-compose.yaml` + configs | `astro dev init` generates clean structure |
| **Startup / Teardown** | `docker compose up -d` / `down` | `astro dev start` / `astro dev stop` |
| **Hot Reloading** | Mounted volumes | Automatic DAG & dependency live sync |
| **DAG Parsing / Testing** | Manual CLI testing in container | `astro dev parse` & `astro dev pytest` |
| **Cloud Deployment** | Custom CI/CD scripts | `astro deploy` (deploys directly to Astronomer Cloud) |

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

## 5. Web UI Features & Native Dark Mode

Airflow **2.10.2** provides a revamped UI experience:

* 🌙 **Native Dark Mode:** Toggle between Light, Dark, and System theme by clicking the Moon/Sun icon in the upper-right corner of the navigation bar at [http://localhost:8080](http://localhost:8080).
* 📊 **Grid View:** High-level run history matrix with drill-down task logs, details, and rendered templates.
* 📈 **Graph View:** Real-time visual dependency graph showing task statuses (queued, running, success, failed, upstream_failed).
* ⚙️ **Admin Controls:** Manage Connections, Variables, Pools, and XComs directly from the UI.

---

## 6. Standalone ETL Examples (`examples/`)

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

## 7. Sample DAGs in this Repository (`dags/`)

All sample pipelines are located in [`dags/`](file:///Users/pkshrestha/git/airflow/dags):

1. [**`01_taskflow_etl.py`**](file:///Users/pkshrestha/git/airflow/dags/01_taskflow_etl.py): Modern TaskFlow API (`@dag`, `@task`, automatic XComs).
2. [**`02_classic_operators.py`**](file:///Users/pkshrestha/git/airflow/dags/02_classic_operators.py): Classic Operators (`BashOperator`, `PythonOperator`, `EmptyOperator`, Jinja templating).
3. [**`03_conditional_branching.py`**](file:///Users/pkshrestha/git/airflow/dags/03_conditional_branching.py): Dynamic Branching (`@task.branch`, TriggerRules).
4. [**`04_data_quality_sensor.py`**](file:///Users/pkshrestha/git/airflow/dags/04_data_quality_sensor.py): Sensors & schema quality validation.
5. [**`08_excel_to_sqlite_dag.py`**](file:///home/pkshrestha/git/pk-learns-to-airflow/dags/08_excel_to_sqlite_dag.py): Multi-sheet Excel extraction, data cleaning, and SQLite loading with quality assertions.
   > 💡 **Note on 1M Dataset DAG:** `08_excel_to_sqlite_pipeline` is currently configured with `schedule=None` and paused by default (`is_paused_upon_creation=True`). This allows you to explore lighter sample DAGs first. To run it, unpause and trigger it via `./airflow dags unpause 08_excel_to_sqlite_pipeline` or through the Web UI.
6. [**`09_parquet_staging_etl.py`**](file:///home/pkshrestha/git/pk-learns-to-airflow/dags/09_parquet_staging_etl.py): High-performance intermediate DAG processing using Parquet staging, column projection, and loading into PostgreSQL warehouse tables (`parquet_clean_orders`, `parquet_category_summary`).
   > 📖 **Full Engineering Spec:** See [**`PARQUET_POSTGRES_SPEC.md`**](file:///home/pkshrestha/git/pk-learns-to-airflow/PARQUET_POSTGRES_SPEC.md) for data contracts, architecture diagrams, and schema definitions.

### 🚀 Triggering the Parquet ➔ PostgreSQL Pipeline
```bash
# 1. Unpause and trigger the DAG
./airflow dags unpause 09_parquet_staging_etl
./airflow dags trigger 09_parquet_staging_etl

# 2. Monitor execution status
./airflow dags list-runs -d 09_parquet_staging_etl --state all

# 3. Query the loaded PostgreSQL warehouse tables directly
docker compose exec postgres psql -U airflow -d airflow -c "
SELECT category, total_orders, total_net_rev, avg_order_value 
FROM parquet_category_summary 
ORDER BY total_net_rev DESC;
"
```

---

## 8. Running Airflow Locally with `uv`

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

## 9. Testing DAGs via CLI (Without Web UI)

You can instantly test and debug your DAGs from the command line without waiting for the scheduler or starting a webserver:

```bash
cd /Users/pkshrestha/git/airflow

# 1. Verify DAG parsing (checks for syntax/import errors)
./airflow dags list

# 2. Test run an entire DAG end-to-end
./airflow dags test 01_taskflow_etl

# 3. Test dynamic branching DAG
./airflow dags test 03_conditional_branching

# 4. Test a specific single task inside a DAG
./airflow tasks test 01_taskflow_etl extract_orders 2026-09-18
```

---

## 10. Airflow Best Practices

1. **Idempotency:** Designing tasks so that running them multiple times with the same execution date produces the exact same result (critical for retries).
2. **Use the TaskFlow API for Python:** Prefer `@task` and `@dag` over classic `PythonOperator` for cleaner syntax and automatic XCom serialization.
3. **Keep Tasks Atomic:** A task should do one discrete job (e.g. Extract, Transform, or Load). If a task fails midway, only that task needs to retry.
4. **Avoid Heavy Computations in Top-Level Code:** The Airflow scheduler continuously parses DAG files every few seconds. Heavy DB queries or API calls outside of `@task` functions will slow down the scheduler.
5. **Use Sensors with `mode="reschedule"`:** For long-waiting sensors, use `reschedule` mode to release worker slots while waiting.
6. **Pass References, Not Giant Payloads, in XCom:** Avoid returning megabytes or gigabytes of raw data (e.g., 1M row dictionaries) directly through XComs. Instead, write data to staging stores (S3, GCS, SQLite, or Parquet) and pass the path/URI via XCom.

---

## 11. Neovim Python Debugger Integration

You can attach Neovim (`nvim-dap`) to DAG tasks running inside the Docker worker container:

```bash
# 1. Open DAG file in Neovim and toggle a breakpoint on any line
nvim dags/09_parquet_staging_etl.py
# Press <leader>db to set breakpoint

# 2. Trigger task with debug listener enabled
docker compose exec -e AIRFLOW_DEBUG=true airflow-worker airflow tasks test 09_parquet_staging_etl transform_and_enrich_parquet 2026-10-09

# 3. In Neovim, press <leader>dc and choose "Airflow: Attach to Docker (Port 5678)"
# Stepping: <leader>dO (Step Over), <leader>di (Step Into), <leader>du (Toggle UI)
```

📖 **Full Step-by-Step Walkthrough:** See [**`NEOVIM-SETUP.md`**](file:///home/pkshrestha/git/pk-learns-to-airflow/NEOVIM-SETUP.md).
