# 🚀 Apache Airflow Deployment & Operations Guide (`DEPLOYMENT.md`)

This guide provides complete instructions for deploying, operating, and scaling this Apache Airflow system in local development, staging, and production environments using **Docker Compose** and containerized architectures.

---

## 📑 Table of Contents
- [1. Architecture Overview](#1-architecture-overview)
- [2. Prerequisites & Resource Requirements](#2-prerequisites--resource-requirements)
- [3. Quick Deployment (1-Command Start)](#3-quick-deployment-1-command-start)
- [4. Container Services Breakdown](#4-container-services-breakdown)
- [5. Configuration & Environment Variables](#5-configuration--environment-variables)
- [6. Scaling Celery Workers](#6-scaling-celery-workers)
- [7. Operational Runbook](#7-operational-runbook)
- [8. Production Hardening & Best Practices](#8-production-hardening--best-practices)
- [9. Disaster Recovery & Backups](#9-disaster-recovery--backups)
- [10. Troubleshooting FAQ](#10-troubleshooting-faq)

---

## 1. Architecture Overview

This project uses the **CeleryExecutor** distributed architecture for Apache Airflow, enabling horizontal task execution scalability and high reliability:

```
                          ┌──────────────────────────┐
                          │   Airflow Web UI & API   │  (Port 8080)
                          └─────────────┬────────────┘
                                        │
        ┌───────────────────────────────┴──────────────────────────────┐
        │                                                              │
        ▼                                                              ▼
┌──────────────┐          ┌───────────────────────┐          ┌──────────────────┐
│  Scheduler   │          │ Celery Message Broker │          │ Triggerer Daemon │
│  (Daemon)    │          │     (Redis:6379)      │          │ (Async Sensors)  │
└───────┬──────┘          └───────────┬───────────┘          └─────────┬────────┘
        │                             │                                │
        │                             ▼                                │
        │                   ┌───────────────────┐                      │
        │                   │  Celery Worker(s) │                      │
        │                   │ (Executes Tasks)  │                      │
        │                   └─────────┬─────────┘                      │
        │                             │                                │
        └─────────────────────────────┼────────────────────────────────┘
                                      ▼
                        ┌───────────────────────────┐
                        │ PostgreSQL Metadata Store │ (Port 5432)
                        └───────────────────────────┘
```

---

## 2. Prerequisites & Resource Requirements

### Minimum Requirements:
- **Docker Engine:** `v20.10.0+`
- **Docker Compose:** `v2.0.0+` (or `compose-plugin`)
- **Memory (RAM):** Minimum 4 GB allocated to Docker (8 GB recommended)
- **CPU:** 2+ cores
- **Disk:** 10 GB free space for images, logs, and database volume

---

## 3. Quick Deployment (1-Command Start)

### Step 1: Clone or Navigate to Project
```bash
cd /Users/pkshrestha/git/airflow
```

### Step 2: Initialize & Launch
```bash
# Using the quickstart script:
./start.sh

# Or using Make:
make up

# Or directly with Docker Compose:
docker compose up --build -d
```

### Step 3: Access Airflow Web Interface
- **Web UI URL:** [http://localhost:8080](http://localhost:8080)
- **Default Username:** `admin`
- **Default Password:** `admin`

---

## 4. Container Services Breakdown

| Service Name | Container Name | Role | Healthcheck / Ports |
| :--- | :--- | :--- | :--- |
| `postgres` | `airflow-postgres` | Metadata Database (PostgreSQL 15) | `pg_isready` |
| `redis` | `airflow-redis` | Task Queue & Celery Broker | Port `6379`, `redis-cli ping` |
| `airflow-init` | `airflow-init` | Runs `airflow db migrate` and registers the initial admin user | Runs once on launch |
| `airflow-webserver` | `airflow-webserver` | Web UI & REST API | Port `8080`, `http://localhost:8080/health` |
| `airflow-scheduler` | `airflow-scheduler` | Evaluates DAG schedules & dispatches tasks | `http://localhost:8974/health` |
| `airflow-worker` | `airflow-worker` | Executes tasks via Celery worker pool | Celery ping inspection |
| `airflow-triggerer` | `airflow-triggerer` | Handles deferrable operators & async sensors | `airflow jobs check` |
| `airflow-cli` | `airflow-cli` | On-demand utility container for Airflow CLI commands | Debug profile |

---

## 5. Configuration & Environment Variables

All runtime configurations are controlled via [`.env`](file:///Users/pkshrestha/git/airflow/.env). You can customize these before deployment:

```bash
# Custom Image Name and UID
AIRFLOW_IMAGE_NAME=airflow-custom:2.9.3
AIRFLOW_UID=50000

# Metadata Database Credentials
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Admin Credentials (used during init)
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_AIRFLOW_WWW_USER_EMAIL=admin@example.com

# Encryption & Security
AIRFLOW__CORE__FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqJZPo=
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
AIRFLOW__CORE__LOAD_EXAMPLES=false
```

> [!TIP]
> To generate a new Fernet key for production, run:
> ```bash
> python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
> ```

---

## 6. Scaling Celery Workers

To scale up worker capacity for high-throughput task execution, scale the `airflow-worker` service horizontally:

```bash
# Scale to 3 parallel worker containers
docker compose up --scale airflow-worker=3 -d

# Verify all workers are connected
docker compose ps airflow-worker
```

---

## 7. Operational Runbook

### Service Management
```bash
# Check status of all containers
docker compose ps

# View real-time logs across all services
docker compose logs -f

# View logs for a specific service
docker compose logs -f airflow-worker
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# Restart a single service
docker compose restart airflow-worker

# Stop all services gracefully
./stop.sh       # or: make down or docker compose down
```

### Executing CLI Commands
```bash
# List all DAGs
docker compose run --rm airflow-cli airflow dags list

# Test run a specific DAG end-to-end
docker compose run --rm airflow-cli airflow dags test 01_taskflow_etl

# Test run a specific task instance
docker compose run --rm airflow-cli airflow tasks test 01_taskflow_etl extract_orders 2026-10-01

# Interactive bash shell inside the Airflow environment
make cli        # or: docker compose run --rm airflow-cli bash
```

### Adding New Python Packages / Rebuilding Image
1. Add packages to [`requirements.txt`](file:///Users/pkshrestha/git/airflow/requirements.txt).
2. Rebuild and restart the containers:
```bash
docker compose up --build -d
```

---

## 8. Production Hardening & Best Practices

When deploying this project to production (AWS EC2/ECS, GCP GCE/GKE, Azure VM, or on-premise):

1. **Change Default Passwords:**
   - Update `POSTGRES_PASSWORD` and `_AIRFLOW_WWW_USER_PASSWORD` in `.env`.
   - Never commit sensitive `.env` files to public git repositories.

2. **Generate Unique Fernet Key:**
   - Always replace the default `AIRFLOW__CORE__FERNET_KEY` with a freshly generated secret.

3. **Configure HTTPS / Reverse Proxy:**
   - Place NGINX, Traefik, or an AWS ALB / Cloudflare in front of port `8080` with SSL/TLS termination.

4. **Resource Constraints:**
   - Add CPU and memory reservations in `docker-compose.yaml` under `deploy.resources` to prevent out-of-memory container crashes.

5. **Log Retention & Rotation:**
   - Mount persistent shared storage (NFS, AWS EFS, or S3 with remote logging) for `./logs` so logs survive container restarts.

---

## 9. Disaster Recovery & Backups

### Backup Metadata Database
```bash
docker compose exec postgres pg_dump -U airflow airflow > airflow_db_backup_$(date +%Y%m%d_%H%M%S).sql
```

### Restore Metadata Database
```bash
docker compose exec -T postgres psql -U airflow airflow < airflow_db_backup_YYYYMMDD_HHMMSS.sql
```

### Complete Clean Reset (Wipes all DB state)
```bash
make clean
# or
docker compose down -v --remove-orphans
```

---

## 10. Troubleshooting FAQ

### Q: Tasks are stuck in `queued` state
- **Cause:** Worker is down or cannot communicate with Redis/PostgreSQL.
- **Fix:** Check `docker compose logs airflow-worker` and verify Redis is healthy (`docker compose ps redis`).

### Q: DAG changes are not showing in the UI
- **Cause:** Scheduler parses DAGs every ~30 seconds by default.
- **Fix:** Wait 30 seconds or trigger parsing directly via `docker compose exec airflow-scheduler airflow dags reserialize`.

### Q: Port 8080 or 6379 already in use
- **Fix:** Change the port mappings in `docker-compose.yaml` (e.g. `"8081:8080"`).
