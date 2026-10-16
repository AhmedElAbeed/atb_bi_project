# Apache Airflow — What It Does in the ATB BI Project

## What is Airflow?

**Apache Airflow** is a workflow orchestrator. It does not move data by itself — it **schedules and runs your scripts in the right order**, retries failures, logs everything, and shows progress in a web UI.

Think of it as a **project manager for your data pipeline**:

| Without Airflow | With Airflow |
|-----------------|--------------|
| You run scripts manually in order | One click (or cron) runs the full chain |
| If step 3 fails, you may not notice | Task turns red, email/log alert, retry |
| No history of past runs | Full audit trail in the UI |

## What Airflow Does *in This Project*

Your BI platform has four layers. Airflow connects them:

```
CSV / Core Banking
       ↓
  [1] Airbyte → PFE_ODS        (raw landing zone)
       ↓
  [2] dbt → PFE_DWH            (dimensions + facts)
       ↓
  [3] Quality checks           (NULL FKs, row counts)
       ↓
  [4] ML training (weekly)     (XGBoost risk model)
       ↓
  Streamlit / Superset         (reporting — not orchestrated by Airflow)
```

### Main DAG: `atb_master_pipeline`

| Stage | Tasks | What happens |
|-------|-------|--------------|
| **1 — Ingestion** | `airbyte_sync`, `validate_ods` | Sync raw tables to `PFE_ODS` (skipped if Airbyte off), then verify row counts |
| **2 — Transform** | `dbt_debug` → `dbt_run_*` → `dbt_test` → `validate_dwh` | Build staging → intermediate → warehouse in SQL Server |
| **3 — Quality** | `stage_3_quality_report` | Check `fact_customer_risk`, `fact_account`, dimensions |
| **4 — ML** | `stage_4_ml_train` (Sundays only) | Retrain XGBoost from `4_ml/src/run_training.py` |

### Supporting DAGs (manual trigger)

- `dag_ingestion` — Airbyte sync only
- `dag_transformation` — dbt only
- `dag_ml_pipeline` — ML training only

## How Airflow Works (Mechanics)

1. **DAG** = Directed Acyclic Graph = a recipe with steps and arrows (dependencies).
2. **Task** = one unit of work (Python function, bash command, etc.).
3. **Scheduler** = wakes up, sees "02:00 UTC — run `atb_master_pipeline`", creates a **DagRun**.
4. **Executor** = runs each task (here: **LocalExecutor** inside Docker).
5. **Webserver** = UI at http://localhost:8080 to monitor, trigger, and debug.

```
Scheduler → picks due DAGs
    ↓
Creates DagRun (one pipeline execution)
    ↓
Runs tasks respecting dependencies (A >> B means B waits for A)
    ↓
Stores logs in ./logs and metadata in PostgreSQL
```

## Quick Start

```bat
cd 2_orchestration
run_airflow.bat
```

Or:

```bash
docker compose up --build -d
```

- **URL:** http://localhost:8080  
- **Login:** `admin` / `admin`  
- **Run now:** DAGs → `atb_master_pipeline` → ▶ Trigger DAG  

### Force ML training on any day

Set in `docker-compose.yml`:

```yaml
ATB_FORCE_ML_TRAIN: "true"
```

### Enable real Airbyte syncs

```yaml
AIRBYTE_SKIP: "false"
AIRBYTE_CONN_CUSTOMER: "<your-connection-uuid>"
# ... other connection IDs
```

## Common Errors (Fixed in This Setup)

| Error | Cause | Fix applied |
|-------|-------|-------------|
| DWH validation always fails | Wrong table names (`FAIT_ACCOUNT`, `DIM_BRANCH`) | Uses real names: `fact_account`, `dim_dao`, etc. |
| dbt staging finds 0 models | `tag:staging` not defined in dbt | Uses `path:models/staging` |
| ML training path wrong in Docker | Only `ml_src` mounted | Full repo mounted at `/opt/airflow/atb_project` |
| Duplicate mock DAG | `atb_pipeline_simple` fakes everything | Ignored via `.airflowignore` |
| Broken Slack DAG | Missing provider package | `atb_bi_warehouse_etl.py` stays ignored |

## Files Map

```
2_orchestration/
├── docker-compose.yml      # Postgres + Airflow services
├── Dockerfile              # Airflow image + ODBC Driver 17
├── dags/
│   ├── atb_master_pipeline.py   ← MAIN production DAG
│   ├── dag_ingestion.py
│   ├── dag_transformation.py
│   ├── dag_ml_pipeline.py
│   └── utils/
│       ├── sql_server_utils.py  # ODS/DWH validation
│       └── pipeline_config.py   # Shared env paths
└── run_airflow.bat
```
