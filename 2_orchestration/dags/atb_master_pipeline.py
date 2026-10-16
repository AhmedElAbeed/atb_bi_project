"""
ATB BI — Master Production Pipeline
====================================

Single daily DAG that orchestrates the full data platform:

  1. Ingestion   → Airbyte sync (optional) + ODS validation
  2. Transform   → dbt staging → intermediate → warehouse
  3. Quality     → SQL checks on facts/dimensions
  4. ML          → XGBoost retraining (optional, weekly)

Schedule: daily 02:00 UTC
UI: http://localhost:8080  (admin / admin)
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_quality_checks import DataQualityChecker
from utils.airbyte_client import AirbyteAPIClient
from utils.pipeline_config import (
    AIRBYTE_SKIP,
    AIRBYTE_URL,
    DBT_ENV_PREFIX,
    DEFAULT_DAG_ARGS,
    ML_ENV_PREFIX,
    ML_SRC_DIR,
    SQL_SERVER_DATABASE,
    SQL_SERVER_HOST,
    SQL_SERVER_PASSWORD,
    SQL_SERVER_PORT,
    SQL_SERVER_USER,
)
from utils.sql_server_utils import validate_dwh_tables, validate_ods_tables

logger = logging.getLogger(__name__)

AIRBYTE_CONNECTIONS = {
    "account": os.getenv("AIRBYTE_CONN_ACCOUNT", ""),
    "currency": os.getenv("AIRBYTE_CONN_CURRENCY", ""),
    "customer": os.getenv("AIRBYTE_CONN_CUSTOMER", ""),
    "dao": os.getenv("AIRBYTE_CONN_DAO", ""),
    "industry": os.getenv("AIRBYTE_CONN_INDUSTRY", ""),
    "sector": os.getenv("AIRBYTE_CONN_SECTOR", ""),
    "target": os.getenv("AIRBYTE_CONN_TARGET", ""),
}

default_args = {
    **DEFAULT_DAG_ARGS,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="atb_master_pipeline",
    default_args=default_args,
    description="ATB BI master pipeline: ingestion → dbt → quality → ML",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ATB", "production", "master"],
    doc_md=__doc__,
)


def run_airbyte_syncs(**context) -> str:
    """Trigger Airbyte syncs when configured; otherwise skip gracefully."""
    if AIRBYTE_SKIP:
        logger.info("AIRBYTE_SKIP=true — skipping sync, validating existing ODS data")
        return "skipped"

    client = AirbyteAPIClient(base_url=AIRBYTE_URL)
    active = {k: v for k, v in AIRBYTE_CONNECTIONS.items() if v}

    if not active:
        logger.warning("No Airbyte connection IDs configured — skipping sync")
        return "no_connections"

    for table, conn_id in active.items():
        logger.info("Syncing %s (%s)", table, conn_id)
        job_id = client.trigger_sync(conn_id)
        client.wait_for_sync(job_id)
        logger.info("✓ %s sync complete (job %s)", table, job_id)

    return "success"


def validate_ods_step(**context) -> None:
    if not validate_ods_tables():
        raise RuntimeError("ODS validation failed — check PFE_ODS tables and row counts")


def validate_dwh_step(**context) -> None:
    if not validate_dwh_tables():
        raise RuntimeError("DWH validation failed — run dbt or check PFE_DWH tables")


def run_quality_report(**context) -> None:
    checker = DataQualityChecker(
        server=f"{SQL_SERVER_HOST},{SQL_SERVER_PORT}",
        database=SQL_SERVER_DATABASE,
        user=SQL_SERVER_USER,
        password=SQL_SERVER_PASSWORD,
    )
    report = checker.run_full_validation()

    risk = report["fact_customer_risk"]
    account = report["fact_account"]
    report["overall_status"] = (
        "PASS"
        if risk["status"] == "PASS" and account["status"] == "PASS"
        else "WARN"
    )

    logger.info("Data quality report:\n%s", json.dumps(report, indent=2, default=str))
    context["ti"].xcom_push(key="quality_report", value=report)

    if report["overall_status"] != "PASS":
        logger.warning("Quality checks returned WARN — review fact table FK nulls")


def should_run_ml(**context) -> bool:
    """Train ML model on Sundays (or when ATB_FORCE_ML_TRAIN=true)."""
    if os.getenv("ATB_FORCE_ML_TRAIN", "").lower() in ("1", "true", "yes"):
        return True
    return datetime.utcnow().weekday() == 6


# ── Stage 1: Ingestion ──────────────────────────────────────────────────────
with TaskGroup("stage_1_ingestion", tooltip="Airbyte sync + ODS checks", dag=dag) as ingestion:
    airbyte_sync = PythonOperator(
        task_id="airbyte_sync",
        python_callable=run_airbyte_syncs,
        dag=dag,
    )
    validate_ods = PythonOperator(
        task_id="validate_ods",
        python_callable=validate_ods_step,
        dag=dag,
    )
    airbyte_sync >> validate_ods

# ── Stage 2: dbt transformation ─────────────────────────────────────────────
with TaskGroup("stage_2_transformation", tooltip="dbt build PFE_DWH", dag=dag) as transformation:
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_ENV_PREFIX} dbt debug --profiles-dir . --target prod",
        dag=dag,
    )
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_ENV_PREFIX} dbt deps --profiles-dir . --target prod",
        dag=dag,
    )
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_ENV_PREFIX} dbt run --select path:models/staging "
            "--profiles-dir . --target prod"
        ),
        dag=dag,
    )
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"{DBT_ENV_PREFIX} dbt run --select path:models/intermediate "
            "--profiles-dir . --target prod"
        ),
        dag=dag,
    )
    dbt_run_warehouse = BashOperator(
        task_id="dbt_run_warehouse",
        bash_command=(
            f"{DBT_ENV_PREFIX} dbt run --select path:models/warehouse "
            "--profiles-dir . --target prod"
        ),
        dag=dag,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_ENV_PREFIX} dbt test --select path:models "
            "--profiles-dir . --target prod --fail-fast"
        ),
        dag=dag,
    )
    validate_dwh = PythonOperator(
        task_id="validate_dwh",
        python_callable=validate_dwh_step,
        dag=dag,
    )

    dbt_debug >> dbt_deps >> dbt_run_staging >> dbt_run_intermediate
    dbt_run_intermediate >> dbt_run_warehouse >> dbt_test >> validate_dwh

# ── Stage 3: Data quality ───────────────────────────────────────────────────
quality_report = PythonOperator(
    task_id="stage_3_quality_report",
    python_callable=run_quality_report,
    dag=dag,
)

# ── Stage 4: ML (weekly) ────────────────────────────────────────────────────
ml_train = BashOperator(
    task_id="stage_4_ml_train",
    bash_command=(
        f"{ML_ENV_PREFIX} "
        f"cd {ML_SRC_DIR} && "
        "python run_training.py"
    ),
    dag=dag,
)

check_ml_schedule = ShortCircuitOperator(
    task_id="check_ml_schedule",
    python_callable=should_run_ml,
    dag=dag,
)

ingestion >> transformation >> quality_report >> check_ml_schedule >> ml_train
