"""
DAG for transformation layer - runs dbt models, tests, and generates documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging
import sys
import os

# Add dags folder to path to allow importing from utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.sql_server_utils import validate_dwh_tables

logger = logging.getLogger(__name__)

# dbt project path (mounted in container)
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_TARGET = "prod"

# Default DAG arguments
default_args = {
    "owner": "ATB BI Team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["admin@atb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "dag_transformation",
    default_args=default_args,
    description="Transformation DAG: run dbt models and tests to build PFE_DWH",
    schedule_interval=None,
    catchup=False,
    tags=["ATB", "transformation", "dbt"],
)


def validate_transformation_results(**context) -> bool:
    """Validate that DWH tables are properly populated"""
    logger.info("Validating DWH tables...")
    success = validate_dwh_tables()
    if not success:
        raise Exception("DWH validation failed - some tables are empty or missing")
    logger.info("✓ All DWH tables validated successfully")
    return True


from utils.pipeline_config import DBT_ENV_PREFIX as DBT_ENV

# Build task group
with TaskGroup("dbt_build", tooltip="dbt build: run, test, snapshot", dag=dag) as dbt_group:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_ENV} dbt debug --profiles-dir . --target prod",
        dag=dag,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_ENV} dbt deps --profiles-dir . --target prod",
        dag=dag,
    )

    dbt_debug >> dbt_deps

# Staging layer tasks
with TaskGroup("dbt_staging", tooltip="Run staging models (clean ODS data)", dag=dag) as staging_group:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_ENV} dbt run --select path:models/staging "
            "--profiles-dir . --target prod --threads 4"
        ),
        execution_timeout=timedelta(hours=2),
        dag=dag,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"{DBT_ENV} dbt test --select path:models/staging "
            "--profiles-dir . --target prod"
        ),
        dag=dag,
    )

    dbt_run_staging >> dbt_test_staging


# Intermediate layer tasks
with TaskGroup("dbt_intermediate", tooltip="Run intermediate models (business logic)", dag=dag) as intermediate_group:

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"{DBT_ENV} dbt run --select path:models/intermediate "
            "--profiles-dir . --target prod --threads 4"
        ),
        execution_timeout=timedelta(hours=1),
        dag=dag,
    )

    dbt_test_intermediate = BashOperator(
        task_id="dbt_test_intermediate",
        bash_command=(
            f"{DBT_ENV} dbt test --select path:models/intermediate "
            "--profiles-dir . --target prod --fail-fast"
        ),
        dag=dag,
    )

    dbt_run_intermediate >> dbt_test_intermediate


# Warehouse layer tasks (dimensions and facts)
with TaskGroup("dbt_warehouse", tooltip="Run warehouse models (dimensional schema)", dag=dag) as warehouse_group:

    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=(
            f"{DBT_ENV} dbt run --select tag:dimension "
            "--profiles-dir . --target prod --threads 4"
        ),
        execution_timeout=timedelta(hours=2),
        dag=dag,
    )

    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"{DBT_ENV} dbt run --select tag:fact "
            "--profiles-dir . --target prod --threads 4"
        ),
        execution_timeout=timedelta(hours=2),
        dag=dag,
    )

    dbt_test_warehouse = BashOperator(
        task_id="dbt_test_warehouse",
        bash_command=(
            f"{DBT_ENV} dbt test --select tag:dimension tag:fact "
            "--profiles-dir . --target prod --fail-fast"
        ),
        dag=dag,
    )

    dbt_run_dimensions >> dbt_run_facts >> dbt_test_warehouse


# Final validation and documentation tasks
validate_task = PythonOperator(
    task_id="validate_dwh_tables",
    python_callable=validate_transformation_results,
    dag=dag,
)

dbt_generate_docs = BashOperator(
    task_id="dbt_generate_docs",
    bash_command=f"{DBT_ENV} dbt docs generate --profiles-dir . --target prod",
    dag=dag,
)

# Better TaskGroup-level dependencies
dbt_group >> staging_group >> intermediate_group >> warehouse_group >> validate_task >> dbt_generate_docs
