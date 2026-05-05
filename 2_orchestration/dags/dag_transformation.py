"""
DAG for transformation layer - runs dbt models, tests, and generates documentation
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging
import os

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
    schedule_interval=None,  # Triggered by full pipeline DAG
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


# dbt commands with environment setup
DBT_ENV = (
    f"cd {DBT_PROJECT_DIR} && "
    f"export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && "
    f"export DBT_TARGET={DBT_TARGET}"
)

# Build task group
with TaskGroup("dbt_build", tooltip="dbt build: run, test, snapshot", dag=dag) as dbt_group:
    
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_ENV} && dbt debug --target ${DBT_TARGET}",
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_ENV} && dbt deps --target ${DBT_TARGET}",
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_ENV} && dbt seed --target ${DBT_TARGET}",
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )

# Staging layer tasks
with TaskGroup("dbt_staging", tooltip="Run staging models (clean ODS data)", dag=dag) as staging_group:
    
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_ENV} && dbt run --select tag:staging "
            "--profiles-dir . --target ${DBT_TARGET} --vars 'environment: production'"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"{DBT_ENV} && dbt test --select tag:staging "
            "--profiles-dir . --target ${DBT_TARGET} --fail-fast"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_run_staging >> dbt_test_staging


# Intermediate layer tasks
with TaskGroup("dbt_intermediate", tooltip="Run intermediate models (business logic)", dag=dag) as intermediate_group:
    
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"{DBT_ENV} && dbt run --select tag:intermediate "
            "--profiles-dir . --target ${DBT_TARGET} --vars 'environment: production'"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_test_intermediate = BashOperator(
        task_id="dbt_test_intermediate",
        bash_command=(
            f"{DBT_ENV} && dbt test --select tag:intermediate "
            "--profiles-dir . --target ${DBT_TARGET} --fail-fast"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_run_intermediate >> dbt_test_intermediate


# Warehouse layer tasks (dimensions and facts)
with TaskGroup("dbt_warehouse", tooltip="Run warehouse models (dimensional schema)", dag=dag) as warehouse_group:
    
    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=(
            f"{DBT_ENV} && dbt run --select tag:dimension "
            "--profiles-dir . --target ${DBT_TARGET} --vars 'environment: production'"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"{DBT_ENV} && dbt run --select tag:fact "
            "--profiles-dir . --target ${DBT_TARGET} --vars 'environment: production'"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
        dag=dag,
    )
    
    dbt_test_warehouse = BashOperator(
        task_id="dbt_test_warehouse",
        bash_command=(
            f"{DBT_ENV} && dbt test --select tag:warehouse "
            "--profiles-dir . --target ${DBT_TARGET} --fail-fast"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
            "DBT_TARGET": DBT_TARGET,
        },
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
    bash_command=f"{DBT_ENV} && dbt docs generate --profiles-dir . --target ${DBT_TARGET}",
    env={
        "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        "DBT_TARGET": DBT_TARGET,
    },
    dag=dag,
)

# Task dependencies
dbt_debug >> dbt_deps >> dbt_seed
dbt_seed >> [staging_group, intermediate_group, warehouse_group]
warehouse_group >> validate_task >> dbt_generate_docs

if __name__ == "__main__":
    dag.cli()
