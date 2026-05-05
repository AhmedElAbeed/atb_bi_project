"""
DAG for ingestion layer - triggers Airbyte syncs and validates PFE_ODS
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import logging

from utils.airbyte_client import AirbyteAPIClient
from utils.sql_server_utils import validate_ods_tables, SQLServerConnection

logger = logging.getLogger(__name__)

# Configuration
AIRBYTE_BASE_URL = "http://host.docker.internal:8006"

# Map CSV files to Airbyte connection IDs
# Connection IDs provided by the user
AIRBYTE_CONNECTIONS = {
    "account": "91205bfd-c0a8-4c76-9813-f616beb21da0",
    "currency": "d6e83091-3f92-4ea5-8ec3-04d60c179f82",
    "customer": "887e6d51-fccf-4547-a97d-970417b57613",
    "dao": "eab9fdb8-3a23-424a-85e0-b6719dab8ce3",
    "industry": "7a3bcd9d-d56f-4ab1-97b1-9da7039c8d59",
    "sector": "461f5b6b-4111-48d6-ae71-34b6e7aaee5a",
    "target": "cfd9fae4-c248-4043-8464-a644d3c9bb77",
}

# Default DAG arguments
default_args = {
    "owner": "ATB BI Team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["admin@atb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "dag_ingestion",
    default_args=default_args,
    description="Ingestion DAG: trigger Airbyte syncs to PFE_ODS",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["ATB", "ingestion", "airbyte"],
)


def trigger_airbyte_sync(connection_id: str, **context) -> str:
    """Trigger an Airbyte sync and return job ID"""
    client = AirbyteAPIClient(base_url=AIRBYTE_BASE_URL)
    
    if connection_id not in AIRBYTE_CONNECTIONS.values():
        logger.warning(f"Connection {connection_id} not recognized, checking if placeholder...")
    
    job_id = client.trigger_sync(connection_id)
    context["task_instance"].xcom_push(key="job_id", value=job_id)
    return job_id


def wait_for_airbyte_sync(connection_key: str, max_wait_minutes: int = 60, **context) -> bool:
    """Wait for Airbyte sync to complete"""
    connection_id = AIRBYTE_CONNECTIONS[connection_key]
    client = AirbyteAPIClient(base_url=AIRBYTE_BASE_URL)
    
    # Get job ID from the previous task
    previous_task_id = f"trigger_sync_{connection_key}"
    job_id = context["task_instance"].xcom_pull(
        task_ids=previous_task_id,
        key="job_id"
    )
    
    if not job_id:
        logger.error(f"No job ID found for {connection_key}")
        raise ValueError(f"No job ID found for {connection_key}")
    
    logger.info(f"Waiting for sync job {job_id} (connection: {connection_key})")
    return client.wait_for_sync(job_id, max_wait_minutes=max_wait_minutes)


def validate_ingestion_results(**context) -> bool:
    """Validate that all ODS tables are properly populated"""
    logger.info("Validating ODS tables...")
    success = validate_ods_tables()
    
    if not success:
        raise Exception("ODS validation failed - some tables have unexpected row counts")
    
    logger.info("✓ All ODS tables validated successfully")
    return True


def get_ods_statistics(**context) -> None:
    """Get and log statistics for all ODS tables"""
    db = SQLServerConnection()
    
    logger.info("\n" + "="*80)
    logger.info("ODS TABLE STATISTICS")
    logger.info("="*80)
    
    tables = ["ACCOUNT", "CUSTOMER", "CURRENCY", "DAO", "INDUSTRY", "SECTOR", "TARGET"]
    
    for table_name in tables:
        try:
            stats = db.get_table_stats("PFE_ODS", table_name)
            logger.info(
                f"{table_name:15} | Rows: {stats['row_count']:>10,} | "
                f"Size: {stats['size_mb']:>8.2f} MB | Last Modified: {stats['last_modified']}"
            )
        except Exception as e:
            logger.error(f"Error getting stats for {table_name}: {e}")
    
    logger.info("="*80 + "\n")


# Build sync task pairs for each connection
sync_tasks = []

with TaskGroup("sync_all_sources", tooltip="Trigger and monitor all 7 Airbyte syncs", dag=dag) as sync_group:
    for connection_key, connection_id in AIRBYTE_CONNECTIONS.items():
        
        trigger_task = PythonOperator(
            task_id=f"trigger_sync_{connection_key}",
            python_callable=trigger_airbyte_sync,
            op_kwargs={"connection_id": connection_id},
            dag=dag,
        )
        
        wait_task = PythonOperator(
            task_id=f"wait_sync_{connection_key}",
            python_callable=wait_for_airbyte_sync,
            op_kwargs={"connection_key": connection_key, "max_wait_minutes": 60},
            dag=dag,
        )
        
        trigger_task >> wait_task
        sync_tasks.append(wait_task)


# Validation and reporting tasks
validate_task = PythonOperator(
    task_id="validate_ods_tables",
    python_callable=validate_ingestion_results,
    dag=dag,
)

stats_task = PythonOperator(
    task_id="log_ods_statistics",
    python_callable=get_ods_statistics,
    dag=dag,
)

# Task dependencies
sync_group >> validate_task >> stats_task

if __name__ == "__main__":
    dag.cli()
