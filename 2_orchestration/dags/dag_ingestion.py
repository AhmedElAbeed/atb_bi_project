"""
DAG for ingestion layer - Airbyte Sync and ODS Validation
"""
from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add dags folder to path to allow importing from utils
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import AirbyteAPIClient, SQLServerConnection, validate_ods_tables

logger = logging.getLogger(__name__)

# Airbyte configuration
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID", "your-connection-id-here")
AIRBYTE_URL = os.getenv("AIRBYTE_URL", "http://host.docker.internal:8006")

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
    "dag_ingestion",
    default_args=default_args,
    description="Ingestion DAG: Sync Airbyte connections and validate ODS",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["ATB", "ingestion", "airbyte"],
)

def run_airbyte_sync(**context):
    """Trigger Airbyte sync and wait for completion"""
    client = AirbyteAPIClient(base_url=AIRBYTE_URL)
    
    if AIRBYTE_CONNECTION_ID == "your-connection-id-here":
        logger.warning("AIRBYTE_CONNECTION_ID not set, running in mock mode")
        return "mock_success"
        
    try:
        job_id = client.trigger_sync(AIRBYTE_CONNECTION_ID)
        client.wait_for_sync(job_id)
        return f"job_{job_id}_success"
    except Exception as e:
        logger.error(f"Airbyte sync failed: {e}")
        raise

def validate_ingestion_results(**context):
    """Validate ODS tables using SQL Server utilities"""
    success = validate_ods_tables()
    if not success:
        logger.warning("ODS validation completed with warnings")
    return success

# Tasks
sync_task = PythonOperator(
    task_id="airbyte_sync",
    python_callable=run_airbyte_sync,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_ods_tables",
    python_callable=validate_ingestion_results,
    dag=dag,
)

# Dependencies
sync_task >> validate_task

if __name__ == "__main__":
    dag.cli()
