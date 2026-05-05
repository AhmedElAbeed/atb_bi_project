"""
Master DAG for full end-to-end pipeline
Orchestrates: ingestion -> transformation -> ML prediction
Includes quality gates to prevent downstream failures
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
import logging

from utils.sql_server_utils import SQLServerConnection, EXPECTED_ROW_COUNTS

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    "owner": "ATB BI Team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["admin@atb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # Master DAG doesn't retry - child DAGs handle that
    "catchup": False,
}

# DAG definition
dag = DAG(
    "dag_full_pipeline",
    default_args=default_args,
    description="Master DAG: orchestrates full ETL pipeline with quality gates",
    schedule_interval="0 2 * * *",  # Daily at 2 AM (adjust as needed)
    catchup=False,
    tags=["ATB", "master", "orchestration"],
    max_active_runs=1,  # Only one pipeline run at a time
)


def check_data_quality_gates(**context) -> bool:
    """
    Quality gate to ensure ODS data meets minimum expectations before transformation
    Returns True if data is good, False to short-circuit downstream tasks
    """
    logger.info("Running data quality gates...")
    
    db = SQLServerConnection()
    all_valid = True
    
    try:
        for table_name, bounds in EXPECTED_ROW_COUNTS["PFE_ODS"].items():
            if not db.validate_table_exists("PFE_ODS", table_name):
                logger.error(f"✗ Table {table_name} does not exist")
                all_valid = False
                continue
            
            count = db.get_row_count("PFE_ODS", table_name)
            
            if bounds["min"] <= count <= bounds["max"]:
                logger.info(f"✓ {table_name}: {count} rows (within bounds)")
            else:
                logger.error(
                    f"✗ {table_name}: {count} rows "
                    f"(expected {bounds['min']}-{bounds['max']})"
                )
                all_valid = False
        
        if all_valid:
            logger.info("✓ All data quality gates passed - proceeding to transformation")
            return True
        else:
            logger.error("✗ Data quality gates failed - stopping pipeline")
            return False
            
    except Exception as e:
        logger.error(f"Error checking data quality gates: {e}")
        raise


def check_dwh_completeness(**context) -> bool:
    """
    Quality gate to ensure DWH transformation completed successfully
    Returns True if DWH is populated, False to short-circuit ML pipeline
    """
    logger.info("Checking DWH completeness...")
    
    db = SQLServerConnection()
    required_tables = [
        "DIM_CUSTOMER", "DIM_BRANCH", "DIM_DATE", "DIM_SECTOR",
        "DIM_INDUSTRY", "DIM_CURRENCY", "DIM_TARGET", "DIM_RISK_PROFILE",
        "FAIT_ACCOUNT", "FAIT_CUSTOMER_RISK"
    ]
    
    all_populated = True
    
    try:
        for table_name in required_tables:
            if not db.validate_table_exists("PFE_DWH", table_name):
                logger.error(f"✗ Table {table_name} does not exist")
                all_populated = False
                continue
            
            count = db.get_row_count("PFE_DWH", table_name)
            if count > 0:
                logger.info(f"✓ {table_name}: {count} rows")
            else:
                logger.error(f"✗ {table_name}: empty")
                all_populated = False
        
        if all_populated:
            logger.info("✓ All DWH tables populated - proceeding to ML pipeline")
            return True
        else:
            logger.error("✗ DWH tables not fully populated - stopping ML pipeline")
            return False
            
    except Exception as e:
        logger.error(f"Error checking DWH completeness: {e}")
        raise


def log_pipeline_start(**context) -> None:
    """Log pipeline start information"""
    execution_date = context["execution_date"]
    logger.info("\n" + "="*80)
    logger.info(f"STARTING FULL ATB BI PIPELINE - Execution Date: {execution_date}")
    logger.info("="*80)
    logger.info("Pipeline Stages:")
    logger.info("  1. Ingestion (Airbyte) -> PFE_ODS")
    logger.info("  2. Transformation (dbt) -> PFE_DWH")
    logger.info("  3. ML Pipeline -> PFE_ML.ML_CUSTOMER_PREDICTIONS")
    logger.info("="*80 + "\n")


def log_pipeline_completion(**context) -> None:
    """Log pipeline completion information"""
    execution_date = context["execution_date"]
    logger.info("\n" + "="*80)
    logger.info(f"FULL ATB BI PIPELINE COMPLETED - Execution Date: {execution_date}")
    logger.info("="*80)
    logger.info("All stages completed successfully!")
    logger.info("Deliverables:")
    logger.info("  ✓ PFE_ODS - Operational Data Store (raw data)")
    logger.info("  ✓ PFE_DWH - Data Warehouse (dimensional schema)")
    logger.info("  ✓ PFE_ML.ML_CUSTOMER_PREDICTIONS - ML predictions")
    logger.info("="*80 + "\n")


# Pipeline start notification
start_task = PythonOperator(
    task_id="pipeline_start",
    python_callable=log_pipeline_start,
    dag=dag,
)

# Ingestion stage
with TaskGroup("ingestion_stage", tooltip="Trigger Airbyte ingestion DAG", dag=dag) as ingestion_stage:
    
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_dag_ingestion",
        trigger_dag_id="dag_ingestion",
        wait_for_completion=True,
        deferrable=True,
        dag=dag,
    )

# Data quality gate after ingestion
quality_gate_1 = ShortCircuitOperator(
    task_id="quality_gate_ods",
    python_callable=check_data_quality_gates,
    dag=dag,
)

# Transformation stage
with TaskGroup("transformation_stage", tooltip="Trigger dbt transformation DAG", dag=dag) as transformation_stage:
    
    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_dag_transformation",
        trigger_dag_id="dag_transformation",
        wait_for_completion=True,
        deferrable=True,
        dag=dag,
    )

# DWH completeness gate
quality_gate_2 = ShortCircuitOperator(
    task_id="quality_gate_dwh",
    python_callable=check_dwh_completeness,
    dag=dag,
)

# ML pipeline stage
with TaskGroup("ml_stage", tooltip="Trigger ML pipeline DAG", dag=dag) as ml_stage:
    
    trigger_ml = TriggerDagRunOperator(
        task_id="trigger_dag_ml_pipeline",
        trigger_dag_id="dag_ml_pipeline",
        wait_for_completion=True,
        deferrable=True,
        dag=dag,
    )

# Pipeline completion notification
completion_task = PythonOperator(
    task_id="pipeline_completion",
    python_callable=log_pipeline_completion,
    dag=dag,
)

# Task dependencies
start_task >> ingestion_stage >> quality_gate_1 >> transformation_stage >> quality_gate_2 >> ml_stage >> completion_task

if __name__ == "__main__":
    dag.cli()
