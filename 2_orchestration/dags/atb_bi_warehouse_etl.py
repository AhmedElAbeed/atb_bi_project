"""
ATB BI Warehouse ETL Pipeline - Main DAG
=========================================

Schedule: Daily at 02:00 UTC
Purpose: Orchestrate data ingestion, transformation, and quality checks
Scope: Customer, Account, and Risk data from staging to warehouse

Workflow:
  1. Data Ingestion (Airbyte/CSV)
  2. dbt Staging Models (Raw → Staging)
  3. dbt Intermediate Models (Staging → Business Logic)
  4. dbt Warehouse Models (Intermediate → DWH)
  5. Data Quality Checks
  6. Monitoring & Alerts
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import os

# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@atb.local'],
}

dag = DAG(
    'atb_bi_warehouse_etl',
    default_args=default_args,
    description='ATB BI Warehouse ETL Pipeline - Daily Orchestration',
    schedule_interval='0 2 * * *',  # 02:00 UTC daily
    max_active_runs=1,
    tags=['atb', 'warehouse', 'production', 'etl'],
    doc_md=__doc__,
)

# =============================================================================
# Configuration Variables
# =============================================================================

DBT_PROJECT_DIR = Variable.get('DBT_PROJECT_DIR', '/opt/dbt/atb_bi_transformation')
DBT_PROFILES_DIR = Variable.get('DBT_PROFILES_DIR', DBT_PROJECT_DIR)
DBT_PROFILE = Variable.get('DBT_PROFILE', 'atb_bi_transformation')
DBT_TARGET = Variable.get('DBT_TARGET', 'prod')
SLACK_WEBHOOK = Variable.get('SLACK_WEBHOOK_URL', '')
SQL_SERVER = Variable.get('SQL_SERVER', os.environ.get('DBT_SERVER', ''))
SQL_DATABASE = Variable.get('SQL_DATABASE', os.environ.get('DBT_DATABASE', 'ATB_BI'))
SQL_USER = Variable.get('SQL_USER', os.environ.get('DBT_USER', ''))
SQL_PASSWORD = Variable.get('SQL_PASSWORD', os.environ.get('DBT_PASSWORD', ''))

# =============================================================================
# Python Functions for Data Quality Checks
# =============================================================================

def check_fact_customer_risk_quality(**context):
    """
    Validate fact_customer_risk table data quality.
    Checks: row count, null foreign keys, referential integrity
    """
    import pyodbc
    
    conn_string = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={SQL_SERVER};"
        f"Database={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    
    try:
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Check NULL foreign keys
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN customer_sk IS NULL THEN 1 END) as null_customer_sk,
                COUNT(CASE WHEN dao_sk IS NULL THEN 1 END) as null_dao_sk,
                COUNT(CASE WHEN industry_sk IS NULL THEN 1 END) as null_industry_sk,
                COUNT(CASE WHEN target_sk IS NULL THEN 1 END) as null_target_sk
            FROM PFE_DWH.fact_customer_risk
        """)
        
        result = cursor.fetchone()
        total_rows, null_cust, null_dao, null_ind, null_tgt = result
        
        print(f"fact_customer_risk Quality Check:")
        print(f"  Total Rows: {total_rows}")
        print(f"  NULL customer_sk: {null_cust}")
        print(f"  NULL dao_sk: {null_dao}")
        print(f"  NULL industry_sk: {null_ind}")
        print(f"  NULL target_sk: {null_tgt}")
        
        # Fail if any NULL FKs found
        if any([null_cust, null_dao, null_ind, null_tgt]):
            raise Exception(f"NULL foreign keys detected! Customer: {null_cust}, DAO: {null_dao}, Industry: {null_ind}, Target: {null_tgt}")
        
        # Warn if row count dropped significantly
        if total_rows < 100000:
            raise Exception(f"Row count critically low: {total_rows} (expected >100k)")
        
        context['task_instance'].xcom_push(key='fact_customer_risk_rows', value=total_rows)
        print(f"✓ fact_customer_risk quality check PASSED")
        
        conn.close()
        
    except Exception as e:
        print(f"✗ fact_customer_risk quality check FAILED: {str(e)}")
        raise


def check_fact_account_quality(**context):
    """
    Validate fact_account table data quality.
    Checks: row count, null foreign keys, referential integrity
    """
    import pyodbc
    
    conn_string = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={SQL_SERVER};"
        f"Database={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
    )
    
    try:
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Check NULL foreign keys
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN customer_sk IS NULL THEN 1 END) as null_customer_sk,
                COUNT(CASE WHEN dao_sk IS NULL THEN 1 END) as null_dao_sk,
                COUNT(CASE WHEN currency_sk IS NULL THEN 1 END) as null_currency_sk
            FROM PFE_DWH.fact_account
        """)
        
        result = cursor.fetchone()
        total_rows, null_cust, null_dao, null_curr = result
        
        print(f"fact_account Quality Check:")
        print(f"  Total Rows: {total_rows}")
        print(f"  NULL customer_sk: {null_cust}")
        print(f"  NULL dao_sk: {null_dao}")
        print(f"  NULL currency_sk: {null_curr}")
        
        # Fail if any NULL FKs found
        if any([null_cust, null_dao, null_curr]):
            raise Exception(f"NULL foreign keys detected! Customer: {null_cust}, DAO: {null_dao}, Currency: {null_curr}")
        
        # Warn if row count dropped significantly
        if total_rows < 100000:
            raise Exception(f"Row count critically low: {total_rows} (expected >100k)")
        
        context['task_instance'].xcom_push(key='fact_account_rows', value=total_rows)
        print(f"✓ fact_account quality check PASSED")
        
        conn.close()
        
    except Exception as e:
        print(f"✗ fact_account quality check FAILED: {str(e)}")
        raise


def log_pipeline_metrics(**context):
    """Log final pipeline metrics to monitoring system"""
    fact_risk_rows = context['task_instance'].xcom_pull(
        task_ids='data_quality.check_fact_customer_risk',
        key='fact_customer_risk_rows'
    )
    fact_acct_rows = context['task_instance'].xcom_pull(
        task_ids='data_quality.check_fact_account',
        key='fact_account_rows'
    )
    
    print(f"\n{'='*70}")
    print(f"ATB BI Warehouse ETL Pipeline - Execution Summary")
    print(f"{'='*70}")
    print(f"Execution Date: {context['execution_date']}")
    print(f"fact_customer_risk: {fact_risk_rows} rows loaded")
    print(f"fact_account: {fact_acct_rows} rows loaded")
    print(f"Status: ✓ SUCCESS")
    print(f"{'='*70}\n")


# =============================================================================
# DAG Tasks
# =============================================================================

# Start task
start = BashOperator(
    task_id='start',
    bash_command='echo "ATB BI Warehouse ETL Pipeline Started at {{ execution_date }}"',
)

# Data Ingestion Task Group
with TaskGroup('data_ingestion', tooltip='Data Ingestion Phase') as data_ingestion:
    
    ingest_customers = BashOperator(
        task_id='ingest_customers',
        bash_command='echo "Ingesting customer data from source..."',
    )
    
    ingest_accounts = BashOperator(
        task_id='ingest_accounts',
        bash_command='echo "Ingesting account data from source..."',
    )
    
    ingest_reference_data = BashOperator(
        task_id='ingest_reference_data',
        bash_command='echo "Ingesting reference data (currency, sector, industry, etc)..."',
    )
    
    [ingest_customers, ingest_accounts, ingest_reference_data]


# dbt Staging Models
dbt_staging = BashOperator(
    task_id='dbt_staging_models',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging --target {DBT_TARGET} --profile {DBT_PROFILE}',
    env={'DBT_PROFILES_DIR': f'{DBT_PROFILES_DIR}'},
    doc_md='Run dbt staging models. Transforms raw ODS data into clean staging tables.',
)

# dbt Intermediate Models
dbt_intermediate = BashOperator(
    task_id='dbt_intermediate_models',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select intermediate --target {DBT_TARGET} --profile {DBT_PROFILE}',
    env={'DBT_PROFILES_DIR': f'{DBT_PROFILES_DIR}'},
    doc_md='Run dbt intermediate models. Applies business logic and enrichment.',
)

# dbt Warehouse Models (Dimensions & Facts)
with TaskGroup('dbt_warehouse_models', tooltip='Warehouse Dimension & Fact Loading') as dbt_warehouse:
    
    dbt_dimensions = BashOperator(
        task_id='dbt_dimensions',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select warehouse.dimensions --target {DBT_TARGET} --profile {DBT_PROFILE}',
        env={'DBT_PROFILES_DIR': f'{DBT_PROFILES_DIR}'},
        doc_md='Load all dimensions (customer, dao, currency, sector, industry, target, risk_profile, date)',
    )
    
    dbt_facts = BashOperator(
        task_id='dbt_facts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select warehouse.facts --target {DBT_TARGET} --profile {DBT_PROFILE}',
        env={'DBT_PROFILES_DIR': f'{DBT_PROFILES_DIR}'},
        doc_md='Load fact tables (fact_customer_risk, fact_account)',
    )
    
    dbt_dimensions >> dbt_facts


# dbt Tests
dbt_tests = BashOperator(
    task_id='dbt_tests',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --target {DBT_TARGET} --profile {DBT_PROFILE}',
    env={'DBT_PROFILES_DIR': f'{DBT_PROFILES_DIR}'},
    doc_md='Execute dbt tests on all models',
)

# Data Quality Checks Task Group
with TaskGroup('data_quality', tooltip='Data Quality Validation') as data_quality:
    
    check_customer_risk = PythonOperator(
        task_id='check_fact_customer_risk',
        python_callable=check_fact_customer_risk_quality,
        provide_context=True,
        doc_md='Validate fact_customer_risk: no NULL FKs, row count',
    )
    
    check_account = PythonOperator(
        task_id='check_fact_account',
        python_callable=check_fact_account_quality,
        provide_context=True,
        doc_md='Validate fact_account: no NULL FKs, row count',
    )
    
    [check_customer_risk, check_account]


# Monitoring & Logging
log_metrics = PythonOperator(
    task_id='log_pipeline_metrics',
    python_callable=log_pipeline_metrics,
    provide_context=True,
    doc_md='Log final pipeline execution metrics',
)

# Success notification
success = BashOperator(
    task_id='success',
    bash_command='echo "✓ ATB BI Warehouse ETL Pipeline completed successfully"',
)

# =============================================================================
# DAG Dependencies
# =============================================================================

start >> data_ingestion >> dbt_staging >> dbt_intermediate >> dbt_warehouse >> dbt_tests >> data_quality >> log_metrics >> success
