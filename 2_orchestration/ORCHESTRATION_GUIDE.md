# Airflow Setup Guide - ATB BI Project

## Overview

This directory contains Apache Airflow orchestration for the ATB BI project. Airflow runs in Docker and orchestrates:

1. **Ingestion DAG** (`dag_ingestion.py`) - Triggers Airbyte syncs to PFE_ODS
2. **Transformation DAG** (`dag_transformation.py`) - Runs dbt to build PFE_DWH
3. **ML Pipeline DAG** (`dag_ml_pipeline.py`) - Trains, evaluates, and deploys models
4. **Full Pipeline DAG** (`dag_full_pipeline.py`) - Master DAG with quality gates

---

## Prerequisites

- Docker Desktop (running)
- Python 3.10+ (local development)
- SQL Server instance accessible at `host.docker.internal:1433`
- Airbyte instance running at `host.docker.internal:8006`
- dbt project in `../3_transformation/`
- ML source code in `../4_ml/src/`

---

## Quick Start (Windows)

### 1. Create Environment File

Create `.env` in `2_orchestration/`:

```bash
# SQL Server Connection
SQL_SERVER_HOST=host.docker.internal
SQL_SERVER_PORT=1433
SQL_SERVER_DATABASE=PFE
SQL_SERVER_USER=sa
SQL_SERVER_PASSWORD=YourPassword123!
SQL_SERVER_DRIVER=ODBC Driver 17 for SQL Server

# Airbyte API
AIRBYTE_BASE_URL=http://host.docker.internal:8006

# Airflow (for custom configs if needed)
AIRFLOW_HOME=/opt/airflow
```

### 2. Build and Start Containers

```powershell
cd 2_orchestration

# Build Airflow image (installs all dependencies)
docker compose build

# Start services (webserver, scheduler, triggerer)
docker compose up -d

# Wait ~30 seconds for initialization
# Then open http://localhost:8080

# Default credentials: admin / admin
```

### 3. Configure Airbyte Connection IDs

Edit `2_orchestration/dags/dag_ingestion.py` and replace placeholder connection IDs:

```python
AIRBYTE_CONNECTIONS = {
    "account": "YOUR_ACCOUNT_CONNECTION_ID",
    "customer": "YOUR_CUSTOMER_CONNECTION_ID",
    # ... etc
}
```

**Where to find connection IDs:**
1. Go to http://localhost:8000 (Airbyte UI)
2. Click "Connections" in left sidebar
3. Each connection has a UUID - copy and paste it

### 4. Verify Setup

When you open http://localhost:8080:

- **DAGs tab**: You should see 4 DAGs
  - `dag_ingestion`
  - `dag_transformation`
  - `dag_ml_pipeline`
  - `dag_full_pipeline`
- **Admin > Connections**: Airflow-to-SQL Server connections (auto-created by default)
- **Logs**: Check for any initialization errors

---

## DAG Details

### dag_ingestion.py

**Purpose**: Trigger and monitor Airbyte syncs for all 7 CSV sources.

**Key Tasks**:
- `sync_all_sources` (task group)
  - `trigger_sync_*` - Calls Airbyte REST API to start sync
  - `wait_sync_*` - Polls job status until complete
- `validate_ods_tables` - Checks row counts match expected ranges
- `log_ods_statistics` - Logs table statistics

**Inputs**: Airbyte connection IDs (configured in code)

**Outputs**:
- Tables in `PFE_ODS` schema:
  - ACCOUNT
  - CUSTOMER
  - CURRENCY
  - DAO
  - INDUSTRY
  - SECTOR
  - TARGET

**Schedule**: Daily at 2 AM (adjustable in `schedule_interval`)

**Critical Configuration**:
```python
AIRBYTE_CONNECTIONS = {
    "account": "YOUR_CONNECTION_ID",  # <- Replace these
    "customer": "YOUR_CONNECTION_ID",
    # ... etc
}
```

---

### dag_transformation.py

**Purpose**: Run dbt models and tests to build the dimensional data warehouse.

**Key Tasks**:
- `dbt_build` (task group)
  - `dbt_debug` - Verify dbt config and SQL Server connection
  - `dbt_deps` - Download dbt packages
  - `dbt_seed` - Load seed data (static dimensions)
- `dbt_staging` (task group)
  - `dbt_run_staging` - Build staging views (clean ODS data)
  - `dbt_test_staging` - Run data quality tests
- `dbt_intermediate` (task group)
  - `dbt_run_intermediate` - Build business logic models
  - `dbt_test_intermediate` - Test intermediate models
- `dbt_warehouse` (task group)
  - `dbt_run_dimensions` - Build dimensional tables
  - `dbt_run_facts` - Build fact tables
  - `dbt_test_warehouse` - Run warehouse tests (CRITICAL)
- `validate_dwh_tables` - Check all DWH tables are populated
- `dbt_generate_docs` - Generate dbt documentation

**Inputs**: PFE_ODS tables (from dag_ingestion)

**Outputs**:
- Warehouse schema `PFE_DWH`:
  - 8 dimensions (DIM_CUSTOMER, DIM_BRANCH, etc.)
  - 2 facts (FAIT_ACCOUNT, FAIT_CUSTOMER_RISK)

**Key Features**:
- **Tag-based execution**: Uses `dbt run --select tag:staging` to run specific groups
- **Fail-fast testing**: If tests fail, pipeline stops (data quality gate)
- **Auto-documentation**: Generates dbt docs after build

**Important Notes**:
- Requires `dbt_project.yml` + `profiles.yml` in `../3_transformation/`
- `profiles.yml` must have SQL Server connection credentials
- Tests in `tests/` directory must pass or DAG fails

---

### dag_ml_pipeline.py

**Purpose**: Feature engineering, model training, evaluation, and prediction.

**Key Tasks**:
- `run_feature_extraction` - Extract features from PFE_DWH
- `model_training` (task group)
  - `run_model_training` - Train Random Forest + XGBoost
  - `evaluate_model_performance` - Compute metrics (Accuracy, F1, AUC-ROC)
- `compute_shap_explainability` - SHAP feature importance
- `write_predictions_to_dwh` - Write predictions to PFE_ML.ML_CUSTOMER_PREDICTIONS
- `log_pipeline_summary` - Log execution summary

**Inputs**: PFE_DWH warehouse tables

**Outputs**:
- Models (pickle files) in `/opt/airflow/ml_outputs/models/`
- Predictions in `PFE_ML.ML_CUSTOMER_PREDICTIONS`
- SHAP plots and data in `/opt/airflow/ml_outputs/`


## Architecture

**Current Status**: **PLACEHOLDER**
- DAG structure is complete
- Tasks call placeholder functions from `4_ml/src/`
- When ML layer is implemented, replace the `run_*` functions

**To Implement**:
1. Create `4_ml/src/features.py` with `extract_features()` function
2. Create `4_ml/src/train.py` with `train_models()` function
3. Create `4_ml/src/evaluate.py` with `evaluate_models()` function
4. Create `4_ml/src/explain.py` with `compute_shap_values()` function
5. Create `4_ml/src/predict.py` with `write_predictions()` function

---

### dag_full_pipeline.py

**Purpose**: Master orchestration DAG with quality gates.

**Task Flow**:
```
pipeline_start
  ↓
ingestion_stage (trigger dag_ingestion)
  ↓
quality_gate_ods (SHORT-CIRCUIT if ODS invalid)
  ↓
transformation_stage (trigger dag_transformation)
  ↓
quality_gate_dwh (SHORT-CIRCUIT if DWH empty)
  ↓
ml_stage (trigger dag_ml_pipeline)
  ↓
pipeline_completion
```

**Key Features**:
- **ShortCircuitOperator**: Stops pipeline if data quality fails
  - `quality_gate_ods`: Ensures ODS row counts are within bounds
  - `quality_gate_dwh`: Ensures all DWH tables are populated
- **TriggerDagRunOperator**: Waits for child DAGs to complete
- **Deferrable tasks**: Uses async mode for efficient resource usage

**Schedule**: Daily at 2 AM

**Run Manually**:
```bash
# Via CLI
airflow dags trigger dag_full_pipeline

# Via Web UI
# DAGs tab → dag_full_pipeline → Trigger DAG
```

---

## Monitoring & Troubleshooting

### View Logs

```powershell
# Follow scheduler logs
docker compose logs -f airflow-scheduler

# Follow webserver logs
docker compose logs -f airflow-webserver

# View logs for specific task
docker exec atb-airflow-webserver cat /opt/airflow/logs/dag_ingestion/task_id/...
```

### Check DAG Status

**Via Web UI** (http://localhost:8080):
- **DAGs page**: Green (success) / Red (failed) indicators
- **Graph View**: Visual task dependency tree
- **Logs View**: Full task execution logs

**Via CLI**:
```powershell
docker exec atb-airflow-webserver airflow dags list
docker exec atb-airflow-webserver airflow dags test dag_full_pipeline 2024-01-01
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Connection refused" to SQL Server | SQL Server not accessible from container | Ensure SQL Server is running; check firewall; use `host.docker.internal` |
| Airbyte API returns 404 | Connection ID is wrong | Update AIRBYTE_CONNECTIONS in dag_ingestion.py with correct IDs |
| dbt test fails | Data quality issues or model bugs | Check dbt logs; fix model SQL; run `dbt debug` |
| "Task timeout" | Ingestion taking too long | Increase `max_wait_minutes` in dag_ingestion.py |
| Container won't start | Port 8080 already in use | Change port: `ports: ["8081:8080"]` in docker-compose.yml |

### Stop & Clean

```powershell
# Stop all containers (preserves data)
docker compose down

# Clean up (remove data volumes)
docker compose down -v

# Remove all Docker artifacts
docker compose remove
```

---

## Advanced Configuration

### Adjust Schedule

In each DAG file, modify `schedule_interval`:

```python
# default_args in dag_ingestion.py
dag = DAG(
  "dag_ingestion",
  schedule_interval="0 2 * * *",  # <- Change this
  # ...
)
```

**Common schedules**:
- `"0 2 * * *"` - Daily at 2 AM
- `"0 */6 * * *"` - Every 6 hours
- `"0 9 * * MON"` - Monday at 9 AM
- `None` - Manual trigger only (for transform/ML DAGs)

### Custom Alerts

Edit email settings in default_args:

```python
default_args = {
  "email": ["your-email@company.com"],
  "email_on_failure": True,
  "email_on_retry": True,
  # ...
}
```

Requires SMTP configuration in Airflow (see Airflow docs).

### Increase Timeouts

In DAGs, modify `retry_delay` and `max_wait_minutes`:

```python
# In dag_ingestion.py
default_args = {
  "retry_delay": timedelta(minutes=10),  # <- Increase wait between retries
}

# In wait_for_airbyte_sync function
max_wait_minutes=120,  # <- Increase from 60 to 120
```

---

## Deployment to Production

### Pre-Flight Checklist

- [ ] `.env` file contains correct SQL Server credentials
- [ ] `.env` file contains correct Airbyte base URL
- [ ] All Airbyte connection IDs are correct in dag_ingestion.py
- [ ] dbt `profiles.yml` is configured with production database
- [ ] All DAGs have been tested in development
- [ ] Data quality thresholds (row counts) are accurate
- [ ] Alerting email addresses are set
- [ ] Database backups are configured

### Deploy Steps

1. **Copy to production server**:
   ```bash
   git push origin deploy-to-prod
   # Or: scp -r 2_orchestration/ user@prod-server:/opt/atb/
   ```

2. **Update credentials** (production values)

3. **Rebuild and start**:
   ```bash
   docker compose build --no-cache
   docker compose up -d
   ```

4. **Verify**:
   - Check http://localhost:8080
   - Run test DAG: `airflow dags test dag_ingestion 2024-01-01`
   - Monitor logs for errors

---

## Useful Commands

```powershell
# SSH into container
docker exec -it atb-airflow-webserver /bin/bash

# List all DAGs
docker exec atb-airflow-webserver airflow dags list

# Test a DAG (without scheduler)
docker exec atb-airflow-webserver airflow dags test dag_ingestion 2024-01-01

# Clear a DAG's task instances (reset)
docker exec atb-airflow-webserver airflow dags delete dag_ingestion

# Restart services
docker compose restart

# View resource usage
docker stats

# Check Postgres database
docker exec -it atb-airflow-postgres psql -U airflow -d airflow
```

---

## Next Steps

1. **Configure Airbyte connection IDs** in `dag_ingestion.py`
2. **Test dag_ingestion** to ensure Airbyte triggers work
3. **Test dag_transformation** with existing ODS data
4. **Implement ML layer** (`4_ml/src/`) functions
5. **Schedule full pipeline** for production runs
6. **Monitor and optimize** based on performance metrics

---

## References

- Apache Airflow: https://airflow.apache.org/docs/
- Airbyte API: https://reference.airbyte.com/
- dbt Documentation: https://docs.getdbt.com/
- SQL Server pyodbc: https://github.com/mkleehammer/pyodbc
```

### 2. Initialize Airflow Database

```bash
export AIRFLOW_HOME=/path/to/atb_bi_project/2_orchestration
airflow db init
```

### 3. Create Airflow Admin User

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@atb.local \
  --password admin123
```

### 4. Configure Variables

In Airflow UI (Admin → Variables), add:

```
Key: DBT_PROJECT_DIR
Value: /path/to/atb_bi_project/3_transformation

Key: DBT_PROFILE
Value: atb_bi_transformation

Key: DBT_TARGET
Value: prod

Key: SLACK_WEBHOOK_URL
Value: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 5. Start Airflow Services

```bash
# Terminal 1: Start Scheduler
airflow scheduler

# Terminal 2: Start Web Server
airflow webserver --port 8080

# Access UI at http://localhost:8080
```

## Main DAG: atb_bi_warehouse_etl

### Schedule
- **Frequency**: Daily
- **Time**: 02:00 UTC
- **Timezone**: UTC
- **Catchup**: Disabled (no retroactive runs)

### Execution Flow

#### Phase 1: Data Ingestion
- Ingest customer data from source
- Ingest account data from source
- Ingest reference data (currency, sector, industry, target, dao)

**Status Bar**: 3 parallel tasks

#### Phase 2: dbt Staging Models
- Transform raw ODS data into clean staging tables
- Standardize column names, data types, null handling
- Creates: stg_customer, stg_account, stg_currency, stg_sector, stg_industry, stg_target

**Tasks**: 1 (orchestrated within dbt)

#### Phase 3: dbt Intermediate Models
- Apply business logic and data enrichment
- Join staging tables with dimensions
- Creates: int_customer_enriched, int_account_enriched, int_customer_risk_score

**Tasks**: 1 (orchestrated within dbt)

#### Phase 4: dbt Warehouse Models
- Load dimensions and facts into DWH
- **Dimensions** (8): customer, dao, currency, sector, industry, target, risk_profile, date
- **Facts** (2): fact_customer_risk, fact_account
- All dimensions have UNKNOWN fallback records
- All facts use INNER JOIN + COALESCE pattern for zero NULL FKs

**Tasks**: 2 (parallel: dimensions → facts)

#### Phase 5: dbt Tests
- Execute schema tests (column existence, data types)
- Relationship tests (foreign key validation)
- Uniqueness tests (no duplicate keys)

**Status**: Must pass all tests before proceeding

#### Phase 6: Data Quality Checks
- Validate fact_customer_risk: no NULL FKs, row count validation
- Validate fact_account: no NULL FKs, row count validation
- Verify all dimensions have UNKNOWN records

**Failure Criteria**:
- Any NULL in critical foreign keys
- Row count < 100k for fact tables
- Missing UNKNOWN dimension records

#### Phase 7: Monitoring & Logging
- Log final pipeline metrics to monitoring system
- Record row counts, execution time, status

#### Phase 8: Success Notification
- Send completion notification to data team

### Task Configuration

| Task | Type | Retry | Timeout | Owner |
|------|------|-------|---------|-------|
| dbt_staging_models | BashOperator | 2 | - | data-engineering |
| dbt_intermediate_models | BashOperator | 2 | - | data-engineering |
| dbt_dimensions | BashOperator | 2 | - | data-engineering |
| dbt_facts | BashOperator | 2 | - | data-engineering |
| dbt_tests | BashOperator | 2 | - | data-engineering |
| check_fact_customer_risk | PythonOperator | 2 | 300s | data-engineering |
| check_fact_account | PythonOperator | 2 | 300s | data-engineering |
| log_pipeline_metrics | PythonOperator | 1 | 60s | data-engineering |

## Data Quality Checks

### check_fact_customer_risk_quality()

Validates the fact_customer_risk table:

```python
Checks:
  ✓ Total row count > 100,000
  ✓ NULL customer_sk = 0
  ✓ NULL risk_profile_sk = 0
  ✓ NULL dao_sk = 0
  ✓ NULL sector_sk = 0
  ✓ NULL industry_sk = 0
  ✓ NULL target_sk = 0

Success Criteria:
  - All NULL checks = 0
  - Row count >= 100,000
```

### check_fact_account_quality()

Validates the fact_account table:

```python
Checks:
  ✓ Total row count > 100,000
  ✓ NULL customer_sk = 0
  ✓ NULL dao_sk = 0
  ✓ NULL currency_sk = 0
  ✓ NULL sector_sk = 0
  ✓ NULL industry_sk = 0
  ✓ NULL target_sk = 0

Success Criteria:
  - All NULL checks = 0
  - Row count >= 100,000
```

## Monitoring & Alerts

### Email Alerts
Configured for failures to: data-team@atb.local

### Slack Integration
If SLACK_WEBHOOK_URL is configured, notifications are sent to Slack channel on:
- DAG success
- DAG failure
- Task failures

### Logs
View execution logs in:
- Web UI: Admin → Logs
- File system: `/airflow/logs/atb_bi_warehouse_etl/`

## Troubleshooting

### Issue: DAG not appearing in Airflow UI
**Solution**: Ensure DAG file is in `dags/` folder and has no syntax errors
```bash
python -m py_compile dags/atb_bi_warehouse_etl.py
```

### Issue: dbt commands fail with "Profile not found"
**Solution**: Ensure DBT_PROFILES_DIR environment variable is set and profiles.yml exists
```bash
export DBT_PROFILES_DIR=/path/to/atb_bi_project/3_transformation
```

### Issue: Data quality checks fail
**Solution**: Check database connectivity and SQL Server credentials
```bash
python -c "import pyodbc; print(pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-B0PDEI7,1434;Database=ATB_BI;UID=airbyte_user;PWD=AZERTY123'))"
```

### Issue: dbt run takes too long
**Solution**: Check database performance and connection pool settings
- Monitor SQL Server: `select * from sys.dm_exec_sessions`
- Review dbt profiles.yml `threads` setting (current: 8)

## Performance Tuning

### dbt Execution Time
- Current setup uses 8 threads in prod target
- Adjust in `profiles.yml` threads setting
- Monitor query execution plans in SQL Server

### Memory Usage
- Monitor Airflow scheduler and webserver memory
- Increase `parallelism` if server has available resources

### Database Connection Pooling
- Configured in `airflow.cfg`: pool_size=10, max_overflow=10
- Adjust based on concurrent task needs

## Manual Triggers

### Run DAG Manually
```bash
airflow dags trigger atb_bi_warehouse_etl
```

### Run Specific Task
```bash
airflow tasks run atb_bi_warehouse_etl dbt_staging_models 2025-01-20
```

### Clear Task History
```bash
airflow tasks clear atb_bi_warehouse_etl -d dbt_facts
```

## Best Practices

1. **Always test DAGs locally before deployment**
   ```bash
   airflow dags list-runs -d atb_bi_warehouse_etl
   ```

2. **Monitor logs actively**
   ```bash
   tail -f /airflow/logs/atb_bi_warehouse_etl/dbt_facts/
   ```

3. **Keep dbt profiles and credentials secure**
   - Never commit passwords to Git
   - Use environment variables or Airflow Secrets

4. **Schedule maintenance tasks during off-peak hours**
   - DAG runs at 02:00 UTC (low-traffic period)
   - Allow 2 hours for execution and data refresh

5. **Review DAG SLA (Service Level Agreement)**
   - Current: 2 hours for complete execution
   - Alert if execution exceeds SLA

## Reference Documentation

### External Resources
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [SQL Server Connection Strings](https://docs.microsoft.com/en-us/sql/connection-strings/connection-strings)

### Internal Resources
- Warehouse Schema: [WAREHOUSE_QUICK_REFERENCE.md](../WAREHOUSE_QUICK_REFERENCE.md)
- Data Quality Report: [PRODUCTION_VALIDATION_REPORT.md](../PRODUCTION_VALIDATION_REPORT.md)
- dbt Project: [dbt_project.yml](../3_transformation/dbt_project.yml)

---

**Version**: 1.0.0  
**Last Updated**: 2025  
**Status**: Ready for Production Deployment
