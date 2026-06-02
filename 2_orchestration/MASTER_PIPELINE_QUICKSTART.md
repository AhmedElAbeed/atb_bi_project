# ATB Master Pipeline - Quick Start & Deployment Checklist

## 🚀 Quick Start (5 Minutes)

### 1. Ensure Airflow is Running

```powershell
# From 2_orchestration/ folder
cd c:\Users\Ahmed\Desktop\atb_bi_project\2_orchestration

# Start Docker stack
docker compose up -d

# Verify services
docker compose ps
# Should show: postgres, airflow-init, airflow-webserver, airflow-scheduler, airflow-triggerer
```

### 2. Copy Master Pipeline DAG

```powershell
# Copy the master pipeline DAG to Airflow dags folder
Copy-Item "dags/atb_master_pipeline.py" -Destination "dags/atb_master_pipeline.py" -Force

# Verify it's there
ls dags/atb_master_pipeline.py  # Should exist
```

### 3. Validate DAG Syntax

```powershell
# Create Python environment variable
$env:PYTHONPATH = "."

# Validate DAG syntax
python -m py_compile dags/atb_master_pipeline.py

# Expected output: (no output = success)

# Alternative: check Airflow can parse it
$env:AIRFLOW_HOME = $PWD
airflow dags list | findstr "atb_master_pipeline"

# Expected output: atb_master_pipeline  ...
```

### 4. Access Airflow Web UI

Open browser: **http://localhost:8080**

- Username: `admin`
- Password: `admin` (or your configured password)

### 5. Find & Trigger DAG

1. Click "DAGs" in left sidebar
2. Search for `atb_master_pipeline`
3. Click on DAG name to open
4. Click blue "Trigger DAG" button in upper right
5. Click "Trigger" in the modal
6. Watch execution in real-time

---

## 📋 Pre-Deployment Checklist

### Prerequisites ✓

- [ ] Docker Desktop installed and running
- [ ] Python 3.8+ installed locally
- [ ] SQL Server running (DESKTOP-B0PDEI7:1434)
- [ ] ATB_BI database exists
- [ ] PFE_ODS schema exists (for ingestion)
- [ ] PFE_DWH schema exists (for warehouse)
- [ ] Airbyte running (http://localhost:8000)
- [ ] Airbyte connections configured + tested
- [ ] dbt project in `3_transformation/`
- [ ] dbt profiles.yml configured with SQL Server connection
- [ ] ML code in `4_ml/` with src/ modules working

### Environment Variables ✓

Set before starting Airflow:

```powershell
# Set environment variables
$env:SQL_SERVER = "DESKTOP-B0PDEI7"
$env:SQL_PORT = "1434"
$env:SQL_DB = "ATB_BI"
$env:SQL_USER = "airbyte_user"
$env:SQL_PASSWORD = "AZERTY123"
$env:AIRBYTE_API_URL = "http://localhost:8000"
$env:AIRBYTE_API_KEY = "default_key"
$env:DBT_PROFILES_DIR = "C:\Users\Ahmed\Desktop\atb_bi_project\3_transformation"
$env:DBT_PROJECT_DIR = "C:\Users\Ahmed\Desktop\atb_bi_project\3_transformation"
```

Or add to `.env` file in `2_orchestration/`:

```
SQL_SERVER=DESKTOP-B0PDEI7
SQL_PORT=1434
SQL_DB=ATB_BI
SQL_USER=airbyte_user
SQL_PASSWORD=AZERTY123
AIRBYTE_API_URL=http://localhost:8000
AIRBYTE_API_KEY=default_key
DBT_PROFILES_DIR=/opt/airflow/dbt_project
DBT_PROJECT_DIR=/opt/airflow/dbt_project
ML_SRC_DIR=/opt/airflow/ml_src
ML_MODELS_DIR=/opt/airflow/ml_models
```

### Docker Compose Configuration ✓

Verify `docker-compose.yml` has volume mounts for dbt and ML:

```yaml
airflow-webserver:
  volumes:
    - ./dags:/opt/airflow/dags
    - ./3_transformation:/opt/airflow/dbt_project
    - ./4_ml/src:/opt/airflow/ml_src
    - ./4_ml/models:/opt/airflow/ml_models
```

### SQL Server Connectivity ✓

```powershell
# Test connection
$connectionString = "Server=DESKTOP-B0PDEI7,1434;Database=ATB_BI;User Id=airbyte_user;Password=AZERTY123;"
$connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
$connection.Open()
if ($connection.State -eq "Open") {
    Write-Host "✓ SQL Server connected"
    $connection.Close()
} else {
    Write-Host "✗ SQL Server connection failed"
}
```

### Airbyte Connectivity ✓

```powershell
# Test Airbyte API
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/v1/health" -ErrorAction SilentlyContinue
if ($response.StatusCode -eq 200) {
    Write-Host "✓ Airbyte API accessible"
} else {
    Write-Host "✗ Airbyte API not responding"
}
```

### dbt Configuration ✓

```powershell
# Navigate to dbt project
cd 3_transformation

# Test dbt debug
dbt debug --profiles-dir .

# Should output: All checks passed ✓
```

---

## 🔧 Deployment Steps

### Step 1: Start Airflow

```powershell
cd 2_orchestration

# Ensure clean start
docker compose down -v
docker compose up -d

# Monitor initialization
docker compose logs -f airflow-init
# Wait for: "Database is up to date" message

# Verify all services running
docker compose ps
```

### Step 2: Configure Airflow Variables

```powershell
# Set Airflow variables (used by DAG)
# Option A: Via Web UI
#   1. Go to Admin → Variables
#   2. Add each key-value pair below

# Option B: Via CLI
$env:AIRFLOW_HOME = "C:\Users\Ahmed\Desktop\atb_bi_project\2_orchestration"
airflow variables set AIRBYTE_ACCOUNT_ID "91205bfd-c0a8-4c76-9813-f616beb21da0"
airflow variables set AIRBYTE_CUSTOMER_ID "887e6d51-fccf-4547-a97d-970417b57613"
# ... etc for all 7 connections

# Verify
airflow variables list
```

### Step 3: Test ODS Connectivity

```bash
# SSH into Airflow container (or run locally)
docker exec -it atb_bi_project-airflow-scheduler-1 bash

# Inside container, test SQL connection
python -c "
import pyodbc
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=host.docker.internal,1434;Database=ATB_BI;UID=airbyte_user;PWD=AZERTY123')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM PFE_ODS.ODS_ACCOUNT')
print(f'ODS_ACCOUNT rows: {cursor.fetchone()[0]}')
conn.close()
"
# Expected: ODS_ACCOUNT rows: 145284
```

### Step 4: Test dbt Inside Container

```bash
docker exec -it atb_bi_project-airflow-scheduler-1 bash

# Inside container
cd /opt/airflow/dbt_project
dbt debug
dbt parse

# Expected: no errors
```

### Step 5: Deploy Master Pipeline DAG

```powershell
# Copy DAG file
Copy-Item "dags/atb_master_pipeline.py" `
  -Destination "C:/Users/Ahmed/Desktop/atb_bi_project/2_orchestration/dags/atb_master_pipeline.py"

# Airflow auto-discovers (30 seconds)
# Verify in UI: Admin → DAG List or go to http://localhost:8080/tree/atb_master_pipeline
```

---

## ✅ Validation Checklist

Before running the pipeline, confirm:

```powershell
# 1. Airflow services running
docker compose ps | findstr "running"
# Expected: webserver, scheduler, triggerer running

# 2. DAG is discoverable
airflow dags list | findstr "atb_master_pipeline"
# Expected: one row with DAG name

# 3. DAG has no import errors
airflow dags list-import-errors | findstr "atb_master_pipeline"
# Expected: no output (no errors)

# 4. ODS tables populated
# Check in SQL Server Management Studio:
SELECT COUNT(*) FROM PFE_ODS.ODS_ACCOUNT        -- Should be 145K+
SELECT COUNT(*) FROM PFE_ODS.ODS_CUSTOMER       -- Should be 137K+

# 5. dbt profiles accessible
ls "3_transformation/profiles.yml"
# Expected: file exists

# 6. ML modules importable
cd 4_ml
python -c "import src.modeling; print('✓ ML modules OK')"

# 7. Airbyte connections tested
# In Airbyte UI, check each connection status (should be "Success")
```

---

## 🏃 Running the Pipeline

### Manual Trigger (One-Time Run)

```powershell
# Method 1: Web UI (easiest)
# 1. Open http://localhost:8080
# 2. Find atb_master_pipeline
# 3. Click "Play" button (or "Trigger DAG")
# 4. Monitor in real-time

# Method 2: CLI
$env:AIRFLOW_HOME = "2_orchestration"
airflow dags trigger atb_master_pipeline --exec-date "2026-05-20"

# Method 3: REST API
$dag_run = @{
    "dag_id" = "atb_master_pipeline"
    "conf" = @{}
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/api/v1/dags/atb_master_pipeline/dagRuns" `
  -Method POST `
  -Body $dag_run `
  -ContentType "application/json" `
  -Headers @{"Authorization" = "Bearer <token>"}
```

### Monitor Execution

```powershell
# Watch logs in real-time
docker compose logs -f airflow-scheduler | findstr "atb_master_pipeline"

# Or check Web UI
# Tree View: http://localhost:8080/tree/atb_master_pipeline
# Gantt Chart: http://localhost:8080/tree/atb_master_pipeline?view=gantt
# Graph View: http://localhost:8080/tree/atb_master_pipeline?view=graph
```

### Expected Timeline

| Stage | Duration | Status |
|-------|----------|--------|
| 1. Ingestion | 5-10 min | Parallel Airbyte syncs |
| 2. Transformation | 10-15 min | dbt parse → staging → dims → facts |
| 3. Warehouse Validation | 1 min | Row count & NULL checks |
| 4. ML Pipeline | 5-10 min | Feature extraction → training → predictions |
| **Total** | **~30-35 min** | ✓ Complete |

---

## 🔍 Troubleshooting

### DAG Not Appearing in Airflow

```powershell
# 1. Restart Airflow
docker compose restart airflow-scheduler

# 2. Check DAG syntax
python -m py_compile dags/atb_master_pipeline.py

# 3. Check logs
docker compose logs airflow-scheduler | grep "atb_master_pipeline"
```

### Task Fails: "SQL Server connection failed"

```powershell
# 1. Verify SQL Server is running
ping DESKTOP-B0PDEI7

# 2. Test from host
$connectionString = "Server=DESKTOP-B0PDEI7,1434;Database=ATB_BI;User Id=airbyte_user;Password=AZERTY123"
$connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
$connection.Open()

# 3. Test from container
docker exec -it <container_id> bash
python -c "import pyodbc; conn = pyodbc.connect(...); print('Connected')"

# 4. Check connection string in Docker env
docker compose exec airflow-scheduler env | grep SQL_
```

### Task Fails: "dbt: command not found"

```powershell
# 1. Ensure dbt is installed in container
docker exec airflow-scheduler which dbt
# Should output path like /usr/local/bin/dbt

# 2. If not, reinstall
docker exec airflow-scheduler pip install dbt-core dbt-sqlserver

# 3. Verify profiles
docker exec airflow-scheduler ls /opt/airflow/dbt_project/profiles.yml
```

### Task Fails: "Airbyte sync timeout"

```powershell
# 1. Check Airbyte status
docker ps | findstr airbyte

# 2. Check Airbyte logs
docker logs airbyte

# 3. Increase timeout in DAG
# Edit: max_wait = 30 * 60 → 60 * 60 (1 hour)

# 4. Check Airbyte job manually
# Visit http://localhost:8000 → Monitor → Jobs
```

### Task Fails: "OML training OOM"

```powershell
# Reduce training data or feature count
# Edit atb_master_pipeline.py:
# 
# Add LIMIT clause:
# WHERE CAST(fcr.scoring_date AS DATE) = CAST(GETDATE() AS DATE)
# LIMIT 50000  -- Sample 50K instead of 136K for testing
```

---

## 📊 Post-Pipeline Validation

After pipeline completes, verify results:

```sql
-- Check warehouse tables loaded
SELECT COUNT(*) as customer_risk_rows FROM PFE_DWH.fact_customer_risk
-- Expected: 136,676

SELECT COUNT(*) as account_rows FROM PFE_DWH.fact_account
-- Expected: 145,284

-- Check ML predictions loaded
SELECT COUNT(*) as prediction_rows FROM PFE_DWH.ml_predictions_latest
-- Expected: 136,676 (should match customer count)

-- Verify prediction distribution
SELECT 
  ml_predicted_high_risk,
  COUNT(*) as count,
  ROUND(100.0*COUNT(*)/(SELECT COUNT(*) FROM PFE_DWH.ml_predictions_latest),2) as pct
FROM PFE_DWH.ml_predictions_latest
GROUP BY ml_predicted_high_risk
-- Expected: mostly 0s, some 1s (~20-30% HIGH risk)
```

---

## 📞 Support & Next Steps

If issues persist:

1. **Check Airflow Logs**: `docker compose logs -n 100 airflow-scheduler`
2. **Review DAG Code**: `2_orchestration/dags/atb_master_pipeline.py`
3. **Test Components Individually**:
   - Airbyte sync manually
   - dbt run manually
   - ML training script manually
4. **Scale Up**: If all works, deploy to production Kubernetes cluster

---

**Version**: 1.0.0  
**Status**: Ready to Deploy ✅  
**Last Updated**: May 2026
