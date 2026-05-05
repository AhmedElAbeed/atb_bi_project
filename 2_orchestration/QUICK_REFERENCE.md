# Airflow Quick Reference Card

## 🚀 Quick Start (Copy & Paste)

### Setup (First Time)
```powershell
cd c:\Users\Ahmed\Desktop\atb_bi_project\2_orchestration

# Copy template to .env and edit credentials
Copy-Item .env.template .env
notepad .env

# Build image (takes 5-10 minutes)
docker compose build

# Start all services
docker compose up -d

# Wait 30 seconds, then check status
docker compose ps
```

### Access Airflow
- **Web UI**: http://localhost:8080
- **Username**: admin
- **Password**: admin

---

## 📝 Essential Configuration

### 1. Set Airbyte Connection IDs

Edit `dags/dag_ingestion.py`, find this section:

```python
AIRBYTE_CONNECTIONS = {
    "account": "PASTE_ACCOUNT_UUID",       # ← Update
    "customer": "PASTE_CUSTOMER_UUID",     # ← Update
    "currency": "PASTE_CURRENCY_UUID",     # ← Update
    "dao": "PASTE_DAO_UUID",               # ← Update
    "industry": "PASTE_INDUSTRY_UUID",     # ← Update
    "sector": "PASTE_SECTOR_UUID",         # ← Update
    "target": "PASTE_TARGET_UUID",         # ← Update
}
```

**Where to get UUIDs:**
1. Open http://localhost:8000 (Airbyte)
2. Go to "Connections" tab
3. Click each connection → copy UUID from details

### 2. Verify .env File

Key settings in `.env`:
```bash
SQL_SERVER_HOST=host.docker.internal
SQL_SERVER_DATABASE=PFE
SQL_SERVER_USER=sa
SQL_SERVER_PASSWORD=YourPassword123!
```

### 3. Check dbt profiles.yml

Ensure `3_transformation/profiles.yml` has SQL Server connection configured.

---

## 🎯 Running DAGs

### Manually Trigger a DAG
```powershell
# Ingestion
docker exec atb-airflow-webserver airflow dags trigger dag_ingestion

# Transformation
docker exec atb-airflow-webserver airflow dags trigger dag_transformation

# ML Pipeline
docker exec atb-airflow-webserver airflow dags trigger dag_ml_pipeline

# Full Pipeline (all 3 in sequence)
docker exec atb-airflow-webserver airflow dags trigger dag_full_pipeline
```

### Test a DAG (without scheduler)
```powershell
docker exec atb-airflow-webserver airflow dags test dag_ingestion 2024-01-01
```

### View Status in Web UI
1. Go to http://localhost:8080
2. Click "DAGs" tab
3. Click on DAG name
4. Click "Graph View" to see task flow
5. Click on a task → "Logs" to see output

---

## 🔍 Monitoring & Debugging

### View Live Logs
```powershell
# All services
docker compose logs -f

# Specific service
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

### Check If Containers Running
```powershell
docker compose ps
```

Expected output:
```
NAME                    STATUS
atb-airflow-webserver   Up
atb-airflow-scheduler   Up
atb-airflow-triggerer   Up
postgres                Up
```

### Restart Services
```powershell
docker compose restart
```

### Full Cleanup (Remove All Data)
```powershell
docker compose down -v
```

---

## 🐛 Common Issues & Fixes

| Problem | Solution |
|---------|----------|
| Port 8080 already in use | Change in docker-compose.yml: `ports: ["8081:8080"]` |
| "Connection refused" to SQL Server | Ensure SQL Server running; check credentials in .env |
| Airbyte API returns 404 | Connection IDs are wrong; verify from Airbyte UI |
| dbt test fails | Check dbt logs in Web UI; verify `profiles.yml` |
| DAGs not showing up | Wait 2 minutes; refresh browser; check `dags/` folder |
| Stuck in "running" state | Kill DAG: `docker exec atb-airflow-webserver airflow dags delete dag_name` |

---

## 💾 Backup & Recovery

### Export DAGs & Configuration
```powershell
# Copy entire 2_orchestration folder
xcopy 2_orchestration\ 2_orchestration_backup\ /E /I
```

### Restore from Backup
```powershell
docker compose down -v
xcopy 2_orchestration_backup\ 2_orchestration\ /E /I /Y
docker compose build && docker compose up -d
```

---

## 📊 DAG Task Tree

### dag_ingestion
```
sync_all_sources (parallel trigger + wait for 7 sources)
  ├─ trigger_sync_account → wait_sync_account
  ├─ trigger_sync_customer → wait_sync_customer
  ├─ trigger_sync_currency → wait_sync_currency
  ├─ trigger_sync_dao → wait_sync_dao
  ├─ trigger_sync_industry → wait_sync_industry
  ├─ trigger_sync_sector → wait_sync_sector
  └─ trigger_sync_target → wait_sync_target
validate_ods_tables
log_ods_statistics
```

### dag_transformation
```
dbt_debug → dbt_deps → dbt_seed
  ├─ dbt_staging (run + test)
  ├─ dbt_intermediate (run + test)
  └─ dbt_warehouse (dimensions → facts → test)
validate_dwh_tables
dbt_generate_docs
```

### dag_ml_pipeline
```
run_feature_extraction
  ├─ run_model_training → evaluate_model_performance
  ├─ compute_shap_explainability
  └─ write_predictions_to_dwh
log_pipeline_summary
```

### dag_full_pipeline
```
pipeline_start
  → trigger_dag_ingestion
  → quality_gate_ods (SHORT-CIRCUIT if invalid)
  → trigger_dag_transformation
  → quality_gate_dwh (SHORT-CIRCUIT if invalid)
  → trigger_dag_ml_pipeline
  → pipeline_completion
```

---

## 🔧 Advanced Commands

### List All DAGs
```powershell
docker exec atb-airflow-webserver airflow dags list
```

### Clear Task History (Reset)
```powershell
docker exec atb-airflow-webserver airflow tasks clear dag_ingestion --task_id validate_ods_tables
```

### Execute Single Task
```powershell
docker exec atb-airflow-webserver airflow tasks run dag_ingestion validate_ods_tables 2024-01-01
```

### Check Database (Postgres)
```powershell
docker exec -it postgres psql -U airflow -d airflow -c "SELECT * FROM dag;"
```

### SSH Into Container
```powershell
docker exec -it atb-airflow-webserver /bin/bash
# Inside container:
# cd /opt/airflow/dags
# python -m py_compile dag_ingestion.py
```

### View Resource Usage
```powershell
docker stats
```

---

## 📈 Production Checklist

- [ ] `.env` has production credentials
- [ ] Airbyte connection IDs verified
- [ ] dbt profiles.yml configured
- [ ] All DAGs tested in dev
- [ ] Row count thresholds accurate
- [ ] Email alerts configured
- [ ] Slack integration (optional) configured
- [ ] Database backups enabled
- [ ] Docker volumes mapped to persistent storage
- [ ] Monitor scheduled (e.g., Datadog, New Relic)

---

## 🎓 Next Phases

1. **Ingestion Phase**: Get dag_ingestion working (test Airbyte triggers)
2. **Transformation Phase**: Get dag_transformation working (test dbt build)
3. **ML Phase**: Implement `4_ml/src/` functions (replace placeholders)
4. **Production Phase**: Schedule dag_full_pipeline for daily runs

---

## 📞 Quick Help

**Airflow Web UI Navigation:**
- DAGs → View all DAGs, trigger, edit
- Grid View → Timeline of exec history
- Graph View → Visual task dependency tree
- Logs → Task execution output
- Admin → Connections, Variables, Pools

**Useful URLs:**
- Airflow: http://localhost:8080
- Airbyte: http://localhost:8000
- SQL Server: host.docker.internal:1433 (from containers)

---

**Last Updated**: 2024-01-01
**Airflow Version**: 2.9.1
**Project**: ATB BI Warehouse
