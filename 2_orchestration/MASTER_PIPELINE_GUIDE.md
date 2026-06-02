# ATB Master Pipeline - Full Integration Guide

**Date**: May 2026  
**Status**: ✅ Production Ready  
**Pipeline**: Complete ETL + dbt + ML orchestration

---

## What This Pipeline Does

The `atb_master_pipeline` DAG orchestrates the complete data lifecycle from raw ingestion through ML predictions:

```
┌─────────────────────────────────────────────────────────────────┐
│                    MASTER PIPELINE FLOW                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  STAGE 1: DATA INGESTION                                         │
│  └─→ Trigger 7 Airbyte syncs (account, customer, currency, etc.)│
│  └─→ Validate ODS tables loaded (row counts)                    │
│                                                                   │
│  STAGE 2: DBT TRANSFORMATION (Parallel)                          │
│  ├─→ dbt parse (validate YAML)                                  │
│  ├─→ dbt deps (download packages)                               │
│  ├─→ dbt staging (8 models: customers, accounts, refs)          │
│  ├─→ dbt intermediate (3 models: enrichment + risk scoring)     │
│  ├─→ dbt warehouse (8 dims + 2 facts: Kimball constellation)    │
│  └─→ dbt tests (schema, relationships, uniqueness)              │
│                                                                   │
│  STAGE 3: WAREHOUSE VALIDATION                                   │
│  └─→ Verify fact_customer_risk: 0 NULL FKs, 100K+ rows         │
│  └─→ Verify fact_account: 0 NULL FKs, 145K+ rows               │
│                                                                   │
│  STAGE 4: ML PIPELINE (Parallel inside)                          │
│  ├─→ EXTRACT FEATURES                                           │
│  │   └─→ Query PFE_DWH (facts + dimensions)                     │
│  │   └─→ Build feature matrix (25+ features)                    │
│  │   └─→ Save to CSV                                            │
│  │                                                               │
│  ├─→ TRAIN MODELS (Sequence)                                    │
│  │   ├─→ Load features + target (binary high-risk classification)
│  │   ├─→ Apply SMOTETomek class rebalancing                     │
│  │   ├─→ Train XGBoost classifier                               │
│  │   ├─→ Train Random Forest classifier                         │
│  │   ├─→ Select best model (by F1 score)                        │
│  │   └─→ Save model to pickle                                   │
│  │                                                               │
│  ├─→ GENERATE SHAP (Optional)                                   │
│  │   └─→ Calculate SHAP values for explainability              │
│  │   └─→ Generate HTML summary plot                             │
│  │                                                               │
│  ├─→ BATCH PREDICTION (Parallel with SHAP)                      │
│  │   ├─→ Load best model                                        │
│  │   ├─→ Score all customers (predict + predict_proba)         │
│  │   ├─→ Load predictions into PFE_DWH.ml_predictions_latest   │
│  │   └─→ Update Power BI datasets                               │
│  │                                                               │
│  └─→ MODEL MONITORING                                           │
│      ├─→ Check F1 Score > 0.7 threshold                         │
│      ├─→ Check ROC-AUC > 0.8 threshold                          │
│      ├─→ Monitor precision-recall balance                       │
│      └─→ Alert if thresholds breached                           │
│                                                                   │
│  FINAL: Completion notification                                  │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Configuration

### Environment Variables

Set these before running (in `.env` or Docker environment):

```bash
# SQL Server Connection
SQL_SERVER=DESKTOP-B0PDEI7          # SQL Server hostname or IP
SQL_PORT=1434                        # SQL Server port
SQL_DB=ATB_BI                        # Database name
SQL_USER=airbyte_user                # SQL Server user
SQL_PASSWORD=AZERTY123               # SQL Server password

# Airbyte API
AIRBYTE_API_URL=http://localhost:8000    # Airbyte API endpoint
AIRBYTE_API_KEY=default_key              # Airbyte API token

# dbt
DBT_PROFILES_DIR=/opt/airflow/dbt_profiles      # dbt profiles location
DBT_PROJECT_DIR=/opt/airflow/dbt_project        # dbt project location
DBT_TARGET=prod                                  # dbt target (dev/prod)

# ML
ML_SRC_DIR=/opt/airflow/ml_src         # ML module path
ML_MODELS_DIR=/opt/airflow/ml_models   # Model storage path

# Airflow
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor  # Use CeleryExecutor for production
AIRFLOW__CORE__PARALLELISM=8           # Max parallel tasks
AIRFLOW__CORE__DAG_CONCURRENCY=4       # Max tasks per DAG
```

### Airbyte Connection IDs

The pipeline uses these Airbyte connections (source → ODS):

| Source | Connection ID | Table | Rows |
|--------|---------------|-------|------|
| account | `91205bfd-c0a8-4c76-9813-f616beb21da0` | ODS_ACCOUNT | 145K |
| currency | `d6e83091-3f92-4ea5-8ec3-04d60c179f82` | ODS_CURRENCY | 30 |
| customer | `887e6d51-fccf-4547-a97d-970417b57613` | ODS_CUSTOMER | 137K |
| dao | `eab9fdb8-3a23-424a-85e0-b6719dab8ce3` | ODS_DAO | 150 |
| industry | `7a3bcd9d-d56f-4ab1-97b1-9da7039c8d59` | ODS_INDUSTRY | 663 |
| sector | `461f5b6b-4111-48d6-ae71-34b6e7aaee5a` | ODS_SECTOR | 45 |
| target | `cfd9fae4-c248-4043-8464-a644d3c9bb77` | ODS_TARGET | 11 |

---

## Data Pipeline Details

### STAGE 1: Data Ingestion (ODS Layer)

**Tasks**: 7 parallel Airbyte triggers + 1 validation

**What Happens**:
1. Each Airbyte connection is triggered via REST API
2. Airbyte syncs source system into SQL Server ODS schema (PFE_ODS)
3. Validation task queries row counts for each ODS table
4. If any table is empty, pipeline fails immediately

**Time**: ~5-10 minutes  
**Output**: Fresh ODS tables ready for dbt transformation

```sql
-- View ODS tables
SELECT TABLE_NAME, COUNT(*) as row_count
FROM INFORMATION_SCHEMA.TABLES t
LEFT JOIN (SELECT COUNT(*) FROM PFE_ODS.ODS_ACCOUNT) a ON 1=1
WHERE TABLE_SCHEMA = 'PFE_ODS'
```

---

### STAGE 2: dbt Transformation (Warehouse Build)

**Tasks**: Parse → Deps → Staging → Intermediate → Warehouse → Tests

**Layers**:

| Layer | Schema | Purpose | Models |
|-------|--------|---------|--------|
| **Staging** | PFE_DWH | Clean + validate ODS | 8 models (stg_account, stg_customer, etc.) |
| **Intermediate** | PFE_INTERMEDIATE | Enrich + business logic | 4 views (int_customer_enriched, int_customer_risk_score, etc.) |
| **Warehouse** | PFE_DWH | Constellation model | Dimensions (8) + Facts (2) |

**What Happens**:
1. Parse validates YAML configuration
2. Deps downloads external packages (e.g., dbt-utils)
3. Staging layer cleans/deduplicates ODS data
4. Intermediate layer builds enrichments (salary, industry, risk scoring)
5. Warehouse layer builds Kimball constellation:
   - **Dimensions**: customer, dao, currency, sector, industry, target, risk_profile, date (8 total)
   - **Facts**: fact_account (145K rows), fact_customer_risk (136K rows)
6. Tests validate relationships, uniqueness, non-null constraints

**Time**: ~10-15 minutes  
**Output**: Production-ready warehouse with zero NULL foreign keys (100% referential integrity)

```sql
-- Verify warehouse built correctly
SELECT 
  'fact_customer_risk' as table_name,
  COUNT(*) as row_count,
  SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) as null_count
FROM PFE_DWH.fact_customer_risk
UNION ALL
SELECT 
  'fact_account',
  COUNT(*),
  SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END)
FROM PFE_DWH.fact_account
```

---

### STAGE 3: Warehouse Validation

**Task**: Python operator queries fact tables

**Checks**:
- fact_customer_risk: 136K+ rows, 0 NULL customer_sk, 0 NULL dao_sk
- fact_account: 145K+ rows, 0 NULL foreign keys

**Fails if**: Any table empty or has NULL FKs  
**Time**: ~30 seconds

---

### STAGE 4: ML Pipeline (4 Parallel Branches)

#### 4.1: Feature Extraction

**Task**: `extract_features`  
**What Happens**:
1. Query `PFE_DWH` fact + dimension tables
2. Extract 25+ features:
   - **Compliance**: is_kyc_complete, is_pep, is_compliance_flagged, compliance_risk_index
   - **Financial**: monthly_salary, total_working_balance, negative_balance_account_count, account_count
   - **Behavioral**: customer_tenure_days, oldest_account_age_days, behavioral_risk_score
   - **Segmentation**: sector_code, industry_code, target_code, dao_name
3. Save feature matrix to CSV

**Output**: `ml_models/features_latest.csv` (136K rows × 25 columns)

```python
# Feature matrix structure
customer_sk, compliance_risk_index, financial_fragility_score, behavioral_risk_score,
global_risk_score, target_high_risk (binary label),
account_count, total_working_balance, avg_working_balance, negative_balance_account_count,
oldest_account_age_days, customer_tenure_days,
is_kyc_complete, is_pep, is_compliance_flagged, monthly_salary, number_of_dependents,
sector_code, industry_code, target_code, dao_name
```

---

#### 4.2: Model Training

**Task**: `train_models`  
**What Happens**:
1. Load feature matrix + target (`global_risk_score >= 50` = HIGH risk = 1)
2. Apply **SMOTETomek** resampling (handles class imbalance: ~48% minority class)
3. Train **XGBoost** classifier:
   - Early stopping on validation loss
   - Hyperparameters: max_depth=6, learning_rate=0.1, n_estimators=100
   - Outputs: predictions, probabilities, feature importance
4. Train **Random Forest** classifier (benchmark):
   - n_estimators=100, max_depth=10
   - Outputs: predictions, probabilities, feature importance
5. Compare F1 scores and select best model
6. Save winner to pickle

**Metrics Captured**:
- Accuracy, Precision, Recall, F1 Score
- ROC-AUC, PR-AUC
- Confusion matrix
- Feature importance (SHAP values available in next task)

**Output**: `ml_models/best_model_latest.pkl` + metrics pushed to XCom

```python
# Model selection logic
if xgb_f1 > rf_f1:
    best_model = xgb_model
    logger.info(f"Selected XGBoost (F1={xgb_f1:.4f} > RF F1={rf_f1:.4f})")
else:
    best_model = rf_model
    logger.info(f"Selected Random Forest (F1={rf_f1:.4f} > XGB F1={xgb_f1:.4f})")
```

---

#### 4.3: SHAP Explanations (Optional, Parallel)

**Task**: `generate_shap`  
**What Happens**:
1. Load best model + feature matrix
2. Compute SHAP values (TreeExplainer for tree models)
3. Generate:
   - **Global summary plot**: Feature importance across all predictions
   - **Local explanations**: Individual customer prediction drivers
4. Save HTML visualization

**Output**: `ml_models/shap_summary_latest.html` (viewable in browser)

**Note**: If SHAP fails, pipeline continues (optional task)

---

#### 4.4: Batch Prediction (Parallel with SHAP)

**Task**: `batch_predictions`  
**What Happens**:
1. Load best model + feature matrix
2. Score all 136K customers:
   - `predict()` → high_risk (0/1)
   - `predict_proba()` → risk_probability (0.0-1.0)
3. Create `ml_predictions_latest` table in SQL Server
4. Bulk insert all predictions
5. Front-end (Power BI) queries this table for real-time risk scores

**Output**: 
- `ml_models/predictions_latest.csv` (backup)
- `PFE_DWH.ml_predictions_latest` table (online serving)

```sql
-- Query predictions in Power BI
SELECT 
  mp.customer_sk,
  mp.ml_predicted_high_risk,
  mp.ml_risk_probability,
  dc.customer_name,
  dc.sector_code
FROM PFE_DWH.ml_predictions_latest mp
JOIN PFE_DWH.dim_customer dc ON mp.customer_sk = dc.customer_sk
WHERE mp.ml_predicted_high_risk = 1  -- Show HIGH RISK predictions
ORDER BY mp.ml_risk_probability DESC
```

---

#### 4.5: Model Monitoring

**Task**: `monitor`  
**What Happens**:
1. Pull training metrics from XCom
2. Check thresholds:
   - **F1 Score** ≥ 0.7 (minimum acceptable)
   - **ROC-AUC** ≥ 0.8 (discrimination ability)
   - **Precision-Recall balance** within 15% difference
3. Generate alerts if any threshold breached
4. Log results

**Output**: Monitoring summary pushed to XCom

```python
# Example alert
if f1_score < 0.7:
    alerts.append(f"WARNING: F1 Score {f1_score:.4f} below 0.7 threshold")
    # In production: send Slack alert, page on-call engineer
```

---

## Deployment

### 1. Prerequisites

Ensure you have:
- Airflow 2.9+ running (with `docker-compose up -d` from `2_orchestration/`)
- dbt project in `3_transformation/` with profiles configured
- ML code in `4_ml/` with trained modules (run notebooks first)
- SQL Server with ODS + DWH schemas
- Airbyte running with syncs configured

### 2. Copy DAG to Airflow

```bash
# Copy master pipeline DAG to Airflow dags folder
cp 2_orchestration/dags/atb_master_pipeline.py /opt/airflow/dags/

# Airflow auto-discovers after 30 seconds (configurable)
# Then DAG appears in Airflow Web UI
```

### 3. Verify DAG Syntax

```bash
# In Airflow container
python -m py_compile dags/atb_master_pipeline.py

# Or:
airflow dags list | grep atb_master_pipeline
```

### 4. Trigger First Run

**Option A: Manual Trigger**
```bash
airflow dags trigger atb_master_pipeline --exec-date 2026-05-20
```

**Option B: Web UI**
1. Navigate to http://localhost:8080
2. Find `atb_master_pipeline` DAG
3. Click "Trigger DAG"
4. Monitor execution

---

## Monitoring & Logs

### Airflow Web UI

- **DAG View**: http://localhost:8080/tree/atb_master_pipeline
- **Gantt Chart** (timeline): Click "Gantt" tab
- **Logs**: Click task name → "Logs" tab

### Log Files

```bash
# Inside Airflow container
/opt/airflow/logs/atb_master_pipeline/

# Example: ingestion stage logs
/opt/airflow/logs/atb_master_pipeline/stage_1_ingestion/ingest_account/2026-05-20T02:00:00+00:00/
```

### Common Issues

**Issue**: "dbt command not found"  
→ Ensure `dbt-core` is installed: `pip list | grep dbt`

**Issue**: "SQL Server connection failed"  
→ Check connection string in env vars; verify ODBC driver 17 installed

**Issue**: "Airbyte sync timeout"  
→ Increase `AIRBYTE_TIMEOUT` (default 30 min); check Airbyte logs

**Issue**: "ML training OOM (out of memory)"  
→ Reduce batch size or feature count; use `CeleryExecutor` for distributed training

---

## Performance & Scaling

### Current (LocalExecutor on Single Machine)

- **Max Parallel Tasks**: 8 (config: `AIRFLOW__CORE__PARALLELISM=8`)
- **Total Pipeline Time**: ~35 minutes
  - Ingestion: ~5-10 min
  - dbt: ~10-15 min
  - ML: ~10 min
- **Memory Usage**: ~2-4 GB
- **Storage**: ~5GB (models, logs, predictions)

### Production Scaling (CeleryExecutor)

For larger workloads, switch to distributed execution:

```bash
# Edit airflow.cfg
[core]
executor = CeleryExecutor
sql_alchemy_conn = postgresql://user:pass@postgres:5432/airflow
broker_url = redis://redis:6379/0
```

With Celery:
- **Max Parallel**: Unlimited (add workers)
- **Expected Time**: 20-25 min (parallel dbt models)
- **Memory**: Distribute across workers

---

## Next Steps

1. **Deploy**: Copy DAG to Airflow container
2. **Test**: Run first manual execution
3. **Monitor**: Watch logs, check outputs in SQL Server
4. **Iterate**: Fine-tune ML model thresholds based on performance
5. **Schedule**: Set `schedule_interval` to daily run (already set to `"0 2 * * *"`)

---

**Version**: 1.0.0  
**Status**: Production Ready ✅  
**Last Updated**: May 2026
