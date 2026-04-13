# ATB BI Project

Intelligence Decisionnelle pour la Gestion du Risque Client

This project builds an end-to-end analytics platform for banking clients, from raw CSV files to risk dashboards and ML predictions.

## 1) Goals

The platform answers 3 decision levels:

- Descriptive: current client financial situation (balances, segmentation, account activity)
- Risk and Compliance: clients with KYC/compliance or financial fragility risks
- Predictive: clients likely to churn or become financially vulnerable

## 2) Functional Scope

### Source Data

7 CSV files:

- ACCOUNT
- CUSTOMER
- CURRENCY
- DAO
- INDUSTRY
- SECTOR
- TARGET

### Data Warehouse Design

Constellation model with shared dimensions and 2 fact tables:

- FAIT_ACCOUNT: descriptive account KPIs
- FAIT_CUSTOMER_RISK: compliance risk, financial fragility, behavioral risk, and global risk tiering

Shared dimensions:

- DIM_CUSTOMER
- DIM_BRANCH
- DIM_DATE
- DIM_SECTOR
- DIM_INDUSTRY
- DIM_CURRENCY
- DIM_TARGET
- DIM_RISK_PROFILE

### Risk Outputs

Main outputs from risk layer:

- COMPLIANCE_RISK_INDEX
- FINANCIAL_FRAGILITY_SCORE
- GLOBAL_RISK_SCORE
- RISK_TIER (Low, Medium, High, Very High)

### ML Outputs

- Churn / vulnerability predictions
- Explainability (SHAP)
- Predictions table for reporting: PFE_ML.ML_CUSTOMER_PREDICTIONS

## 3) Technical Stack

- Docker
- Airbyte (ingestion)
- Apache Airflow (orchestration)
- dbt (transformations and modeling)
- Microsoft SQL Server + SSMS
- Python (scikit-learn, XGBoost, SHAP, imbalanced-learn)
- Power BI

## 4) Prerequisites

You already have:

- Python installed
- Docker installed
- SSMS installed

Also required:

- SQL Server instance running and accessible
- Git (recommended)
- VS Code or similar IDE

Recommended versions:

- Python 3.10 or 3.11
- Docker Desktop latest stable
- dbt-core + dbt-sqlserver adapter

## 5) Recommended Project Structure

Create this structure at project root:

```text
atb_bi_project/
├── 1_ingestion/
│   ├── sources/
│   └── destinations/
├── 2_orchestration/
│   ├── dags/
│   └── docker-compose.yml
├── 3_transformation/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── warehouse/
│   │       ├── dimensions/
│   │       └── facts/
│   ├── tests/
│   ├── seeds/
│   ├── dbt_project.yml
│   └── profiles.yml
├── 4_ml/
│   ├── notebooks/
│   ├── src/
│   ├── models/
│   ├── outputs/
│   └── requirements.txt
├── 5_reporting/
│   └── powerbi/
├── data/
│   ├── raw/
│   └── processed/
├── docs/
├── docker-compose.yml
├── .env
└── README.md
```

## 6) Environment Configuration

### 6.1 Move Raw Data

Place your CSV files in:

- data/raw/

(You currently have files in Data/. You can keep them there initially, but standardizing to data/raw/ is recommended.)

### 6.2 Create SQL Server Schemas (SSMS)

Run these SQL statements in your target database:

```sql
CREATE SCHEMA PFE_ODS;
GO
CREATE SCHEMA PFE_DWH;
GO
CREATE SCHEMA PFE_ML;
GO
```

### 6.3 Create Python Virtual Environment

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
```

### 6.4 Install Core Python Packages

```powershell
pip install pandas numpy scikit-learn xgboost shap imbalanced-learn pyodbc sqlalchemy jupyter
pip install dbt-core dbt-sqlserver apache-airflow
```

Note: In production, keep Airflow and Airbyte running with Docker and use Python env mostly for dbt + ML.

## 7) Ingestion Layer (Airbyte -> PFE_ODS)

1. Start Airbyte using Docker Compose.
2. Create source connectors for each CSV dataset.
3. Create SQL Server destination connector.
4. Map each CSV stream to corresponding PFE_ODS table.
5. Run sync and validate row counts in SSMS.

Validation checklist:

- All 7 tables created in PFE_ODS
- Expected row counts loaded
- Correct data types for keys, dates, balances, and flags

## 8) Transformation Layer (dbt)

### 8.1 Configure dbt profile

In profiles.yml, configure SQL Server connection:

- host
- port
- database
- schema (target schemas used by models)
- username/password or trusted auth

### 8.2 Build Sequence

```powershell
cd 3_transformation
$dbtProfiles = (Get-Location).Path
$env:DBT_PROFILES_DIR = $dbtProfiles

dbt debug
dbt deps
dbt seed
dbt run
dbt test
```

### 8.3 Model Flow

- staging: clean ODS raw data
- intermediate: enrich business logic and risk indicators
- warehouse dimensions: conformed dimensions
- warehouse facts:
  - fait_account
  - fait_customer_risk

## 9) Orchestration Layer (Airflow)

Create DAGs in 2_orchestration/dags/:

- dag_ingestion.py
- dag_transformation.py
- dag_ml_pipeline.py
- dag_full_pipeline.py

Recommended order:

1. ingestion DAG
2. transformation DAG
3. ml DAG
4. full pipeline DAG for scheduled runs

Typical full pipeline:

- Trigger Airbyte sync
- Run dbt models/tests
- Run ML training/inference
- Export predictions to PFE_ML

## 10) Machine Learning Layer

### 10.1 Data Extraction

Extract curated dataset from PFE_DWH (especially FAIT_CUSTOMER_RISK + dimensions).

### 10.2 Feature Engineering

Include:

- compliance features (KYC, PEP, flags, restrictions)
- fragility features (balance, salary, employment, dependents, tenure)
- behavioral features (account age, account count, balance-tenure indicators)

### 10.3 Modeling

- Handle class imbalance with SMOTETomek
- Train baseline Random Forest
- Train primary XGBoost
- Evaluate with Accuracy, F1, AUC-ROC, Brier Score

### 10.4 Explainability

- Use SHAP for global and local feature importance

### 10.5 Deployment Output

Write predictions into:

- PFE_ML.ML_CUSTOMER_PREDICTIONS

Include columns such as:

- CUSTOMER_ID
- SCORE_PROBA
- PREDICTION_LABEL
- MODEL_VERSION
- PREDICTION_DATE

## 11) Reporting Layer (Power BI)

Build dashboards on top of PFE_DWH and PFE_ML:

- Descriptive view: balances, segmentation, trends
- Risk view: tier distribution, high-risk segments, branch risk heatmap
- Predictive view: vulnerability probability and churn watchlist

## 12) End-to-End Runbook

Use this order every cycle:

1. Load/refresh CSV files
2. Run Airbyte ingestion to PFE_ODS
3. Run dbt (run + test) to PFE_DWH
4. Run ML pipeline and write to PFE_ML
5. Refresh Power BI dataset

## 13) Minimum Deliverables Checklist

- Airbyte connections configured and reusable
- Airflow DAGs operational
- dbt project with tests and docs
- Constellation schema deployed
- FAIT_CUSTOMER_RISK populated with risk metrics
- ML model trained and evaluated
- SHAP analysis exported
- Predictions table updated
- Power BI dashboard published

## 14) Scientific References

- Chen et al. (2020). Do you know your customer? Bank risk assessment based on machine learning. Applied Soft Computing.
  - https://www.sciencedirect.com/science/article/abs/pii/S1568494619305605
- Vaduva et al. (2024). Improving Churn Detection in the Banking Sector. Electronics, 13(22), 4527.
  - https://www.mdpi.com/2079-9292/13/22/4527
- Ashraf (2024). Bank Customer Churn Prediction Using Machine Learning. Applied Finance and Banking.
  - https://ideas.repec.org/a/spt/apfiba/v14y2024i4f14_4_5.html
- Credit Risk Prediction Using Machine Learning and Deep Learning. Risks, 12(11), 174.
  - https://www.mdpi.com/2227-9091/12/11/174
- Kimball and Ross (2013). Dimensional Modeling Techniques.
  - https://www.kimballgroup.com/wp-content/uploads/2013/08/2013.09-Kimball-Dimensional-Modeling-Techniques11.pdf

## 15) Notes

- Start simple if needed: prioritize SQL Server + dbt + Power BI first, then add Airflow and ML automation.
- Keep business logic (risk score formulas) in dbt SQL for transparency and traceability.
- Version all configs, DAGs, and ML artifacts in Git.
