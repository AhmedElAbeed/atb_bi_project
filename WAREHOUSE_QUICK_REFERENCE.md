# ATB BI Warehouse - Quick Reference Guide

## Getting Started

### dbt Commands

```powershell
# Navigate to transformation folder
cd 3_transformation

# Parse all models (validate configuration)
dbt parse --profile atb_bi_transformation --target dev

# Run all models (execute ETL)
dbt run --profile atb_bi_transformation --target dev

# Run only fact tables
dbt run --select fact_* --profile atb_bi_transformation --target dev

# Run with full refresh (drop and recreate)
dbt run --full-refresh --profile atb_bi_transformation --target dev

# Execute tests
dbt test --profile atb_bi_transformation --target dev

# Generate documentation
dbt docs generate --profile atb_bi_transformation --target dev
```

### Database Connection

**Server**: DESKTOP-B0PDEI7,1434  
**Database**: ATB_BI  
**User**: airbyte_user  
**Schema (Warehouse)**: PFE_DWH  
**Schema (Staging)**: PFE_ODS  
**Schema (Intermediate)**: PFE_INTERMEDIATE  

---

## Warehouse Schema Map

### Staging Layer (PFE_ODS)
```
stg_account.sql           → Account master data
stg_customer.sql          → Customer master data
stg_currency.sql          → Currency reference
stg_dao.sql               → Desk Account Officers
stg_sector.sql            → Sector classifications
stg_industry.sql          → Industry classifications
stg_target.sql            → Target classifications
```

### Intermediate Layer (PFE_INTERMEDIATE)
```
int_customer_enriched.sql         → Enriched customer with dimensions
int_account_enriched.sql          → Enriched account with dimensions
int_customer_risk_score.sql       → Risk scoring logic
int_customer_account_activity.sql → Account activity aggregations
```

### Warehouse Layer (PFE_DWH)

#### Dimensions
```
dim_customer       (customer_sk)       → 284,000+ distinct customers
dim_dao            (dao_sk)            → 135+ distinct DAOs
dim_currency       (currency_sk)       → 5+ currencies
dim_sector         (sector_sk)         → 11+ sectors
dim_industry       (industry_sk)       → 25+ industries
dim_target         (target_sk)         → 6+ targets
dim_risk_profile   (risk_profile_sk)   → Risk profiles by customer/date
dim_date           (date_sk)           → Date dimension (1900-2099)
```

#### Facts
```
fact_customer_risk  (136,676 rows)     → Customer risk scores by date
fact_account        (145,284 rows)     → Account balances and details
```

---

## Key Metrics

| Metric | Value | Status |
|--------|-------|--------|
| Total Customers | 102,059 | ✅ |
| Total Accounts | 145,284 | ✅ |
| Risk Profiles | 136,676 | ✅ |
| NULL Foreign Keys | 0 | ✅ Perfect |
| Referential Integrity | 100% | ✅ Perfect |
| UNKNOWN Records | 6 | ✅ All Dimensions |
| dbt Models | 27 | ✅ All Valid |

---

## Common Queries

### Verify Referential Integrity
```sql
USE ATB_BI;

-- Check fact_customer_risk foreign keys
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN customer_sk IS NULL THEN 1 END) as null_customer_sk,
    COUNT(CASE WHEN dao_sk IS NULL THEN 1 END) as null_dao_sk,
    COUNT(CASE WHEN industry_sk IS NULL THEN 1 END) as null_industry_sk,
    COUNT(CASE WHEN target_sk IS NULL THEN 1 END) as null_target_sk
FROM PFE_DWH.fact_customer_risk;

-- Check fact_account foreign keys
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN customer_sk IS NULL THEN 1 END) as null_customer_sk,
    COUNT(CASE WHEN dao_sk IS NULL THEN 1 END) as null_dao_sk,
    COUNT(CASE WHEN currency_sk IS NULL THEN 1 END) as null_currency_sk
FROM PFE_DWH.fact_account;
```

### View UNKNOWN Records
```sql
use ATB_BI;

-- UNKNOWN customers
SELECT * FROM PFE_DWH.dim_customer WHERE customer_id = -1;

-- UNKNOWN DAOs
SELECT * FROM PFE_DWH.dim_dao WHERE account_officer_id = -1;

-- UNKNOWN Currency
SELECT * FROM PFE_DWH.dim_currency WHERE currency_code = 'UNK';
```

### Risk Distribution
```sql
USE ATB_BI;

SELECT 
    risk_tier,
    COUNT(*) as customer_count,
    CAST(AVG(global_risk_score) as DECIMAL(10,4)) as avg_risk_score
FROM PFE_DWH.fact_customer_risk
WHERE risk_tier <> 'UNKNOWN'
GROUP BY risk_tier
ORDER BY avg_risk_score DESC;
```

---

## Troubleshooting

### Issue: dbt command not found
**Solution**: Ensure Python environment is activated and dbt-core is installed
```powershell
pip install dbt-core dbt-sqlserver
```

### Issue: Cannot connect to database
**Solution**: Verify connection string and credentials in `profiles.yml`
```powershell
Verify: Server=DESKTOP-B0PDEI7,1434 and user=airbyte_user
```

### Issue: Model fails after changes
**Solution**: Run full refresh to rebuild from staging
```powershell
dbt run --full-refresh --select fact_*
```

---

## File Locations

📁 Project Root: `c:\Users\Ahmed\Desktop\atb_bi_project\`

- **Configuration**: `3_transformation/dbt_project.yml`, `profiles.yml`
- **Models**: `3_transformation/models/{staging,intermediate,warehouse}/`
- **Tests**: `3_transformation/tests/`
- **Documentation**: `PRODUCTION_VALIDATION_REPORT.md`, `README_ATB_BI_PROJECT.md`
- **Data**: `data/{raw,processed}/`
- **Orchestration**: `2_orchestration/dags/` (Airflow)
- **ML**: `4_ml/{models,notebooks,src}/`
- **Reporting**: `5_reporting/powerbi/`

---

## Support & Documentation

- **dbt Documentation**: Run `dbt docs generate` then `dbt docs serve`
- **Model Dependencies**: View in `target/graph.gpickle`
- **Test Reports**: Check `target/run_results.json`
- **Query Logs**: See `logs/query_log.sql`

---

**Last Updated**: 2025 | **Warehouse Version**: 1.0.0 | **Status**: Production Ready ✅
