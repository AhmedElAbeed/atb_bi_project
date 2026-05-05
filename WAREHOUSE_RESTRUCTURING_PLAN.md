# Data Warehouse Professional Restructuring Plan

## Current Problems Identified

### 1. **Duplicate Columns in Staging**
- Tables have duplicate column definitions (e.g., stg_account has account_id listed twice)
- Indicates problems in model definitions or SQL generation

### 2. **Mixed Keys in Fact Tables** ❌
- fact_account contains BOTH surrogate keys (_sk) AND business keys (customer_id, account_officer_id, etc.)
- This violates star schema principles and wastes storage
- Should only have: surrogate keys + measures + dates

### 3. **Intermediate Tables in Warehouse Schema** ❌
- int_account_enriched, int_customer_account_activity, int_customer_enriched, int_customer_risk_score exist in PFE_DWH
- They should be in PFE_INTERMEDIATE (or not materialize at all, be views)
- These confuse reporting tools and add unnecessary data

### 4. **Data Type Issues**
- Risk scores stored as INT - loses decimal precision
- Should be: DECIMAL(10,4)
- Compliance/Financial/Behavioral scores need precision

### 5. **Schema Organization**
- dbt_project.yml doesn't explicitly assign schemas to layers
- Need to mandate: staging→PFE_DWH, intermediate→PFE_INTERMEDIATE, warehouse→PFE_DWH

### 6. **Audit Tables in Warehouse**
- audit_invalid_customer_industry, audit_invalid_customer_target in PFE_DWH
- Should be in separate PFE_AUDIT schema or separate from analytical warehouse

---

## Target Architecture

### Three Schema Structure

```
ATB_BI Database
├── PFE_ODS (Raw Operational Data Store)
│   ├── ODS_ACCOUNT
│   ├── ODS_CUSTOMER
│   ├── ODS_DAO
│   ├── ODS_CURRENCY
│   ├── ODS_SECTOR
│   ├── ODS_INDUSTRY
│   └── ODS_TARGET
│
├── PFE_INTERMEDIATE (Business Logic Layer - VIEWS)
│   ├── int_account_enriched
│   ├── int_customer_account_activity
│   ├── int_customer_enriched
│   └── int_customer_risk_score
│
└── PFE_DWH (Analytical Warehouse - CONSTELLATION MODEL)
    ├── DIMENSIONS (8 tables)
    │   ├── dim_customer (FK-free, dimension attributes only)
    │   ├── dim_dao
    │   ├── dim_currency
    │   ├── dim_sector
    │   ├── dim_industry
    │   ├── dim_target
    │   ├── dim_date
    │   └── dim_risk_profile
    │
    └── FACTS (2 tables)
        ├── fact_account (ONLY: surrogate keys + measures + dates)
        └── fact_customer_risk (ONLY: surrogate keys + measures + dates)
```

### Dimensional Schema Rules

**FACT TABLES (fact_account, fact_customer_risk):**
- Surrogate keys ONLY (customer_sk, dao_sk, currency_sk, etc.) ✓
- NO business keys (customer_id, account_officer_id, currency_code) ✗
- Measures: working_balance, account_count, etc. ✓
- Load dates: load_date_sk, load_date ✓
- Foreign key relationships: dimensional conformity ✓

**DIMENSION TABLES (dim_*):**
- Surrogate key: *_sk (PK) ✓
- Business key: customer_id, currency_code, etc. ✓
- Dimensional attributes: names, codes, flags, etc. ✓
- Slowly changing dimensions: effective_date, end_date (if needed) ✓

---

## Restructuring Checklist

### Phase 1: Fix dbt Configuration
- [ ] Update dbt_project.yml to assign schemas explicitly:
  - staging → materialized: table, schema: PFE_DWH
  - intermediate → materialized: view, schema: PFE_INTERMEDIATE
  - warehouse → materialized: table, schema: PFE_DWH

### Phase 2: Fix Staging Models (11 files)
- [ ] stg_account - Remove duplicate columns
- [ ] stg_customer - Remove duplicate columns
- [ ] stg_customer_resolved - Remove duplicate columns
- [ ] stg_currency - Remove duplicate columns
- [ ] stg_dao - Remove duplicate columns
- [ ] stg_industry - Remove duplicate columns
- [ ] stg_sector - Remove duplicate columns
- [ ] stg_target - Remove duplicate columns

### Phase 3: Configure Intermediate Layer
- [ ] Update dbt_project.yml intermediate materialization: view
- [ ] Keep 4 intermediate models as views:
  - int_account_enriched
  - int_customer_account_activity
  - int_customer_enriched
  - int_customer_risk_score

### Phase 4: Rebuild Dimension Models (8 files)
- [ ] dim_customer - Include all customer attributes, NO business table joins
- [ ] dim_dao - DAO hierarchies
- [ ] dim_currency - Currency details
- [ ] dim_sector - Sector metadata
- [ ] dim_industry - Industry metadata
- [ ] dim_target - Target segments
- [ ] dim_date - Complete date dimension
- [ ] dim_risk_profile - Risk tier definitions

### Phase 5: Rebuild Fact Tables (2 files) - CRITICAL
- [ ] **fact_account** - REMOVE all business keys, keep ONLY:
  - fact_account_sk (PK)
  - customer_sk, dao_sk, currency_sk, sector_sk, industry_sk, target_sk, opening_date_sk, load_date_sk
  - Measures: working_balance, is_negative_balance, account_age_days
  - Dates: opening_date, load_date
  
- [ ] **fact_customer_risk** - REMOVE all business keys, keep ONLY:
  - fact_customer_risk_sk (PK)
  - customer_sk, risk_profile_sk, dao_sk, sector_sk, industry_sk, target_sk, scoring_date_sk, load_date_sk
  - Measures: account_count, total_working_balance, avg_working_balance, etc.
  - Risk scores as DECIMAL(10,4): compliance_risk_index, financial_fragility_score, behavioral_risk_score, global_risk_score
  - Dates: scoring_date, load_date

### Phase 6: Fix Data Types
- [ ] All risk scores: INT → DECIMAL(10,4)
- [ ] Flags: Ensure BIT type (0/1)
- [ ] Dates: DATE type for date fields
- [ ] Balances: DECIMAL(18,2) for currency

### Phase 7: Handle Audit Tables
- [ ] Consider moving audit tables to PFE_AUDIT schema (optional)
- [ ] Or: Keep in separate audit layer, not in analytical warehouse

### Phase 8: Update Schema Documentation
- [ ] Update models/staging/schema.yml
- [ ] Update models/intermediate/schema.yml
- [ ] Update models/warehouse/schema.yml

### Phase 9: Validation
- [ ] Run: dbt parse
- [ ] Run: dbt debug
- [ ] Verify: All 10 dimensions + 2 facts in PFE_DWH
- [ ] Verify: All int_* as views in PFE_INTERMEDIATE
- [ ] Verify: No business keys in fact tables

---

## Benefits of This Structure

✓ **Clean Star Schema** - Facts have only surrogate keys, dimensions have business keys
✓ **Storage Efficiency** - No redundant business key columns on billions of fact rows
✓ **Query Performance** - Simple joins on surrogate keys (integers vs strings)
✓ **Data Integrity** - Referential integrity guaranteed by design
✓ **Reporting** - Power BI sees clean, normalized dimensional model
✓ **Maintainability** - Clear separation of concerns (ODS, logic, analytics)
✓ **Scalability** - Intermediate layer can evolve without touching warehouse
✓ **Professional** - Enterprise-grade data warehouse design

---

## Risk Mitigation

- ⚠️ **Pre-rebuild**: Run full dbt test suite to ensure no regressions
- ⚠️ **During rebuild**: Keep old models as backup, work on copies first
- ⚠️ **Post-rebuild**: Revalidate all Power BI connections
- ⚠️ **Data comparison**: Run record counts before/after for dimensions and facts

