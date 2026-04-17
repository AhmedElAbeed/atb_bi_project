# ATB BI Project — Vision Globale sur la Clientèle Bancaire
## Full Technical Documentation

**Database:** `atb_bi`
**Stack:** Airbyte · dbt · SQL Server (SSMS) · Python · Power BI
**Schema architecture:** Constellation model (2 fact tables)
**Author:** Abdelmajid Sbouri
**Institution:** Faculté des Sciences Économiques et de Gestion de Nabeul — Université de Carthage
**Internship host:** Arab Tunisian Bank (ATB)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Database State — What is in SSMS Right Now](#3-database-state)
4. [dbt Project Structure](#4-dbt-project-structure)
5. [Layer 1 — Staging Models](#5-layer-1--staging-models)
6. [Layer 2 — Intermediate Models](#6-layer-2--intermediate-models)
7. [Layer 3 — Warehouse Models (Dimensions)](#7-layer-3--warehouse-models-dimensions)
8. [Layer 3 — Warehouse Models (Fact Tables)](#8-layer-3--warehouse-models-fact-tables)
9. [Constellation Schema](#9-constellation-schema)
10. [dbt Tests and Data Quality](#10-dbt-tests-and-data-quality)
11. [Running the Pipeline](#11-running-the-pipeline)
12. [ML Layer Overview](#12-ml-layer-overview)
13. [Power BI Connection](#13-power-bi-connection)

---

## 1. Project Overview

This project builds a full Business Intelligence platform for the Arab Tunisian Bank
(ATB) covering two analytical domains:

**Domain 1 — Customer Portfolio Analytics**
Descriptive analysis of the bank's client base: account balances, segmentation by
sector, industry, nationality, employment status, and agency performance.

**Domain 2 — Customer Risk and Compliance Analytics**
Risk profiling of each client using KYC data, compliance flags, PEP status, and
financial fragility indicators. This domain was entirely absent from prior work on
the same dataset and represents the core contribution of this project.

The data warehouse uses a **constellation schema** — two fact tables sharing common
dimensions — which allows both analytical domains to be explored independently or
in combination through Power BI.

Current implementation status: the active dbt pipeline is focused on staging and
intermediate cleaning. The warehouse dimensions and fact tables are documented as
the target end-state, but they should only be rebuilt after source cleansing and
key quality checks are stable.

---

## 2. Architecture

```
RAW DATA (7 CSV files)
         │
         ▼
┌─────────────────────┐
│   Airbyte (Docker)  │  ← extracts CSVs, loads into SSMS
└─────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  atb_bi database — PFE_ODS schema       │
│                                         │
│  ods_account    ods_customer            │
│  ods_currency   ods_dao                 │
│  ods_industry   ods_sector              │
│  ods_target     ods_rejet               │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────┐
│   dbt (VS Code)     │  ← cleans, transforms, builds DWH
└─────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  atb_bi database — PFE_DWH schema                       │
│                                                         │
│  DIMENSIONS                  FACT TABLES                │
│  dim_customer                fait_account               │
│  dim_branch                  fait_customer_risk (NEW)   │
│  dim_currency                                           │
│  dim_sector                                             │
│  dim_industry                                           │
│  dim_target                                             │
│  dim_date                                               │
│  dim_risk_profile (NEW)                                 │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  atb_bi database — PFE_ML       │
│                                 │
│  ml_customer_predictions        │
│  ml_feature_importance          │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────┐
│   Power BI          │  ← connects to PFE_DWH + PFE_ML
└─────────────────────┘
```

---

## 3. Database State

### Current state after Airbyte ingestion

Database: `atb_bi`

```sql
-- verify all ODS tables loaded correctly
USE atb_bi;

SELECT
    t.name        AS table_name,
    s.name        AS schema_name,
    p.rows        AS row_count
FROM sys.tables t
JOIN sys.schemas s    ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE s.name = 'PFE_ODS'
  AND p.index_id IN (0,1)
ORDER BY t.name;
```

Expected result:

| table_name   | row_count |
|--------------|-----------|
| ods_account  | ~78 000   |
| ods_customer | ~137 000  |
| ods_currency | ~20       |
| ods_dao      | ~136      |
| ods_industry | ~100      |
| ods_sector   | ~15       |
| ods_target   | ~11       |

### Create schemas if not already done

```sql
USE atb_bi;
CREATE SCHEMA PFE_ODS;
CREATE SCHEMA PFE_DWH;
CREATE SCHEMA PFE_ML;
```

---

## 4. dbt Project Structure

```
3_transformation/
│
├── dbt_project.yml                  ← project config, schema assignments
├── profiles.yml                     ← SQL Server connection (NOT committed to git)
│
├── models/
│   │
│   ├── staging/                     ← Layer 1: clean raw ODS data
│   │   ├── stg_account.sql
│   │   ├── stg_customer.sql
│   │   ├── stg_currency.sql
│   │   ├── stg_dao.sql
│   │   ├── stg_industry.sql
│   │   ├── stg_sector.sql
│   │   ├── stg_target.sql
│   │   └── schema.yml               ← column docs + basic tests
│   │
│   ├── intermediate/                ← Layer 2: business logic + enrichment
│   │   ├── int_customer_enriched.sql
│   │   ├── int_account_enriched.sql
│   │   ├── int_customer_risk_score.sql
│   │   └── schema.yml
│   │
│   └── warehouse/                   ← Layer 3: final DWH tables (target design)
│       ├── dimensions/
│       │   ├── dim_customer.sql
│       │   ├── dim_branch.sql
│       │   ├── dim_currency.sql
│       │   ├── dim_sector.sql
│       │   ├── dim_industry.sql
│       │   ├── dim_target.sql
│       │   ├── dim_date.sql
│       │   └── dim_risk_profile.sql
│       ├── facts/
│       │   ├── fait_account.sql
│       │   └── fait_customer_risk.sql
│       └── schema.yml
│
├── tests/
│   ├── test_balance_not_null.sql
│   ├── test_risk_score_range.sql
│   └── test_no_orphan_accounts.sql
│
└── seeds/
    └── risk_tier_mapping.csv        ← maps score ranges to Low/Medium/High/Critical
```

### profiles.yml — SQL Server connection

### dbt_project.yml

```yaml
name: "atb_bi_project"
version: "1.0.0"
config-version: 2

profile: "atb_bi_project"

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]

models:
  atb_bi_project:
    staging:
      +schema: PFE_ODS_STAGING
      +materialized: view
    intermediate:
      +schema: PFE_INTERMEDIATE
      +materialized: view
    warehouse:
      +schema: PFE_DWH
      +materialized: table
      dimensions:
        +materialized: table
      facts:
        +materialized: table
```

> **Why views for staging and intermediate, tables for warehouse?**
> Staging and intermediate models are just transformations — no need to
> store them physically. The final warehouse tables are materialized as
> real tables because Power BI and the ML model need to query them fast.

> **Current working rule:** staging is the only place where raw data is cleaned
> and standardized right now. Warehouse tables stay paused until the source data
> is stable enough to support reliable dimension and fact loads.

---

## 5. Layer 1 — Staging Models

The staging layer does three things only:
- Rename columns to a clean consistent standard
- Cast data types correctly
- Add a `loaded_at` timestamp for traceability

No business logic here. No joins. No filtering beyond removing completely
empty rows.

The staging layer also preserves source truth:
- valid numeric zeros stay as zeros
- blank text values become `NULL`
- missing source values are not invented or backfilled here
- business rules and risk scoring belong in the intermediate layer

This means a blank posting restriction code is expected when the source record
does not contain one, while a working balance of `0` is preserved as a real zero
if that is what the source system loaded.

---

### stg_account.sql

```sql
-- models/staging/stg_account.sql
-- Source: PFE_ODS.ods_account
-- One row per bank account

with source as (
    select * from atb_bi.PFE_ODS.ods_account
),

cleaned as (
    select
        -- identifiers
        cast(RECID         as bigint)       as account_id,
        cast(CUSTOMER_NO   as bigint)       as customer_code,

        -- attributes
        cast(CATEGORY      as varchar(100)) as account_category,
        cast(CURRENCY      as varchar(10))  as currency_code,
        cast(ACCOUNT_OFFICER as int)        as account_officer_code,

        -- measures
        cast(WORKING_BALANCE as float)      as working_balance,

        -- dates
        try_cast(
            concat(
                substring(cast(OPENING_DATE as varchar), 1, 4), '-',
                substring(cast(OPENING_DATE as varchar), 5, 2), '-',
                substring(cast(OPENING_DATE as varchar), 7, 2)
            )
        as date)                            as opening_date,

        -- metadata
        getdate()                           as loaded_at

    from source
    where RECID is not null
      and CUSTOMER_NO is not null
)

select * from cleaned
```

> **Note on OPENING_DATE:** The raw data stores dates as integers
> like `20211019`. The cast above converts this to a proper DATE type
> `2021-10-19` by splitting the string into year-month-day parts.

---

### stg_customer.sql

```sql
-- models/staging/stg_customer.sql
-- Source: PFE_ODS.ods_customer
-- One row per bank customer

with source as (
    select * from atb_bi.PFE_ODS.ods_customer
),

cleaned as (
    select
        -- identifiers
        cast(CUSTOMER_CODE    as bigint)       as customer_code,
        cast(ACCOUNT_OFFICER  as int)          as account_officer_code,

        -- demographic attributes
        nullif(trim(cast(NATIONALITY      as varchar(10))),  '') as nationality,
        nullif(trim(cast(RESIDENCE        as varchar(10))),  '') as residence,
        nullif(trim(cast(TITLE            as varchar(20))),  '') as title,
        nullif(trim(cast(GENDER           as varchar(20))),  '') as gender,
        nullif(trim(cast(MARITAL_STATUS   as varchar(50))),  '') as marital_status,
        nullif(trim(cast(EMPLOYMENT_STATUS as varchar(100))),'') as employment_status,
        nullif(trim(cast(OCCUPATION       as varchar(100))),'') as occupation,
        nullif(trim(cast(JOB_TITLE        as varchar(100))),'') as job_title,
        nullif(trim(cast(RESIDENCE_STATUS as varchar(100))),'') as residence_status,

        -- numeric attributes
        try_cast(NO_OF_DEPENDENTS as int)   as no_of_dependents,
        try_cast(SALARY           as float) as salary,

        -- segmentation foreign keys
        try_cast(SECTOR   as int) as sector_code,
        try_cast(INDUSTRY as int) as industry_code,
        try_cast(TARGET   as int) as target_code,

        -- KYC and compliance fields (used in risk fact table)
        nullif(trim(cast(KYC_COMPLETE     as varchar(10))),  '') as kyc_complete,
        try_cast(L_SCORE_KYC  as int)                           as kyc_score_raw,
        nullif(trim(cast(L_PEP            as varchar(10))),  '') as pep_flag,
        nullif(trim(cast(L_FLAG_CONF      as varchar(10))),  '') as compliance_flag,
        nullif(trim(cast(L_DECS_CONF      as varchar(200))), '') as compliance_description,
        nullif(trim(cast(POSTING_RESTRICT_46 as varchar(50))),'') as posting_restriction,
        nullif(trim(cast(L_NATURE_CLIENT  as varchar(50))),  '') as client_nature,
        nullif(trim(cast(SEGMENT          as varchar(50))),  '') as segment,

        -- dates
        try_cast(
            concat(
                substring(cast(CUSTOMER_SINCE as varchar), 1, 4), '-',
                substring(cast(CUSTOMER_SINCE as varchar), 5, 2), '-',
                substring(cast(CUSTOMER_SINCE as varchar), 7, 2)
            )
        as date)                                                 as customer_since,

        try_cast(LAST_KYC_REVIEW_DATE       as date)            as last_kyc_review_date,
        try_cast(AUTO_NEXT_KYC_REVIEW_DATE  as date)            as next_kyc_review_date,

        -- metadata
        getdate() as loaded_at

    from source
    where CUSTOMER_CODE is not null
)

select * from cleaned
```

---

### stg_currency.sql

```sql
-- models/staging/stg_currency.sql

with source as (
    select * from atb_bi.PFE_ODS.ods_currency
),

cleaned as (
    select
        cast(CURRENCY_CODE    as varchar(10))  as currency_code,
        cast(NUMERIC_CCY_CODE as int)          as numeric_currency_code,
        cast(CCY_NAME         as varchar(100)) as currency_name,
        cast(NO_OF_DECIMALS   as int)          as decimal_places,
        getdate()                              as loaded_at
    from source
    where CURRENCY_CODE is not null
)

select * from cleaned
```

---

### stg_dao.sql

```sql
-- models/staging/stg_dao.sql
-- DAO = Department/Agency Officers table

with source as (
    select * from atb_bi.PFE_ODS.ods_dao
),

cleaned as (
    select
        cast(ACCOUNT_OFFICER as int)          as account_officer_code,
        cast(AREA            as varchar(100)) as area,
        cast(NAME            as varchar(100)) as branch_name,
        cast(DEPT_PARENT     as int)          as parent_department_code,
        getdate()                             as loaded_at
    from source
    where ACCOUNT_OFFICER is not null
)

select * from cleaned
```

---

### stg_industry.sql

```sql
-- models/staging/stg_industry.sql

with source as (
    select * from atb_bi.PFE_ODS.ods_industry
),

cleaned as (
    select
        cast(INDUSTRY_CODE as int)          as industry_code,
        cast(DESCRIPTION   as varchar(200)) as industry_description,
        getdate()                           as loaded_at
    from source
    where INDUSTRY_CODE is not null
)

select * from cleaned
```

---

### stg_sector.sql

```sql
-- models/staging/stg_sector.sql

with source as (
    select * from atb_bi.PFE_ODS.ods_sector
),

cleaned as (
    select
        cast(SECTOR_CODE as int)          as sector_code,
        cast(DESCRIPTION as varchar(200)) as sector_description,
        getdate()                         as loaded_at
    from source
    where SECTOR_CODE is not null
)

select * from cleaned
```

---

### stg_target.sql

```sql
-- models/staging/stg_target.sql

with source as (
    select * from atb_bi.PFE_ODS.ods_target
),

cleaned as (
    select
        cast(TARGET_CODE as int)          as target_code,
        cast(DESCRIPTION as varchar(200)) as target_description,
        getdate()                         as loaded_at
    from source
    where TARGET_CODE is not null
)

select * from cleaned
```

---

## 6. Layer 2 — Intermediate Models

The intermediate layer applies business logic. It joins staging models
together and computes derived fields that the warehouse layer will use.

---

### int_customer_enriched.sql

```sql
-- models/intermediate/int_customer_enriched.sql
-- Joins customer with their sector, industry, target and branch labels
-- Computes tenure in days

with customers as (
    select * from {{ ref('stg_customer') }}
),

sectors as (
    select * from {{ ref('stg_sector') }}
),

industries as (
    select * from {{ ref('stg_industry') }}
),

targets as (
    select * from {{ ref('stg_target') }}
),

branches as (
    select * from {{ ref('stg_dao') }}
),

enriched as (
    select
        c.customer_code,
        c.account_officer_code,
        c.nationality,
        c.residence,
        c.title,
        c.gender,
        c.marital_status,
        c.employment_status,
        c.occupation,
        c.job_title,
        c.residence_status,
        c.no_of_dependents,
        c.salary,
        c.segment,
        c.client_nature,
        c.customer_since,
        c.last_kyc_review_date,
        c.next_kyc_review_date,

        -- KYC and compliance fields passed through
        c.kyc_complete,
        c.kyc_score_raw,
        c.pep_flag,
        c.compliance_flag,
        c.compliance_description,
        c.posting_restriction,

        -- labels from dimension tables
        s.sector_description,
        i.industry_description,
        t.target_description,
        b.branch_name,
        b.area,

        -- foreign keys for DWH
        c.sector_code,
        c.industry_code,
        c.target_code,

        -- computed: how long has this person been a client
        datediff(day, c.customer_since, getdate()) as tenure_days,

        -- computed: is KYC overdue
        case
            when c.next_kyc_review_date < getdate() then 1
            else 0
        end as is_kyc_overdue

    from customers c
    left join sectors    s on c.sector_code          = s.sector_code
    left join industries i on c.industry_code        = i.industry_code
    left join targets    t on c.target_code          = t.target_code
    left join branches   b on c.account_officer_code = b.account_officer_code
)

select * from enriched
```

---

### int_account_enriched.sql

```sql
-- models/intermediate/int_account_enriched.sql
-- Joins accounts with their currency info and customer profile
-- Computes account-level derived metrics

with accounts as (
    select * from {{ ref('stg_account') }}
),

currencies as (
    select * from {{ ref('stg_currency') }}
),

customers as (
    select * from {{ ref('int_customer_enriched') }}
),

enriched as (
    select
        a.account_id,
        a.customer_code,
        a.account_category,
        a.currency_code,
        a.account_officer_code,
        a.working_balance,
        a.opening_date,

        -- currency label
        cur.currency_name,

        -- computed account metrics
        datediff(day, a.opening_date, getdate()) as account_age_days,

        case
            when a.working_balance < 0  then 1
            else 0
        end as is_overdraft,

        case
            when a.working_balance < 0  then a.working_balance
            else 0
        end as overdraft_amount,

        case
            when a.working_balance >= 0 then a.working_balance
            else 0
        end as credit_balance,

        -- customer context
        c.sector_code,
        c.industry_code,
        c.target_code,
        c.branch_name,
        c.area,
        c.nationality,
        c.employment_status,
        c.tenure_days

    from accounts a
    left join currencies cur on a.currency_code          = cur.currency_code
    left join customers  c   on a.customer_code          = c.customer_code
)

select * from enriched
```

---

### int_customer_risk_score.sql

```sql
-- models/intermediate/int_customer_risk_score.sql
-- Computes the three risk pillars and the global composite risk score
-- Scientific basis:
--   Pillar 1 (compliance): McKinsey KYC Risk Framework (2019)
--   Pillar 2 (financial):  Dumitrescu et al. (2022), Journal of Banking and Finance
--   Pillar 3 (behavioral): Hamori et al. (2018), Risks journal

with customers as (
    select * from {{ ref('int_customer_enriched') }}
),

accounts as (
    select
        customer_code,
        count(*)                          as account_count,
        sum(working_balance)              as total_balance,
        min(working_balance)              as min_balance,
        max(account_age_days)             as max_account_age,
        sum(case when is_overdraft = 1
                 then 1 else 0 end)       as overdraft_account_count
    from {{ ref('int_account_enriched') }}
    group by customer_code
),

risk_computed as (
    select
        c.customer_code,
        c.account_officer_code,
        c.sector_code,
        c.industry_code,
        c.target_code,
        c.tenure_days,
        c.kyc_score_raw,
        c.pep_flag,
        c.compliance_flag,
        c.posting_restriction,
        c.kyc_complete,
        c.is_kyc_overdue,
        c.employment_status,
        c.salary,
        c.no_of_dependents,

        a.account_count,
        a.total_balance,
        a.overdraft_account_count,

        -- ─────────────────────────────────────────────────────
        -- PILLAR 1: Compliance Risk Index (max = 1.0)
        -- Weights based on McKinsey KYC Risk Framework (2019)
        -- ─────────────────────────────────────────────────────
        round(
            (
                -- KYC score normalized to 0-0.3 range (score 1-10)
                (coalesce(c.kyc_score_raw, 5) - 1.0) / 9.0 * 0.30

                -- PEP status: +0.25 if PEP
              + case when upper(c.pep_flag) = 'OUI' then 0.25 else 0.0 end

                -- Compliance flag: +0.20 if ever flagged
              + case when upper(c.compliance_flag) = 'OUI' then 0.20 else 0.0 end

                -- Posting restriction: +0.15 if account is restricted
              + case when c.posting_restriction is not null
                          and len(trim(c.posting_restriction)) > 0
                     then 0.15 else 0.0 end

                -- KYC incomplete: +0.10 if not complete
              + case when upper(c.kyc_complete) != 'YES'
                          or c.kyc_complete is null
                     then 0.10 else 0.0 end
            )
        , 4) as compliance_risk_index,

        -- ─────────────────────────────────────────────────────
        -- PILLAR 2: Financial Fragility Score (max = 1.0)
        -- Weights based on Dumitrescu et al. (2022)
        -- ─────────────────────────────────────────────────────
        round(
            (
                -- Overdraft is the strongest signal: +0.40
                case when a.overdraft_account_count > 0 then 0.40 else 0.0 end

                -- Employment instability: ETUDIANT/SANS EMPLOI = higher risk
              + case
                    when upper(c.employment_status) like '%SANS EMPLOI%' then 0.25
                    when upper(c.employment_status) like '%ETUDIANT%'    then 0.15
                    when upper(c.employment_status) like '%OTHER%'       then 0.10
                    else 0.0
                end

                -- Missing salary = unknown financial capacity
              + case when c.salary is null or c.salary = 0 then 0.15 else 0.0 end

                -- High dependent load relative to salary
              + case
                    when c.no_of_dependents > 4
                     and (c.salary is null or c.salary < 1000)
                    then 0.20
                    when c.no_of_dependents > 2
                     and (c.salary is null or c.salary < 500)
                    then 0.10
                    else 0.0
                end
            )
        , 4) as financial_fragility_score,

        -- ─────────────────────────────────────────────────────
        -- PILLAR 3: Behavioral Risk Score (max = 1.0)
        -- Based on Hamori et al. (2018)
        -- ─────────────────────────────────────────────────────
        round(
            (
                -- Very new customer (< 6 months): less known = higher risk
                case
                    when c.tenure_days < 180  then 0.30
                    when c.tenure_days < 365  then 0.15
                    else 0.0
                end

                -- Multiple accounts with total negative balance
              + case
                    when a.account_count > 2
                     and a.total_balance < 0
                    then 0.30
                    when a.account_count > 1
                     and a.total_balance < 0
                    then 0.20
                    else 0.0
                end

                -- KYC overdue for an existing customer
              + case when c.is_kyc_overdue = 1 then 0.40 else 0.0 end
            )
        , 4) as behavioral_risk_score

    from customers c
    left join accounts a on c.customer_code = a.customer_code
),

final as (
    select
        *,

        -- ─────────────────────────────────────────────────────
        -- GLOBAL RISK SCORE: weighted average of 3 pillars
        -- Compliance 40% + Financial 35% + Behavioral 25%
        -- ─────────────────────────────────────────────────────
        round(
            (compliance_risk_index   * 0.40)
          + (financial_fragility_score * 0.35)
          + (behavioral_risk_score   * 0.25)
        , 4) as global_risk_score,

        -- ─────────────────────────────────────────────────────
        -- RISK TIER: human-readable classification
        -- ─────────────────────────────────────────────────────
        case
            when (compliance_risk_index * 0.40)
               + (financial_fragility_score * 0.35)
               + (behavioral_risk_score * 0.25) >= 0.70 then 'CRITICAL'
            when (compliance_risk_index * 0.40)
               + (financial_fragility_score * 0.35)
               + (behavioral_risk_score * 0.25) >= 0.45 then 'HIGH'
            when (compliance_risk_index * 0.40)
               + (financial_fragility_score * 0.35)
               + (behavioral_risk_score * 0.25) >= 0.20 then 'MEDIUM'
            else 'LOW'
        end as risk_tier

    from risk_computed
)

select * from final
```

---

## 7. Layer 3 — Warehouse Models (Dimensions)

---

### dim_customer.sql

```sql
-- models/warehouse/dimensions/dim_customer.sql

with source as (
    select * from {{ ref('int_customer_enriched') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_code']) }} as id_dim_customer,
    customer_code,
    nationality,
    residence,
    title,
    gender,
    marital_status,
    employment_status,
    occupation,
    job_title,
    residence_status,
    no_of_dependents,
    segment,
    client_nature,
    customer_since,
    tenure_days,
    last_kyc_review_date,
    next_kyc_review_date,
    getdate() as dbt_updated_at
from source
```

---

### dim_branch.sql

```sql
-- models/warehouse/dimensions/dim_branch.sql

with source as (
    select * from {{ ref('stg_dao') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['account_officer_code']) }} as id_dim_branch,
    account_officer_code,
    branch_name,
    area,
    parent_department_code,
    getdate() as dbt_updated_at
from source
```

---

### dim_currency.sql

```sql
-- models/warehouse/dimensions/dim_currency.sql

with source as (
    select * from {{ ref('stg_currency') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as id_dim_currency,
    currency_code,
    numeric_currency_code,
    currency_name,
    decimal_places,
    getdate() as dbt_updated_at
from source
```

---

### dim_sector.sql

```sql
-- models/warehouse/dimensions/dim_sector.sql

with source as (
    select * from {{ ref('stg_sector') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['sector_code']) }} as id_dim_sector,
    sector_code,
    sector_description,
    getdate() as dbt_updated_at
from source
```

---

### dim_industry.sql

```sql
-- models/warehouse/dimensions/dim_industry.sql

with source as (
    select * from {{ ref('stg_industry') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['industry_code']) }} as id_dim_industry,
    industry_code,
    industry_description,
    getdate() as dbt_updated_at
from source
```

---

### dim_target.sql

```sql
-- models/warehouse/dimensions/dim_target.sql

with source as (
    select * from {{ ref('stg_target') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['target_code']) }} as id_dim_target,
    target_code,
    target_description,
    getdate() as dbt_updated_at
from source
```

---

### dim_risk_profile.sql

```sql
-- models/warehouse/dimensions/dim_risk_profile.sql
-- NEW dimension — stores the categorical risk attributes of each customer
-- This is the dimension exclusive to fait_customer_risk

with source as (
    select * from {{ ref('int_customer_risk_score') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_code']) }} as id_dim_risk_profile,
    customer_code,

    -- raw compliance attributes
    kyc_score_raw,
    pep_flag,
    compliance_flag,
    posting_restriction,
    kyc_complete,
    is_kyc_overdue,

    -- computed scores
    compliance_risk_index,
    financial_fragility_score,
    behavioral_risk_score,
    global_risk_score,
    risk_tier,

    getdate() as dbt_updated_at
from source
```

---

### dim_date.sql

```sql
-- models/warehouse/dimensions/dim_date.sql
-- Generates a complete date dimension from 2015 to 2030

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2015-01-01' as date)",
        end_date   = "cast('2030-12-31' as date)"
    ) }}
),

final as (
    select
        cast(date_day as date)                          as date,
        year(date_day)                                  as year,
        month(date_day)                                 as month_number,
        datename(month, date_day)                       as month_name,
        datepart(quarter, date_day)                     as quarter,
        'Q' + cast(datepart(quarter, date_day) as varchar) as quarter_label,
        datepart(week, date_day)                        as week_number,
        datename(weekday, date_day)                     as day_name,
        datepart(dayofyear, date_day)                   as day_of_year,
        cast(year(date_day) as varchar)
            + '-'
            + datename(month, date_day)                 as year_month_label
    from date_spine
)

select * from final
```

---

## 8. Layer 3 — Warehouse Models (Fact Tables)

---

### fait_account.sql

```sql
-- models/warehouse/facts/fait_account.sql
-- Central fact table for account-level financial analysis
-- Answers: what is the financial state of each account?

with accounts as (
    select * from {{ ref('int_account_enriched') }}
),

dim_customer as (
    select id_dim_customer, customer_code
    from {{ ref('dim_customer') }}
),

dim_branch as (
    select id_dim_branch, account_officer_code
    from {{ ref('dim_branch') }}
),

dim_currency as (
    select id_dim_currency, currency_code
    from {{ ref('dim_currency') }}
),

dim_sector as (
    select id_dim_sector, sector_code
    from {{ ref('dim_sector') }}
),

dim_industry as (
    select id_dim_industry, industry_code
    from {{ ref('dim_industry') }}
),

dim_target as (
    select id_dim_target, target_code
    from {{ ref('dim_target') }}
)

select
    -- surrogate key
    {{ dbt_utils.generate_surrogate_key(['a.account_id']) }} as id_fait_account,

    -- foreign keys to dimensions
    dc.id_dim_customer,
    db.id_dim_branch,
    dcu.id_dim_currency,
    ds.id_dim_sector,
    di.id_dim_industry,
    dt.id_dim_target,
    a.opening_date,

    -- measures
    a.working_balance,
    a.is_overdraft,
    a.overdraft_amount,
    a.credit_balance,
    a.account_age_days,
    a.account_count,

    -- metadata
    getdate() as dbt_updated_at

from accounts a
left join dim_customer dc  on a.customer_code          = dc.customer_code
left join dim_branch   db  on a.account_officer_code   = db.account_officer_code
left join dim_currency dcu on a.currency_code          = dcu.currency_code
left join dim_sector   ds  on a.sector_code            = ds.sector_code
left join dim_industry di  on a.industry_code          = di.industry_code
left join dim_target   dt  on a.target_code            = dt.target_code
```

---

### fait_customer_risk.sql

```sql
-- models/warehouse/facts/fait_customer_risk.sql
-- Second fact table — customer risk and compliance analysis
-- Answers: how risky is each customer and why?
-- Scientific basis: Hamori et al. (2018), Dumitrescu et al. (2022)

with risk as (
    select * from {{ ref('int_customer_risk_score') }}
),

dim_customer as (
    select id_dim_customer, customer_code
    from {{ ref('dim_customer') }}
),

dim_branch as (
    select id_dim_branch, account_officer_code
    from {{ ref('dim_branch') }}
),

dim_sector as (
    select id_dim_sector, sector_code
    from {{ ref('dim_sector') }}
),

dim_industry as (
    select id_dim_industry, industry_code
    from {{ ref('dim_industry') }}
),

dim_target as (
    select id_dim_target, target_code
    from {{ ref('dim_target') }}
),

dim_risk_profile as (
    select id_dim_risk_profile, customer_code
    from {{ ref('dim_risk_profile') }}
)

select
    -- surrogate key
    {{ dbt_utils.generate_surrogate_key(['r.customer_code']) }} as id_fait_customer_risk,

    -- foreign keys to shared dimensions
    dc.id_dim_customer,
    db.id_dim_branch,
    ds.id_dim_sector,
    di.id_dim_industry,
    dt.id_dim_target,

    -- foreign key to exclusive dimension
    drp.id_dim_risk_profile,

    -- date (snapshot date — when this risk score was computed)
    cast(getdate() as date) as risk_snapshot_date,

    -- Pillar 1: compliance measures
    r.kyc_score_raw,
    case when upper(r.pep_flag)         = 'OUI' then 1 else 0 end as is_pep,
    case when upper(r.compliance_flag)  = 'OUI' then 1 else 0 end as is_flagged_compliance,
    case when r.posting_restriction is not null
              and len(trim(r.posting_restriction)) > 0
         then 1 else 0 end                                         as has_posting_restriction,
    case when upper(r.kyc_complete) != 'YES'
              or r.kyc_complete is null
         then 1 else 0 end                                         as kyc_incomplete_flag,
    r.is_kyc_overdue,
    r.compliance_risk_index,

    -- Pillar 2: financial fragility measures
    r.overdraft_account_count,
    r.total_balance,
    r.financial_fragility_score,

    -- Pillar 3: behavioral measures
    r.tenure_days,
    r.account_count,
    r.behavioral_risk_score,

    -- final composite output
    r.global_risk_score,
    r.risk_tier,

    -- metadata
    getdate() as dbt_updated_at

from risk r
left join dim_customer     dc  on r.customer_code          = dc.customer_code
left join dim_branch       db  on r.account_officer_code   = db.account_officer_code
left join dim_sector       ds  on r.sector_code            = ds.sector_code
left join dim_industry     di  on r.industry_code          = di.industry_code
left join dim_target       dt  on r.target_code            = dt.target_code
left join dim_risk_profile drp on r.customer_code          = drp.customer_code
```

---

## 9. Constellation Schema

```
                    ┌─────────────────┐
                    │  DIM_CUSTOMER   │
                    │─────────────────│
                    │ id_dim_customer │
                    │ customer_code   │
                    │ nationality     │
                    │ gender          │
                    │ employment_status│
                    │ segment         │
                    │ tenure_days     │
                    └────────┬────────┘
                             │ shared
            ┌────────────────┼────────────────┐
            │                │                │
┌───────────▼──────┐         │    ┌───────────▼──────────┐
│  FAIT_ACCOUNT    │         │    │  FAIT_CUSTOMER_RISK   │
│──────────────────│         │    │──────────────────────│
│ id_dim_customer  │◄────────┘    │ id_dim_customer       │
│ id_dim_branch    │◄────────┐    │ id_dim_branch         │◄───┐
│ id_dim_currency  │◄──┐     └────│ id_dim_sector         │◄──┐│
│ id_dim_sector    │◄──┤          │ id_dim_industry       │◄─┐││
│ id_dim_industry  │◄──┤          │ id_dim_target         │◄┐│││
│ id_dim_target    │◄──┤          │ id_dim_risk_profile   │ ││││
│ opening_date     │   │          │ risk_snapshot_date    │ ││││
│ working_balance  │   │          │ global_risk_score     │ ││││
│ is_overdraft     │   │          │ risk_tier             │ ││││
│ account_age_days │   │          │ compliance_risk_index │ ││││
└──────────────────┘   │          │ financial_fragility   │ ││││
                        │          └───────────────────────┘ ││││
        ┌───────────────┘                                      ││││
        │  ┌──────────────────────────────────────────────────┘│││
        │  │  ┌─────────────────────────────────────────────────┘││
        │  │  │  ┌──────────────────────────────────────────────┘│
        │  │  │  │  ┌────────────────────────────────────────────┘
        │  │  │  │  │
  ┌─────▼──┐ │  │  │ ┌──────────────┐    ┌──────────────────┐
  │DIM_    │ │  │  └─►  DIM_TARGET  │    │ DIM_RISK_PROFILE │
  │BRANCH  │ │  │    │──────────────│    │──────────────────│
  │────────│ │  │    │ id_dim_target│    │ id_dim_risk_prof │
  │id_dim_ │ │  │    │ target_code  │    │ customer_code    │
  │branch  │ │  │    │ description  │    │ kyc_score_raw    │
  │area    │ │  │    └──────────────┘    │ pep_flag         │
  └────────┘ │  │                        │ compliance_flag   │
             │  │  ┌──────────────┐      │ global_risk_score │
             │  └──► DIM_INDUSTRY │      │ risk_tier        │
     ┌───────┘     │──────────────│      └──────────────────┘
     │             │ industry_code│
     │             └──────────────┘
     │
     │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
     └───► DIM_CURRENCY │    │  DIM_SECTOR  │    │   DIM_DATE   │
          │──────────── │    │──────────────│    │──────────────│
          │currency_code│    │ sector_code  │    │ date         │
          │currency_name│    │ description  │    │ year, month  │
          └─────────────┘    └──────────────┘    │ quarter      │
                                                  └──────────────┘

Legend:
  ◄── shared dimension  (used by both fact tables)
  ◄── exclusive dim     (DIM_CURRENCY → FAIT_ACCOUNT only)
                        (DIM_RISK_PROFILE → FAIT_CUSTOMER_RISK only)
```

---

## 10. dbt Tests and Data Quality

### schema.yml — staging tests

```yaml
# models/staging/schema.yml
version: 2

models:
  - name: stg_account
    columns:
      - name: account_id
        tests: [unique, not_null]
      - name: customer_code
        tests: [not_null]
      - name: working_balance
        tests: [not_null]

  - name: stg_customer
    columns:
      - name: customer_code
        tests: [unique, not_null]

  - name: stg_currency
    columns:
      - name: currency_code
        tests: [unique, not_null]
```

### Custom test — risk score must be between 0 and 1

```sql
-- tests/test_risk_score_range.sql
select customer_code
from {{ ref('fait_customer_risk') }}
where global_risk_score < 0
   or global_risk_score > 1
```

This test passes if it returns zero rows. If it returns rows, a score
is out of range and something in the calculation logic is wrong.

### Custom test — no orphan accounts

```sql
-- tests/test_no_orphan_accounts.sql
select a.account_id
from {{ ref('fait_account') }} a
left join {{ ref('dim_customer') }} dc on a.id_dim_customer = dc.id_dim_customer
where dc.id_dim_customer is null
```

---

## 11. Running the Pipeline

### First time setup

```powershell
# install dbt for SQL Server
pip install dbt-sqlserver

# go to your project
cd C:\Users\Ahmed\Desktop\atb_bi_project\3_transformation

# verify connection works
dbt debug
```

Expected output of `dbt debug`:
```
Connection test: OK
```

### Run the full pipeline

```powershell
# run all models in correct dependency order
dbt run

# run only staging layer
dbt run --select staging

# run only warehouse layer
dbt run --select warehouse

# run a single model
dbt run --select fait_customer_risk

# run tests
dbt test

# run everything + tests in one command
dbt build
```

### Check what dbt will run (dry run)

```powershell
dbt ls
```

### After running — verify in SSMS

```sql
USE atb_bi;

-- check dimensions
SELECT COUNT(*) FROM PFE_DWH.dim_customer;
SELECT COUNT(*) FROM PFE_DWH.dim_branch;
SELECT COUNT(*) FROM PFE_DWH.dim_risk_profile;

-- check fact tables
SELECT COUNT(*) FROM PFE_DWH.fait_account;
SELECT COUNT(*) FROM PFE_DWH.fait_customer_risk;

-- check risk distribution
SELECT
    risk_tier,
    count(*)                        as customer_count,
    round(avg(global_risk_score),4) as avg_score,
    round(avg(compliance_risk_index),4) as avg_compliance,
    round(avg(financial_fragility_score),4) as avg_financial
FROM PFE_DWH.fait_customer_risk
GROUP BY risk_tier
ORDER BY avg_score desc;
```

Expected risk distribution (approximate):

| risk_tier | customer_count | avg_score |
|-----------|---------------|-----------|
| CRITICAL  | ~2 000        | 0.75+     |
| HIGH      | ~15 000       | 0.50      |
| MEDIUM    | ~45 000       | 0.30      |
| LOW       | ~75 000       | 0.10      |

---

## 12. ML Layer Overview

After `dbt run` completes, the ML pipeline reads from `PFE_DWH` and
writes predictions back to `PFE_ML`.

```powershell
cd C:\Users\Ahmed\Desktop\atb_bi_project\4_ml

# install dependencies
pip install -r requirements.txt

# run feature extraction + training + prediction
python src/train_model.py
python src/predict.py
python src/export_to_db.py
```

Output tables created in SSMS:
- `PFE_ML.ml_customer_predictions` — churn probability per customer
- `PFE_ML.ml_feature_importance` — SHAP values per feature

Power BI connects to both tables to display the ML predictions
dashboard alongside the standard BI dashboards.

### requirements.txt

```
pandas==2.1.0
sqlalchemy==2.0.0
pyodbc==4.0.39
scikit-learn==1.3.0
xgboost==2.0.0
imbalanced-learn==0.11.0
shap==0.43.0
matplotlib==3.7.0
seaborn==0.12.0
joblib==1.3.0
```

---

## 13. Power BI Connection

Connect Power BI to SSMS:

1. Open Power BI Desktop
2. Get Data → SQL Server
3. Server: `localhost`
4. Database: `atb_bi`
5. Import mode (not DirectQuery — faster for dashboards)

Import these tables:
```
PFE_DWH.dim_customer
PFE_DWH.dim_branch
PFE_DWH.dim_currency
PFE_DWH.dim_sector
PFE_DWH.dim_industry
PFE_DWH.dim_target
PFE_DWH.dim_date
PFE_DWH.dim_risk_profile
PFE_DWH.fait_account
PFE_DWH.fait_customer_risk
PFE_ML.ml_customer_predictions
```

### Relationships to set in Power BI model view

| From | To | Key |
|---|---|---|
| fait_account | dim_customer | id_dim_customer |
| fait_account | dim_branch | id_dim_branch |
| fait_account | dim_currency | id_dim_currency |
| fait_account | dim_sector | id_dim_sector |
| fait_account | dim_industry | id_dim_industry |
| fait_account | dim_target | id_dim_target |
| fait_account | dim_date | opening_date = date |
| fait_customer_risk | dim_customer | id_dim_customer |
| fait_customer_risk | dim_branch | id_dim_branch |
| fait_customer_risk | dim_sector | id_dim_sector |
| fait_customer_risk | dim_industry | id_dim_industry |
| fait_customer_risk | dim_target | id_dim_target |
| fait_customer_risk | dim_risk_profile | id_dim_risk_profile |
| ml_customer_predictions | dim_customer | customer_code |

### Dashboards to build

| Dashboard | Fact table used | Key visuals |
|---|---|---|
| 1. Accueil | — | project intro page |
| 2. Analyse Segments Clients | fait_account | accounts by employment, nationality, gender |
| 3. Structure Financière | fait_account | balance by currency, sector, overdraft rate |
| 4. Performance Agences | fait_account | average balance by branch and area |
| 5. Risque et Conformité | fait_customer_risk | risk tier distribution, PEP count, KYC gaps |
| 6. Prédictions ML | ml_customer_predictions | churn probability, top at-risk clients |
