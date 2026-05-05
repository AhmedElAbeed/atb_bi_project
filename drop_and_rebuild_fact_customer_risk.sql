-- ============================================================
-- Step 1 – Diagnostics: see why rows were excluded before
-- ============================================================
USE [ATB_BI];

-- How many rows are in the intermediate model right now?
-- (Run this query manually against your source view/table if needed)
-- The filters below mirror what fact_customer_risk.sql applies.

SELECT
    COUNT(*)                                             AS total_rows,
    SUM(CASE WHEN customer_id         IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN scoring_date        IS NULL THEN 1 ELSE 0 END) AS null_scoring_date,
    SUM(CASE WHEN account_officer_id  IS NULL THEN 1 ELSE 0 END) AS null_account_officer_id,
    SUM(CASE WHEN account_officer_id  = 0     THEN 1 ELSE 0 END) AS zero_account_officer_id,
    SUM(CASE WHEN customer_tenure_days IS NULL THEN 1 ELSE 0 END) AS null_tenure_days,
    SUM(CASE WHEN customer_since_date  IS NULL THEN 1 ELSE 0 END) AS null_customer_since_date
FROM [ATB_BI].[PFE_DWH].[int_customer_risk_score]; -- adjust schema if different

-- ============================================================
-- Step 2 – Drop the stale fact table so dbt-fusion can
--           create it fresh without schema-drift errors.
-- ============================================================
IF OBJECT_ID('[PFE_DWH].[fact_customer_risk]', 'U') IS NOT NULL
    DROP TABLE [PFE_DWH].[fact_customer_risk];

PRINT 'Table dropped (or did not exist). Run dbt now.';

-- ============================================================
-- Step 3 – After dbt run, validate the rebuilt fact
-- ============================================================
-- Uncomment and run AFTER dbt completes:
/*
SELECT
    COUNT(*)                                             AS total_rows,
    SUM(CASE WHEN fact_customer_risk_sk IS NULL THEN 1 ELSE 0 END) AS null_sk,
    SUM(CASE WHEN customer_sk           IS NULL THEN 1 ELSE 0 END) AS null_customer_sk,
    SUM(CASE WHEN dao_sk                IS NULL THEN 1 ELSE 0 END) AS null_dao_sk,
    SUM(CASE WHEN dao_sk                = 0     THEN 1 ELSE 0 END) AS zero_dao_sk,
    SUM(CASE WHEN customer_since_date   IS NULL THEN 1 ELSE 0 END) AS null_since_date,
    SUM(CASE WHEN customer_tenure_days  IS NULL THEN 1 ELSE 0 END) AS null_tenure_days,
    SUM(CASE WHEN risk_tier             IS NULL THEN 1 ELSE 0 END) AS null_risk_tier,
    MIN(dao_sk) AS min_dao_sk,
    MAX(dao_sk) AS max_dao_sk,
    COUNT(DISTINCT dao_sk) AS distinct_dao_sk_count
FROM [ATB_BI].[PFE_DWH].[fact_customer_risk];
*/
