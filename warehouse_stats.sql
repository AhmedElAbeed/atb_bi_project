-- ============================================================
-- ATB BI PROJECT - WAREHOUSE DATA QUALITY & DISTRIBUTION SUMMARY
-- ============================================================

PRINT '=========================================='
PRINT 'DATA WAREHOUSE STATISTICS SUMMARY'
PRINT '=========================================='

PRINT ''
PRINT '1. FACT_CUSTOMER_RISK - RISK TIER DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  risk_tier,
  COUNT(*) as customer_count,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) as percentage
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
GROUP BY risk_tier
ORDER BY customer_count DESC

PRINT ''
PRINT '2. FACT_CUSTOMER_RISK - RISK COMPONENT DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  CONCAT('Compliance Risk (40%): ', ROUND(AVG(compliance_risk_index),2)) as avg_compliance,
  CONCAT('Financial Fragility (35%): ', ROUND(AVG(financial_fragility_score),2)) as avg_fragility,
  CONCAT('Behavioral Risk (25%): ', ROUND(AVG(behavioral_risk_score),2)) as avg_behavioral,
  CONCAT('Global Risk Score: ', ROUND(AVG(global_risk_score),2)) as avg_global_score
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)

PRINT ''
PRINT '3. FACT_CUSTOMER_RISK - DATA QUALITY CHECK (NULL VALUES)'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN oldest_account_opening_date IS NULL THEN 1 ELSE 0 END) as null_oldest_account,
  ROUND(100.0*SUM(CASE WHEN oldest_account_opening_date IS NULL THEN 1 ELSE 0 END)/COUNT(*),2) as null_percentage,
  SUM(CASE WHEN total_working_balance IS NULL THEN 1 ELSE 0 END) as null_balance,
  SUM(CASE WHEN negative_balance_account_count IS NULL THEN 1 ELSE 0 END) as null_negative_count
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)

PRINT ''
PRINT '4. FACT_CUSTOMER_RISK - ACCOUNT METRICS DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  CONCAT('Customers with overdraft accounts: ', 
    COUNT(CASE WHEN negative_balance_account_count > 0 THEN 1 END), 
    ' (', ROUND(100.0*COUNT(CASE WHEN negative_balance_account_count > 0 THEN 1 END)/COUNT(*),2), '%)') as overdraft_customers,
  CONCAT('Avg account count per customer: ', ROUND(AVG(CAST(account_count AS FLOAT)),2)) as avg_accounts,
  CONCAT('Customers with single account: ', 
    COUNT(CASE WHEN account_count = 1 THEN 1 END),
    ' (', ROUND(100.0*COUNT(CASE WHEN account_count = 1 THEN 1 END)/COUNT(*),2), '%)') as single_account_customers
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)

PRINT ''
PRINT '5. FACT_ACCOUNT - ACCOUNT LEVEL STATISTICS'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  COUNT(*) as total_accounts,
  SUM(CASE WHEN is_negative_balance = 1 THEN 1 ELSE 0 END) as overdraft_accounts,
  ROUND(100.0*SUM(CASE WHEN is_negative_balance = 1 THEN 1 ELSE 0 END)/COUNT(*),2) as overdraft_percentage,
  ROUND(AVG(CAST(working_balance AS FLOAT)),2) as avg_balance,
  ROUND(AVG(CAST(account_age_days AS FLOAT)),2) as avg_account_age_days
FROM PFE_DWH.FACT_ACCOUNT

PRINT ''
PRINT '6. DIM_CUSTOMER - CUSTOMER PROFILE DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  COUNT(*) as total_customers,
  SUM(CASE WHEN is_kyc_complete = 1 THEN 1 ELSE 0 END) as kyc_complete,
  ROUND(100.0*SUM(CASE WHEN is_kyc_complete = 1 THEN 1 ELSE 0 END)/COUNT(*),2) as kyc_complete_pct,
  SUM(CASE WHEN is_pep = 1 THEN 1 ELSE 0 END) as pep_customers,
  ROUND(100.0*SUM(CASE WHEN is_pep = 1 THEN 1 END)/COUNT(*),2) as pep_percentage,
  SUM(CASE WHEN is_compliance_flagged = 1 THEN 1 ELSE 0 END) as flagged_customers,
  ROUND(100.0*SUM(CASE WHEN is_compliance_flagged = 1 THEN 1 ELSE 0 END)/COUNT(*),2) as flagged_percentage
FROM PFE_DWH.DIM_CUSTOMER

PRINT ''
PRINT '7. DIM_CUSTOMER - EMPLOYMENT STATUS DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT TOP 10
  employment_status,
  COUNT(*) as count,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) as percentage
FROM PFE_DWH.DIM_CUSTOMER
WHERE employment_status IS NOT NULL
GROUP BY employment_status
ORDER BY count DESC

PRINT ''
PRINT '8. DIM_SECTOR - SECTOR DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  COUNT(*) as total_sectors
FROM PFE_DWH.DIM_SECTOR

PRINT ''
PRINT '9. DIM_INDUSTRY - INDUSTRY DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  COUNT(*) as total_industries
FROM PFE_DWH.DIM_INDUSTRY

PRINT ''
PRINT '10. DIM_TARGET - TARGET SEGMENT DISTRIBUTION'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  COUNT(*) as total_segments
FROM PFE_DWH.DIM_TARGET

PRINT ''
PRINT '11. CROSS-FACT RELATIONSHIP - Customers vs Accounts'
PRINT '-' + REPLICATE('-', 50)
SELECT 
  CONCAT('Unique customers in FACT_CUSTOMER_RISK: ', COUNT(DISTINCT customer_sk)) as customers_risk_fact
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)

PRINT ''
SELECT 
  CONCAT('Unique customers in FACT_ACCOUNT: ', COUNT(DISTINCT customer_sk)) as customers_account_fact
FROM PFE_DWH.FACT_ACCOUNT

PRINT ''
PRINT '=========================================='
PRINT 'END OF SUMMARY'
PRINT '=========================================='
