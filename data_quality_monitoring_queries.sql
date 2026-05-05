-- ============================================================================
-- ATB BI PROJECT - DATA QUALITY MONITORING QUERIES
-- ============================================================================
-- Use these queries regularly to monitor warehouse health and data distributions
-- ============================================================================

-- ============================================================================
-- QUERY 1: Monthly Risk Tier Trend (for Power BI integration)
-- ============================================================================
SELECT 
  CAST(scoring_date AS DATE) as scoring_date,
  risk_tier,
  COUNT(*) as customer_count,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(PARTITION BY CAST(scoring_date AS DATE)),2) as percentage
FROM PFE_DWH.FACT_CUSTOMER_RISK
GROUP BY CAST(scoring_date AS DATE), risk_tier
ORDER BY scoring_date DESC, customer_count DESC
-- Expected: LOW ~47%, MEDIUM ~52%, HIGH ~0%

-- ============================================================================
-- QUERY 2: Risk Component Health Check
-- ============================================================================
SELECT 
  'Compliance Risk Index' as component,
  ROUND(AVG(compliance_risk_index),2) as avg_score,
  ROUND(MIN(compliance_risk_index),2) as min_score,
  ROUND(MAX(compliance_risk_index),2) as max_score,
  ROUND(STDEV(compliance_risk_index),2) as std_dev
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
UNION ALL
SELECT 
  'Financial Fragility Score',
  ROUND(AVG(financial_fragility_score),2),
  ROUND(MIN(financial_fragility_score),2),
  ROUND(MAX(financial_fragility_score),2),
  ROUND(STDEV(financial_fragility_score),2)
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
UNION ALL
SELECT 
  'Behavioral Risk Score',
  ROUND(AVG(behavioral_risk_score),2),
  ROUND(MIN(behavioral_risk_score),2),
  ROUND(MAX(behavioral_risk_score),2),
  ROUND(STDEV(behavioral_risk_score),2)
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)

-- ============================================================================
-- QUERY 3: Data Quality Scorecard
-- ============================================================================
SELECT 
  'Risk Tier NULL Count' as metric,
  SUM(CASE WHEN risk_tier IS NULL THEN 1 ELSE 0 END) as null_count,
  ROUND(100.0*SUM(CASE WHEN risk_tier IS NULL THEN 1 ELSE 0 END)/COUNT(*),2) as null_percentage
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
UNION ALL
SELECT 
  'Oldest Account Date NULL Count',
  SUM(CASE WHEN oldest_account_opening_date IS NULL THEN 1 ELSE 0 END),
  ROUND(100.0*SUM(CASE WHEN oldest_account_opening_date IS NULL THEN 1 ELSE 0 END)/COUNT(*),2)
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
UNION ALL
SELECT 
  'Working Balance NULL Count',
  SUM(CASE WHEN total_working_balance IS NULL THEN 1 ELSE 0 END),
  ROUND(100.0*SUM(CASE WHEN total_working_balance IS NULL THEN 1 ELSE 0 END)/COUNT(*),2)
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
-- Expected: All zeros (no NULLs in primary measures)

-- ============================================================================
-- QUERY 4: Overdraft Trend Analysis
-- ============================================================================
SELECT 
  CAST(fcr.scoring_date AS DATE) as scoring_date,
  COUNT(*) as total_customers,
  COUNT(CASE WHEN fcr.negative_balance_account_count > 0 THEN 1 END) as customers_in_overdraft,
  ROUND(100.0*COUNT(CASE WHEN fcr.negative_balance_account_count > 0 THEN 1 END)/COUNT(*),2) as overdraft_percentage
FROM PFE_DWH.FACT_CUSTOMER_RISK fcr
GROUP BY CAST(fcr.scoring_date AS DATE)
ORDER BY scoring_date DESC
-- Expected trend: Relatively stable around 11-12%

-- ============================================================================
-- QUERY 5: KYC Compliance Trend
-- ============================================================================
SELECT 
  COUNT(*) as total_customers,
  COUNT(CASE WHEN is_kyc_complete = 1 THEN 1 END) as kyc_complete,
  ROUND(100.0*COUNT(CASE WHEN is_kyc_complete = 1 THEN 1 END)/COUNT(*),2) as kyc_complete_pct,
  COUNT(CASE WHEN is_pep = 1 THEN 1 END) as pep_customers,
  ROUND(100.0*COUNT(CASE WHEN is_pep = 1 THEN 1 END)/COUNT(*),2) as pep_percentage,
  COUNT(CASE WHEN is_compliance_flagged = 1 THEN 1 END) as flagged_customers,
  ROUND(100.0*COUNT(CASE WHEN is_compliance_flagged = 1 THEN 1 END)/COUNT(*),2) as flagged_percentage
FROM PFE_DWH.DIM_CUSTOMER
-- Expected: KYC ~74%, PEP ~0.15%, Flagged ~0.3%

-- ============================================================================
-- QUERY 6: Account Portfolio Composition
-- ============================================================================
SELECT 
  COUNT(*) as total_customers,
  ROUND(AVG(CAST(account_count AS FLOAT)),2) as avg_accounts_per_customer,
  COUNT(CASE WHEN account_count = 0 THEN 1 END) as zero_account_customers,
  ROUND(100.0*COUNT(CASE WHEN account_count = 0 THEN 1 END)/COUNT(*),2) as zero_account_pct,
  COUNT(CASE WHEN account_count = 1 THEN 1 END) as single_account_customers,
  ROUND(100.0*COUNT(CASE WHEN account_count = 1 THEN 1 END)/COUNT(*),2) as single_account_pct,
  COUNT(CASE WHEN account_count >= 2 THEN 1 END) as multi_account_customers,
  ROUND(100.0*COUNT(CASE WHEN account_count >= 2 THEN 1 END)/COUNT(*),2) as multi_account_pct
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
-- Expected: ~68% single, ~1% multi, ~25% zero

-- ============================================================================
-- QUERY 7: Top Sectors by Customer Count
-- ============================================================================
SELECT TOP 10
  s.sector_code,
  s.sector_name,
  COUNT(DISTINCT dc.customer_sk) as customer_count,
  ROUND(100.0*COUNT(DISTINCT dc.customer_sk)/SUM(COUNT(DISTINCT dc.customer_sk)) OVER(),2) as percentage
FROM PFE_DWH.DIM_CUSTOMER dc
LEFT JOIN PFE_DWH.DIM_SECTOR s ON dc.sector_code = s.sector_code
GROUP BY s.sector_code, s.sector_name
ORDER BY customer_count DESC

-- ============================================================================
-- QUERY 8: High Risk Customer Alert
-- ============================================================================
SELECT 
  dc.customer_id,
  dc.customer_name,
  drp.global_risk_score,
  drp.risk_tier,
  drp.compliance_risk_index,
  drp.financial_fragility_score,
  drp.behavioral_risk_score,
  dc.sector_code,
  dc.employment_status,
  DATEDIFF(DAY, drp.scoring_date, GETDATE()) as days_since_scoring
FROM PFE_DWH.FACT_CUSTOMER_RISK fcr
JOIN PFE_DWH.DIM_CUSTOMER dc ON fcr.customer_sk = dc.customer_sk
JOIN PFE_DWH.DIM_RISK_PROFILE drp ON fcr.risk_profile_sk = drp.risk_profile_sk
WHERE drp.risk_tier IN ('HIGH', 'VERY_HIGH')
  AND CAST(drp.scoring_date AS DATE) = CAST(GETDATE() AS DATE)
ORDER BY drp.global_risk_score DESC
-- Action: Send to compliance team for review

-- ============================================================================
-- QUERY 9: Fact-Dimension Coverage Check
-- ============================================================================
SELECT 
  'FACT_CUSTOMER_RISK' as table_name,
  COUNT(*) as total_records,
  SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) as null_customer_sk,
  SUM(CASE WHEN risk_profile_sk IS NULL THEN 1 ELSE 0 END) as null_risk_profile_sk,
  SUM(CASE WHEN scoring_date_sk IS NULL THEN 1 ELSE 0 END) as null_scoring_date_sk
FROM PFE_DWH.FACT_CUSTOMER_RISK
WHERE CAST(scoring_date AS DATE) = CAST(GETDATE() AS DATE)
UNION ALL
SELECT 
  'FACT_ACCOUNT',
  COUNT(*),
  SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN opening_date_sk IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN load_date_sk IS NULL THEN 1 ELSE 0 END)
FROM PFE_DWH.FACT_ACCOUNT
-- Expected: All zeros (no NULL foreign keys)

-- ============================================================================
-- QUERY 10: Risk Tier Migration (for ML Model Monitoring)
-- ============================================================================
-- This query tracks customers transitioning between risk tiers (useful for model validation)
WITH current AS (
  SELECT 
    customer_id,
    risk_tier as current_tier,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY scoring_date DESC) as rn
  FROM PFE_DWH.FACT_CUSTOMER_RISK fcr
  JOIN PFE_DWH.DIM_CUSTOMER dc ON fcr.customer_sk = dc.customer_sk
),
previous AS (
  SELECT 
    customer_id,
    risk_tier as previous_tier,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY scoring_date DESC) as rn
  FROM PFE_DWH.FACT_CUSTOMER_RISK fcr
  JOIN PFE_DWH.DIM_CUSTOMER dc ON fcr.customer_sk = dc.customer_sk
)
SELECT 
  c.customer_id,
  c.current_tier,
  p.previous_tier,
  CASE 
    WHEN c.current_tier = p.previous_tier THEN 'STABLE'
    WHEN c.current_tier = 'LOW' AND p.previous_tier IN ('MEDIUM', 'HIGH', 'VERY_HIGH') THEN 'IMPROVED'
    WHEN c.current_tier IN ('MEDIUM', 'HIGH', 'VERY_HIGH') AND p.previous_tier = 'LOW' THEN 'DEGRADED'
    ELSE 'CHANGED'
  END as transition,
  COUNT(*) as customer_count
FROM current c
LEFT JOIN previous p ON c.customer_id = p.customer_id AND c.rn = p.rn - 1
WHERE c.rn = 1 AND p.rn = 2
GROUP BY c.current_tier, p.previous_tier, 
  CASE 
    WHEN c.current_tier = p.previous_tier THEN 'STABLE'
    WHEN c.current_tier = 'LOW' AND p.previous_tier IN ('MEDIUM', 'HIGH', 'VERY_HIGH') THEN 'IMPROVED'
    WHEN c.current_tier IN ('MEDIUM', 'HIGH', 'VERY_HIGH') AND p.previous_tier = 'LOW' THEN 'DEGRADED'
    ELSE 'CHANGED'
  END
ORDER BY customer_count DESC

-- ============================================================================
-- INSTALLATION: Save these queries in a SQL Agent job for daily/weekly auto-monitoring
-- ============================================================================
