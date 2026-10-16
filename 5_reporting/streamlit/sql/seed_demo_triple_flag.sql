-- Demo: create triple-flag cases (PEP + high balance + ML high-risk)
-- Run AFTER ml_customer_predictions is populated.
-- Marks top PEP customers (by balance) as ML high-risk for dashboard demo.

;WITH balance_cut AS (
    SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
        ORDER BY CAST(total_working_balance AS FLOAT)
    ) OVER () AS p75_balance
    FROM PFE_DWH.fact_customer_risk
    WHERE total_working_balance > 0
),
pep_high AS (
    SELECT TOP 12
        c.customer_id,
        f.risk_tier,
        CAST(f.total_working_balance AS FLOAT) AS total_working_balance,
        d.dao_area
    FROM PFE_DWH.dim_customer c
    JOIN PFE_DWH.fact_customer_risk f ON c.customer_sk = f.customer_sk
    LEFT JOIN PFE_DWH.dim_dao d ON f.dao_sk = d.dao_sk
    CROSS JOIN (SELECT TOP 1 p75_balance FROM balance_cut) bc
    WHERE c.customer_id <> -1
      AND c.is_pep = 1
      AND CAST(f.total_working_balance AS FLOAT) >= bc.p75_balance
    ORDER BY f.total_working_balance DESC
)
UPDATE p
SET
    risk_prediction     = 1,
    risk_probability    = 0.88 + (ABS(CHECKSUM(NEWID())) % 10) * 0.01,
    disagreement_flag   = CASE
        WHEN UPPER(ISNULL(p.rules_risk_tier, f.risk_tier)) IN ('LOW', 'MEDIUM', 'UNKNOWN') THEN 1
        ELSE 0
    END,
    is_pep              = 1,
    total_working_balance = ph.total_working_balance,
    dao_area            = ph.dao_area,
    prediction_timestamp = SYSUTCDATETIME()
FROM PFE_ML.ml_customer_predictions p
INNER JOIN pep_high ph ON p.customer_id = ph.customer_id
INNER JOIN PFE_DWH.fact_customer_risk f ON f.customer_id = p.customer_id;

SELECT
    COUNT(*) AS triple_flag_count
FROM PFE_ML.ml_customer_predictions p
JOIN PFE_DWH.dim_customer c ON c.customer_id = p.customer_id AND c.is_pep = 1
WHERE p.risk_prediction = 1
  AND p.total_working_balance > 0;
