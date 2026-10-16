CREATE OR ALTER VIEW PFE_DWH.vw_superset_client_risk_base AS
SELECT
    fcr.fact_customer_risk_sk,
    fcr.customer_sk,
    fcr.risk_profile_sk,
    fcr.dao_sk,
    fcr.sector_sk,
    fcr.industry_sk,
    fcr.target_sk,
    fcr.scoring_date_sk,
    fcr.load_date_sk,
    fcr.customer_id,
    dc.account_officer_id,
    dc.dao_name,
    dc.dao_area,
    dc.sector_code,
    dc.sector_description,
    dc.industry_code,
    dc.industry_description,
    dc.target_code,
    dc.target_description,
    dc.customer_since_date,
    dc.customer_tenure_days,
    dc.title,
    dc.gender,
    dc.marital_status,
    dc.number_of_dependents,
    dc.employment_status,
    dc.job_title,
    dc.monthly_salary,
    dc.nationality_code,
    dc.residence_country_code,
    dc.residence_status,
    dc.last_kyc_review_date,
    dc.next_kyc_review_date,
    dc.is_kyc_complete,
    dc.is_pep,
    dc.is_compliance_flagged,
    dc.has_compliance_decision,
    dc.posting_restriction_code,
    dc.segment_code,
    dc.client_nature_code,
    dc.legal_capacity_flag,
    dc.beneficial_owner_flag,
    rp.risk_tier,
    rp.compliance_risk_index,
    rp.financial_fragility_score,
    rp.behavioral_risk_score,
    rp.global_risk_score,
    fcr.account_count,
    fcr.total_working_balance,
    fcr.avg_working_balance,
    fcr.negative_balance_account_count,
    fcr.oldest_account_opening_date,
    fcr.newest_account_opening_date,
    fcr.scoring_date,
    fcr.load_date
FROM PFE_DWH.fact_customer_risk fcr
INNER JOIN PFE_DWH.dim_customer dc
    ON fcr.customer_sk = dc.customer_sk
INNER JOIN PFE_DWH.dim_risk_profile rp
    ON fcr.risk_profile_sk = rp.risk_profile_sk;

CREATE OR ALTER VIEW PFE_DWH.vw_superset_account_portfolio AS
SELECT
    fa.fact_account_sk,
    fa.customer_sk,
    fa.dao_sk,
    fa.currency_sk,
    fa.sector_sk,
    fa.industry_sk,
    fa.target_sk,
    fa.opening_date_sk,
    fa.load_date_sk,
    fa.working_balance,
    fa.is_negative_balance,
    fa.account_age_days,
    fa.load_date,
    dc.customer_id,
    dc.dao_name,
    dc.dao_area,
    dc.employment_status,
    dc.gender,
    dc.marital_status,
    dc.residence_status,
    cur.currency_code,
    sec.sector_code,
    sec.sector_description,
    ind.industry_code,
    ind.industry_description,
    tgt.target_code,
    tgt.target_description,
    d_open.full_date AS opening_date,
    d_load.full_date AS load_date_date
FROM PFE_DWH.fact_account fa
INNER JOIN PFE_DWH.dim_customer dc
    ON fa.customer_sk = dc.customer_sk
INNER JOIN PFE_DWH.dim_currency cur
    ON fa.currency_sk = cur.currency_sk
INNER JOIN PFE_DWH.dim_sector sec
    ON fa.sector_sk = sec.sector_sk
INNER JOIN PFE_DWH.dim_industry ind
    ON fa.industry_sk = ind.industry_sk
INNER JOIN PFE_DWH.dim_target tgt
    ON fa.target_sk = tgt.target_sk
INNER JOIN PFE_DWH.dim_date d_open
    ON fa.opening_date_sk = d_open.date_sk
INNER JOIN PFE_DWH.dim_date d_load
    ON fa.load_date_sk = d_load.date_sk;

CREATE OR ALTER VIEW PFE_DWH.vw_superset_branch_performance AS
SELECT
    dao.dao_area,
    dao.dao_name,
    COUNT(DISTINCT base.customer_id) AS customer_count,
    COUNT(*) AS scoring_rows,
    AVG(CAST(base.global_risk_score AS DECIMAL(10,4))) AS avg_global_risk_score,
    AVG(CAST(base.compliance_risk_index AS DECIMAL(10,4))) AS avg_compliance_risk_index,
    AVG(CAST(base.financial_fragility_score AS DECIMAL(10,4))) AS avg_financial_fragility_score,
    AVG(CAST(base.behavioral_risk_score AS DECIMAL(10,4))) AS avg_behavioral_risk_score,
    SUM(CAST(base.total_working_balance AS DECIMAL(18,2))) AS total_working_balance,
    SUM(CASE WHEN base.risk_tier IN ('HIGH', 'VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_customers
FROM PFE_DWH.vw_superset_client_risk_base base
INNER JOIN PFE_DWH.dim_dao dao
    ON base.dao_sk = dao.dao_sk
GROUP BY dao.dao_area, dao.dao_name;

CREATE OR ALTER VIEW PFE_DWH.vw_superset_risk_distribution AS
SELECT
    scoring_date,
    risk_tier,
    COUNT(*) AS customer_count,
    AVG(CAST(global_risk_score AS DECIMAL(10,4))) AS avg_global_risk_score,
    AVG(CAST(compliance_risk_index AS DECIMAL(10,4))) AS avg_compliance_risk_index,
    AVG(CAST(financial_fragility_score AS DECIMAL(10,4))) AS avg_financial_fragility_score,
    AVG(CAST(behavioral_risk_score AS DECIMAL(10,4))) AS avg_behavioral_risk_score
FROM PFE_DWH.vw_superset_client_risk_base
GROUP BY scoring_date, risk_tier;

CREATE OR ALTER VIEW PFE_DWH.vw_superset_ml_predictions AS
SELECT
    p.customer_sk,
    p.customer_id,
    p.risk_score_ml,
    p.risk_prediction_ml,
    p.prediction_confidence,
    p.model_version,
    p.model_type,
    p.prediction_timestamp,
    dc.dao_name,
    dc.dao_area,
    dc.gender,
    dc.employment_status,
    dc.sector_description,
    dc.industry_description,
    dc.target_description
FROM PFE_DWH.ml_customer_risk_scores p
LEFT JOIN PFE_DWH.dim_customer dc
    ON p.customer_sk = dc.customer_sk;

CREATE OR ALTER VIEW PFE_DWH.vw_superset_executive_overview AS
SELECT
    CAST(GETDATE() AS DATE) AS snapshot_date,
    COUNT(DISTINCT customer_id) AS customer_count,
    COUNT(DISTINCT dao_sk) AS dao_count,
    COUNT(DISTINCT sector_sk) AS sector_count,
    COUNT(DISTINCT industry_sk) AS industry_count,
    COUNT(*) AS scoring_rows,
    SUM(CASE WHEN risk_tier IN ('HIGH', 'VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_customers,
    AVG(CAST(global_risk_score AS DECIMAL(10,4))) AS avg_global_risk_score,
    AVG(CAST(total_working_balance AS DECIMAL(18,2))) AS avg_working_balance,
    SUM(CAST(total_working_balance AS DECIMAL(18,2))) AS total_working_balance
FROM PFE_DWH.vw_superset_client_risk_base;
