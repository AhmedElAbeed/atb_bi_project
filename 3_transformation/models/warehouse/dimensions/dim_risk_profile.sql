with risk_profile as (
    select
        customer_id,
        risk_tier,
        compliance_risk_index,
        financial_fragility_score,
        behavioral_risk_score,
        global_risk_score,
        scoring_date
    from {{ ref('int_customer_risk_score') }}
)
select
    row_number() over (order by customer_id, scoring_date) as risk_profile_sk,
    customer_id,
    risk_tier,
    compliance_risk_index,
    financial_fragility_score,
    behavioral_risk_score,
    global_risk_score,
    scoring_date
from risk_profile
