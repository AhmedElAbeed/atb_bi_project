with risk as (
    select *
    from {{ ref('int_customer_risk_score') }}
),
profiles as (
    select
        risk_profile_id,
        risk_tier
    from {{ ref('dim_risk_profile') }}
)
select
    r.customer_id,
    r.account_officer_id as branch_id,
    r.sector_code,
    r.industry_code,
    r.target_code,
    p.risk_profile_id,
    r.account_count,
    r.total_working_balance,
    r.avg_working_balance,
    r.negative_balance_account_count,
    r.compliance_risk_index,
    r.financial_fragility_score,
    r.behavioral_risk_score,
    r.global_risk_score,
    r.risk_tier,
    r.scoring_date,
    cast(convert(varchar(8), r.scoring_date, 112) as int) as scoring_date_key
from risk r
left join profiles p
    on r.risk_tier = p.risk_tier
