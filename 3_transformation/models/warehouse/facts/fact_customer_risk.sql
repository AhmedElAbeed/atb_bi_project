with risk_base as (
    select *
    from {{ ref('int_customer_risk_score') }}
),
dim_customer as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
),
dim_risk_profile as (
    select risk_profile_sk, customer_id, scoring_date
    from {{ ref('dim_risk_profile') }}
),
dim_dao as (
    select dao_sk, account_officer_id
    from {{ ref('dim_dao') }}
),
dim_sector as (
    select sector_sk, sector_code
    from {{ ref('dim_sector') }}
),
dim_industry as (
    select industry_sk, industry_code
    from {{ ref('dim_industry') }}
),
dim_target as (
    select target_sk, target_code
    from {{ ref('dim_target') }}
),
dim_date as (
    select date_sk, full_date
    from {{ ref('dim_date') }}
)
select
    row_number() over (order by r.customer_id, r.scoring_date) as fact_customer_risk_sk,
    c.customer_sk,
    rp.risk_profile_sk,
    d.dao_sk,
    s.sector_sk,
    i.industry_sk,
    t.target_sk,
    dd_score.date_sk as scoring_date_sk,
    dd_load.date_sk as load_date_sk,
    r.customer_id,
    r.account_officer_id,
    r.sector_code,
    r.industry_code,
    r.target_code,
    r.customer_since_date,
    r.customer_tenure_days,
    r.account_count,
    r.total_working_balance,
    r.avg_working_balance,
    r.negative_balance_account_count,
    r.oldest_account_opening_date,
    r.newest_account_opening_date,
    r.compliance_risk_index,
    r.financial_fragility_score,
    r.behavioral_risk_score,
    r.global_risk_score,
    r.risk_tier,
    r.scoring_date,
    cast(getdate() as date) as load_date
from risk_base r
left join dim_customer c
    on r.customer_id = c.customer_id
left join dim_risk_profile rp
    on r.customer_id = rp.customer_id
   and r.scoring_date = rp.scoring_date
left join dim_dao d
    on r.account_officer_id = d.account_officer_id
left join dim_sector s
    on r.sector_code = s.sector_code
left join dim_industry i
    on r.industry_code = i.industry_code
left join dim_target t
    on r.target_code = t.target_code
left join dim_date dd_score
    on r.scoring_date = dd_score.full_date
left join dim_date dd_load
    on cast(getdate() as date) = dd_load.full_date
