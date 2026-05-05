{{
  config(
    materialized='table',
    unique_key=['customer_id', 'scoring_date']
  )
}}

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
    where customer_id is not null
      and customer_id <> -1
      and scoring_date is not null
),
all_risks as (
    select * from risk_profile
    
    union all
    
    select
        cast(-1 as bigint) as customer_id,
        'UNKNOWN' as risk_tier,
        cast(0 as decimal(10, 4)) as compliance_risk_index,
        cast(0 as decimal(10, 4)) as financial_fragility_score,
        cast(0 as decimal(10, 4)) as behavioral_risk_score,
        cast(0.0000 as decimal(10, 4)) as global_risk_score,
        cast('1900-01-01' as date) as scoring_date
)
select
    row_number() over (order by customer_id, scoring_date) as risk_profile_sk,
    coalesce(customer_id, -1) as customer_id,
    coalesce(risk_tier, 'UNKNOWN') as risk_tier,
    cast(coalesce(compliance_risk_index, 0) as decimal(10, 4)) as compliance_risk_index,
    cast(coalesce(financial_fragility_score, 0) as decimal(10, 4)) as financial_fragility_score,
    cast(coalesce(behavioral_risk_score, 0) as decimal(10, 4)) as behavioral_risk_score,
    cast(coalesce(global_risk_score, 0.0000) as decimal(10, 4)) as global_risk_score,
    coalesce(scoring_date, cast('1900-01-01' as date)) as scoring_date
from all_risks
