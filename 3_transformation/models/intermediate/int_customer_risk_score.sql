with customer_enriched as (
    select *
    from {{ ref('int_customer_enriched') }}
),
account_activity as (
    select *
    from {{ ref('int_customer_account_activity') }}
),
base as (
    select
        c.customer_id,
        c.account_officer_id,
        c.sector_code,
        c.industry_code,
        c.target_code,
        c.customer_since_date,
        c.customer_tenure_days,
        c.number_of_dependents,
        c.employment_status,
        c.monthly_salary,
        c.last_kyc_review_date,
        c.next_kyc_review_date,
        c.is_kyc_complete,
        c.is_pep,
        c.is_compliance_flagged,
        c.has_compliance_decision,
        c.posting_restriction_code,
        coalesce(a.account_count, 0) as account_count,
        coalesce(a.total_working_balance, 0) as total_working_balance,
        coalesce(a.avg_working_balance, 0) as avg_working_balance,
        coalesce(a.negative_balance_account_count, 0) as negative_balance_account_count,
        a.oldest_account_opening_date,
        a.newest_account_opening_date
    from customer_enriched c
    left join account_activity a
        on c.customer_id = a.customer_id
),
scored as (
    select
        *,
        case
            when (
                (case when is_kyc_complete = 0 then 35 else 0 end) +
                (case when is_pep = 1 then 30 else 0 end) +
                (case when is_compliance_flagged = 1 then 25 else 0 end) +
                (case when has_compliance_decision = 1 then 10 else 0 end) +
                (case when nullif(posting_restriction_code, '') is not null then 15 else 0 end) +
                (case when datediff(day, last_kyc_review_date, cast(getdate() as date)) > 365 then 10 else 0 end)
            ) > 100 then 100
            else (
                (case when is_kyc_complete = 0 then 35 else 0 end) +
                (case when is_pep = 1 then 30 else 0 end) +
                (case when is_compliance_flagged = 1 then 25 else 0 end) +
                (case when has_compliance_decision = 1 then 10 else 0 end) +
                (case when nullif(posting_restriction_code, '') is not null then 15 else 0 end) +
                (case when datediff(day, last_kyc_review_date, cast(getdate() as date)) > 365 then 10 else 0 end)
            )
        end as compliance_risk_index,
        case
            when (
                (case when monthly_salary is null or monthly_salary <= 0 then 25 else 0 end) +
                (case when account_count = 0 then 20 else 0 end) +
                (case when total_working_balance < 0 then 30 else 0 end) +
                (case when avg_working_balance between 0 and 500 then 10 else 0 end) +
                (case when number_of_dependents >= 4 then 10 else 0 end) +
                (case when upper(coalesce(employment_status, '')) in ('UNEMPLOYED', 'SANS EMPLOI', 'CHOMEUR') then 20 else 0 end)
            ) > 100 then 100
            else (
                (case when monthly_salary is null or monthly_salary <= 0 then 25 else 0 end) +
                (case when account_count = 0 then 20 else 0 end) +
                (case when total_working_balance < 0 then 30 else 0 end) +
                (case when avg_working_balance between 0 and 500 then 10 else 0 end) +
                (case when number_of_dependents >= 4 then 10 else 0 end) +
                (case when upper(coalesce(employment_status, '')) in ('UNEMPLOYED', 'SANS EMPLOI', 'CHOMEUR') then 20 else 0 end)
            )
        end as financial_fragility_score,
        case
            when (
                (case when customer_tenure_days < 365 then 25 else 0 end) +
                (case when negative_balance_account_count > 0 then 25 else 0 end) +
                (case when account_count = 1 then 10 else 0 end) +
                (case when total_working_balance < 100 then 15 else 0 end) +
                (case when datediff(day, last_kyc_review_date, cast(getdate() as date)) > 730 then 20 else 0 end)
            ) > 100 then 100
            else (
                (case when customer_tenure_days < 365 then 25 else 0 end) +
                (case when negative_balance_account_count > 0 then 25 else 0 end) +
                (case when account_count = 1 then 10 else 0 end) +
                (case when total_working_balance < 100 then 15 else 0 end) +
                (case when datediff(day, last_kyc_review_date, cast(getdate() as date)) > 730 then 20 else 0 end)
            )
        end as behavioral_risk_score
    from base
)
select
    customer_id,
    account_officer_id,
    sector_code,
    industry_code,
    target_code,
    customer_since_date,
    customer_tenure_days,
    account_count,
    total_working_balance,
    avg_working_balance,
    negative_balance_account_count,
    oldest_account_opening_date,
    newest_account_opening_date,
    compliance_risk_index,
    financial_fragility_score,
    behavioral_risk_score,
    cast(round((compliance_risk_index * 0.40) + (financial_fragility_score * 0.35) + (behavioral_risk_score * 0.25), 2) as decimal(10, 2)) as global_risk_score,
    case
        when (compliance_risk_index * 0.40) + (financial_fragility_score * 0.35) + (behavioral_risk_score * 0.25) < 25 then 'LOW'
        when (compliance_risk_index * 0.40) + (financial_fragility_score * 0.35) + (behavioral_risk_score * 0.25) < 50 then 'MEDIUM'
        when (compliance_risk_index * 0.40) + (financial_fragility_score * 0.35) + (behavioral_risk_score * 0.25) < 75 then 'HIGH'
        else 'VERY_HIGH'
    end as risk_tier,
    cast(getdate() as date) as scoring_date
from scored
