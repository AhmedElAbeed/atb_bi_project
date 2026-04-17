with customers as (
    select *
    from {{ ref('stg_customer') }}
),
sector as (
    select
        sector_code,
        sector_description
    from {{ ref('stg_sector') }}
),
industry as (
    select
        industry_code,
        industry_description
    from {{ ref('stg_industry') }}
),
target as (
    select
        target_code,
        target_description
    from {{ ref('stg_target') }}
),
dao as (
    select
        account_officer_id,
        dao_name,
        dao_area,
        parent_department_code
    from {{ ref('stg_dao') }}
)
select
    c.customer_id,
    c.sector_code,
    s.sector_description,
    c.industry_code,
    i.industry_description,
    c.target_code,
    t.target_description,
    c.account_officer_id,
    d.dao_name,
    d.dao_area,
    d.parent_department_code,
    c.customer_since_date,
    datediff(day, c.customer_since_date, cast(getdate() as date)) as customer_tenure_days,
    c.title,
    c.gender,
    c.marital_status,
    c.number_of_dependents,
    c.employment_status,
    c.job_title,
    c.monthly_salary,
    c.nationality_code,
    c.residence_country_code,
    c.residence_status,
    c.last_kyc_review_date,
    c.next_kyc_review_date,
    c.is_kyc_complete,
    c.is_pep,
    c.is_compliance_flagged,
    c.has_compliance_decision,
    c.posting_restriction_code,
    c.segment_code,
    c.client_nature_code,
    c.legal_capacity_flag,
    c.beneficial_owner_flag,
    c.publication_date,
    c.turnover_amount,
    c.turnover_year,
    c.kyc_score,
    c.source_record_id,
    c.extracted_at_utc
from customers c
left join sector s
    on c.sector_code = s.sector_code
left join industry i
    on c.industry_code = i.industry_code
left join target t
    on c.target_code = t.target_code
left join dao d
    on c.account_officer_id = d.account_officer_id
