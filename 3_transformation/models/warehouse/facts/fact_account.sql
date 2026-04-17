with account_base as (
    select *
    from {{ ref('int_account_enriched') }}
),
dim_customer as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
),
dim_dao as (
    select dao_sk, account_officer_id
    from {{ ref('dim_dao') }}
),
dim_currency as (
    select currency_sk, currency_code
    from {{ ref('dim_currency') }}
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
)
select
    row_number() over (order by a.account_id) as fact_account_sk,
    a.account_id,
    c.customer_sk,
    d.dao_sk,
    cur.currency_sk,
    s.sector_sk,
    i.industry_sk,
    t.target_sk,
    a.customer_id,
    a.account_officer_id,
    a.currency_code,
    a.sector_code,
    a.industry_code,
    a.target_code,
    a.category_code,
    a.opening_date,
    a.account_age_days,
    a.working_balance,
    a.is_negative_balance,
    a.source_record_id,
    a.extracted_at_utc,
    cast(getdate() as date) as load_date
from account_base a
left join dim_customer c
    on a.customer_id = c.customer_id
left join dim_dao d
    on a.account_officer_id = d.account_officer_id
left join dim_currency cur
    on a.currency_code = cur.currency_code
left join dim_sector s
    on a.sector_code = s.sector_code
left join dim_industry i
    on a.industry_code = i.industry_code
left join dim_target t
    on a.target_code = t.target_code
