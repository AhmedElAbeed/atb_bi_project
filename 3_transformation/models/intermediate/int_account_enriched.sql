with accounts as (
    select *
    from {{ ref('stg_account') }}
),
customers as (
    select
        customer_id,
        sector_code,
        industry_code,
        target_code
    from {{ ref('stg_customer') }}
),
dao as (
    select
        account_officer_id,
        dao_name,
        dao_area
    from {{ ref('stg_dao') }}
),
currency as (
    select
        currency_code,
        currency_name,
        no_of_decimals
    from {{ ref('stg_currency') }}
)
select
    a.account_id,
    a.customer_id,
    a.account_officer_id,
    a.category_code,
    a.currency_code,
    a.opening_date,
    a.working_balance,
    cast(case when a.working_balance < 0 then 1 else 0 end as bit) as is_negative_balance,
    datediff(day, a.opening_date, cast(getdate() as date)) as account_age_days,
    c.sector_code,
    c.industry_code,
    c.target_code,
    d.dao_name,
    d.dao_area,
    cur.currency_name,
    cur.no_of_decimals,
    a.source_record_id,
    a.extracted_at_utc
from accounts a
left join customers c
    on a.customer_id = c.customer_id
left join dao d
    on a.account_officer_id = d.account_officer_id
left join currency cur
    on a.currency_code = cur.currency_code
