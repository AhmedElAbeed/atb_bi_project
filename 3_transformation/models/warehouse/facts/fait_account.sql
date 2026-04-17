with accounts as (
    select *
    from {{ ref('int_account_enriched') }}
),
customers as (
    select
        customer_id
    from {{ ref('dim_customer') }}
)
select
    account_id,
    coalesce(c.customer_id, cast(-1 as bigint)) as customer_id,
    account_officer_id as branch_id,
    sector_code,
    industry_code,
    target_code,
    currency_code,
    cast(convert(varchar(8), opening_date, 112) as int) as opening_date_key,
    opening_date,
    account_age_days,
    working_balance,
    is_negative_balance,
    cast(getdate() as date) as snapshot_date,
    cast(convert(varchar(8), cast(getdate() as date), 112) as int) as snapshot_date_key
from accounts
left join customers c
    on accounts.customer_id = c.customer_id
