{{
  config(
    materialized='table',
    unique_key='fact_account_sk',
    indexes=[
      {'columns': ['customer_sk'], 'type': 'btree'},
      {'columns': ['opening_date_sk'], 'type': 'btree'},
      {'columns': ['load_date_sk'], 'type': 'btree'}
    ]
  )
}}

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
),
dim_date as (
    select date_sk, full_date
    from {{ ref('dim_date') }}
)
select
    cast(a.account_id as bigint) as fact_account_sk,
    c.customer_sk,
    d.dao_sk,
    cur.currency_sk,
    s.sector_sk,
    i.industry_sk,
    t.target_sk,
    dd_open.date_sk as opening_date_sk,
    dd_load.date_sk as load_date_sk,
    coalesce(a.working_balance, cast(0.00 as decimal(18, 2))) as working_balance,
    coalesce(a.is_negative_balance, cast(0 as bit)) as is_negative_balance,
    coalesce(a.account_age_days, 0) as account_age_days,
    cast(getdate() as date) as load_date
from account_base a
inner join dim_customer c
    on a.customer_id = c.customer_id
inner join dim_dao d
    on coalesce(a.account_officer_id, -1) = d.account_officer_id
inner join dim_currency cur
    on coalesce(a.currency_code, 'UNK') = cur.currency_code
inner join dim_sector s
    on coalesce(a.sector_code, -1) = s.sector_code
inner join dim_industry i
    on coalesce(a.industry_code, -1) = i.industry_code
inner join dim_target t
    on coalesce(a.target_code, -1) = t.target_code
inner join dim_date dd_open
    on a.opening_date = dd_open.full_date
inner join dim_date dd_load
    on cast(getdate() as date) = dd_load.full_date
