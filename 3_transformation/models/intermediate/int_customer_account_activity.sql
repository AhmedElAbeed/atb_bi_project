with accounts as (
    select *
    from {{ ref('stg_account') }}
),
all_customers as (
    select distinct
        customer_id
    from {{ ref('stg_customer') }}
),
account_aggregates as (
    select
        customer_id,
        count(*) as account_count,
        coalesce(sum(working_balance), 0.00) as total_working_balance,
        coalesce(avg(working_balance), 0.00) as avg_working_balance,
        coalesce(min(working_balance), 0.00) as min_working_balance,
        coalesce(max(working_balance), 0.00) as max_working_balance,
        sum(case when is_negative_balance = 1 then 1 else 0 end) as negative_balance_account_count,
        min(opening_date) as oldest_account_opening_date,
        max(opening_date) as newest_account_opening_date,
        coalesce(cast(datediff(day, min(opening_date), cast(getdate() as date)) as int), 0) as oldest_account_age_days,
        coalesce(cast(avg(cast(datediff(day, opening_date, cast(getdate() as date)) as int)) as int), 0) as avg_account_age_days
    from accounts
    group by customer_id
)
select
    ac.customer_id,
    coalesce(aa.account_count, 0) as account_count,
    coalesce(aa.total_working_balance, 0.00) as total_working_balance,
    coalesce(aa.avg_working_balance, 0.00) as avg_working_balance,
    coalesce(aa.min_working_balance, 0.00) as min_working_balance,
    coalesce(aa.max_working_balance, 0.00) as max_working_balance,
    coalesce(aa.negative_balance_account_count, 0) as negative_balance_account_count,
    aa.oldest_account_opening_date,
    aa.newest_account_opening_date,
    coalesce(aa.oldest_account_age_days, 0) as oldest_account_age_days,
    coalesce(aa.avg_account_age_days, 0) as avg_account_age_days
from all_customers ac
left join account_aggregates aa on ac.customer_id = aa.customer_id
