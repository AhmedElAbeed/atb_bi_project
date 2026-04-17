with accounts as (
    select *
    from {{ ref('stg_account') }}
)
select
    customer_id,
    count(*) as account_count,
    sum(working_balance) as total_working_balance,
    avg(working_balance) as avg_working_balance,
    sum(case when working_balance < 0 then 1 else 0 end) as negative_balance_account_count,
    min(opening_date) as oldest_account_opening_date,
    max(opening_date) as newest_account_opening_date
from accounts
group by customer_id
