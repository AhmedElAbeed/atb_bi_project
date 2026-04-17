with totals as (
    select
        count(*) as total_rows,
        sum(
            case
                when opening_date is null then 1
                else 0
            end
        ) as null_opening_date_rows
    from {{ ref('stg_account') }}
),
rate_check as (
    select
        cast(null_opening_date_rows as float) / nullif(cast(total_rows as float), 0) as null_rate
    from totals
)
select null_rate
from rate_check
where null_rate > 0.02
