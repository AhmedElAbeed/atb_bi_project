with date_bounds as (
    select
        coalesce(min(customer_since_date), cast('2015-01-01' as date)) as min_dt,
        coalesce(max(next_kyc_review_date), cast(dateadd(year, 2, getdate()) as date)) as max_dt
    from {{ ref('stg_customer') }}
),
numbers as (
    select
        row_number() over (order by (select null)) - 1 as n
    from sys.all_objects a
    cross join sys.all_objects b
),
calendar as (
    select
        dateadd(day, n.n, b.min_dt) as date_day
    from numbers n
    cross join date_bounds b
    where n.n <= datediff(day, b.min_dt, b.max_dt)
)
select
    cast(convert(varchar(8), date_day, 112) as int) as date_key,
    date_day as full_date,
    datepart(day, date_day) as day_number,
    datepart(month, date_day) as month_number,
    datename(month, date_day) as month_name,
    datepart(quarter, date_day) as quarter_number,
    datepart(year, date_day) as year_number,
    datepart(week, date_day) as week_number,
    datename(weekday, date_day) as day_name,
    case when datepart(weekday, date_day) in (1, 7) then cast(1 as bit) else cast(0 as bit) end as is_weekend
from calendar
