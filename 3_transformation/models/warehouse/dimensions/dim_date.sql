-- ══════════════════════════════════════════════════════════════════════════════
-- dim_date: Date dimension
-- Always includes today (scoring_date/load_date) and the 1900-01-01 sentinel
-- so fact table JOINs never fail.
-- ══════════════════════════════════════════════════════════════════════════════
with source_dates as (
    -- Account opening dates
    select cast(opening_date as date) as date_day
    from {{ ref('int_account_enriched') }}

    union all

    -- Customer since dates
    select cast(customer_since_date as date) as date_day
    from {{ ref('int_customer_enriched') }}

    union all

    -- KYC review dates
    select cast(last_kyc_review_date as date) as date_day
    from {{ ref('int_customer_enriched') }}

    union all

    select cast(next_kyc_review_date as date) as date_day
    from {{ ref('int_customer_enriched') }}

    union all

    -- Risk scoring dates
    select cast(oldest_account_opening_date as date) as date_day
    from {{ ref('int_customer_risk_score') }}

    union all

    select cast(newest_account_opening_date as date) as date_day
    from {{ ref('int_customer_risk_score') }}

    union all

    select cast(scoring_date as date) as date_day
    from {{ ref('int_customer_risk_score') }}

    union all

    -- Today (load date) — always present so fact JOINs succeed
    select cast(getdate() as date) as date_day

    union all

    -- Sentinel for UNKNOWN / default dates
    select cast('1900-01-01' as date) as date_day
),
distinct_dates as (
    select distinct date_day
    from source_dates
    where date_day is not null
)
select
    cast(convert(varchar(8), date_day, 112) as int) as date_sk,
    date_day as full_date,
    datepart(year, date_day) as year_number,
    datepart(quarter, date_day) as quarter_number,
    datepart(month, date_day) as month_number,
    datename(month, date_day) as month_name,
    datepart(day, date_day) as day_of_month,
    datepart(weekday, date_day) as day_of_week_number,
    datename(weekday, date_day) as day_name,
    cast(case when datepart(weekday, date_day) in (1, 7) then 1 else 0 end as bit) as is_weekend,
    cast(case when eomonth(date_day) = date_day then 1 else 0 end as bit) as is_month_end,
    cast(case when datefromparts(year(date_day), 12, 31) = date_day then 1 else 0 end as bit) as is_year_end
from distinct_dates
