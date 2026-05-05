with industry_base as (
    select
        industry_code,
        industry_description
    from {{ ref('stg_industry') }}
    where industry_code is not null
      and industry_code <> -1
),
industry as (
    select *
    from industry_base

    union all

    select
        cast(-1 as int) as industry_code,
        'UNKNOWN' as industry_description
)
select
    row_number() over (order by industry_code) as industry_sk,
    coalesce(industry_code, -1) as industry_code,
    coalesce(industry_description, 'UNKNOWN') as industry_description
from industry
