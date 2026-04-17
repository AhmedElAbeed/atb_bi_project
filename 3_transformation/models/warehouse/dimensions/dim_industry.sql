with industry as (
    select
        industry_code,
        industry_description
    from {{ ref('stg_industry') }}
)
select
    row_number() over (order by industry_code) as industry_sk,
    industry_code,
    industry_description
from industry
