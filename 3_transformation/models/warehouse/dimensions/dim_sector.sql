with sector_base as (
    select
        sector_code,
        sector_description
    from {{ ref('stg_sector') }}
    where sector_code is not null
      and sector_code <> -1
),
sector as (
    select *
    from sector_base

    union all

    select
        cast(-1 as int) as sector_code,
        'UNKNOWN' as sector_description
)
select
    row_number() over (order by sector_code) as sector_sk,
    coalesce(sector_code, -1) as sector_code,
    coalesce(sector_description, 'UNKNOWN') as sector_description
from sector
