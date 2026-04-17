with sector as (
    select
        sector_code,
        sector_description
    from {{ ref('stg_sector') }}
)
select
    row_number() over (order by sector_code) as sector_sk,
    sector_code,
    sector_description
from sector
