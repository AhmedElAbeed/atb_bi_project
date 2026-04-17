select
    sector_code,
    sector_description
from {{ ref('stg_sector') }}
