select
    target_code,
    target_description
from {{ ref('stg_target') }}
