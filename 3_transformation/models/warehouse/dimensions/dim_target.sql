with target as (
    select
        target_code,
        target_description
    from {{ ref('stg_target') }}
)
select
    row_number() over (order by target_code) as target_sk,
    target_code,
    target_description
from target
