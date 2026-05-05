with target_base as (
    select
        target_code,
        target_description
    from {{ ref('stg_target') }}
    where target_code is not null
      and target_code <> -1
),
target as (
    select *
    from target_base

    union all

    select
        cast(-1 as int) as target_code,
        'UNKNOWN' as target_description
)
select
    row_number() over (order by target_code) as target_sk,
    coalesce(target_code, -1) as target_code,
    coalesce(target_description, 'UNKNOWN') as target_description
from target
