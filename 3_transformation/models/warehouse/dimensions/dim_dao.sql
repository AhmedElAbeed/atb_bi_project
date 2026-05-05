with dao_base as (
    select
        account_officer_id,
        dao_name,
        dao_area,
        parent_department_code
    from {{ ref('stg_dao') }}
    where account_officer_id is not null
      and account_officer_id <> -1
),
dao as (
    select *
    from dao_base

    union all

    select
        cast(-1 as int) as account_officer_id,
        'UNKNOWN' as dao_name,
        'UNKNOWN' as dao_area,
        cast(-1 as int) as parent_department_code
)
select
    row_number() over (order by account_officer_id) as dao_sk,
    account_officer_id,
    coalesce(dao_name, 'UNKNOWN') as dao_name,
    coalesce(dao_area, 'UNKNOWN') as dao_area,
    coalesce(parent_department_code, -1) as parent_department_code
from dao
