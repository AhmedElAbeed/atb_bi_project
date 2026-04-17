with dao as (
    select
        account_officer_id,
        dao_name,
        dao_area,
        parent_department_code
    from {{ ref('stg_dao') }}
)
select
    row_number() over (order by account_officer_id) as dao_sk,
    account_officer_id,
    dao_name,
    dao_area,
    parent_department_code
from dao
