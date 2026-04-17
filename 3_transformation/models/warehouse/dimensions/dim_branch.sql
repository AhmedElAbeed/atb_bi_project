select
    account_officer_id as branch_id,
    dao_name as branch_name,
    dao_area as branch_area,
    parent_department_code
from {{ ref('stg_dao') }}
