with source_data as (
    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        account_officer,
        area,
        name,
        dept_parent
    from {{ source('ods', 'ODS_DAO') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by nullif(ltrim(rtrim(account_officer)), '')
            order by _airbyte_extracted_at desc, _airbyte_raw_id desc
        ) as rn
    from source_data
)
select
    try_cast(nullif(ltrim(rtrim(account_officer)), '') as int) as account_officer_id,
    nullif(ltrim(rtrim(area)), '') as dao_area,
    nullif(ltrim(rtrim(name)), '') as dao_name,
    nullif(ltrim(rtrim(dept_parent)), '') as parent_department_code,
    try_cast(_airbyte_raw_id as varchar(255)) as source_record_id,
    dateadd(second, try_cast(_airbyte_extracted_at / 1000 as bigint), '1970-01-01') as extracted_at_utc
from ranked
where rn = 1
