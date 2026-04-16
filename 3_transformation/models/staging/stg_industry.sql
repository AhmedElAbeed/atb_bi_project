with source_data as (
    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        industry_code,
        description
    from {{ source('ods', 'ODS_INDUSTRY') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by nullif(ltrim(rtrim(industry_code)), '')
            order by _airbyte_extracted_at desc, _airbyte_raw_id desc
        ) as rn
    from source_data
)
select
    try_cast(nullif(ltrim(rtrim(industry_code)), '') as int) as industry_code,
    nullif(ltrim(rtrim(description)), '') as industry_description,
    try_cast(_airbyte_raw_id as varchar(255)) as source_record_id,
    dateadd(second, try_cast(_airbyte_extracted_at / 1000 as bigint), '1970-01-01') as extracted_at_utc
from ranked
where rn = 1
