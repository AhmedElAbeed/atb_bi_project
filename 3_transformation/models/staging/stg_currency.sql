with source_data as (
    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        currency_code,
        numeric_ccy_code,
        ccy_name,
        no_of_decimals
    from {{ source('ods', 'ODS_CURRENCY') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by nullif(ltrim(rtrim(currency_code)), '')
            order by _airbyte_extracted_at desc, _airbyte_raw_id desc
        ) as rn
    from source_data
)
select
    upper(nullif(ltrim(rtrim(currency_code)), '')) as currency_code,
    try_cast(numeric_ccy_code as int) as numeric_ccy_code,
    nullif(ltrim(rtrim(ccy_name)), '') as currency_name,
    try_cast(no_of_decimals as int) as no_of_decimals,
    try_cast(_airbyte_raw_id as varchar(255)) as source_record_id,
    dateadd(second, try_cast(_airbyte_extracted_at / 1000 as bigint), '1970-01-01') as extracted_at_utc
from ranked
where rn = 1
