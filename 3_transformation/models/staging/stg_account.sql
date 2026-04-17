with source_data as (
    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        recid,
        customer_no,
        category,
        currency,
        account_officer,
        working_balance,
        opening_date
    from {{ source('ods', 'ODS_ACCOUNT') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by nullif(ltrim(rtrim(recid)), '')
            order by _airbyte_extracted_at desc, _airbyte_raw_id desc
        ) as rn
    from source_data
)
select
    try_cast(nullif(ltrim(rtrim(recid)), '') as bigint) as account_id,
    try_cast(nullif(ltrim(rtrim(customer_no)), '') as bigint) as customer_id,
    nullif(ltrim(rtrim(category)), '') as category_code,
    upper(nullif(ltrim(rtrim(currency)), '')) as currency_code,
    try_cast(nullif(ltrim(rtrim(account_officer)), '') as int) as account_officer_id,
    try_cast(working_balance as decimal(18, 2)) as working_balance,
    case
        when try_cast(working_balance as decimal(18, 2)) = 0 then cast(1 as bit)
        else cast(0 as bit)
    end as is_zero_balance,
    try_convert(date, nullif(ltrim(rtrim(opening_date)), ''), 112) as opening_date,
    try_cast(_airbyte_raw_id as varchar(255)) as source_record_id,
    dateadd(second, try_cast(_airbyte_extracted_at / 1000 as bigint), '1970-01-01') as extracted_at_utc
from ranked
where rn = 1
