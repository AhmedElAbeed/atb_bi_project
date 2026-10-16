-- Verify stg_customer row count matches deduped ODS grain (latest record per customer_code).
with ods_ranked as (
    select
        row_number() over (
            partition by nullif(ltrim(rtrim(customer_code)), '')
            order by _airbyte_extracted_at desc, _airbyte_raw_id desc
        ) as rn
    from {{ source('ods', 'ODS_CUSTOMER') }}
    where nullif(ltrim(rtrim(customer_code)), '') is not null
      and try_cast(nullif(ltrim(rtrim(customer_code)), '') as bigint) is not null
),
ods_profile as (
    select count(*) as ods_rows
    from ods_ranked
    where rn = 1
),
stg_profile as (
    select count(*) as stg_rows
    from {{ ref('stg_customer') }}
)
select o.ods_rows, s.stg_rows
from ods_profile o
cross join stg_profile s
where o.ods_rows <> s.stg_rows
