with currency_base as (
    select
        currency_code,
        currency_name,
        no_of_decimals
    from {{ ref('stg_currency') }}
    where currency_code is not null
      and currency_code <> 'UNK'
),
currency as (
    select *
    from currency_base

    union all

    select
        'UNK' as currency_code,
        'UNKNOWN' as currency_name,
        cast(2 as int) as no_of_decimals
)
select
    row_number() over (order by currency_code) as currency_sk,
    coalesce(currency_code, 'UNK') as currency_code,
    coalesce(currency_name, 'UNKNOWN') as currency_name,
    coalesce(no_of_decimals, 2) as no_of_decimals
from currency
