with currency as (
    select
        currency_code,
        currency_name,
        no_of_decimals
    from {{ ref('stg_currency') }}
)
select
    row_number() over (order by currency_code) as currency_sk,
    currency_code,
    currency_name,
    no_of_decimals
from currency
