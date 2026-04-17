select
    currency_code,
    numeric_ccy_code,
    currency_name,
    no_of_decimals
from {{ ref('stg_currency') }}
