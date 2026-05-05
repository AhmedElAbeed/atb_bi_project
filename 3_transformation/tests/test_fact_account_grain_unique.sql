select
    fact_account_sk,
    count(*) as row_count
from {{ ref('fact_account') }}
group by fact_account_sk
having count(*) > 1
