select
    customer_id,
    scoring_date,
    count(*) as row_count
from {{ ref('fact_customer_risk') }}
group by customer_id, scoring_date
having count(*) > 1
