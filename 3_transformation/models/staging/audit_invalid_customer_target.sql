select
    c.customer_id,
    c.target_code,
    c.source_record_id,
    c.extracted_at_utc
from {{ ref('stg_customer') }} c
left join {{ ref('stg_target') }} t
    on c.target_code = t.target_code
where c.target_code is not null
  and t.target_code is null
