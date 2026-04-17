select
    c.customer_id,
    c.industry_code,
    c.source_record_id,
    c.extracted_at_utc
from {{ ref('stg_customer') }} c
left join {{ ref('stg_industry') }} i
    on c.industry_code = i.industry_code
where c.industry_code is not null
  and i.industry_code is null
