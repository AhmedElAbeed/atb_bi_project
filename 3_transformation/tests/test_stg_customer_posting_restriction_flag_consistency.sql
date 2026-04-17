select
    customer_id,
    posting_restriction_code,
    has_posting_restriction
from {{ ref('stg_customer') }}
where (
        posting_restriction_code is null
    and has_posting_restriction <> cast(0 as bit)
) or (
        posting_restriction_code is not null
    and has_posting_restriction <> cast(1 as bit)
)
