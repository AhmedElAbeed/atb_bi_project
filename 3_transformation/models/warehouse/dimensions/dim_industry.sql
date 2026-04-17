select
    industry_code,
    industry_description
from {{ ref('stg_industry') }}
