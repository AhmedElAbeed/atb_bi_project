with customers as (
    select *
    from {{ ref('stg_customer') }}
),
industry_dictionary as (
    select industry_code
    from {{ ref('stg_industry') }}
),
target_dictionary as (
    select target_code
    from {{ ref('stg_target') }}
)
select
    c.*,
    c.industry_code as industry_code_raw,
    c.target_code as target_code_raw,
    case
        when c.industry_code is null then null
        when i.industry_code is not null then c.industry_code
        else null
    end as industry_code_resolved,
    case
        when c.target_code is null then null
        when t.target_code is not null then c.target_code
        else null
    end as target_code_resolved,
    cast(case when c.industry_code is not null and i.industry_code is null then 1 else 0 end as bit) as is_industry_code_remapped,
    cast(case when c.target_code is not null and t.target_code is null then 1 else 0 end as bit) as is_target_code_remapped
from customers c
left join industry_dictionary i
    on c.industry_code = i.industry_code
left join target_dictionary t
    on c.target_code = t.target_code
