with checks as (
    select 'dim_dao' as dim_name, count(*) as unknown_count
    from {{ ref('dim_dao') }}
    where account_officer_id = -1

    union all

    select 'dim_sector' as dim_name, count(*) as unknown_count
    from {{ ref('dim_sector') }}
    where sector_code = -1

    union all

    select 'dim_industry' as dim_name, count(*) as unknown_count
    from {{ ref('dim_industry') }}
    where industry_code = -1

    union all

    select 'dim_target' as dim_name, count(*) as unknown_count
    from {{ ref('dim_target') }}
    where target_code = -1

    union all

    select 'dim_currency' as dim_name, count(*) as unknown_count
    from {{ ref('dim_currency') }}
    where currency_code = 'UNK'

    union all

    select 'dim_customer' as dim_name, count(*) as unknown_count
    from {{ ref('dim_customer') }}
    where customer_id = -1

    union all

    select 'dim_risk_profile' as dim_name, count(*) as unknown_count
    from {{ ref('dim_risk_profile') }}
    where customer_id = -1
)
select *
from checks
where unknown_count <> 1
