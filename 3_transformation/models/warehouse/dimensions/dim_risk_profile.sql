select
    cast(1 as int) as risk_profile_id,
    'LOW' as risk_tier,
    cast(0 as decimal(10, 2)) as min_score,
    cast(24.99 as decimal(10, 2)) as max_score,
    'Low risk profile' as risk_profile_label
union all
select
    cast(2 as int) as risk_profile_id,
    'MEDIUM' as risk_tier,
    cast(25 as decimal(10, 2)) as min_score,
    cast(49.99 as decimal(10, 2)) as max_score,
    'Medium risk profile' as risk_profile_label
union all
select
    cast(3 as int) as risk_profile_id,
    'HIGH' as risk_tier,
    cast(50 as decimal(10, 2)) as min_score,
    cast(74.99 as decimal(10, 2)) as max_score,
    'High risk profile' as risk_profile_label
union all
select
    cast(4 as int) as risk_profile_id,
    'VERY_HIGH' as risk_tier,
    cast(75 as decimal(10, 2)) as min_score,
    cast(100 as decimal(10, 2)) as max_score,
    'Very high risk profile' as risk_profile_label
