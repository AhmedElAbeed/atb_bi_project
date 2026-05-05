{{
  config(
    materialized='table',
    unique_key='fact_customer_risk_sk',
    indexes=[
      {'columns': ['customer_sk'], 'type': 'btree'},
      {'columns': ['scoring_date_sk'], 'type': 'btree'},
      {'columns': ['load_date_sk'], 'type': 'btree'}
    ]
  )
}}

-- ══════════════════════════════════════════════════════════════════════════════
-- fact_customer_risk
-- Grain: ONE row per customer per scoring_date
--
-- Design decisions:
--   • INNER JOIN on mandatory dims (customer, risk_profile, scoring_date, load_date)
--   • LEFT JOIN on optional dims (dao, sector, industry, target) →
--     COALESCE to the UNKNOWN sentinel (-1) so FKs are NEVER NULL
--   • Customers with 0 accounts keep their correct 0 balances but get
--     meaningful default dates (1900-01-01) instead of NULL
-- ══════════════════════════════════════════════════════════════════════════════

with risk_base as (
    select *
    from {{ ref('int_customer_risk_score') }}
    -- Drop rows that are missing mandatory business keys
    where customer_id           is not null
      and scoring_date          is not null
      and customer_since_date   is not null
),
risk_dedup as (
    select
        *,
        row_number() over (
            partition by customer_id, scoring_date
            order by scoring_date desc
        ) as rn
    from risk_base
),

dim_customer as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
    where customer_id <> -1  -- exclude UNKNOWN sentinel from join
),

dim_risk_profile as (
    select risk_profile_sk, customer_id, scoring_date
    from {{ ref('dim_risk_profile') }}
    where customer_id <> -1  -- exclude UNKNOWN sentinel from join
),

dim_dao as (
    select dao_sk, account_officer_id
    from {{ ref('dim_dao') }}
),

dim_sector as (
    select sector_sk, sector_code
    from {{ ref('dim_sector') }}
),

dim_industry as (
    select industry_sk, industry_code
    from {{ ref('dim_industry') }}
),

dim_target as (
    select target_sk, target_code
    from {{ ref('dim_target') }}
),

dim_date as (
    select date_sk, full_date
    from {{ ref('dim_date') }}
),

-- ─── Lookup the UNKNOWN sentinel SK for each optional dimension ──────────────
unknown_dao as (
    select min(dao_sk) as dao_sk
    from {{ ref('dim_dao') }}
    where account_officer_id = -1
),
unknown_sector as (
    select min(sector_sk) as sector_sk
    from {{ ref('dim_sector') }}
    where sector_code = -1
),
unknown_industry as (
    select min(industry_sk) as industry_sk
    from {{ ref('dim_industry') }}
    where industry_code = -1
),
unknown_target as (
    select min(target_sk) as target_sk
    from {{ ref('dim_target') }}
    where target_code = -1
),

-- ─── Resolve NULL/invalid codes BEFORE joining ──────────────────────────────
enriched as (
    select
        r.customer_id,
        r.account_officer_id,
        r.sector_code,
        r.industry_code,
        r.target_code,
        r.customer_since_date,
        r.customer_tenure_days,
        r.account_count,
        r.total_working_balance,
        r.avg_working_balance,
        r.negative_balance_account_count,
        r.oldest_account_opening_date,
        r.newest_account_opening_date,
        r.compliance_risk_index,
        r.financial_fragility_score,
        r.behavioral_risk_score,
        r.global_risk_score,
        r.risk_tier,
        r.scoring_date,
        -- Resolved keys for dimension lookups
        case
            when r.account_officer_id is null or r.account_officer_id = 0 then -1
            else r.account_officer_id
        end as ao_id_resolved,
        coalesce(r.sector_code,   -1) as sector_code_resolved,
        coalesce(r.industry_code, -1) as industry_code_resolved,
        coalesce(r.target_code,   -1) as target_code_resolved
    from risk_dedup r
    where r.rn = 1
)

select
    -- Deterministic and collision-free within model grain
    row_number() over (order by e.customer_id, e.scoring_date) as fact_customer_risk_sk,

    -- ── DIMENSION FOREIGN KEYS (guaranteed non-NULL) ────────────────────────
    c.customer_sk,
    rp.risk_profile_sk,
    coalesce(d.dao_sk,  u_dao.dao_sk)           as dao_sk,
    coalesce(s.sector_sk, u_sec.sector_sk)      as sector_sk,
    coalesce(i.industry_sk, u_ind.industry_sk)  as industry_sk,
    coalesce(t.target_sk, u_tgt.target_sk)      as target_sk,
    dd_score.date_sk                            as scoring_date_sk,
    dd_load.date_sk                             as load_date_sk,

    -- ── DEGENERATE DIMENSIONS (business keys kept on the fact) ──────────────
    e.customer_id,
    coalesce(e.ao_id_resolved, -1) as account_officer_id,
    coalesce(e.sector_code_resolved, -1) as sector_code,
    coalesce(e.industry_code_resolved, -1) as industry_code,
    coalesce(e.target_code_resolved, -1) as target_code,

    -- ── CUSTOMER ATTRIBUTES ─────────────────────────────────────────────────
    coalesce(e.customer_since_date, cast('1900-01-01' as date)) as customer_since_date,
    coalesce(e.customer_tenure_days, 0) as customer_tenure_days,

    -- ── MEASURES ────────────────────────────────────────────────────────────
    coalesce(e.account_count, 0) as account_count,
    coalesce(e.total_working_balance,  cast(0.00 as decimal(18,2))) as total_working_balance,
    coalesce(e.avg_working_balance,    cast(0.00 as decimal(18,2))) as avg_working_balance,
    coalesce(e.negative_balance_account_count, 0) as negative_balance_account_count,

    -- ── ACCOUNT DATE SPAN (default to sentinel when no accounts) ───────────
    coalesce(e.oldest_account_opening_date, cast('1900-01-01' as date)) as oldest_account_opening_date,
    coalesce(e.newest_account_opening_date, cast('1900-01-01' as date)) as newest_account_opening_date,

    -- ── RISK SCORES ─────────────────────────────────────────────────────────
    cast(coalesce(e.compliance_risk_index,     0) as decimal(10,4)) as compliance_risk_index,
    cast(coalesce(e.financial_fragility_score, 0) as decimal(10,4)) as financial_fragility_score,
    cast(coalesce(e.behavioral_risk_score,     0) as decimal(10,4)) as behavioral_risk_score,
    cast(coalesce(e.global_risk_score,         0) as decimal(10,4)) as global_risk_score,

    -- ── RISK CLASSIFICATION ─────────────────────────────────────────────────
    coalesce(e.risk_tier, 'UNKNOWN') as risk_tier,

    -- ── AUDIT DATES ─────────────────────────────────────────────────────────
    e.scoring_date,
    cast(getdate() as date) as load_date

from enriched e

-- ── MANDATORY: customer must exist in dim_customer ──────────────────────────
inner join dim_customer c
    on e.customer_id = c.customer_id

-- ── MANDATORY: risk profile must exist ──────────────────────────────────────
inner join dim_risk_profile rp
    on e.customer_id  = rp.customer_id
   and e.scoring_date = rp.scoring_date

-- ── MANDATORY: scoring date must be in dim_date ─────────────────────────────
inner join dim_date dd_score
    on e.scoring_date = dd_score.full_date

-- ── MANDATORY: today's load date must be in dim_date ────────────────────────
inner join dim_date dd_load
    on cast(getdate() as date) = dd_load.full_date

-- ── OPTIONAL: DAO — LEFT JOIN, fall back to UNKNOWN ─────────────────────────
left join dim_dao d
    on e.ao_id_resolved = d.account_officer_id
   and d.account_officer_id <> -1

-- ── OPTIONAL: Sector — LEFT JOIN, fall back to UNKNOWN ──────────────────────
left join dim_sector s
    on e.sector_code_resolved = s.sector_code
   and s.sector_code <> -1

-- ── OPTIONAL: Industry — LEFT JOIN, fall back to UNKNOWN ────────────────────
left join dim_industry i
    on e.industry_code_resolved = i.industry_code
   and i.industry_code <> -1

-- ── OPTIONAL: Target — LEFT JOIN, fall back to UNKNOWN ──────────────────────
left join dim_target t
    on e.target_code_resolved = t.target_code
   and t.target_code <> -1

-- ── UNKNOWN sentinel lookups (scalar-safe single row each) ───────────────────
cross join unknown_dao      u_dao
cross join unknown_sector   u_sec
cross join unknown_industry u_ind
cross join unknown_target   u_tgt
