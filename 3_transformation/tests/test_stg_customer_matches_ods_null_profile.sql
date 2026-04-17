with ods_profile as (
    select
        count(*) as ods_rows,
        sum(case when salary is null then 1 else 0 end) as ods_salary_null,
        sum(case when posting_restrict_46 is null then 1 else 0 end) as ods_posting_null,
        sum(case when employment_status is null then 1 else 0 end) as ods_employment_null,
        sum(case when job_title is null then 1 else 0 end) as ods_job_title_null
    from {{ source('ods', 'ODS_CUSTOMER') }}
),
stg_profile as (
    select
        count(*) as stg_rows,
        sum(case when monthly_salary is null then 1 else 0 end) as stg_salary_null,
        sum(case when posting_restriction_code is null then 1 else 0 end) as stg_posting_null,
        sum(case when employment_status is null then 1 else 0 end) as stg_employment_null,
        sum(case when job_title is null then 1 else 0 end) as stg_job_title_null
    from {{ ref('stg_customer') }}
),
comparison as (
    select
        o.ods_rows,
        s.stg_rows,
        o.ods_salary_null,
        s.stg_salary_null,
        o.ods_posting_null,
        s.stg_posting_null,
        o.ods_employment_null,
        s.stg_employment_null,
        o.ods_job_title_null,
        s.stg_job_title_null
    from ods_profile o
    cross join stg_profile s
)
select *
from comparison
where ods_rows <> stg_rows
   or ods_salary_null <> stg_salary_null
   or ods_posting_null <> stg_posting_null
   or ods_employment_null <> stg_employment_null
   or ods_job_title_null <> stg_job_title_null
