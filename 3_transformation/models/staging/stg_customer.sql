with source_data as (
    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        customer_code,
        sector,
        account_officer,
        industry,
        target,
        nationality,
        residence,
        legal_doc_name,
        posting_restrict_46,
        customer_since,
        title,
        gender,
        marital_status,
        no_of_dependents,
        employment_status,
        occupation,
        job_title,
        employment_start,
        salary,
        residence_status,
        last_kyc_review_date,
        auto_next_kyc_review_date,
        kyc_complete,
        segment,
        l_nature_client,
        l_capacite_jur,
        l_benf_reel_cpt,
        l_publi_date,
        l_chiffre_aff,
        l_ann_chiff_aff,
        l_pep,
        l_score_kyc,
        l_flag_conf,
        l_decs_conf
    from {{ source('ods', 'ODS_CUSTOMER') }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by nullif(ltrim(rtrim(customer_code)), '')
            order by _airbyte_extracted_at desc, _airbyte_raw_id desc
        ) as rn
    from source_data
)
select
    try_cast(nullif(ltrim(rtrim(customer_code)), '') as bigint) as customer_id,
    try_cast(nullif(ltrim(rtrim(sector)), '') as int) as sector_code,
    try_cast(nullif(ltrim(rtrim(account_officer)), '') as int) as account_officer_id,
    try_cast(nullif(ltrim(rtrim(industry)), '') as int) as industry_code,
    try_cast(nullif(ltrim(rtrim(target)), '') as int) as target_code,
    upper(nullif(ltrim(rtrim(nationality)), '')) as nationality_code,
    upper(nullif(ltrim(rtrim(residence)), '')) as residence_country_code,
    nullif(ltrim(rtrim(legal_doc_name)), '') as legal_document_type,
    nullif(ltrim(rtrim(posting_restrict_46)), '') as posting_restriction_code,
    try_convert(date, nullif(ltrim(rtrim(customer_since)), ''), 112) as customer_since_date,
    upper(nullif(ltrim(rtrim(title)), '')) as title,
    upper(nullif(ltrim(rtrim(gender)), '')) as gender,
    upper(nullif(ltrim(rtrim(marital_status)), '')) as marital_status,
    try_cast(nullif(ltrim(rtrim(no_of_dependents)), '') as int) as number_of_dependents,
    upper(nullif(ltrim(rtrim(employment_status)), '')) as employment_status,
    nullif(ltrim(rtrim(occupation)), '') as occupation_code,
    nullif(ltrim(rtrim(job_title)), '') as job_title,
    try_convert(date, nullif(ltrim(rtrim(employment_start)), ''), 112) as employment_start_date,
    try_cast(salary as decimal(18, 2)) as monthly_salary,
    upper(nullif(ltrim(rtrim(residence_status)), '')) as residence_status,
    try_convert(date, nullif(ltrim(rtrim(last_kyc_review_date)), ''), 112) as last_kyc_review_date,
    try_convert(date, nullif(ltrim(rtrim(auto_next_kyc_review_date)), ''), 112) as next_kyc_review_date,
    case
        when upper(nullif(ltrim(rtrim(kyc_complete)), '')) in ('OUI', 'YES', 'Y', '1', 'TRUE') then cast(1 as bit)
        when upper(nullif(ltrim(rtrim(kyc_complete)), '')) in ('NON', 'NO', 'N', '0', 'FALSE') then cast(0 as bit)
        else null
    end as is_kyc_complete,
    try_cast(nullif(ltrim(rtrim(segment)), '') as int) as segment_code,
    upper(nullif(ltrim(rtrim(l_nature_client)), '')) as client_nature_code,
    upper(nullif(ltrim(rtrim(l_capacite_jur)), '')) as legal_capacity_flag,
    upper(nullif(ltrim(rtrim(l_benf_reel_cpt)), '')) as beneficial_owner_flag,
    try_convert(date, nullif(ltrim(rtrim(l_publi_date)), ''), 112) as publication_date,
    try_cast(nullif(ltrim(rtrim(l_chiffre_aff)), '') as decimal(18, 2)) as turnover_amount,
    try_cast(nullif(ltrim(rtrim(l_ann_chiff_aff)), '') as int) as turnover_year,
    case
        when upper(nullif(ltrim(rtrim(l_pep)), '')) in ('OUI', 'YES', 'Y', '1', 'TRUE') then cast(1 as bit)
        when upper(nullif(ltrim(rtrim(l_pep)), '')) in ('NON', 'NO', 'N', '0', 'FALSE') then cast(0 as bit)
        else null
    end as is_pep,
    nullif(ltrim(rtrim(l_score_kyc)), '') as kyc_score,
    case
        when upper(nullif(ltrim(rtrim(l_flag_conf)), '')) in ('OUI', 'YES', 'Y', '1', 'TRUE') then cast(1 as bit)
        when upper(nullif(ltrim(rtrim(l_flag_conf)), '')) in ('NON', 'NO', 'N', '0', 'FALSE') then cast(0 as bit)
        else null
    end as is_compliance_flagged,
    case
        when upper(nullif(ltrim(rtrim(l_decs_conf)), '')) in ('OUI', 'YES', 'Y', '1', 'TRUE') then cast(1 as bit)
        when upper(nullif(ltrim(rtrim(l_decs_conf)), '')) in ('NON', 'NO', 'N', '0', 'FALSE') then cast(0 as bit)
        else null
    end as has_compliance_decision,
    try_cast(_airbyte_raw_id as varchar(255)) as source_record_id,
    dateadd(second, try_cast(_airbyte_extracted_at / 1000 as bigint), '1970-01-01') as extracted_at_utc
from ranked
where rn = 1
