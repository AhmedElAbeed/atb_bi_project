"""Data loaders for PFE_DWH tables (aggregated SQL for performance)."""
from __future__ import annotations

import streamlit as st

from dashboard.config import DEFAULT_TOP_N, ML_SCHEMA, SCHEMA
from dashboard.db import sql_query


def _dao_ml_filter() -> str:
    try:
        from dashboard.auth import dao_filter_ml_sql
        return dao_filter_ml_sql()
    except Exception:
        return ""


def _dao_dim_filter(alias: str = "d") -> str:
    try:
        from dashboard.auth import dao_filter_sql
        return dao_filter_sql(alias)
    except Exception:
        return ""


def _table_count(table: str) -> int:
    df = sql_query(f"SELECT COUNT(*) AS n FROM {SCHEMA}.{table}")
    return int(df.iloc[0]["n"])


# ─── Overview ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_warehouse_overview() -> dict:
    tables = [
        "dim_currency", "dim_customer", "dim_date", "dim_dao",
        "dim_industry", "dim_sector", "dim_target",
        "fact_account", "fact_customer_risk",
    ]
    rows = {}
    for t in tables:
        try:
            rows[t] = _table_count(t)
        except Exception:
            rows[t] = 0
    return rows


@st.cache_data(ttl=300, show_spinner=False)
def load_executive_kpis() -> dict:
    q = f"""
    SELECT
        (SELECT COUNT(*) FROM {SCHEMA}.dim_customer WHERE customer_id <> -1) AS customers,
        (SELECT COUNT(*) FROM {SCHEMA}.fact_account) AS accounts,
        (SELECT AVG(CAST(global_risk_score AS FLOAT)) FROM {SCHEMA}.fact_customer_risk) AS avg_risk,
        (SELECT SUM(CAST(working_balance AS FLOAT)) FROM {SCHEMA}.fact_account) AS total_balance,
        (SELECT SUM(CASE WHEN risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*),0) FROM {SCHEMA}.fact_customer_risk) AS high_risk_pct,
        (SELECT SUM(CASE WHEN is_negative_balance = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*),0) FROM {SCHEMA}.fact_account) AS overdraft_pct
    """
    row = sql_query(q).iloc[0]
    return {k: float(row[k]) if row[k] is not None else 0.0 for k in row.index}


# ─── fact_customer_risk ────────────────────────────────────────────────────────

@st.cache_data(ttl=180, show_spinner=False)
def load_risk_tier_summary() -> "pd.DataFrame":
    import pandas as pd

    return sql_query(f"""
    SELECT risk_tier, COUNT(*) AS customer_count,
           AVG(CAST(global_risk_score AS FLOAT)) AS avg_score,
           AVG(CAST(compliance_risk_index AS FLOAT)) AS avg_compliance,
           AVG(CAST(financial_fragility_score AS FLOAT)) AS avg_fragility,
           AVG(CAST(behavioral_risk_score AS FLOAT)) AS avg_behavioral
    FROM {SCHEMA}.fact_customer_risk
    GROUP BY risk_tier
    ORDER BY avg_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_score_bins() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        FLOOR(CAST(global_risk_score AS FLOAT) / 5) * 5 AS score_bin,
        COUNT(*) AS customer_count
    FROM {SCHEMA}.fact_customer_risk
    GROUP BY FLOOR(CAST(global_risk_score AS FLOAT) / 5) * 5
    ORDER BY score_bin
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_by_sector(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        s.sector_description,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    WHERE s.sector_code <> -1
    GROUP BY s.sector_description
    ORDER BY avg_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_by_dao_area(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        d.dao_area,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.account_officer_id <> -1
    GROUP BY d.dao_area
    ORDER BY avg_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_pillar_corr() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP 5000
        CAST(compliance_risk_index AS FLOAT) AS compliance,
        CAST(financial_fragility_score AS FLOAT) AS fragility,
        CAST(behavioral_risk_score AS FLOAT) AS behavioral,
        CAST(global_risk_score AS FLOAT) AS global_score,
        risk_tier
    FROM {SCHEMA}.fact_customer_risk
    ORDER BY NEWID()
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_sample(top_n: int = DEFAULT_TOP_N) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        f.customer_id, f.risk_tier, f.global_risk_score,
        f.compliance_risk_index, f.financial_fragility_score, f.behavioral_risk_score,
        f.account_count, f.total_working_balance, f.scoring_date,
        s.sector_description, d.dao_name, d.dao_area, t.target_description
    FROM {SCHEMA}.fact_customer_risk f
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    LEFT JOIN {SCHEMA}.dim_target t ON f.target_sk = t.target_sk
    ORDER BY f.global_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_balance_vs_risk() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP 8000
        CAST(total_working_balance AS FLOAT) AS total_balance,
        CAST(global_risk_score AS FLOAT) AS global_risk_score,
        risk_tier
    FROM {SCHEMA}.fact_customer_risk
    WHERE total_working_balance IS NOT NULL
    ORDER BY NEWID()
    """)


# ─── fact_account ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=180, show_spinner=False)
def load_account_kpis() -> dict:
    row = sql_query(f"""
    SELECT
        COUNT(*) AS accounts,
        SUM(CAST(working_balance AS FLOAT)) AS total_balance,
        AVG(CAST(working_balance AS FLOAT)) AS avg_balance,
        AVG(CAST(account_age_days AS FLOAT)) AS avg_age_days,
        SUM(CASE WHEN is_negative_balance = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*),0) AS overdraft_pct
    FROM {SCHEMA}.fact_account
    """).iloc[0]
    return {k: float(row[k]) if row[k] is not None else 0.0 for k in row.index}


@st.cache_data(ttl=180, show_spinner=False)
def load_accounts_by_currency() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT c.currency_name, c.currency_code,
           COUNT(*) AS account_count,
           SUM(CAST(f.working_balance AS FLOAT)) AS total_balance,
           AVG(CAST(f.working_balance AS FLOAT)) AS avg_balance
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_currency c ON f.currency_sk = c.currency_sk
    WHERE c.currency_code <> 'UNK'
    GROUP BY c.currency_name, c.currency_code
    ORDER BY total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_accounts_by_sector(top_n: int = 12) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        s.sector_description,
        COUNT(*) AS account_count,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    WHERE s.sector_code <> -1
    GROUP BY s.sector_description
    ORDER BY total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_accounts_by_dao_area(top_n: int = 12) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        d.dao_area,
        COUNT(*) AS account_count,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.account_officer_id <> -1
    GROUP BY d.dao_area
    ORDER BY account_count DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_account_age_distribution() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE
            WHEN account_age_days < 365 THEN '< 1 year'
            WHEN account_age_days < 1825 THEN '1-5 years'
            WHEN account_age_days < 3650 THEN '5-10 years'
            ELSE '10+ years'
        END AS age_bucket,
        COUNT(*) AS account_count
    FROM {SCHEMA}.fact_account
    GROUP BY
        CASE
            WHEN account_age_days < 365 THEN '< 1 year'
            WHEN account_age_days < 1825 THEN '1-5 years'
            WHEN account_age_days < 3650 THEN '5-10 years'
            ELSE '10+ years'
        END
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_account_sample(top_n: int = 5000) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        f.fact_account_sk, f.working_balance, f.is_negative_balance, f.account_age_days,
        c.currency_name, s.sector_description, d.dao_name, d.dao_area, t.target_description,
        dt.full_date AS opening_date
    FROM {SCHEMA}.fact_account f
    LEFT JOIN {SCHEMA}.dim_currency c ON f.currency_sk = c.currency_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    LEFT JOIN {SCHEMA}.dim_target t ON f.target_sk = t.target_sk
    LEFT JOIN {SCHEMA}.dim_date dt ON f.opening_date_sk = dt.date_sk
    ORDER BY f.working_balance DESC
    """)


# ─── Dimensions ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_dim_currency() -> "pd.DataFrame":
    return sql_query(f"SELECT * FROM {SCHEMA}.dim_currency ORDER BY currency_code")


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_customer_stats() -> dict:
    row = sql_query(f"""
    SELECT
        COUNT(*) AS total,
        AVG(CAST(monthly_salary AS FLOAT)) AS avg_salary,
        AVG(CAST(customer_tenure_days AS FLOAT)) AS avg_tenure,
        SUM(CASE WHEN is_kyc_complete = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*),0) AS kyc_pct,
        SUM(CASE WHEN is_pep = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*),0) AS pep_pct,
        SUM(CASE WHEN is_compliance_flagged = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*),0) AS compliance_pct
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1
    """).iloc[0]
    return {k: float(row[k]) if row[k] is not None else 0.0 for k in row.index}


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_gender() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT gender, COUNT(*) AS customer_count
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1 AND gender <> 'UNKNOWN'
    GROUP BY gender
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_employment(top_n: int = 10) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)} employment_status, COUNT(*) AS customer_count
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1 AND employment_status <> 'UNKNOWN'
    GROUP BY employment_status
    ORDER BY customer_count DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_by_sector(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)} sector_description, COUNT(*) AS customer_count,
           AVG(CAST(monthly_salary AS FLOAT)) AS avg_salary
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1 AND sector_code <> -1
    GROUP BY sector_description
    ORDER BY customer_count DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_salary_bins() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE
            WHEN monthly_salary <= 0 THEN 'No salary'
            WHEN monthly_salary < 1000 THEN '< 1k'
            WHEN monthly_salary < 3000 THEN '1k - 3k'
            WHEN monthly_salary < 6000 THEN '3k - 6k'
            ELSE '6k+'
        END AS salary_band,
        COUNT(*) AS customer_count
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1
    GROUP BY
        CASE
            WHEN monthly_salary <= 0 THEN 'No salary'
            WHEN monthly_salary < 1000 THEN '< 1k'
            WHEN monthly_salary < 3000 THEN '1k - 3k'
            WHEN monthly_salary < 6000 THEN '3k - 6k'
            ELSE '6k+'
        END
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_dao() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT * FROM {SCHEMA}.dim_dao
    WHERE account_officer_id <> -1
    ORDER BY dao_area, dao_name
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dao_by_area() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT dao_area, COUNT(*) AS branch_count
    FROM {SCHEMA}.dim_dao
    WHERE account_officer_id <> -1
    GROUP BY dao_area
    ORDER BY branch_count DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_sector() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT * FROM {SCHEMA}.dim_sector WHERE sector_code <> -1 ORDER BY sector_description
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_industry(top_n: int = 20) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)} i.industry_description, COUNT(c.customer_sk) AS customer_count
    FROM {SCHEMA}.dim_industry i
    LEFT JOIN {SCHEMA}.dim_customer c ON c.industry_code = i.industry_code AND c.customer_id <> -1
    WHERE i.industry_code <> -1
    GROUP BY i.industry_description
    ORDER BY customer_count DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_industry_all() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT * FROM {SCHEMA}.dim_industry WHERE industry_code <> -1 ORDER BY industry_description
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_target() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT * FROM {SCHEMA}.dim_target WHERE target_code <> -1 ORDER BY target_description
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_target_customer_counts() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT t.target_description, COUNT(c.customer_sk) AS customer_count
    FROM {SCHEMA}.dim_target t
    LEFT JOIN {SCHEMA}.dim_customer c ON c.target_code = t.target_code AND c.customer_id <> -1
    WHERE t.target_code <> -1
    GROUP BY t.target_description
    ORDER BY customer_count DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_date_by_year() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT year_number AS year, COUNT(*) AS date_count
    FROM {SCHEMA}.dim_date
    GROUP BY year_number
    ORDER BY year_number
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_date_weekend() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
           COUNT(*) AS date_count
    FROM {SCHEMA}.dim_date
    GROUP BY CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dim_date_months() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT month_name, COUNT(*) AS date_count
    FROM {SCHEMA}.dim_date
    GROUP BY month_name, month_number
    ORDER BY month_number
    """)


# ─── Cross-analytics (BI studies) ─────────────────────────────────────────────

@st.cache_data(ttl=180, show_spinner=False)
def load_kyc_vs_risk() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE WHEN c.is_kyc_complete = 1 THEN 'KYC complete' ELSE 'KYC incomplete' END AS kyc_status,
        f.risk_tier,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1
    GROUP BY CASE WHEN c.is_kyc_complete = 1 THEN 'KYC complete' ELSE 'KYC incomplete' END, f.risk_tier
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_compliance_flags_vs_risk() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE
            WHEN c.is_pep = 1 THEN 'PEP'
            WHEN c.is_compliance_flagged = 1 THEN 'Compliance flagged'
            WHEN c.is_kyc_complete = 0 THEN 'KYC incomplete'
            ELSE 'Clean profile'
        END AS compliance_category,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1
    GROUP BY
        CASE
            WHEN c.is_pep = 1 THEN 'PEP'
            WHEN c.is_compliance_flagged = 1 THEN 'Compliance flagged'
            WHEN c.is_kyc_complete = 0 THEN 'KYC incomplete'
            ELSE 'Clean profile'
        END
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_salary_vs_risk_sample(top_n: int = 6000) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        CAST(c.monthly_salary AS FLOAT) AS monthly_salary,
        CAST(f.global_risk_score AS FLOAT) AS global_risk_score,
        f.risk_tier,
        c.employment_status
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1 AND c.monthly_salary > 0
    ORDER BY NEWID()
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_heatmap_sector_dao(top_sectors: int = 8, top_areas: int = 8) -> "pd.DataFrame":
    return sql_query(f"""
    WITH top_s AS (
        SELECT TOP {int(top_sectors)} s.sector_description
        FROM {SCHEMA}.fact_customer_risk f
        JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
        WHERE s.sector_code <> -1
        GROUP BY s.sector_description
        ORDER BY COUNT(*) DESC
    ),
    top_a AS (
        SELECT TOP {int(top_areas)} d.dao_area
        FROM {SCHEMA}.fact_customer_risk f
        JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
        WHERE d.account_officer_id <> -1
        GROUP BY d.dao_area
        ORDER BY COUNT(*) DESC
    )
    SELECT
        s.sector_description,
        d.dao_area,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        COUNT(*) AS customer_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE s.sector_description IN (SELECT sector_description FROM top_s)
      AND d.dao_area IN (SELECT dao_area FROM top_a)
    GROUP BY s.sector_description, d.dao_area
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_accounts_by_opening_year() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        dt.year_number AS opening_year,
        COUNT(*) AS account_count,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance,
        SUM(CASE WHEN f.is_negative_balance = 1 THEN 1 ELSE 0 END) AS overdraft_count
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_date dt ON f.opening_date_sk = dt.date_sk
    WHERE dt.year_number BETWEEN 1990 AND 2030
    GROUP BY dt.year_number
    ORDER BY dt.year_number
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_industry_risk(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        i.industry_description,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_industry i ON f.industry_sk = i.industry_sk
    WHERE i.industry_code <> -1
    GROUP BY i.industry_description
    HAVING COUNT(*) >= 50
    ORDER BY avg_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_target_risk() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        t.target_description,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        AVG(CAST(f.total_working_balance AS FLOAT)) AS avg_balance,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_target t ON f.target_sk = t.target_sk
    WHERE t.target_code <> -1
    GROUP BY t.target_description
    ORDER BY avg_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_by_gender() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        c.gender,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1 AND c.gender IN ('M', 'F')
    GROUP BY c.gender
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_sector_account_risk_join(top_n: int = 12) -> "pd.DataFrame":
    return sql_query(f"""
    WITH risk_by_sector AS (
        SELECT
            s.sector_description,
            COUNT(DISTINCT f.customer_sk) AS customers,
            AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score
        FROM {SCHEMA}.fact_customer_risk f
        JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
        WHERE s.sector_code <> -1
        GROUP BY s.sector_description
    ),
    acct_by_sector AS (
        SELECT
            s.sector_description,
            COUNT(*) AS accounts,
            SUM(CAST(a.working_balance AS FLOAT)) AS total_balance
        FROM {SCHEMA}.fact_account a
        JOIN {SCHEMA}.dim_sector s ON a.sector_sk = s.sector_sk
        WHERE s.sector_code <> -1
        GROUP BY s.sector_description
    )
    SELECT TOP {int(top_n)}
        r.sector_description,
        r.customers,
        a.accounts,
        a.total_balance,
        r.avg_risk_score
    FROM risk_by_sector r
    JOIN acct_by_sector a ON r.sector_description = a.sector_description
    ORDER BY a.total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_currency_exposure() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        c.currency_code,
        c.currency_name,
        COUNT(*) AS account_count,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance,
        AVG(CAST(f.account_age_days AS FLOAT)) AS avg_age_days,
        SUM(CASE WHEN f.is_negative_balance = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0) AS overdraft_pct
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_currency c ON f.currency_sk = c.currency_sk
    WHERE c.currency_code <> 'UNK'
    GROUP BY c.currency_code, c.currency_name
    ORDER BY total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_by_employment(top_n: int = 10) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        c.employment_status,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1 AND c.employment_status NOT IN ('UNKNOWN', '')
    GROUP BY c.employment_status
    ORDER BY avg_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_tenure_risk_bins() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE
            WHEN c.customer_tenure_days < 365 THEN '< 1 year'
            WHEN c.customer_tenure_days < 1825 THEN '1-5 years'
            WHEN c.customer_tenure_days < 3650 THEN '5-10 years'
            ELSE '10+ years'
        END AS tenure_band,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1
    GROUP BY
        CASE
            WHEN c.customer_tenure_days < 365 THEN '< 1 year'
            WHEN c.customer_tenure_days < 1825 THEN '1-5 years'
            WHEN c.customer_tenure_days < 3650 THEN '5-10 years'
            ELSE '10+ years'
        END
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_overdraft_risk_cross() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE WHEN f.negative_balance_account_count > 0 THEN 'Has overdraft' ELSE 'No overdraft' END AS overdraft_status,
        f.risk_tier,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score
    FROM {SCHEMA}.fact_customer_risk f
    GROUP BY
        CASE WHEN f.negative_balance_account_count > 0 THEN 'Has overdraft' ELSE 'No overdraft' END,
        f.risk_tier
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_balance_percentiles() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY CAST(working_balance AS FLOAT)) OVER () AS p10,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(working_balance AS FLOAT)) OVER () AS p25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CAST(working_balance AS FLOAT)) OVER () AS p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(working_balance AS FLOAT)) OVER () AS p75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY CAST(working_balance AS FLOAT)) OVER () AS p90,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY CAST(working_balance AS FLOAT)) OVER () AS p99
    FROM {SCHEMA}.fact_account
    """).drop_duplicates()


@st.cache_data(ttl=180, show_spinner=False)
def load_kyc_pep_matrix() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE WHEN c.is_kyc_complete = 1 THEN 'KYC OK' ELSE 'KYC missing' END AS kyc_status,
        CASE WHEN c.is_pep = 1 THEN 'PEP' ELSE 'Non-PEP' END AS pep_status,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id <> -1
    GROUP BY
        CASE WHEN c.is_kyc_complete = 1 THEN 'KYC OK' ELSE 'KYC missing' END,
        CASE WHEN c.is_pep = 1 THEN 'PEP' ELSE 'Non-PEP' END
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_overdraft_by_sector(top_n: int = 12) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        s.sector_description,
        COUNT(*) AS account_count,
        SUM(CASE WHEN f.is_negative_balance = 1 THEN 1 ELSE 0 END) AS overdraft_count,
        SUM(CASE WHEN f.is_negative_balance = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0) AS overdraft_pct,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    WHERE s.sector_code <> -1
    GROUP BY s.sector_description
    ORDER BY overdraft_pct DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_risk_pillar_radar() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT risk_tier,
           AVG(CAST(compliance_risk_index AS FLOAT)) AS compliance,
           AVG(CAST(financial_fragility_score AS FLOAT)) AS fragility,
           AVG(CAST(behavioral_risk_score AS FLOAT)) AS behavioral
    FROM {SCHEMA}.fact_customer_risk
    GROUP BY risk_tier
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_accounts_opening_monthly() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        dt.year_number AS year,
        dt.month_number AS month,
        dt.month_name,
        COUNT(*) AS account_count,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_date dt ON f.opening_date_sk = dt.date_sk
    WHERE dt.year_number >= 2015
    GROUP BY dt.year_number, dt.month_number, dt.month_name
    ORDER BY dt.year_number, dt.month_number
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_accounts_by_opening_monthly() -> "pd.DataFrame":
    """Alias for load_accounts_opening_monthly."""
    return load_accounts_opening_monthly()


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_compliance_flags() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE
            WHEN is_pep = 1 THEN 'PEP'
            WHEN is_compliance_flagged = 1 THEN 'Compliance flagged'
            WHEN is_kyc_complete = 0 THEN 'KYC incomplete'
            ELSE 'Clean profile'
        END AS flag_type,
        COUNT(*) AS customer_count
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1
    GROUP BY
        CASE
            WHEN is_pep = 1 THEN 'PEP'
            WHEN is_compliance_flagged = 1 THEN 'Compliance flagged'
            WHEN is_kyc_complete = 0 THEN 'KYC incomplete'
            ELSE 'Clean profile'
        END
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_tenure_bins() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        CASE
            WHEN customer_tenure_days < 365 THEN '< 1 year'
            WHEN customer_tenure_days < 1825 THEN '1-5 years'
            WHEN customer_tenure_days < 3650 THEN '5-10 years'
            ELSE '10+ years'
        END AS tenure_band,
        COUNT(*) AS customer_count,
        AVG(CAST(monthly_salary AS FLOAT)) AS avg_salary
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1
    GROUP BY
        CASE
            WHEN customer_tenure_days < 365 THEN '< 1 year'
            WHEN customer_tenure_days < 1825 THEN '1-5 years'
            WHEN customer_tenure_days < 3650 THEN '5-10 years'
            ELSE '10+ years'
        END
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_marital() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT marital_status, COUNT(*) AS customer_count
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1 AND marital_status NOT IN ('UNKNOWN', '')
    GROUP BY marital_status
    ORDER BY customer_count DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_residence() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT residence_status, COUNT(*) AS customer_count
    FROM {SCHEMA}.dim_customer
    WHERE customer_id <> -1 AND residence_status <> 'UNKNOWN'
    GROUP BY residence_status
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_dao_accounts_by_area(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        d.dao_area,
        COUNT(*) AS account_count,
        SUM(CAST(f.working_balance AS FLOAT)) AS total_balance,
        SUM(CASE WHEN f.is_negative_balance = 1 THEN 1 ELSE 0 END) AS overdraft_count
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.account_officer_id <> -1
    GROUP BY d.dao_area
    ORDER BY total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_dao_risk_by_area(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        d.dao_area,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.account_officer_id <> -1
    GROUP BY d.dao_area
    ORDER BY avg_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_date_account_openings() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT dt.year_number AS year, COUNT(*) AS account_count,
           SUM(CAST(f.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_date dt ON f.opening_date_sk = dt.date_sk
    WHERE dt.year_number BETWEEN 2000 AND 2030
    GROUP BY dt.year_number
    ORDER BY dt.year_number
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_date_quarter_counts() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT quarter_number AS quarter, COUNT(*) AS date_count
    FROM {SCHEMA}.dim_date
    GROUP BY quarter_number
    ORDER BY quarter_number
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_industry_accounts(top_n: int = 15) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        i.industry_description,
        COUNT(a.fact_account_sk) AS account_count,
        SUM(CAST(a.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account a
    JOIN {SCHEMA}.dim_industry i ON a.industry_sk = i.industry_sk
    WHERE i.industry_code <> -1
    GROUP BY i.industry_description
    ORDER BY total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_target_accounts() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT t.target_description,
           COUNT(a.fact_account_sk) AS account_count,
           SUM(CAST(a.working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_account a
    JOIN {SCHEMA}.dim_target t ON a.target_sk = t.target_sk
    WHERE t.target_code <> -1
    GROUP BY t.target_description
    ORDER BY total_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_currency_risk_exposure() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT c.currency_code, c.currency_name,
           COUNT(DISTINCT a.fact_account_sk) AS accounts,
           SUM(CAST(a.working_balance AS FLOAT)) AS total_balance,
           AVG(CAST(r.global_risk_score AS FLOAT)) AS avg_risk_score
    FROM {SCHEMA}.fact_account a
    JOIN {SCHEMA}.dim_currency c ON a.currency_sk = c.currency_sk
    JOIN {SCHEMA}.dim_customer dc ON a.customer_sk = dc.customer_sk
    JOIN {SCHEMA}.fact_customer_risk r ON r.customer_sk = dc.customer_sk
    WHERE c.currency_code <> 'UNK'
    GROUP BY c.currency_code, c.currency_name
    ORDER BY accounts DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_sector_overdraft_rate(top_n: int = 12) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        s.sector_description,
        SUM(CASE WHEN f.is_negative_balance = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0) AS overdraft_pct,
        COUNT(*) AS account_count
    FROM {SCHEMA}.fact_account f
    JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    WHERE s.sector_code <> -1
    GROUP BY s.sector_description
    ORDER BY overdraft_pct DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_high_risk_watchlist(top_n: int = 100) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        f.customer_id, f.risk_tier, f.global_risk_score,
        f.compliance_risk_index, f.financial_fragility_score, f.behavioral_risk_score,
        f.total_working_balance, c.is_kyc_complete, c.is_pep, c.is_compliance_flagged,
        s.sector_description, d.dao_area
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE f.risk_tier IN ('HIGH', 'VERY_HIGH')
    ORDER BY f.global_risk_score DESC
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_etl_freshness() -> dict:
    row = sql_query(f"""
    SELECT
        (SELECT MAX(CAST(load_date AS DATE)) FROM {SCHEMA}.fact_account) AS last_account_load,
        (SELECT MAX(CAST(load_date AS DATE)) FROM {SCHEMA}.fact_customer_risk) AS last_risk_load,
        (SELECT MAX(CAST(scoring_date AS DATE)) FROM {SCHEMA}.fact_customer_risk) AS last_scoring_date,
        (SELECT MAX(dt.full_date) FROM {SCHEMA}.fact_account f
         JOIN {SCHEMA}.dim_date dt ON f.load_date_sk = dt.date_sk) AS last_account_load_dim
    """).iloc[0]
    return {k: str(v)[:10] if v is not None else "—" for k, v in row.items()}


@st.cache_data(ttl=180, show_spinner=False)
def load_drilldown_customers(
    risk_tier: str | None = None,
    sector: str | None = None,
    top_n: int = 200,
) -> "pd.DataFrame":
    tier_clause = f"AND f.risk_tier = '{risk_tier}'" if risk_tier and risk_tier != "ALL" else ""
    sector_clause = ""
    if sector and sector != "ALL":
        safe = sector.replace("'", "''")
        sector_clause = f"AND s.sector_description = N'{safe}'"
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        f.customer_id, f.risk_tier, f.global_risk_score,
        f.total_working_balance, f.compliance_risk_index,
        c.is_kyc_complete, c.is_pep, c.is_compliance_flagged,
        c.monthly_salary, s.sector_description, d.dao_area
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE c.customer_id <> -1 {tier_clause} {sector_clause}
    ORDER BY f.global_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_pep_customers(top_n: int = 500) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        c.customer_id, c.customer_sk,
        f.risk_tier, f.global_risk_score,
        c.is_kyc_complete, c.last_kyc_review_date, c.next_kyc_review_date,
        c.monthly_salary, s.sector_description, d.dao_area
    FROM {SCHEMA}.dim_customer c
    LEFT JOIN {SCHEMA}.fact_customer_risk f ON c.customer_sk = f.customer_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE c.customer_id <> -1 AND c.is_pep = 1
    ORDER BY f.global_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_kyc_overdue_customers(top_n: int = 500) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        c.customer_id, c.is_kyc_complete,
        c.last_kyc_review_date, c.next_kyc_review_date,
        DATEDIFF(day, c.last_kyc_review_date, CAST(GETDATE() AS DATE)) AS days_since_kyc,
        f.risk_tier, f.global_risk_score,
        c.is_pep, c.is_compliance_flagged,
        s.sector_description, d.dao_area
    FROM {SCHEMA}.dim_customer c
    LEFT JOIN {SCHEMA}.fact_customer_risk f ON c.customer_sk = f.customer_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE c.customer_id <> -1
      AND (
          c.is_kyc_complete = 0
          OR c.next_kyc_review_date < CAST(GETDATE() AS DATE)
          OR DATEDIFF(day, c.last_kyc_review_date, CAST(GETDATE() AS DATE)) > 365
      )
    ORDER BY days_since_kyc DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_compliance_watchlist(top_n: int = 200) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        f.customer_id, f.risk_tier, f.global_risk_score,
        f.compliance_risk_index, f.financial_fragility_score,
        c.is_kyc_complete, c.is_pep, c.is_compliance_flagged,
        c.has_compliance_decision, f.total_working_balance,
        s.sector_description, d.dao_area
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_customer c ON f.customer_sk = c.customer_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE c.customer_id <> -1
      AND (
          f.risk_tier IN ('HIGH', 'VERY_HIGH')
          OR c.is_pep = 1
          OR c.is_compliance_flagged = 1
          OR c.is_kyc_complete = 0
      )
    ORDER BY f.global_risk_score DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_sector_comparison(sector_a: str, sector_b: str) -> "pd.DataFrame":
    sa, sb = sector_a.replace("'", "''"), sector_b.replace("'", "''")
    return sql_query(f"""
    SELECT s.sector_description,
           COUNT(DISTINCT r.customer_sk) AS customers,
           COUNT(a.fact_account_sk) AS accounts,
           SUM(CAST(a.working_balance AS FLOAT)) AS total_balance,
           AVG(CAST(r.global_risk_score AS FLOAT)) AS avg_risk,
           SUM(CASE WHEN r.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) * 100.0
               / NULLIF(COUNT(DISTINCT r.customer_sk), 0) AS high_risk_pct,
           SUM(CASE WHEN a.is_negative_balance = 1 THEN 1 ELSE 0 END) * 100.0
               / NULLIF(COUNT(a.fact_account_sk), 0) AS overdraft_pct
    FROM {SCHEMA}.dim_sector s
    LEFT JOIN {SCHEMA}.fact_customer_risk r ON r.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.fact_account a ON a.sector_sk = s.sector_sk
    WHERE s.sector_description IN (N'{sa}', N'{sb}')
    GROUP BY s.sector_description
    ORDER BY s.sector_description
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_dao_comparison(area_a: str, area_b: str) -> "pd.DataFrame":
    aa, ab = area_a.replace("'", "''"), area_b.replace("'", "''")
    return sql_query(f"""
    SELECT d.dao_area,
           COUNT(DISTINCT r.customer_sk) AS customers,
           COUNT(a.fact_account_sk) AS accounts,
           SUM(CAST(a.working_balance AS FLOAT)) AS total_balance,
           AVG(CAST(r.global_risk_score AS FLOAT)) AS avg_risk,
           SUM(CASE WHEN r.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) * 100.0
               / NULLIF(COUNT(DISTINCT r.customer_sk), 0) AS high_risk_pct
    FROM {SCHEMA}.dim_dao d
    LEFT JOIN {SCHEMA}.fact_customer_risk r ON r.dao_sk = d.dao_sk
    LEFT JOIN {SCHEMA}.fact_account a ON a.dao_sk = d.dao_sk
    WHERE d.dao_area IN (N'{aa}', N'{ab}') AND d.account_officer_id <> -1
    GROUP BY d.dao_area
    ORDER BY d.dao_area
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_sector_names() -> list[str]:
    df = sql_query(f"""
    SELECT sector_description FROM {SCHEMA}.dim_sector
    WHERE sector_code <> -1 ORDER BY sector_description
    """)
    return df["sector_description"].tolist()


@st.cache_data(ttl=300, show_spinner=False)
def load_dao_area_names() -> list[str]:
    df = sql_query(f"""
    SELECT DISTINCT dao_area FROM {SCHEMA}.dim_dao
    WHERE account_officer_id <> -1 ORDER BY dao_area
    """)
    return df["dao_area"].tolist()


# ─── PFE_ML predictions & alert inbox ────────────────────────────────────────

@st.cache_data(ttl=120, show_spinner=False)
def ml_predictions_available() -> bool:
    try:
        df = sql_query(
            f"SELECT TOP 1 prediction_id FROM {ML_SCHEMA}.ml_customer_predictions"
        )
        return not df.empty
    except Exception:
        return False


@st.cache_data(ttl=120, show_spinner=False)
def load_ml_predictions_summary() -> dict:
    try:
        row = sql_query(f"""
        SELECT
            COUNT(*) AS total_predictions,
            SUM(CASE WHEN risk_prediction = 1 THEN 1 ELSE 0 END) AS ml_high_risk,
            SUM(CASE WHEN disagreement_flag = 1 THEN 1 ELSE 0 END) AS disagreements,
            MAX(prediction_timestamp) AS last_scored,
            MAX(model_name) AS model_name,
            MAX(model_version) AS model_version
        FROM {ML_SCHEMA}.ml_customer_predictions
        """).iloc[0]
        return {k: row[k] for k in row.index}
    except Exception:
        return {}


@st.cache_data(ttl=120, show_spinner=False)
def load_ml_disagreement_alerts(top_n: int = 200) -> "pd.DataFrame":
    return sql_query(f"""
    SELECT TOP {int(top_n)}
        p.customer_id,
        p.risk_probability,
        p.risk_prediction,
        p.rules_risk_tier,
        p.rules_global_score,
        p.is_pep,
        p.total_working_balance,
        p.dao_area,
        p.model_name,
        p.model_version,
        p.prediction_timestamp
    FROM {ML_SCHEMA}.ml_customer_predictions p
    WHERE p.disagreement_flag = 1
    {_dao_ml_filter()}
    ORDER BY p.risk_probability DESC
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_triple_flag_alerts(top_n: int = 100) -> "pd.DataFrame":
    """PEP + high balance + ML high-risk — highest regulatory priority."""
    return sql_query(f"""
    WITH balance_cut AS (
        SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY CAST(total_working_balance AS FLOAT)
        ) OVER () AS p75_balance
        FROM {SCHEMA}.fact_customer_risk
        WHERE total_working_balance > 0
    )
    SELECT TOP {int(top_n)}
        c.customer_id,
        f.risk_tier AS rules_risk_tier,
        CAST(f.global_risk_score AS FLOAT) AS rules_global_score,
        CAST(f.total_working_balance AS FLOAT) AS total_working_balance,
        p.risk_probability,
        p.risk_prediction,
        p.disagreement_flag,
        c.is_kyc_complete,
        s.sector_description,
        d.dao_area,
        d.dao_name,
        p.model_name,
        p.model_version,
        p.prediction_timestamp
    FROM {SCHEMA}.dim_customer c
    JOIN {SCHEMA}.fact_customer_risk f ON c.customer_sk = f.customer_sk
    JOIN {ML_SCHEMA}.ml_customer_predictions p ON p.customer_id = c.customer_id
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    CROSS JOIN (SELECT TOP 1 p75_balance FROM balance_cut) bc
    WHERE c.customer_id <> -1
      AND c.is_pep = 1
      AND CAST(f.total_working_balance AS FLOAT) >= bc.p75_balance
      AND p.risk_prediction = 1
      {_dao_ml_filter()}
    ORDER BY p.risk_probability DESC, f.total_working_balance DESC
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_triple_flag_demo_relaxed(top_n: int = 12) -> "pd.DataFrame":
    """Fallback: PEP + balance >= P50 + ML prob >= 0.70 (demo when strict triple-flag is empty)."""
    return sql_query(f"""
    WITH balance_cut AS (
        SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY CAST(total_working_balance AS FLOAT)
        ) OVER () AS p50_balance
        FROM {SCHEMA}.fact_customer_risk WHERE total_working_balance > 0
    )
    SELECT TOP {int(top_n)}
        c.customer_id, f.risk_tier AS rules_risk_tier,
        CAST(f.global_risk_score AS FLOAT) AS rules_global_score,
        CAST(f.total_working_balance AS FLOAT) AS total_working_balance,
        p.risk_probability, p.risk_prediction, p.disagreement_flag,
        c.is_kyc_complete, s.sector_description, d.dao_area, d.dao_name,
        p.model_name, p.model_version, p.prediction_timestamp
    FROM {SCHEMA}.dim_customer c
    JOIN {SCHEMA}.fact_customer_risk f ON c.customer_sk = f.customer_sk
    JOIN {ML_SCHEMA}.ml_customer_predictions p ON p.customer_id = c.customer_id
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    CROSS JOIN (SELECT TOP 1 p50_balance FROM balance_cut) bc
    WHERE c.customer_id <> -1 AND c.is_pep = 1
      AND CAST(f.total_working_balance AS FLOAT) >= bc.p50_balance
      AND p.risk_probability >= 0.70
      {_dao_ml_filter()}
    ORDER BY p.risk_probability DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_pep_high_balance_alerts(top_n: int = 200) -> "pd.DataFrame":
    return sql_query(f"""
    WITH balance_cut AS (
        SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY CAST(total_working_balance AS FLOAT)
        ) OVER () AS p75_balance
        FROM {SCHEMA}.fact_customer_risk
        WHERE total_working_balance > 0
    )
    SELECT TOP {int(top_n)}
        c.customer_id,
        c.is_pep,
        f.risk_tier,
        f.global_risk_score,
        CAST(f.total_working_balance AS FLOAT) AS total_working_balance,
        c.is_kyc_complete,
        s.sector_description,
        d.dao_area,
        d.dao_name
    FROM {SCHEMA}.dim_customer c
    JOIN {SCHEMA}.fact_customer_risk f ON c.customer_sk = f.customer_sk
    LEFT JOIN {SCHEMA}.dim_sector s ON f.sector_sk = s.sector_sk
    LEFT JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    CROSS JOIN (SELECT TOP 1 p75_balance FROM balance_cut) bc
    WHERE c.customer_id <> -1
      AND c.is_pep = 1
      AND CAST(f.total_working_balance AS FLOAT) >= bc.p75_balance
      {_dao_dim_filter("d")}
    ORDER BY f.total_working_balance DESC
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_branch_risk_heatmap(top_areas: int = 12) -> "pd.DataFrame":
    return sql_query(f"""
    WITH top_areas AS (
        SELECT TOP {int(top_areas)} d.dao_area
        FROM {SCHEMA}.fact_customer_risk f
        JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
        WHERE d.account_officer_id <> -1
        GROUP BY d.dao_area
        ORDER BY COUNT(*) DESC
    )
    SELECT
        d.dao_area,
        f.risk_tier,
        COUNT(*) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.dao_area IN (SELECT dao_area FROM top_areas)
    GROUP BY d.dao_area, f.risk_tier
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_dao_risk_treemap() -> "pd.DataFrame":
    return sql_query(f"""
    SELECT
        d.dao_area,
        d.dao_name,
        COUNT(DISTINCT f.customer_sk) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count,
        SUM(CAST(f.total_working_balance AS FLOAT)) AS total_balance
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.account_officer_id <> -1
    GROUP BY d.dao_area, d.dao_name
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_dao_geo_risk() -> "pd.DataFrame":
    """Branch-area aggregates for map overlay (dao_area as region proxy)."""
    return sql_query(f"""
    SELECT
        d.dao_area,
        COUNT(DISTINCT f.customer_sk) AS customer_count,
        AVG(CAST(f.global_risk_score AS FLOAT)) AS avg_risk_score,
        SUM(CASE WHEN f.risk_tier IN ('HIGH','VERY_HIGH') THEN 1 ELSE 0 END) AS high_risk_count
    FROM {SCHEMA}.fact_customer_risk f
    JOIN {SCHEMA}.dim_dao d ON f.dao_sk = d.dao_sk
    WHERE d.account_officer_id <> -1
    GROUP BY d.dao_area
    """)


@st.cache_data(ttl=180, show_spinner=False)
def load_table_sample(table: str, top_n: int = 200) -> "pd.DataFrame":
    allowed = {
        "dim_currency", "dim_customer", "dim_date", "dim_dao",
        "dim_industry", "dim_sector", "dim_target",
        "fact_account", "fact_customer_risk",
    }
    if table not in allowed:
        raise ValueError(f"Table not allowed: {table}")
    return sql_query(f"SELECT TOP {int(top_n)} * FROM {SCHEMA}.{table}")
