from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from config import OUTPUT_DIR, PROJECT_ROOT, WarehouseConnection, build_sqlalchemy_uri


FEATURE_QUERY = """
SELECT
    fcr.fact_customer_risk_sk,
    fcr.customer_id,
    fcr.scoring_date,
    fcr.load_date,
    fcr.customer_tenure_days,
    fcr.account_count,
    fcr.total_working_balance,
    fcr.avg_working_balance,
    fcr.negative_balance_account_count,
    fcr.oldest_account_opening_date,
    fcr.newest_account_opening_date,
    fcr.account_officer_id,
    fcr.sector_code,
    fcr.industry_code,
    fcr.target_code,
    fcr.customer_since_date,
    fcr.compliance_risk_index,
    fcr.financial_fragility_score,
    fcr.behavioral_risk_score,
    fcr.global_risk_score,
    fcr.risk_tier,
    dc.customer_sk,
    dc.dao_name,
    dc.dao_area,
    dc.gender,
    dc.marital_status,
    dc.number_of_dependents,
    dc.employment_status,
    dc.job_title,
    dc.monthly_salary,
    dc.nationality_code,
    dc.residence_country_code,
    dc.residence_status,
    dc.last_kyc_review_date,
    dc.next_kyc_review_date,
    dc.is_kyc_complete,
    dc.is_pep,
    dc.is_compliance_flagged,
    dc.has_compliance_decision,
    dc.posting_restriction_code,
    dc.segment_code,
    dc.client_nature_code,
    dc.legal_capacity_flag,
    dc.beneficial_owner_flag
FROM {schema}.fact_customer_risk fcr
JOIN {schema}.dim_customer dc
    ON fcr.customer_sk = dc.customer_sk
"""

RAW_DATA_CANDIDATES = [
    PROJECT_ROOT / "data" / "raw",
    PROJECT_ROOT / "1_ingestion" / "Data" / "raw",
]


def make_engine(uri: str | None = None):
    return create_engine(uri or build_sqlalchemy_uri(), fast_executemany=True, pool_pre_ping=True)


def build_feature_query(schema: str | None = None) -> str:
    warehouse_schema = schema or WarehouseConnection().schema
    return FEATURE_QUERY.format(schema=warehouse_schema)


def load_dataframe(sql: str, uri: str | None = None) -> pd.DataFrame:
    engine = make_engine(uri)
    return pd.read_sql(sql, engine)


def _read_cached_frame(cache_path: Path) -> pd.DataFrame:
    date_columns = [
        'scoring_date',
        'load_date',
        'customer_since_date',
        'oldest_account_opening_date',
        'newest_account_opening_date',
        'last_kyc_review_date',
        'next_kyc_review_date',
    ]
    preview = pd.read_csv(cache_path, nrows=0)
    parse_dates = [column for column in date_columns if column in preview.columns]
    return pd.read_csv(cache_path, parse_dates=parse_dates)


def _parse_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str).replace({'nan': None, 'None': None, '': None}), errors='coerce', format='%Y%m%d')


def _raw_data_path(filename: str) -> Path:
    for candidate in RAW_DATA_CANDIDATES:
        path = candidate / filename
        if path.exists():
            return path
    raise FileNotFoundError(filename)


def _load_raw_inputs() -> dict[str, pd.DataFrame]:
    frames = {}
    for filename in ['customer.csv', 'account.csv', 'dao.csv', 'sector.csv', 'industry.csv', 'target.csv', 'currency.csv']:
        frames[filename] = pd.read_csv(_raw_data_path(filename), sep='|')
    return frames


def _normalize_text(series: pd.Series, default: str = 'UNKNOWN') -> pd.Series:
    return series.astype(str).replace({'nan': default, 'None': default, '': default}).fillna(default)


def _build_local_feature_frame() -> pd.DataFrame:
    raw = _load_raw_inputs()
    customers = raw['customer.csv'].copy()
    accounts = raw['account.csv'].copy()
    dao = raw['dao.csv'].copy()

    customers['CUSTOMER_CODE'] = pd.to_numeric(customers['CUSTOMER_CODE'], errors='coerce')
    customers['ACCOUNT_OFFICER'] = pd.to_numeric(customers['ACCOUNT_OFFICER'], errors='coerce')
    customers['SECTOR'] = pd.to_numeric(customers['SECTOR'], errors='coerce')
    customers['INDUSTRY'] = pd.to_numeric(customers['INDUSTRY'], errors='coerce')
    customers['TARGET'] = pd.to_numeric(customers['TARGET'], errors='coerce')
    customers['NO_OF_DEPENDENTS'] = pd.to_numeric(customers['NO_OF_DEPENDENTS'], errors='coerce')
    customers['SALARY'] = pd.to_numeric(customers['SALARY'], errors='coerce')
    customers['SEGMENT'] = pd.to_numeric(customers['SEGMENT'], errors='coerce')
    customers['KYC_COMPLETE'] = _normalize_text(customers['KYC_COMPLETE']).str.upper().isin(['YES', 'Y', '1', 'TRUE'])
    customers['L_PEP'] = _normalize_text(customers['L_PEP']).str.upper().isin(['YES', 'Y', '1', 'TRUE'])
    customers['L_FLAG_CONF'] = _normalize_text(customers['L_FLAG_CONF']).str.upper().isin(['YES', 'Y', '1', 'TRUE'])
    customers['L_DECS_CONF'] = _normalize_text(customers['L_DECS_CONF']).str.upper().isin(['YES', 'Y', '1', 'TRUE'])

    customers['CUSTOMER_SINCE'] = _parse_date_series(customers['CUSTOMER_SINCE'])
    customers['LAST_KYC_REVIEW_DATE'] = _parse_date_series(customers['LAST_KYC_REVIEW_DATE'])
    customers['AUTO_NEXT_KYC_REVIEW_DATE'] = _parse_date_series(customers['AUTO_NEXT_KYC_REVIEW_DATE'])
    customers['L_PUBLI_DATE'] = pd.to_datetime(customers['L_PUBLI_DATE'].astype(str), errors='coerce', format='%Y%m%d')

    dao['ACCOUNT_OFFICER'] = pd.to_numeric(dao['ACCOUNT_OFFICER'], errors='coerce')

    accounts['CUSTOMER_NO'] = pd.to_numeric(accounts['CUSTOMER_NO'], errors='coerce')
    accounts['ACCOUNT_OFFICER'] = pd.to_numeric(accounts['ACCOUNT_OFFICER'], errors='coerce')
    accounts['WORKING_BALANCE'] = pd.to_numeric(accounts['WORKING_BALANCE'], errors='coerce')
    accounts['OPENING_DATE'] = pd.to_datetime(accounts['OPENING_DATE'].astype(str), errors='coerce', format='%Y%m%d')

    account_activity = (
        accounts.groupby('CUSTOMER_NO', dropna=False)
        .agg(
            account_count=('RECID', 'count'),
            total_working_balance=('WORKING_BALANCE', 'sum'),
            avg_working_balance=('WORKING_BALANCE', 'mean'),
            negative_balance_account_count=('WORKING_BALANCE', lambda s: int((s < 0).sum())),
            oldest_account_opening_date=('OPENING_DATE', 'min'),
            newest_account_opening_date=('OPENING_DATE', 'max'),
            account_officer_id=('ACCOUNT_OFFICER', 'first'),
        )
        .reset_index()
        .rename(columns={'CUSTOMER_NO': 'customer_id'})
    )

    base = customers.rename(columns={
        'CUSTOMER_CODE': 'customer_id',
        'ACCOUNT_OFFICER': 'account_officer_id',
        'SECTOR': 'sector_code',
        'INDUSTRY': 'industry_code',
        'TARGET': 'target_code',
        'NATIONALITY': 'nationality_code',
        'RESIDENCE': 'residence_country_code',
        'POSTING_RESTRICT_46': 'posting_restriction_code',
        'CUSTOMER_SINCE': 'customer_since_date',
        'TITLE': 'title',
        'GENDER': 'gender',
        'MARITAL_STATUS': 'marital_status',
        'NO_OF_DEPENDENTS': 'number_of_dependents',
        'EMPLOYMENT_STATUS': 'employment_status',
        'JOB_TITLE': 'job_title',
        'SALARY': 'monthly_salary',
        'RESIDENCE_STATUS': 'residence_status',
        'LAST_KYC_REVIEW_DATE': 'last_kyc_review_date',
        'AUTO_NEXT_KYC_REVIEW_DATE': 'next_kyc_review_date',
        'KYC_COMPLETE': 'is_kyc_complete',
        'SEGMENT': 'segment_code',
        'L_NATURE_CLIENT': 'client_nature_code',
        'L_CAPACITE_JUR': 'legal_capacity_flag',
        'L_BENF_REEL_CPT': 'beneficial_owner_flag',
        'L_PEP': 'is_pep',
        'L_SCORE_KYC': 'kyc_score',
        'L_FLAG_CONF': 'is_compliance_flagged',
        'L_DECS_CONF': 'has_compliance_decision',
    }).copy()

    dao_lookup = dao.rename(columns={
        'ACCOUNT_OFFICER': 'account_officer_id',
        'AREA': 'dao_area',
        'NAME': 'dao_name',
        'DEPT_PARENT': 'parent_department_code',
    })[['account_officer_id', 'dao_name', 'dao_area', 'parent_department_code']]

    frame = base.merge(account_activity, how='left', on='customer_id', suffixes=('', '_account'))
    if 'account_officer_id_account' in frame.columns:
        frame['account_officer_id'] = frame['account_officer_id'].fillna(frame['account_officer_id_account'])
        frame = frame.drop(columns=['account_officer_id_account'])
    frame = frame.merge(dao_lookup, how='left', on='account_officer_id')
    frame['customer_sk'] = range(1, len(frame) + 1)
    frame['fact_customer_risk_sk'] = frame['customer_sk']
    frame['scoring_date'] = pd.Timestamp.today().normalize()
    frame['load_date'] = pd.Timestamp.today().normalize()
    frame['customer_tenure_days'] = (frame['scoring_date'] - frame['customer_since_date']).dt.days.fillna(0).astype(int)
    frame['nationality_code'] = _normalize_text(frame['nationality_code'])
    frame['residence_country_code'] = _normalize_text(frame['residence_country_code'])
    frame['residence_status'] = _normalize_text(frame['residence_status'])
    frame['employment_status'] = _normalize_text(frame['employment_status'])
    frame['job_title'] = _normalize_text(frame['job_title'])
    frame['gender'] = _normalize_text(frame['gender'])
    frame['marital_status'] = _normalize_text(frame['marital_status'])
    frame['dao_name'] = _normalize_text(frame['dao_name'])
    frame['dao_area'] = _normalize_text(frame['dao_area'])
    frame['posting_restriction_code'] = _normalize_text(frame['posting_restriction_code'])
    frame['client_nature_code'] = _normalize_text(frame['client_nature_code'])
    frame['legal_capacity_flag'] = _normalize_text(frame['legal_capacity_flag'])
    frame['beneficial_owner_flag'] = _normalize_text(frame['beneficial_owner_flag'])
    frame['is_kyc_complete'] = frame['is_kyc_complete'].astype(int)
    frame['is_pep'] = frame['is_pep'].astype(int)
    frame['is_compliance_flagged'] = frame['is_compliance_flagged'].astype(int)
    frame['has_compliance_decision'] = frame['has_compliance_decision'].astype(int)

    frame['compliance_risk_index'] = frame.apply(_compliance_risk_index, axis=1)
    frame['financial_fragility_score'] = frame.apply(_financial_fragility_score, axis=1)
    frame['behavioral_risk_score'] = frame.apply(_behavioral_risk_score, axis=1)
    frame['global_risk_score'] = (frame['compliance_risk_index'] * 0.40 + frame['financial_fragility_score'] * 0.35 + frame['behavioral_risk_score'] * 0.25).round(2)
    frame['risk_tier'] = pd.cut(frame['global_risk_score'], bins=[-np.inf, 24, 49, 74, np.inf], labels=['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH']).astype(str)

    return frame[[
        'fact_customer_risk_sk', 'customer_sk', 'customer_id', 'scoring_date', 'load_date', 'customer_tenure_days',
        'account_count', 'total_working_balance', 'avg_working_balance', 'negative_balance_account_count',
        'oldest_account_opening_date', 'newest_account_opening_date', 'account_officer_id', 'sector_code',
        'industry_code', 'target_code', 'customer_since_date', 'compliance_risk_index', 'financial_fragility_score',
        'behavioral_risk_score', 'global_risk_score', 'risk_tier', 'dao_name', 'dao_area', 'gender', 'marital_status',
        'number_of_dependents', 'employment_status', 'job_title', 'monthly_salary', 'nationality_code',
        'residence_country_code', 'residence_status', 'last_kyc_review_date', 'next_kyc_review_date',
        'is_kyc_complete', 'is_pep', 'is_compliance_flagged', 'has_compliance_decision', 'posting_restriction_code',
        'segment_code', 'client_nature_code', 'legal_capacity_flag', 'beneficial_owner_flag'
    ]]


def _safe_int(value: object, default: int = 0) -> int:
    if pd.isna(value):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    if pd.isna(value):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _compliance_risk_index(row: pd.Series) -> int:
    score = 0
    if _safe_int(row.get('is_kyc_complete', 0)) == 0:
        score += 35
    if _safe_int(row.get('is_pep', 0)) == 1:
        score += 30
    if _safe_int(row.get('is_compliance_flagged', 0)) == 1:
        score += 25
    if _safe_int(row.get('has_compliance_decision', 0)) == 1:
        score += 10
    if pd.notna(row.get('posting_restriction_code')) and str(row.get('posting_restriction_code')).upper() not in {'', 'UNKNOWN', 'NAN'}:
        score += 15
    last_review = row.get('last_kyc_review_date')
    today = pd.Timestamp.today().normalize()
    if pd.isna(last_review):
        score += 15
    elif (today - last_review).days > 365:
        score += 10
    return min(score, 100)


def _financial_fragility_score(row: pd.Series) -> int:
    score = 0
    salary = row.get('monthly_salary')
    if pd.isna(salary) or _safe_float(salary) <= 0:
        score += 25
    if _safe_int(row.get('account_count', 0)) == 0:
        score += 20
    if _safe_float(row.get('total_working_balance', 0)) < 0:
        score += 30
    if _safe_float(row.get('total_working_balance', 0)) >= 0 and 0 <= _safe_float(row.get('avg_working_balance', 0)) <= 500:
        score += 10
    if _safe_int(row.get('number_of_dependents', 0)) >= 4:
        score += 10
    employment = str(row.get('employment_status', '')).upper()
    if employment in {'UNEMPLOYED', 'SANS EMPLOI', 'CHOMEUR'}:
        score += 20
    return min(score, 100)


def _behavioral_risk_score(row: pd.Series) -> int:
    score = 0
    if _safe_int(row.get('customer_tenure_days', 0)) < 365:
        score += 25
    if _safe_int(row.get('negative_balance_account_count', 0)) > 0:
        score += 25
    if _safe_int(row.get('account_count', 0)) == 1:
        score += 10
    if _safe_int(row.get('account_count', 0)) > 0 and _safe_float(row.get('total_working_balance', 0)) < 100:
        score += 15
    last_review = row.get('last_kyc_review_date')
    today = pd.Timestamp.today().normalize()
    if pd.isna(last_review):
        score += 20
    elif (today - last_review).days > 730:
        score += 20
    return min(score, 100)


def _read_from_cache_or_local(cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        return _read_cached_frame(cache_path)
    return _build_local_feature_frame()


def load_feature_frame(uri: str | None = None, cache_name: str = "customer_risk_features.csv") -> pd.DataFrame:
    cache_path = OUTPUT_DIR / cache_name
    sql = build_feature_query()
    try:
        frame = load_dataframe(sql, uri=uri)
        frame.to_csv(cache_path, index=False)
        return frame
    except Exception:
        if cache_path.exists():
            return _read_cached_frame(cache_path)
        try:
            frame = _build_local_feature_frame()
            frame.to_csv(cache_path, index=False)
            return frame
        except Exception:
            return _read_from_cache_or_local(cache_path)
