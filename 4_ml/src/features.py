from __future__ import annotations

from typing import Iterable, Tuple

import numpy as np
import pandas as pd

from config import OUTPUT_DIR, RISK_TARGET_THRESHOLD

LEAKAGE_COLUMNS = {
    "fact_customer_risk_sk",
    "customer_sk",
    "global_risk_score",
    "compliance_risk_index",
    "financial_fragility_score",
    "behavioral_risk_score",
    "risk_tier",
    "load_date",
    "scoring_date",
}

DERIVED_NUMERIC_COLUMNS = [
    "days_since_last_kyc_review",
    "days_until_next_kyc_review",
    "balance_per_account",
    "negative_balance_rate",
    "salary_to_balance_ratio",
    "has_overdraft",
    "is_new_customer",
    "is_kyc_stale",
    "is_kyc_very_stale",
]

CATEGORICAL_COLUMNS = [
    "employment_status",
    "gender",
    "marital_status",
    "residence_status",
    "sector_code",
    "industry_code",
    "target_code",
    "segment_code",
    "dao_area",
    "client_nature_flag",
    "legal_capacity_flag",
    "beneficial_owner_flag",
]


def _coerce_dates(frame: pd.DataFrame) -> pd.DataFrame:
    for column in [
        "scoring_date",
        "load_date",
        "customer_since_date",
        "oldest_account_opening_date",
        "newest_account_opening_date",
        "last_kyc_review_date",
        "next_kyc_review_date",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def build_targets(frame: pd.DataFrame, risk_threshold: float = RISK_TARGET_THRESHOLD) -> pd.DataFrame:
    frame = frame.copy()
    frame["risk_target"] = (frame["global_risk_score"].fillna(0) >= risk_threshold).astype(int)
    frame["risk_probability_target"] = frame["risk_target"]
    if "churn_target" not in frame.columns:
        frame["churn_target"] = pd.NA
    return frame


def engineer_features(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.loc[:, ~frame.columns.duplicated()].copy()
    frame = _coerce_dates(frame)
    scoring_date = frame["scoring_date"].fillna(frame["load_date"])

    if "last_kyc_review_date" in frame.columns:
        last_review = frame["last_kyc_review_date"].fillna(scoring_date)
        frame["days_since_last_kyc_review"] = (scoring_date - last_review).dt.days.fillna(9999).astype(float)
    else:
        frame["days_since_last_kyc_review"] = 9999.0

    if "next_kyc_review_date" in frame.columns:
        next_review = frame["next_kyc_review_date"].fillna(scoring_date)
        frame["days_until_next_kyc_review"] = (next_review - scoring_date).dt.days.fillna(0).astype(float)
    else:
        frame["days_until_next_kyc_review"] = 0.0

    account_count = frame.get("account_count", pd.Series(0, index=frame.index)).replace(0, np.nan)
    total_balance = frame.get("total_working_balance", pd.Series(0, index=frame.index))
    negative_accounts = frame.get("negative_balance_account_count", pd.Series(0, index=frame.index))
    monthly_salary = frame.get("monthly_salary", pd.Series(0, index=frame.index))

    frame["balance_per_account"] = (total_balance / account_count).replace([np.inf, -np.inf], np.nan).fillna(0)
    frame["negative_balance_rate"] = (negative_accounts / account_count).replace([np.inf, -np.inf], np.nan).fillna(0)
    frame["salary_to_balance_ratio"] = (monthly_salary / total_balance.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0)
    frame["has_overdraft"] = (frame.get("negative_balance_account_count", 0) > 0).astype(int)
    frame["is_new_customer"] = (frame.get("customer_tenure_days", 0) < 365).astype(int)
    frame["is_kyc_stale"] = (frame["days_since_last_kyc_review"] > 365).astype(int)
    frame["is_kyc_very_stale"] = (frame["days_since_last_kyc_review"] > 730).astype(int)

    if "client_nature_flag" not in frame.columns and "client_nature_code" in frame.columns:
        frame["client_nature_flag"] = frame["client_nature_code"]

    return frame


def select_feature_columns(frame: pd.DataFrame) -> Tuple[list[str], list[str], list[str]]:
    available_categorical = [column for column in CATEGORICAL_COLUMNS if column in frame.columns]
    available_numeric = [
        column
        for column in [
            "customer_tenure_days",
            "account_count",
            "total_working_balance",
            "avg_working_balance",
            "negative_balance_account_count",
            "monthly_salary",
            "number_of_dependents",
            "days_since_last_kyc_review",
            "days_until_next_kyc_review",
            "balance_per_account",
            "negative_balance_rate",
            "salary_to_balance_ratio",
            "has_overdraft",
            "is_new_customer",
            "is_kyc_stale",
            "is_kyc_very_stale",
            "is_kyc_complete",
            "is_pep",
            "is_compliance_flagged",
            "has_compliance_decision",
            "sector_code",
            "industry_code",
            "target_code",
            "segment_code",
            "account_officer_id",
        ]
        if column in frame.columns
    ]
    leakage = [column for column in LEAKAGE_COLUMNS if column in frame.columns]
    feature_columns = [column for column in available_numeric + available_categorical if column not in leakage]
    return feature_columns, available_numeric, available_categorical


def build_model_frame(frame: pd.DataFrame) -> pd.DataFrame:
    engineered = engineer_features(build_targets(frame))
    feature_columns, _, _ = select_feature_columns(engineered)
    target_columns = ["risk_target", "risk_probability_target"]
    if engineered["churn_target"].notna().any():
        target_columns.append("churn_target")
    keep_columns = list(dict.fromkeys(
        column
        for column in engineered.columns
        if column in feature_columns
        or column in target_columns
        or column in {"customer_id", "scoring_date", "load_date", "customer_since_date", "oldest_account_opening_date", "newest_account_opening_date", "last_kyc_review_date", "next_kyc_review_date"}
    ))
    return engineered.loc[:, keep_columns].copy()


def split_temporal(frame: pd.DataFrame, date_column: str = "scoring_date", test_fraction: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered = frame.sort_values(date_column).reset_index(drop=True)
    cutoff = int(len(ordered) * (1 - test_fraction))
    cutoff = max(1, min(cutoff, len(ordered) - 1))
    return ordered.iloc[:cutoff].copy(), ordered.iloc[cutoff:].copy()
