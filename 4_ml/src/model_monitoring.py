"""Model monitoring and data drift detection for ATB BI ML pipeline."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


@dataclass
class DriftReport:
    """Data drift detection report."""
    timestamp: str
    n_features_drifted: int
    total_features: int
    drift_features: List[str]
    drift_thresholds: Dict[str, float]
    drift_scores: Dict[str, float]
    alert_level: str  # "green", "yellow", "red"
    recommendations: List[str]


def calculate_kolmogorov_smirnov_drift(
    reference_data: pd.Series,
    current_data: pd.Series,
    threshold: float = 0.05
) -> Tuple[float, bool]:
    """
    Detect drift using Kolmogorov-Smirnov test.
    
    Args:
        reference_data: Reference distribution (training data)
        current_data: Current distribution (production data)
        threshold: Significance level for drift detection
    
    Returns:
        (p_value, is_drifted)
    """
    # Remove NaN values
    ref_clean = reference_data.dropna().values
    curr_clean = current_data.dropna().values
    
    if len(ref_clean) == 0 or len(curr_clean) == 0:
        return 1.0, False
    
    statistic, p_value = stats.ks_2samp(ref_clean, curr_clean)
    is_drifted = p_value < threshold
    
    return p_value, is_drifted


def calculate_wasserstein_drift(
    reference_data: pd.Series,
    current_data: pd.Series,
    threshold: float = 0.1
) -> Tuple[float, bool]:
    """
    Detect drift using Wasserstein distance (earth mover distance).
    
    Args:
        reference_data: Reference distribution
        current_data: Current distribution
        threshold: Distance threshold for drift
    
    Returns:
        (distance, is_drifted)
    """
    try:
        from scipy.stats import wasserstein_distance
        
        ref_clean = reference_data.dropna().values
        curr_clean = current_data.dropna().values
        
        if len(ref_clean) == 0 or len(curr_clean) == 0:
            return 0.0, False
        
        distance = wasserstein_distance(ref_clean, curr_clean)
        is_drifted = distance > threshold
        
        return distance, is_drifted
    except ImportError:
        logger.warning("scipy not available for Wasserstein distance")
        return 0.0, False


def calculate_chi_square_drift(
    reference_data: pd.Series,
    current_data: pd.Series,
    threshold: float = 0.05
) -> Tuple[float, bool]:
    """
    Detect categorical drift using chi-square test.
    
    Args:
        reference_data: Reference distribution
        current_data: Current distribution
        threshold: Significance level
    
    Returns:
        (p_value, is_drifted)
    """
    # Create contingency table
    categories = pd.concat([reference_data, current_data]).unique()
    
    ref_counts = reference_data.value_counts().reindex(categories, fill_value=0)
    curr_counts = current_data.value_counts().reindex(categories, fill_value=0)
    
    # Chi-square test
    try:
        chi2, p_value, dof, expected = stats.chi2_contingency(
            np.array([ref_counts, curr_counts])
        )
        is_drifted = p_value < threshold
        return p_value, is_drifted
    except:
        return 1.0, False


def detect_feature_drift(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    numeric_features: List[str],
    categorical_features: List[str],
    significance_level: float = 0.05
) -> Dict[str, Tuple[float, bool]]:
    """
    Detect drift in all features.
    
    Returns:
        Dict mapping feature name to (test_statistic, is_drifted)
    """
    drift_results = {}
    
    # Numeric features: KS test
    for feature in numeric_features:
        if feature in reference_df.columns and feature in current_df.columns:
            p_value, is_drifted = calculate_kolmogorov_smirnov_drift(
                reference_df[feature],
                current_df[feature],
                threshold=significance_level
            )
            drift_results[feature] = (p_value, is_drifted)
    
    # Categorical features: chi-square test
    for feature in categorical_features:
        if feature in reference_df.columns and feature in current_df.columns:
            p_value, is_drifted = calculate_chi_square_drift(
                reference_df[feature].astype(str),
                current_df[feature].astype(str),
                threshold=significance_level
            )
            drift_results[feature] = (p_value, is_drifted)
    
    return drift_results


def detect_target_drift(
    reference_targets: pd.Series,
    current_targets: pd.Series,
    significance_level: float = 0.05
) -> Dict[str, Any]:
    """Detect drift in target variable (concept drift)."""
    ref_rate = reference_targets.mean()
    curr_rate = current_targets.mean()
    
    # Chi-square test for proportion change
    refs = reference_targets.value_counts().sort_index()
    currs = current_targets.value_counts().sort_index()
    
    try:
        chi2, p_value, dof, expected = stats.chi2_contingency(
            np.array([refs.values, currs.values])
        )
        is_drifted = p_value < significance_level
    except:
        p_value = 1.0
        is_drifted = False
    
    return {
        "reference_positive_rate": float(ref_rate),
        "current_positive_rate": float(curr_rate),
        "rate_change": float(abs(curr_rate - ref_rate)),
        "p_value": float(p_value),
        "is_drifted": is_drifted
    }


def generate_drift_report(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    numeric_features: List[str],
    categorical_features: List[str],
    target_column: Optional[str] = None,
    significance_level: float = 0.05,
    drift_threshold: float = 0.1
) -> DriftReport:
    """Generate comprehensive data drift report."""
    logger.info("Generating data drift report...")
    
    # Detect feature drift
    feature_drift = detect_feature_drift(
        reference_df,
        current_df,
        numeric_features,
        categorical_features,
        significance_level
    )
    
    drifted_features = [f for f, (_, is_drifted) in feature_drift.items() if is_drifted]
    drift_scores = {f: stat for f, (stat, _) in feature_drift.items()}
    
    # Detect target drift
    target_drift_info = {}
    if target_column and target_column in reference_df.columns and target_column in current_df.columns:
        target_drift_info = detect_target_drift(
            reference_df[target_column],
            current_df[target_column],
            significance_level
        )
        if target_drift_info.get("is_drifted"):
            drifted_features.append(target_column)
    
    # Determine alert level
    drift_pct = len(drifted_features) / (len(numeric_features) + len(categorical_features))
    if drift_pct > 0.2:
        alert_level = "red"
    elif drift_pct > 0.1:
        alert_level = "yellow"
    else:
        alert_level = "green"
    
    # Generate recommendations
    recommendations = []
    if alert_level == "red":
        recommendations.append("CRITICAL: Significant data drift detected. Retrain model immediately.")
        recommendations.append("Review feature engineering pipeline for issues.")
        recommendations.append("Check data quality in source systems.")
    elif alert_level == "yellow":
        recommendations.append("WARNING: Moderate data drift detected. Monitor closely.")
        recommendations.append("Consider retraining model in next cycle.")
    else:
        recommendations.append("Data distribution stable. No action required.")
    
    if target_drift_info.get("is_drifted"):
        recommendations.append(f"Target variable drift detected: {target_drift_info['reference_positive_rate']:.1%} → {target_drift_info['current_positive_rate']:.1%}")
    
    report = DriftReport(
        timestamp=datetime.now().isoformat(),
        n_features_drifted=len(drifted_features),
        total_features=len(numeric_features) + len(categorical_features),
        drift_features=drifted_features,
        drift_thresholds={f: significance_level for f in drifted_features},
        drift_scores=drift_scores,
        alert_level=alert_level,
        recommendations=recommendations
    )
    
    logger.info(f"Drift Report: {alert_level.upper()} - {len(drifted_features)}/{report.total_features} features drifted")
    
    return report


def save_drift_report(
    report: DriftReport,
    output_dir: Path
) -> Path:
    """Save drift report to JSON file."""
    report_dict = {
        "timestamp": report.timestamp,
        "n_features_drifted": report.n_features_drifted,
        "total_features": report.total_features,
        "drift_pct": report.n_features_drifted / report.total_features if report.total_features > 0 else 0,
        "drifted_features": report.drift_features,
        "drift_scores": report.drift_scores,
        "alert_level": report.alert_level,
        "recommendations": report.recommendations
    }
    
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"drift_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report_dict, indent=2))
    
    logger.info(f"Saved drift report to {report_path}")
    
    return report_path


def monitor_model_performance(
    predictions: pd.Series,
    actuals: Optional[pd.Series] = None,
    predictions_threshold: float = 0.5
) -> Dict[str, Any]:
    """Monitor model performance metrics over time."""
    monitoring_data = {
        "timestamp": datetime.now().isoformat(),
        "n_predictions": len(predictions),
        "prediction_rate": float((predictions >= predictions_threshold).mean()),
        "avg_probability": float(predictions.mean()),
        "std_probability": float(predictions.std()),
        "median_probability": float(predictions.median()),
        "q25_probability": float(predictions.quantile(0.25)),
        "q75_probability": float(predictions.quantile(0.75))
    }
    
    if actuals is not None:
        from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
        
        binary_preds = (predictions >= predictions_threshold).astype(int)
        monitoring_data["accuracy"] = float(accuracy_score(actuals, binary_preds))
        monitoring_data["f1_score"] = float(f1_score(actuals, binary_preds, zero_division=0))
        monitoring_data["roc_auc"] = float(roc_auc_score(actuals, predictions))
    
    return monitoring_data


def setup_monitoring_schedule(
    interval_days: int = 1,
    output_dir: Path = Path("monitoring_logs")
) -> Dict[str, Any]:
    """Setup monitoring schedule configuration."""
    config = {
        "interval_days": interval_days,
        "checks": [
            {
                "name": "data_drift",
                "enabled": True,
                "frequency_days": interval_days,
                "thresholds": {
                    "drift_alert_level": "yellow",
                    "drift_percentage": 0.15
                }
            },
            {
                "name": "model_performance",
                "enabled": True,
                "frequency_days": interval_days,
                "thresholds": {
                    "min_accuracy": 0.75,
                    "min_f1": 0.70
                }
            },
            {
                "name": "prediction_volume",
                "enabled": True,
                "frequency_days": interval_days,
                "thresholds": {
                    "min_predictions_per_day": 100,
                    "max_null_predictions": 0.01
                }
            }
        ],
        "output_directory": str(output_dir),
        "retention_days": 90
    }
    
    return config
