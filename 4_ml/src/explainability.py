from __future__ import annotations

from typing import Any, Tuple

import numpy as np
import pandas as pd
import shap


def normalize_shap_values(shap_values: Any) -> np.ndarray:
    if isinstance(shap_values, list):
        if len(shap_values) == 2:
            return np.asarray(shap_values[1])
        return np.asarray(shap_values[0])
    return np.asarray(shap_values)


def _resolve_pipeline(model: Any):
    return model.best_estimator_ if hasattr(model, "best_estimator_") else model


def align_features(model: Any, X: pd.DataFrame) -> pd.DataFrame:
    pipeline = _resolve_pipeline(model)
    if hasattr(pipeline, "feature_names_in_"):
        return X[list(pipeline.feature_names_in_)]
    return X


def get_feature_matrix(model: Any, X: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    pipeline = _resolve_pipeline(model)
    X = align_features(model, X)
    preprocessor = pipeline.named_steps["preprocessor"]
    transformed = preprocessor.transform(X)
    if hasattr(preprocessor, "get_feature_names_out"):
        feature_names = list(preprocessor.get_feature_names_out())
    else:
        feature_names = [f"feature_{index}" for index in range(transformed.shape[1])]
    return transformed, feature_names


def build_tree_explainer(model: Any) -> shap.TreeExplainer:
    pipeline = _resolve_pipeline(model)
    return shap.TreeExplainer(pipeline.named_steps["model"])


def compute_shap_values(model: Any, X: pd.DataFrame, sample_size: int = 1000) -> tuple[Any, np.ndarray, list[str]]:
    X = align_features(model, X)
    sampled = X.sample(min(len(X), sample_size), random_state=42)
    transformed, feature_names = get_feature_matrix(model, sampled)
    explainer = build_tree_explainer(model)
    shap_values = normalize_shap_values(explainer.shap_values(transformed))
    return shap_values, transformed, feature_names


def shap_importance_frame(shap_values: np.ndarray, feature_names: list[str]) -> pd.DataFrame:
    mean_abs = np.abs(np.asarray(shap_values)).mean(axis=0)
    return pd.DataFrame({"feature": feature_names, "mean_abs_shap": mean_abs}).sort_values("mean_abs_shap", ascending=False)
