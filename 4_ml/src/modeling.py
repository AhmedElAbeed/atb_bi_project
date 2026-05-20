from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

import joblib
import mlflow
import numpy as np
import pandas as pd
from imblearn.combine import SMOTETomek
from imblearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    classification_report,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

from config import MODEL_DIR, MLFLOW_EXPERIMENT_NAME, MLFLOW_TRACKING_URI, RANDOM_STATE


@dataclass(frozen=True)
class ModelSpec:
    name: str
    pipeline: Pipeline
    param_distributions: Dict[str, Any]


def build_preprocessor(numeric_features: list[str], categorical_features: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("numeric", SimpleImputer(strategy="median"), numeric_features),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical_features,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )


def build_model_specs(numeric_features: list[str], categorical_features: list[str]) -> list[ModelSpec]:
    preprocessor = build_preprocessor(numeric_features, categorical_features)
    xgb_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("sampler", SMOTETomek(random_state=RANDOM_STATE)),
            (
                "model",
                XGBClassifier(
                    objective="binary:logistic",
                    eval_metric="logloss",
                    tree_method="hist",
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    rf_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("sampler", SMOTETomek(random_state=RANDOM_STATE)),
            (
                "model",
                RandomForestClassifier(
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    xgb_params = {
        "model__n_estimators": [200, 300, 500, 700],
        "model__max_depth": [3, 4, 6, 8],
        "model__learning_rate": [0.01, 0.03, 0.05, 0.1],
        "model__subsample": [0.7, 0.85, 1.0],
        "model__colsample_bytree": [0.7, 0.85, 1.0],
        "model__min_child_weight": [1, 3, 5, 10],
        "model__gamma": [0, 0.5, 1, 5],
        "model__reg_alpha": [0, 0.1, 0.5, 1],
        "model__reg_lambda": [1, 2, 5, 10],
    }
    rf_params = {
        "model__n_estimators": [300, 500, 800],
        "model__max_depth": [None, 8, 12, 16],
        "model__min_samples_split": [2, 5, 10],
        "model__min_samples_leaf": [1, 2, 4],
        "model__max_features": ["sqrt", "log2", 0.5],
        "model__class_weight": [None, "balanced"],
    }
    return [
        ModelSpec(name="xgboost", pipeline=xgb_pipeline, param_distributions=xgb_params),
        ModelSpec(name="random_forest", pipeline=rf_pipeline, param_distributions=rf_params),
    ]


def optimize_threshold(y_true: pd.Series, y_prob: np.ndarray) -> tuple[float, float]:
    precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
    if len(thresholds) == 0:
        return 0.5, 0.0
    f1_scores = 2 * precision[:-1] * recall[:-1] / np.maximum(precision[:-1] + recall[:-1], 1e-9)
    best_index = int(np.nanargmax(f1_scores))
    return float(thresholds[best_index]), float(f1_scores[best_index])


def evaluate_model(model: Any, X_test: pd.DataFrame, y_test: pd.Series, threshold: float = 0.5) -> dict[str, float]:
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= threshold).astype(int)
    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, y_proba)),
        "pr_auc": float(average_precision_score(y_test, y_proba)),
        "brier_score": float(brier_score_loss(y_test, y_proba)),
    }
    return metrics


def tune_model(spec: ModelSpec, X_train: pd.DataFrame, y_train: pd.Series, n_iter: int = 20) -> RandomizedSearchCV:
    cv = StratifiedKFold(n_splits=2, shuffle=True, random_state=RANDOM_STATE)
    search = RandomizedSearchCV(
        estimator=spec.pipeline,
        param_distributions=spec.param_distributions,
        n_iter=n_iter,
        scoring="f1",
        cv=cv,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbose=1,
    )
    search.fit(X_train, y_train)
    return search


def save_model(model: Any, filename: str) -> str:
    artifact_path = MODEL_DIR / filename
    joblib.dump(model, artifact_path)
    return str(artifact_path)


def start_mlflow_run(experiment_name: str = MLFLOW_EXPERIMENT_NAME, tracking_uri: str = MLFLOW_TRACKING_URI) -> None:
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)


def log_metrics_and_artifacts(
    model_name: str,
    best_model: Any,
    metrics: dict[str, float],
    param_summary: dict[str, Any],
    artifact_filename: str,
) -> str:
    start_mlflow_run()
    with mlflow.start_run(run_name=model_name):
        mlflow.log_params(param_summary)
        mlflow.log_metrics(metrics)
        artifact_path = save_model(best_model, artifact_filename)
        mlflow.log_artifact(artifact_path)
    return artifact_path
