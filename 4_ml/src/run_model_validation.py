from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "4_ml" / "src"
sys.path.insert(0, str(SRC))

from data_access import load_feature_frame
from features import build_model_frame, engineer_features, select_feature_columns, split_temporal
import modeling
import joblib


def main() -> None:
    frame = build_model_frame(load_feature_frame())
    engineered = engineer_features(frame)
    engineered = engineered.loc[:, ~engineered.columns.duplicated()].copy()

    feature_columns, _, _ = select_feature_columns(engineered)
    feature_columns = list(dict.fromkeys(feature_columns))

    train_frame, test_frame = split_temporal(engineered, date_column="scoring_date", test_fraction=0.2)
    X_train = train_frame[feature_columns].copy()
    y_train = train_frame["risk_target"].astype(int)
    X_test = test_frame[feature_columns].copy()
    y_test = test_frame["risk_target"].astype(int)

    models = {
        "xgboost": ROOT / "4_ml" / "models" / "xgboost_best.pkl",
        "random_forest": ROOT / "4_ml" / "models" / "random_forest_best.pkl",
        "random_forest_joblib": ROOT / "4_ml" / "models" / "random_forest_best_model.joblib",
        "xgboost_joblib": ROOT / "4_ml" / "models" / "xgboost_best_model.joblib",
    }

    resolved = {}
    if models["xgboost"].exists():
        resolved["xgboost"] = models["xgboost"]
    elif models["xgboost_joblib"].exists():
        resolved["xgboost"] = models["xgboost_joblib"]

    if models["random_forest"].exists():
        resolved["random_forest"] = models["random_forest"]
    elif models["random_forest_joblib"].exists():
        resolved["random_forest"] = models["random_forest_joblib"]

    if len(resolved) < 2:
        raise SystemExit("Missing model artifacts. Run training first.")

    results = {}
    for name, model_path in resolved.items():
        model = joblib.load(model_path)
        train_proba = model.predict_proba(X_train)[:, 1]
        threshold, _ = modeling.optimize_threshold(y_train, train_proba)
        train_metrics = modeling.evaluate_model(model, X_train, y_train, threshold=threshold)
        test_metrics = modeling.evaluate_model(model, X_test, y_test, threshold=threshold)
        results[name] = {
            "threshold": threshold,
            "artifact": str(model_path),
            "train_metrics": train_metrics,
            "test_metrics": test_metrics,
            "overfitting_gap": {
                "f1_gap": train_metrics["f1"] - test_metrics["f1"],
                "roc_auc_gap": train_metrics["roc_auc"] - test_metrics["roc_auc"],
                "accuracy_gap": train_metrics["accuracy"] - test_metrics["accuracy"],
            },
        }

    best_model = max(results.keys(), key=lambda m: results[m]["test_metrics"]["f1"])

    summary = {
        "best_model_by_test_f1": best_model,
        "results": results,
    }

    output_dir = ROOT / "4_ml" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "model_validation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    plot_df = pd.DataFrame(
        [
            {
                "model": model_name,
                "split": split,
                "accuracy": metrics["accuracy"],
                "f1": metrics["f1"],
                "roc_auc": metrics["roc_auc"],
                "pr_auc": metrics["pr_auc"],
                "brier_score": metrics["brier_score"],
            }
            for model_name, values in results.items()
            for split, metrics in [
                ("train", values["train_metrics"]),
                ("test", values["test_metrics"]),
            ]
        ]
    )

    for metric in ["accuracy", "f1", "roc_auc", "pr_auc"]:
        fig, ax = plt.subplots(figsize=(8, 5))
        pivot = plot_df.pivot(index="model", columns="split", values=metric)
        pivot.plot(kind="bar", ax=ax)
        ax.set_title(f"{metric.upper()} train vs test")
        ax.set_ylabel(metric)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(output_dir / f"validation_{metric}.png", dpi=150)
        plt.close(fig)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
