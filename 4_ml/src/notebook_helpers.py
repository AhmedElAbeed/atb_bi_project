"""Shared utilities for ML notebooks: paths, styling, and comparison plots."""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    RocCurveDisplay,
    PrecisionRecallDisplay,
    confusion_matrix,
)


def find_ml_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "4_ml" / "src").exists():
            return candidate / "4_ml"
        if candidate.name == "4_ml" and (candidate / "src").exists():
            return candidate
    raise FileNotFoundError("Could not locate the 4_ml workspace.")


def setup_notebook(ml_root: Path | None = None) -> Path:
    """Add src to path and apply a consistent plot theme."""
    root = ml_root or find_ml_root()
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    sns.set_theme(style="whitegrid", context="notebook", palette="colorblind")
    plt.rcParams.update({"figure.dpi": 120, "savefig.dpi": 150, "font.size": 10})
    return root


PIPELINE_MARKDOWN = """
## End-to-end ML pipeline

```mermaid
flowchart LR
    A[Warehouse / Raw CSV] --> B[01 EDA]
    B --> C[02 Feature Engineering]
    C --> D[03 Model Training]
    D --> E[04 SHAP Explainability]
    D --> F[05 Model Selection Report]
    F --> G[Deploy best model]
```

| Step | Notebook | Purpose |
|------|----------|---------|
| 1 | `01_eda` | Understand data quality, target balance, leakage |
| 2 | `02_feature_engineering` | Build model-ready features, temporal train/test split |
| 3 | `03_model_training` | Train XGBoost + Random Forest with SMOTETomek |
| 4 | `04_shap_explainability` | Global/local feature importance |
| 5 | `05_model_selection_report` | Compare metrics, pick production model |
"""


def metrics_comparison_bar(
    metrics_df: pd.DataFrame,
    metric_cols: list[str] | None = None,
    title: str = "Model comparison",
    figsize: tuple[float, float] = (12, 5),
) -> plt.Figure:
    metric_cols = metric_cols or ["accuracy", "f1", "precision", "recall", "roc_auc", "pr_auc"]
    plot_df = metrics_df.melt(id_vars=["model"], value_vars=metric_cols, var_name="metric", value_name="score")
    fig, ax = plt.subplots(figsize=figsize)
    sns.barplot(data=plot_df, x="metric", y="score", hue="model", ax=ax)
    ax.set_ylim(0, 1.05)
    ax.set_title(title)
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    return fig


def train_test_gap_plot(gap_df: pd.DataFrame, output_path: Path | None = None) -> plt.Figure:
    melted = gap_df.melt(id_vars="model", var_name="gap_metric", value_name="gap_value")
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=melted, x="gap_metric", y="gap_value", hue="model", ax=ax)
    ax.axhline(0, color="black", linewidth=1)
    ax.set_title("Overfitting check: train minus test (lower is better)")
    ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    if output_path:
        fig.savefig(output_path, bbox_inches="tight")
    return fig


def plot_roc_pr_grid(
    models: dict[str, object],
    X_test: pd.DataFrame,
    y_test: pd.Series,
    figsize: tuple[float, float] = (14, 5),
) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=figsize)
    for name, model in models.items():
        y_proba = model.predict_proba(X_test)[:, 1]
        RocCurveDisplay.from_predictions(y_test, y_proba, name=name, ax=axes[0])
        PrecisionRecallDisplay.from_predictions(y_test, y_proba, name=name, ax=axes[1])
    axes[0].set_title("ROC curves (test set)")
    axes[1].set_title("Precision–Recall curves (test set)")
    axes[0].plot([0, 1], [0, 1], "k--", alpha=0.4)
    fig.tight_layout()
    return fig


def plot_confusion_matrices(
    models: dict[str, object],
    X_test: pd.DataFrame,
    y_test: pd.Series,
    thresholds: dict[str, float],
    figsize: tuple[float, float] = (12, 5),
) -> plt.Figure:
    n = len(models)
    fig, axes = plt.subplots(1, n, figsize=figsize)
    if n == 1:
        axes = [axes]
    for ax, (name, model) in zip(axes, models.items()):
        proba = model.predict_proba(X_test)[:, 1]
        pred = (proba >= thresholds[name]).astype(int)
        cm = confusion_matrix(y_test, pred)
        disp = ConfusionMatrixDisplay(cm, display_labels=["Low risk", "High risk"])
        disp.plot(ax=ax, colorbar=False, cmap="Blues")
        ax.set_title(f"{name}\n(threshold={thresholds[name]:.3f})")
    fig.tight_layout()
    return fig


def radar_chart(metrics_df: pd.DataFrame, metrics: list[str] | None = None) -> plt.Figure:
    metrics = metrics or ["accuracy", "f1", "roc_auc", "pr_auc", "recall"]
    labels = np.array(metrics)
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False)
    angles = np.concatenate([angles, angles[:1]])

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw=dict(polar=True))
    for _, row in metrics_df.iterrows():
        values = row[metrics].astype(float).values
        values = np.concatenate([values, values[:1]])
        ax.plot(angles, values, linewidth=2, label=row["model"])
        ax.fill(angles, values, alpha=0.15)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels([m.upper() for m in labels])
    ax.set_ylim(0, 1)
    ax.set_title("Multi-metric profile (test set)")
    ax.legend(loc="upper right", bbox_to_anchor=(1.25, 1.05))
    fig.tight_layout()
    return fig
