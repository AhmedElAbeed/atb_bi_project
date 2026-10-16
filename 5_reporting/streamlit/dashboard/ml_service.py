"""ML model loading, scoring, and evaluation helpers for the Streamlit dashboard."""
from __future__ import annotations

import json
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.metrics import (
    auc,
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)

APP_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = APP_DIR.parents[1]
ML_ROOT = PROJECT_ROOT / "4_ml"
ML_SRC = ML_ROOT / "src"
ML_OUTPUTS = ML_ROOT / "outputs"
ML_MODELS = ML_ROOT / "models"

if str(ML_SRC) not in sys.path:
    sys.path.insert(0, str(ML_SRC))

from features import engineer_features, select_feature_columns  # noqa: E402
from data_access import load_feature_frame  # noqa: E402


def _latest_artifact(pattern: str) -> Path | None:
    hits = list(ML_MODELS.glob(pattern))
    hits.extend(ML_ROOT.glob(f"mlruns/**/artifacts/{pattern}"))
    hits = [p for p in hits if p.is_file()]
    if not hits:
        return None
    return max(hits, key=lambda p: p.stat().st_mtime)


def load_training_summary() -> dict[str, Any] | None:
    path = ML_OUTPUTS / "training_summary.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _mlflow_tracking_uri() -> str:
    explicit = os.getenv("MLFLOW_TRACKING_URI", "").strip()
    if explicit:
        return explicit
    store = (ML_ROOT / "mlruns").resolve()
    return store.as_uri()


def _read_metric_file(path: Path) -> float | None:
    if not path.is_file():
        return None
    try:
        return float(path.read_text(encoding="utf-8").strip())
    except (TypeError, ValueError):
        return None


def _scan_mlruns_from_disk() -> dict[str, Any]:
    """Filesystem fallback when MLflow client returns empty (Docker path mismatch, etc.)."""
    out: dict[str, Any] = {
        "registered_models": [],
        "production_versions": [],
        "recent_runs": [],
        "experiments": [],
    }
    mlruns = ML_ROOT / "mlruns"
    if not mlruns.is_dir():
        return out

    models_dir = mlruns / "models"
    if models_dir.is_dir():
        for model_dir in sorted(models_dir.iterdir()):
            if not model_dir.is_dir():
                continue
            meta_path = model_dir / "meta.yaml"
            name = model_dir.name
            if meta_path.is_file():
                for line in meta_path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("name:"):
                        name = line.split(":", 1)[1].strip()
                        break
            versions = []
            for ver_dir in sorted(model_dir.glob("version-*")):
                vmeta = ver_dir / "meta.yaml"
                if not vmeta.is_file():
                    continue
                raw = vmeta.read_text(encoding="utf-8")
                version = ver_dir.name.replace("version-", "")
                stage = "None"
                run_id = ""
                for line in raw.splitlines():
                    if line.startswith("current_stage:"):
                        stage = line.split(":", 1)[1].strip()
                    if line.startswith("run_id:"):
                        run_id = line.split(":", 1)[1].strip()
                versions.append({
                    "version": version,
                    "stage": stage,
                    "status": "READY",
                    "run_id": run_id,
                    "created": None,
                })
                if stage == "Production":
                    out["production_versions"].append({
                        "model": name,
                        "version": version,
                        "run_id": run_id,
                    })
            out["registered_models"].append({"name": name, "versions": versions})

    exp_name = os.getenv("ATB_MLFLOW_EXPERIMENT", "atb_bi_customer_risk")
    for exp_dir in mlruns.iterdir():
        if not exp_dir.is_dir() or exp_dir.name in ("models", "0", ".trash"):
            continue
        exp_meta = exp_dir / "meta.yaml"
        if not exp_meta.is_file():
            continue
        ename = exp_dir.name
        for line in exp_meta.read_text(encoding="utf-8").splitlines():
            if line.startswith("name:"):
                ename = line.split(":", 1)[1].strip()
                break
        out["experiments"].append({
            "name": ename,
            "id": exp_dir.name,
            "lifecycle": "active",
        })
        if ename != exp_name:
            continue
        runs = []
        for run_dir in exp_dir.iterdir():
            if not run_dir.is_dir() or run_dir.name == "models":
                continue
            rmeta = run_dir / "meta.yaml"
            if not rmeta.is_file():
                continue
            raw = rmeta.read_text(encoding="utf-8")
            run_id = run_dir.name
            run_name = run_id
            start_time = None
            status = "FINISHED"
            for line in raw.splitlines():
                if line.startswith("run_id:"):
                    run_id = line.split(":", 1)[1].strip()
                if line.startswith("run_name:"):
                    run_name = line.split(":", 1)[1].strip() or run_name
                if line.startswith("start_time:"):
                    try:
                        start_time = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
            metrics = {}
            metrics_dir = run_dir / "metrics"
            if metrics_dir.is_dir():
                for mf in metrics_dir.iterdir():
                    val = _read_metric_file(mf)
                    if val is not None:
                        metrics[mf.name] = round(val, 4)
            runs.append({
                "run_id": run_id,
                "name": run_name,
                "status": status,
                "start": start_time,
                "metrics": metrics,
                "params": {},
            })
        runs.sort(key=lambda r: r.get("start") or 0, reverse=True)
        out["recent_runs"] = runs[:15]

    return out


def sync_training_summary_from_mlflow() -> dict[str, Any] | None:
    """Build training_summary.json from Production model + latest production run."""
    disk = _scan_mlruns_from_disk()
    prod = (disk.get("production_versions") or [{}])[0]
    runs = disk.get("recent_runs") or []
    prod_run = next((r for r in runs if r.get("name", "").endswith("_production")), runs[0] if runs else None)
    if not prod_run and not prod.get("run_id"):
        return None

    run_id = prod.get("run_id") or (prod_run or {}).get("run_id")
    metrics = (prod_run or {}).get("metrics") or {}
    algo = "xgboost"
    if prod_run and prod_run.get("name"):
        algo = str(prod_run["name"]).replace("_production", "")

    summary = {
        "best_model": algo,
        "results": {
            algo: {
                "metrics": metrics,
                "threshold": metrics.get("threshold", 0.5),
            }
        },
        "registry": {
            "registry_name": prod.get("model", "atb_customer_risk_classifier"),
            "version": prod.get("version", "1"),
            "run_id": run_id,
            "stage": "Production",
        },
    }
    ML_OUTPUTS.mkdir(parents=True, exist_ok=True)
    out_path = ML_OUTPUTS / "training_summary.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    load_training_summary.cache_clear() if hasattr(load_training_summary, "cache_clear") else None
    return summary


def load_mlflow_registry_details() -> dict[str, Any]:
    """Full MLflow registry snapshot for the dedicated MLflow page."""
    card = load_model_registry_card()
    out: dict[str, Any] = {
        "card": card,
        "tracking_uri": _mlflow_tracking_uri(),
        "experiments": [],
        "registered_models": [],
        "production_versions": [],
        "recent_runs": [],
        "available": False,
    }
    try:
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(_mlflow_tracking_uri())
        client = MlflowClient()

        for exp in client.search_experiments():
            out["experiments"].append({
                "name": exp.name,
                "id": exp.experiment_id,
                "lifecycle": exp.lifecycle_stage,
            })

        for rm in client.search_registered_models():
            versions = []
            for mv in client.search_model_versions(f"name='{rm.name}'"):
                versions.append({
                    "version": mv.version,
                    "stage": mv.current_stage,
                    "status": mv.status,
                    "run_id": mv.run_id,
                    "created": mv.creation_timestamp,
                })
                if mv.current_stage == "Production":
                    out["production_versions"].append({
                        "model": rm.name,
                        "version": mv.version,
                        "run_id": mv.run_id,
                    })
            out["registered_models"].append({
                "name": rm.name,
                "versions": versions,
            })

        exp_name = os.getenv("ATB_MLFLOW_EXPERIMENT", "atb_bi_customer_risk")
        exp = client.get_experiment_by_name(exp_name)
        if exp:
            runs = client.search_runs(
                experiment_ids=[exp.experiment_id],
                order_by=["start_time DESC"],
                max_results=15,
            )
            for run in runs:
                out["recent_runs"].append({
                    "run_id": run.info.run_id,
                    "name": run.info.run_name,
                    "status": run.info.status,
                    "start": run.info.start_time,
                    "metrics": {k: round(v, 4) for k, v in (run.data.metrics or {}).items()},
                    "params": dict(run.data.params or {}),
                })

        out["available"] = bool(out["registered_models"] or out["recent_runs"] or card.get("available"))

        if not out["registered_models"] and not out["recent_runs"]:
            disk = _scan_mlruns_from_disk()
            if disk["registered_models"]:
                out["registered_models"] = disk["registered_models"]
            if disk["recent_runs"]:
                out["recent_runs"] = disk["recent_runs"]
            if disk["experiments"]:
                out["experiments"] = disk["experiments"]
            if disk["production_versions"]:
                out["production_versions"] = disk["production_versions"]
            out["available"] = bool(
                out["registered_models"] or out["recent_runs"] or card.get("available")
            )
            out["source"] = "filesystem_fallback"
    except Exception as exc:
        out["error"] = str(exc)
        disk = _scan_mlruns_from_disk()
        out["registered_models"] = disk["registered_models"]
        out["recent_runs"] = disk["recent_runs"]
        out["production_versions"] = disk["production_versions"]
        out["experiments"] = disk["experiments"]
        out["available"] = bool(
            out["registered_models"] or out["recent_runs"] or card.get("available")
        )
        if out["available"]:
            out["source"] = "filesystem_fallback"
        else:
            out["available"] = card.get("available", False)

    if not card.get("available") and out.get("recent_runs"):
        try:
            sync_training_summary_from_mlflow()
            out["card"] = load_model_registry_card()
        except Exception:
            pass

    return out


def load_model_registry_card() -> dict[str, Any]:
    """MLflow registry card for dashboard — metrics, version, Production stage."""
    summary = load_training_summary()
    if not summary:
        return {"available": False}

    best = summary.get("best_model", "xgboost")
    entry = (summary.get("results") or {}).get(best, {})
    metrics = entry.get("metrics") or {}
    registry = summary.get("registry") or {}

    path = ML_OUTPUTS / "training_summary.json"
    trained_at = "—"
    if path.exists():
        from datetime import datetime
        trained_at = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")

    return {
        "available": True,
        "model_name": registry.get("registry_name", "atb_customer_risk_classifier"),
        "version": registry.get("version", entry.get("version", "—")),
        "stage": registry.get("stage", "Production"),
        "best_algorithm": best,
        "threshold": entry.get("threshold", metrics.get("threshold")),
        "metrics": {
            "roc_auc": metrics.get("roc_auc"),
            "f1": metrics.get("f1"),
            "precision": metrics.get("precision"),
            "recall": metrics.get("recall"),
            "pr_auc": metrics.get("pr_auc"),
        },
        "run_id": registry.get("run_id"),
        "trained_at": trained_at,
    }


def load_feature_columns() -> list[str]:
    path = ML_OUTPUTS / "feature_engineering" / "feature_columns.txt"
    if path.exists():
        cols = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return list(dict.fromkeys(cols))
    return []


@lru_cache(maxsize=4)
def load_model(model_name: str = "xgboost") -> tuple[Any | None, str, Path | None]:
    """Return (pipeline, info_message, artifact_path)."""
    summary = load_training_summary()
    if summary and summary.get("best_model"):
        model_name = summary["best_model"]

    candidates = [
        ML_MODELS / f"{model_name}_best.pkl",
        ML_MODELS / f"{model_name}_best_model.joblib",
    ]
    artifact = _latest_artifact(f"{model_name}_best.pkl") or _latest_artifact(f"{model_name}_best_model.joblib")
    for c in candidates:
        if c.exists():
            artifact = c
            break

    if artifact is None:
        return None, f"No artifact found for {model_name}. Run: cd 4_ml && python src/run_training.py", None

    try:
        return joblib.load(artifact), f"Loaded {model_name} from {artifact.name}", artifact
    except Exception as exc:
        return None, f"Failed to load model: {exc}", artifact


def get_model_threshold(model_name: str | None = None) -> float:
    summary = load_training_summary()
    if summary and model_name:
        entry = (summary.get("results") or {}).get(model_name, {})
        if entry.get("threshold"):
            return float(entry["threshold"])
    if summary:
        best = summary.get("best_model")
        entry = (summary.get("results") or {}).get(best or "", {})
        if entry.get("threshold"):
            return float(entry["threshold"])
    return 0.5


def list_output_images() -> dict[str, Path]:
    labels = {
        "roc_pr": "05_roc_pr_curves.png",
        "metrics": "05_test_metrics_comparison.png",
        "confusion": "05_confusion_matrices.png",
        "radar": "05_radar_comparison.png",
        "shap": "shap_summary.png",
        "validation_roc": "validation_roc_auc.png",
        "validation_pr": "validation_pr_auc.png",
        "model_bars": "03_model_comparison_bars.png",
    }
    found: dict[str, Path] = {}
    for key, name in labels.items():
        path = ML_OUTPUTS / name
        if path.exists():
            found[key] = path
    return found


@lru_cache(maxsize=1)
def load_scoring_frame(max_rows: int = 8000) -> pd.DataFrame:
    frame = load_feature_frame()
    if len(frame) > max_rows:
        frame = frame.sample(max_rows, random_state=42)
    return frame


def prepare_features(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    engineered = engineer_features(raw.copy())
    feature_columns, _, _ = select_feature_columns(engineered)
    feature_columns = list(dict.fromkeys(feature_columns))
    return engineered, feature_columns


def score_frame(model: Any, frame: pd.DataFrame, threshold: float) -> pd.DataFrame:
    engineered, feature_columns = prepare_features(frame)
    missing = [c for c in feature_columns if c not in engineered.columns]
    for col in missing:
        engineered[col] = np.nan
    X = engineered[feature_columns]
    proba = model.predict_proba(X)[:, 1]
    out = frame.copy()
    if "customer_id" not in out.columns and "customer_id" in engineered.columns:
        out["customer_id"] = engineered["customer_id"]
    out["ml_risk_probability"] = proba
    out["ml_predicted_high_risk"] = (proba >= threshold).astype(int)
    out["ml_risk_label"] = np.where(proba >= threshold, "HIGH RISK (ML)", "LOW RISK (ML)")
    if "global_risk_score" in out.columns:
        out["rules_high_risk"] = (out["global_risk_score"].fillna(0) >= 50).astype(int)
    return out


def predict_single(model: Any, inputs: dict[str, Any], threshold: float) -> dict[str, Any]:
    row = pd.DataFrame([inputs])
    if "scoring_date" not in row.columns:
        row["scoring_date"] = pd.Timestamp.today().normalize()
    if "load_date" not in row.columns:
        row["load_date"] = row["scoring_date"]
    if "last_kyc_review_date" not in row.columns and "days_since_last_kyc_review" in row.columns:
        row["last_kyc_review_date"] = row["scoring_date"] - pd.to_timedelta(
            row["days_since_last_kyc_review"].fillna(365), unit="D"
        )
    if "next_kyc_review_date" not in row.columns:
        row["next_kyc_review_date"] = row["scoring_date"] + pd.Timedelta(days=365)

    engineered, feature_columns = prepare_features(row)
    missing = [c for c in feature_columns if c not in engineered.columns]
    for col in missing:
        engineered[col] = np.nan
    X = engineered[feature_columns]
    proba = float(model.predict_proba(X)[:, 1][0])
    pred = int(proba >= threshold)
    return {
        "probability": proba,
        "predicted_class": pred,
        "label": "HIGH RISK" if pred else "LOW RISK",
        "threshold": threshold,
        "confidence_pct": proba * 100 if pred else (1 - proba) * 100,
    }


def metrics_comparison_df() -> pd.DataFrame:
    summary = load_training_summary()
    if not summary:
        return pd.DataFrame()
    rows = []
    for model_name, payload in (summary.get("results") or {}).items():
        for metric, value in (payload.get("metrics") or {}).items():
            rows.append({"model": model_name, "metric": metric, "value": float(value)})
    return pd.DataFrame(rows)


def compute_curve_data(model: Any, sample_size: int = 4000) -> dict[str, Any]:
    frame = load_scoring_frame()
    if len(frame) > sample_size:
        frame = frame.sample(sample_size, random_state=42)
    if "global_risk_score" not in frame.columns:
        raise ValueError("global_risk_score missing — cannot evaluate curves.")
    y_true = (frame["global_risk_score"].fillna(0) >= 50).astype(int).values

    engineered, feature_columns = prepare_features(frame)
    X = engineered[feature_columns]
    y_proba = model.predict_proba(X)[:, 1]

    fpr, tpr, _ = roc_curve(y_true, y_proba)
    prec, rec, _ = precision_recall_curve(y_true, y_proba)
    roc_auc = roc_auc_score(y_true, y_proba)
    pr_auc = average_precision_score(y_true, y_proba)
    threshold = get_model_threshold()
    y_pred = (y_proba >= threshold).astype(int)
    cm = confusion_matrix(y_true, y_pred)

    return {
        "fpr": fpr,
        "tpr": tpr,
        "precision": prec,
        "recall": rec,
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "confusion_matrix": cm,
        "y_proba": y_proba,
        "y_true": y_true,
        "threshold": threshold,
    }


def plot_roc_pr(curve_data: dict[str, Any]) -> go.Figure:
    fig = make_subplots(rows=1, cols=2, subplot_titles=("ROC curve", "Precision–Recall curve"))
    fig.add_trace(
        go.Scatter(x=curve_data["fpr"], y=curve_data["tpr"], mode="lines", name=f"ROC AUC={curve_data['roc_auc']:.3f}", line=dict(color="#38bdf8", width=3)),
        row=1,
        col=1,
    )
    fig.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="Random", line=dict(dash="dash", color="#64748b")), row=1, col=1)
    fig.add_trace(
        go.Scatter(x=curve_data["recall"], y=curve_data["precision"], mode="lines", name=f"PR AUC={curve_data['pr_auc']:.3f}", line=dict(color="#a78bfa", width=3)),
        row=1,
        col=2,
    )
    fig.update_xaxes(title_text="False positive rate", row=1, col=1)
    fig.update_yaxes(title_text="True positive rate", row=1, col=1)
    fig.update_xaxes(title_text="Recall", row=1, col=2)
    fig.update_yaxes(title_text="Precision", row=1, col=2)
    fig.update_layout(template="plotly_dark", height=420, showlegend=True, paper_bgcolor="rgba(0,0,0,0)")
    return fig


def plot_confusion_matrix(cm: np.ndarray) -> go.Figure:
    labels = [["True Neg", "False Pos"], ["False Neg", "True Pos"]]
    fig = go.Figure(
        data=go.Heatmap(
            z=cm,
            x=["Predicted 0", "Predicted 1"],
            y=["Actual 0", "Actual 1"],
            text=[[f"{cm[0,0]:,}", f"{cm[0,1]:,}"], [f"{cm[1,0]:,}", f"{cm[1,1]:,}"]],
            texttemplate="%{text}",
            colorscale="Blues",
        )
    )
    fig.update_layout(template="plotly_dark", title="Confusion matrix (sample)", height=360, paper_bgcolor="rgba(0,0,0,0)")
    return fig


def plot_probability_distribution(scored: pd.DataFrame) -> go.Figure:
    fig = px.histogram(
        scored,
        x="ml_risk_probability",
        nbins=40,
        color="ml_predicted_high_risk",
        title="ML predicted risk probability distribution",
        labels={"ml_risk_probability": "P(high risk)", "ml_predicted_high_risk": "Predicted class"},
        color_discrete_map={0: "#22c55e", 1: "#ef4444"},
    )
    fig.update_layout(template="plotly_dark", height=380, paper_bgcolor="rgba(0,0,0,0)")
    return fig


def build_alert_list(scored: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """Customers where ML says high risk but DWH tier is LOW or MEDIUM."""
    if scored.empty:
        return scored
    low_tiers = {"LOW", "MEDIUM", "UNKNOWN"}
    mask = (
        (scored["ml_risk_probability"] >= threshold)
        & scored.get("risk_tier", pd.Series(dtype=str)).astype(str).str.upper().isin(low_tiers)
    )
    cols = [
        c for c in [
            "customer_id", "risk_tier", "global_risk_score",
            "compliance_risk_index", "financial_fragility_score", "behavioral_risk_score",
            "ml_risk_probability", "ml_risk_label", "is_kyc_complete", "is_pep",
            "total_working_balance", "negative_balance_account_count",
        ]
        if c in scored.columns
    ]
    out = scored.loc[mask, cols].copy()
    if not out.empty:
        out = out.sort_values("ml_risk_probability", ascending=False)
    return out


def agreement_stats(scored: pd.DataFrame, threshold: float) -> dict[str, float]:
    if "global_risk_score" not in scored.columns:
        return {}
    rules = (scored["global_risk_score"].fillna(0) >= 50).astype(int)
    ml = (scored["ml_risk_probability"] >= threshold).astype(int)
    agree = (rules == ml).mean() * 100
    ml_high_rules_low = ((ml == 1) & (rules == 0)).sum()
    rules_high_ml_low = ((rules == 1) & (ml == 0)).sum()
    return {
        "agreement_pct": float(agree),
        "ml_high_rules_low": int(ml_high_rules_low),
        "rules_high_ml_low": int(rules_high_ml_low),
        "ml_high_rate": float(ml.mean() * 100),
        "rules_high_rate": float(rules.mean() * 100),
    }


def plot_threshold_sensitivity(frame: pd.DataFrame, model: Any, thresholds: list[float]) -> go.Figure:
    """F1-like sensitivity: precision/recall at different thresholds (vs rules label)."""
    if "global_risk_score" not in frame.columns:
        return go.Figure()
    y_true = (frame["global_risk_score"].fillna(0) >= 50).astype(int).values
    engineered, feature_columns = prepare_features(frame)
    X = engineered[feature_columns]
    proba = model.predict_proba(X)[:, 1]
    rows = []
    for t in thresholds:
        pred = (proba >= t).astype(int)
        tp = ((pred == 1) & (y_true == 1)).sum()
        fp = ((pred == 1) & (y_true == 0)).sum()
        fn = ((pred == 0) & (y_true == 1)).sum()
        precision = tp / max(tp + fp, 1)
        recall = tp / max(tp + fn, 1)
        f1 = 2 * precision * recall / max(precision + recall, 1e-9)
        rows.append({"threshold": t, "precision": precision, "recall": recall, "f1": f1})
    df = pd.DataFrame(rows)
    fig = go.Figure()
    for col, color in [("precision", "#38bdf8"), ("recall", "#34d399"), ("f1", "#f59e0b")]:
        fig.add_trace(go.Scatter(x=df["threshold"], y=df[col], mode="lines+markers", name=col.upper(), line=dict(color=color)))
    fig.update_layout(
        template="plotly_dark", title="Threshold what-if (vs rules label ≥50)",
        xaxis_title="ML threshold", yaxis_title="Score", height=400, paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def plot_ml_vs_rules(scored: pd.DataFrame) -> go.Figure:
    if "global_risk_score" not in scored.columns:
        return go.Figure()
    fig = px.scatter(
        scored.sample(min(3000, len(scored)), random_state=42),
        x="global_risk_score",
        y="ml_risk_probability",
        color="ml_predicted_high_risk",
        opacity=0.45,
        title="ML probability vs warehouse global risk score",
        labels={"global_risk_score": "DWH global risk score", "ml_risk_probability": "ML P(high risk)"},
        color_discrete_map={0: "#22c55e", 1: "#ef4444"},
    )
    fig.update_layout(template="plotly_dark", height=400, paper_bgcolor="rgba(0,0,0,0)")
    return fig
