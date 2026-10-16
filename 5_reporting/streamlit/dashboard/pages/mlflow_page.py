"""MLflow Model Registry — experiments, runs, Production model."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from dashboard import loaders, ml_service
from dashboard.config import ATB_PRIMARY, ATB_SILVER
from dashboard.i18n import t
from dashboard.ui import insight, kpi_row, page_header, section


def _fmt_ts(ms: int | None) -> str:
    if not ms:
        return "—"
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ms)


def render() -> None:
    page_header(
        t("mlflow_title"),
        t("mlflow_sub"),
        table="PFE_ML + 4_ml/mlruns",
    )

    insight(t("mlflow_explainer"))

    col_run, col_uri = st.columns([1, 2])
    with col_run:
        if st.button(t("mlflow_test_run"), help=t("mlflow_test_run_help")):
            import subprocess
            import sys

            script = ml_service.ML_SRC / "run_test_training.py"
            with st.spinner(t("mlflow_test_running")):
                try:
                    proc = subprocess.run(
                        [sys.executable, str(script)],
                        cwd=str(ml_service.ML_SRC),
                        capture_output=True,
                        text=True,
                        timeout=600,
                    )
                    if proc.returncode == 0:
                        st.success(t("mlflow_test_done"))
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(t("mlflow_test_fail"))
                        st.code(proc.stderr or proc.stdout or "unknown error")
                except Exception as exc:
                    st.error(f"{t('mlflow_test_fail')}: {exc}")

    details = ml_service.load_mlflow_registry_details()
    card = details.get("card") or {}
    ml_summary = loaders.load_ml_predictions_summary() if loaders.ml_predictions_available() else {}

    if details.get("error"):
        st.warning(f"MLflow client: {details['error']}")

    if not details.get("available") and not card.get("available"):
        st.info("Run training first: `cd 4_ml/src && python run_training.py`")
        return

    # ── Production model card ───────────────────────────────────────────────────
    section(t("alert_model_card"))
    m = card.get("metrics") or {}
    kpi_row([
        ("Registry", card.get("model_name", "—"), ATB_PRIMARY),
        ("Version", f"v{card.get('version', '?')}", ATB_SILVER),
        ("Stage", card.get("stage", "Production"), "#22c55e"),
        ("Algorithm", str(card.get("best_algorithm", "")).upper(), None),
        ("ROC AUC", f"{m.get('roc_auc', 0):.4f}" if m.get("roc_auc") else "—", ATB_PRIMARY),
        ("F1", f"{m.get('f1', 0):.4f}" if m.get("f1") else "—", None),
    ])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Precision", f"{m.get('precision', 0):.4f}" if m.get("precision") else "—")
    with c2:
        st.metric("Recall", f"{m.get('recall', 0):.4f}" if m.get("recall") else "—")
    with c3:
        st.metric("PR AUC", f"{m.get('pr_auc', 0):.4f}" if m.get("pr_auc") else "—")
    with c4:
        thr = card.get("threshold")
        st.metric("Threshold", f"{float(thr):.3f}" if thr is not None else "—")

    st.caption(
        f"**Trained:** {card.get('trained_at', '—')} · "
        f"**Run ID:** `{card.get('run_id', '—')}` · "
        f"**Tracking URI:** `{details.get('tracking_uri', '—')}`"
    )

    if ml_summary:
        section("Warehouse predictions (PFE_ML)")
        kpi_row([
            ("Scored customers", f"{ml_summary.get('total_predictions', 0):,}", ATB_PRIMARY),
            ("ML high-risk", f"{ml_summary.get('ml_high_risk', 0):,}", "#f59e0b"),
            ("Disagreements", f"{ml_summary.get('disagreements', 0):,}", "#ef4444"),
            ("Last scored", str(ml_summary.get("last_scored", "—"))[:19], None),
        ])

    # ── Registered models ───────────────────────────────────────────────────────
    section(t("mlflow_registry"))
    reg_models = details.get("registered_models") or []
    if not reg_models:
        st.caption("No registered models in MLflow yet — `register_model_to_production()` runs after training.")
    else:
        for rm in reg_models:
            st.markdown(f"#### `{rm['name']}`")
            if rm["versions"]:
                vdf = pd.DataFrame(rm["versions"])
                vdf["created"] = vdf["created"].apply(_fmt_ts)
                st.dataframe(vdf, use_container_width=True, hide_index=True)
            else:
                st.caption("No versions registered.")

    prod = details.get("production_versions") or []
    if prod:
        st.success(
            f"**Production:** {prod[0]['model']} v{prod[0]['version']} "
            f"(run `{prod[0]['run_id'][:12]}…`)"
        )

    # ── Recent experiment runs ──────────────────────────────────────────────────
    section(t("mlflow_runs"))
    runs = details.get("recent_runs") or []
    if not runs:
        st.caption("No experiment runs found in `4_ml/mlruns/`.")
    else:
        rows = []
        for r in runs:
            metrics = r.get("metrics") or {}
            rows.append({
                "run_name": r.get("name"),
                "run_id": r.get("run_id"),
                "status": r.get("status"),
                "started": _fmt_ts(r.get("start")),
                "roc_auc": metrics.get("roc_auc"),
                "f1": metrics.get("f1"),
                "precision": metrics.get("precision"),
                "recall": metrics.get("recall"),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    section(t("mlflow_pipeline"))
    st.markdown(
        """
        | Step | What MLflow records |
        |------|----------------------|
        | **Train** | Parameters (XGBoost hyperparams), metrics (ROC AUC, F1, precision, recall) |
        | **Log** | Model artifact saved under `4_ml/mlruns/` |
        | **Register** | `mlflow.register_model()` → `atb_customer_risk_classifier` |
        | **Promote** | Version moved to **Production** stage |
        | **Score** | `run_predictions.py` writes to `PFE_ML.ml_customer_predictions` |
        """
    )
