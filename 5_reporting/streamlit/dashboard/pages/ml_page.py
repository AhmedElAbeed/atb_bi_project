"""ML model — live demo, batch scoring, alerts, SHAP, full predictor, XGBoost metrics."""
from __future__ import annotations

import streamlit as st

from dashboard import loaders, ml_service
from dashboard.charts import apply_theme, bar_chart
from dashboard.db import sql_query
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def _lookup_customer_frame(customer_id: int):
    """Lookup by fact customer_id with a fallback through dim_customer source_record_id."""
    q_primary = f"""
    SELECT TOP 1 fcr.*, dc.gender, dc.marital_status, dc.employment_status,
           dc.residence_status, dc.monthly_salary, dc.number_of_dependents,
           dc.dao_area, dc.is_kyc_complete, dc.is_pep, dc.is_compliance_flagged,
           dc.has_compliance_decision, dc.segment_code, dc.client_nature_code AS client_nature_flag,
           dc.legal_capacity_flag, dc.beneficial_owner_flag,
           dc.last_kyc_review_date, dc.next_kyc_review_date
    FROM PFE_DWH.fact_customer_risk fcr
    JOIN PFE_DWH.dim_customer dc ON fcr.customer_sk = dc.customer_sk
    WHERE fcr.customer_id = {customer_id}
    ORDER BY fcr.scoring_date DESC
    """
    row = sql_query(q_primary)
    if not row.empty:
        return row

    # Some environments use source_record_id formatting instead of raw numeric customer_id.
    q_fallback = f"""
    SELECT TOP 1 fcr.*, dc.gender, dc.marital_status, dc.employment_status,
           dc.residence_status, dc.monthly_salary, dc.number_of_dependents,
           dc.dao_area, dc.is_kyc_complete, dc.is_pep, dc.is_compliance_flagged,
           dc.has_compliance_decision, dc.segment_code, dc.client_nature_code AS client_nature_flag,
           dc.legal_capacity_flag, dc.beneficial_owner_flag,
           dc.last_kyc_review_date, dc.next_kyc_review_date
    FROM PFE_DWH.fact_customer_risk fcr
    JOIN PFE_DWH.dim_customer dc ON fcr.customer_sk = dc.customer_sk
    WHERE dc.source_record_id = '{customer_id}'
       OR dc.source_record_id = RIGHT('0000000000' + CAST({customer_id} AS VARCHAR(10)), 10)
    ORDER BY fcr.scoring_date DESC
    """
    return sql_query(q_fallback)


def _default_customer_id() -> int:
    """Return a real customer id from DWH to avoid dead default values."""
    try:
        row = sql_query(
            """
            SELECT TOP 1 customer_id
            FROM PFE_DWH.fact_customer_risk
            WHERE customer_id IS NOT NULL
            ORDER BY scoring_date DESC, customer_id DESC
            """
        )
        if not row.empty:
            return int(row.iloc[0]["customer_id"])
    except Exception:
        pass
    return 100001


def _rules_vs_ml_banner() -> None:
    st.markdown(
        """
        ### Two layers working together

        | | **Rules engine (DWH)** | **ML (XGBoost)** |
        |--|------------------------|------------------|
        | **What** | 3-pillar points → `global_risk_score` | Predicts P(high risk) from customer attributes |
        | **Role** | Official auditable score | Screening assistant + disagreement alerts |
        | **When** | Always computed in ETL | On-demand in this dashboard |
        """
    )


def _threshold_control(default: float) -> float:
    if "ml_threshold" not in st.session_state:
        st.session_state.ml_threshold = default
    threshold = st.slider(
        "ML decision threshold (what-if — no retraining)",
        0.05, 0.95, float(st.session_state.ml_threshold), 0.05,
        help="P(high risk) ≥ threshold → ML flags HIGH",
    )
    st.session_state.ml_threshold = threshold
    st.caption("Rules engine threshold: **global_risk_score ≥ 50** (fixed)")
    return threshold


def _render_side_by_side(scored_row, threshold: float) -> None:
    rules_score = float(scored_row.get("global_risk_score", 0))
    rules_tier = str(scored_row.get("risk_tier", "—"))
    rules_high = rules_score >= 50
    ml_prob = float(scored_row.get("ml_risk_probability", 0))
    ml_high = ml_prob >= threshold
    agree = rules_high == ml_high
    cols = st.columns(3)
    for col, (title, val, sub, color) in zip(cols, [
        ("RULES ENGINE", f"{rules_score:.1f}", f"Tier: {rules_tier} · {'HIGH' if rules_high else 'LOW'}", "#ef4444" if rules_high else "#22c55e"),
        ("XGBoost ML", f"{ml_prob:.1%}", f"{'HIGH RISK' if ml_high else 'LOW RISK'} · thr {threshold:.2f}", "#ef4444" if ml_high else "#22c55e"),
        ("AGREEMENT", "✓ MATCH" if agree else "⚠ REVIEW", "Rules and ML agree" if agree else "Manual review suggested", "#22c55e" if agree else "#f59e0b"),
    ]):
        with col:
            st.markdown(
                f'<div style="padding:1rem;border-radius:12px;border:2px solid {color};'
                f'background:rgba(15,23,42,0.9);text-align:center;">'
                f'<div style="color:#94a3b8;font-size:0.72rem;">{title}</div>'
                f'<div style="font-size:2rem;font-weight:800;color:{color};">{val}</div>'
                f'<div style="color:#cbd5e1;font-size:0.85rem;">{sub}</div></div>',
                unsafe_allow_html=True,
            )


def _render_live_demo(model, threshold: float) -> None:
    section("Live demo — score customer by ID")
    if "ml_live_customer_default" not in st.session_state:
        st.session_state["ml_live_customer_default"] = _default_customer_id()
    lookup_id = st.number_input(
        "Customer ID",
        min_value=1,
        value=int(st.session_state["ml_live_customer_default"]),
        step=1,
    )
    if st.button("Load & score from database", type="primary"):
        row = _lookup_customer_frame(int(lookup_id))
        if row.empty:
            st.warning(
                "Customer not found with this ID in `fact_customer_risk`. "
                "Try another ID from your DWH population."
            )
        else:
            scored = ml_service.score_frame(model, row, threshold).iloc[0]
            st.session_state["last_scored_customer"] = scored
            _render_side_by_side(scored, threshold)
            st.progress(min(float(scored["ml_risk_probability"]), 1.0))
            pillars = {k: scored.get(k) for k in [
                "compliance_risk_index", "financial_fragility_score", "behavioral_risk_score",
                "is_kyc_complete", "is_pep", "total_working_balance", "negative_balance_account_count",
            ] if k in scored.index}
            st.json(pillars)


def _render_batch_and_alerts(model, threshold: float) -> None:
    section("Batch scoring + ML vs rules agreement")
    sample_n = st.slider("Customers to score", 500, 8000, 3000, step=500, key="batch_n")
    if st.button("Run batch scoring", type="primary", key="batch_btn"):
        with st.spinner("Scoring…"):
            frame = ml_service.load_scoring_frame()
            if len(frame) > sample_n:
                frame = frame.sample(sample_n, random_state=42)
            st.session_state["batch_scored"] = ml_service.score_frame(model, frame, threshold)

    scored = st.session_state.get("batch_scored")
    if scored is None:
        st.info("Click **Run batch scoring** to compute agreement and alerts.")
        return

    stats = ml_service.agreement_stats(scored, threshold)
    kpi_row([
        ("Scored", f"{len(scored):,}", None),
        ("Agreement %", f"{stats.get('agreement_pct', 0):.1f}%", "#38bdf8"),
        ("ML high / Rules low", f"{stats.get('ml_high_rules_low', 0):,}", "#ef4444"),
        ("Rules high / ML low", f"{stats.get('rules_high_ml_low', 0):,}", "#f59e0b"),
    ])

    plot_with_ai(ml_service.plot_probability_distribution(scored), "ml_prob_dist",
                 "ML probability distribution", "P(high risk) across scored customers.",
                 scored[["ml_risk_probability"]].head(500))
    fig = ml_service.plot_ml_vs_rules(scored)
    if fig.data:
        plot_with_ai(fig, "ml_vs_rules", "ML vs rules scatter",
                     "X = DWH score, Y = ML probability.", scored[["global_risk_score", "ml_risk_probability"]].head(500))

    section("Alert list — ML HIGH but tier LOW/MEDIUM")
    alerts = ml_service.build_alert_list(scored, threshold)
    insight(f"<strong>{len(alerts):,} alert(s)</strong> — ML flags high risk but rules tier is still LOW/MEDIUM.")
    if alerts.empty:
        st.success("No disagreement alerts at this threshold.")
    else:
        st.dataframe(alerts.head(100), use_container_width=True, height=360)
        st.download_button("Download alerts CSV", alerts.to_csv(index=False).encode("utf-8"),
                           "ml_disagreement_alerts.csv", "text/csv")


def _render_curves_shap(model, threshold: float) -> None:
    section("ROC & PR curves")
    try:
        curves = ml_service.compute_curve_data(model)
        from sklearn.metrics import confusion_matrix
        y_pred = (curves["y_proba"] >= threshold).astype(int)
        cm = confusion_matrix(curves["y_true"], y_pred)
        plot_with_ai(ml_service.plot_roc_pr(curves), "ml_roc_pr", "ROC & PR curves",
                     "Model discrimination — higher AUC = better.", None)
        c1, c2 = st.columns(2)
        with c1:
            plot_with_ai(ml_service.plot_confusion_matrix(cm), "ml_cm", "Confusion matrix",
                         f"At threshold {threshold:.2f}.", None)
        with c2:
            st.metric("ROC AUC", f"{curves['roc_auc']:.4f}")
            st.metric("PR AUC", f"{curves['pr_auc']:.4f}")
    except Exception as exc:
        st.warning(str(exc))

    section("SHAP — feature importance (top drivers)")
    images = ml_service.list_output_images()
    if images.get("shap"):
        st.image(str(images["shap"]), caption="SHAP summary — KYC, overdraft, salary", use_container_width=True)
    else:
        st.info("Generate via `4_ml/notebooks/04_shap_explainability.ipynb`")

    section("Threshold what-if (precision / recall / F1)")
    frame = ml_service.load_scoring_frame(2000)
    thresholds = [round(x, 2) for x in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]]
    plot_with_ai(ml_service.plot_threshold_sensitivity(frame, model, thresholds), "ml_thr",
                 "Threshold sensitivity", "Move threshold without retraining.", None)


def _render_model_performance(summary: dict, model_info: str) -> None:
    best = summary.get("best_model", "xgboost")
    metrics = (summary.get("results") or {}).get(best, {}).get("metrics", {})
    kpi_row([
        ("Model", best.upper(), "#38bdf8"),
        ("ROC AUC", f"{metrics.get('roc_auc', 0):.4f}", "#34d399"),
        ("F1", f"{metrics.get('f1', 0):.4f}", "#f59e0b"),
        ("Precision", f"{metrics.get('precision', 0):.4f}", None),
        ("Recall", f"{metrics.get('recall', 0):.4f}", None),
        ("PR AUC", f"{metrics.get('pr_auc', 0):.4f}", None),
    ])
    st.success(f"✓ Model loaded — {model_info}")

    metrics_df = ml_service.metrics_comparison_df()
    if not metrics_df.empty:
        section("XGBoost vs Random Forest (test set)")
        for metric in ["roc_auc", "f1", "precision", "recall", "pr_auc"]:
            subset = metrics_df[metrics_df["metric"] == metric]
            if not subset.empty:
                plot_with_ai(bar_chart(subset, x="model", y="value", title=metric.upper(), color="model"),
                             f"ml_{metric}", f"{metric} comparison", f"Test {metric}.", subset)

    section("Training & validation charts (4_ml/outputs)")
    images = ml_service.list_output_images()
    if images:
        cols = st.columns(2)
        i = 0
        for key, path in images.items():
            with cols[i % 2]:
                st.image(str(path), caption=path.name, use_container_width=True)
            i += 1
    else:
        st.warning("No training charts found. Run: cd 4_ml && python src/run_training.py")


def _load_lookup_options() -> dict:
    opts: dict = {
        "gender": ["M", "F", "UNKNOWN"],
        "marital_status": ["S", "M", "D", "W", "UNKNOWN"],
        "employment_status": ["EMPLOYED", "SELF_EMPLOYED", "UNEMPLOYED", "RETIRED", "STUDENT", "UNKNOWN"],
        "residence_status": ["RESIDENT", "NON_RESIDENT", "UNKNOWN"],
        "client_nature_flag": ["Y", "N", "UNKNOWN"],
        "legal_capacity_flag": ["Y", "N", "UNKNOWN"],
        "beneficial_owner_flag": ["Y", "N", "UNKNOWN"],
    }
    try:
        sector = loaders.load_dim_sector()
        opts["sector_code"] = sector["sector_code"].astype(int).tolist()
    except Exception:
        opts["sector_code"] = [-1, 1, 2, 3]
    try:
        industry = loaders.load_dim_industry_all()
        opts["industry_code"] = industry["industry_code"].astype(int).head(100).tolist()
    except Exception:
        opts["industry_code"] = [-1, 1]
    try:
        target = loaders.load_dim_target()
        opts["target_code"] = target["target_code"].astype(int).tolist()
    except Exception:
        opts["target_code"] = [-1, 1]
    try:
        dao = loaders.load_dim_dao()
        opts["dao_area"] = sorted(dao["dao_area"].dropna().unique().tolist())
    except Exception:
        opts["dao_area"] = ["UNKNOWN"]
    return opts


def _render_full_predictor(model, threshold: float) -> None:
    """Full manual form — same as original (all features for accurate ML scoring)."""
    section("Interactive risk predictor — full profile form")
    st.caption("Fill all attributes used at training time. More fields = more accurate than the short form.")

    opts = _load_lookup_options()

    with st.form("ml_predict_form"):
        st.markdown("**Customer profile**")
        c1, c2, c3 = st.columns(3)
        with c1:
            gender = st.selectbox("Gender", opts["gender"])
            employment_status = st.selectbox("Employment", opts["employment_status"])
            monthly_salary = st.number_input("Monthly salary (DT)", min_value=0.0, value=2500.0, step=100.0)
        with c2:
            marital_status = st.selectbox("Marital status", opts["marital_status"])
            residence_status = st.selectbox("Residence status", opts["residence_status"])
            number_of_dependents = st.number_input("Dependents", 0, 20, 0)
        with c3:
            customer_tenure_days = st.number_input("Customer tenure (days)", 0, 15000, 1200, 30)
            days_since_kyc = st.number_input("Days since last KYC review", 0, 3000, 180, 30)
            segment_code = st.number_input("Segment code", value=-1, step=1)

        st.markdown("**Accounts & balances**")
        a1, a2, a3 = st.columns(3)
        with a1:
            account_count = st.number_input("Number of accounts", 0, 50, 2)
            total_working_balance = st.number_input("Total working balance (DT)", value=15000.0, step=500.0)
        with a2:
            avg_working_balance = st.number_input("Average working balance (DT)", value=7500.0, step=500.0)
            negative_balance_account_count = st.number_input("Accounts in overdraft", 0, 20, 0)
        with a3:
            account_officer_id = st.number_input("Account officer ID", value=-1, step=1)

        st.markdown("**Segmentation & compliance**")
        s1, s2, s3 = st.columns(3)
        with s1:
            sector_code = st.selectbox("Sector code", opts["sector_code"])
            industry_code = st.selectbox("Industry code", opts["industry_code"])
            target_code = st.selectbox("Target segment code", opts["target_code"])
        with s2:
            dao_area = st.selectbox("Branch area (DAO)", opts["dao_area"])
            is_kyc_complete = st.checkbox("KYC complete", value=True)
            is_pep = st.checkbox("PEP (politically exposed)", value=False)
        with s3:
            is_compliance_flagged = st.checkbox("Compliance flagged", value=False)
            has_compliance_decision = st.checkbox("Has compliance decision", value=False)
            client_nature_flag = st.selectbox("Client nature", opts["client_nature_flag"])
            legal_capacity_flag = st.selectbox("Legal capacity", opts["legal_capacity_flag"])
            beneficial_owner_flag = st.selectbox("Beneficial owner", opts["beneficial_owner_flag"])

        submitted = st.form_submit_button("Predict risk with XGBoost", type="primary")

    if submitted:
        inputs = {
            "gender": gender, "marital_status": marital_status,
            "employment_status": employment_status, "residence_status": residence_status,
            "monthly_salary": monthly_salary, "number_of_dependents": number_of_dependents,
            "customer_tenure_days": customer_tenure_days, "account_count": account_count,
            "total_working_balance": total_working_balance, "avg_working_balance": avg_working_balance,
            "negative_balance_account_count": negative_balance_account_count,
            "account_officer_id": account_officer_id, "sector_code": sector_code,
            "industry_code": industry_code, "target_code": target_code, "segment_code": segment_code,
            "dao_area": dao_area, "is_kyc_complete": int(is_kyc_complete), "is_pep": int(is_pep),
            "is_compliance_flagged": int(is_compliance_flagged),
            "has_compliance_decision": int(has_compliance_decision),
            "client_nature_flag": client_nature_flag, "legal_capacity_flag": legal_capacity_flag,
            "beneficial_owner_flag": beneficial_owner_flag,
            "days_since_last_kyc_review": float(days_since_kyc),
        }
        try:
            result = ml_service.predict_single(model, inputs, threshold)
            color = "#ef4444" if result["predicted_class"] else "#22c55e"
            st.markdown(
                f'<div style="padding:1.2rem;border-radius:12px;border:2px solid {color};text-align:center;">'
                f'<div style="font-size:1.6rem;font-weight:800;color:{color};">{result["label"]}</div>'
                f'<div style="font-size:2.2rem;font-weight:800;">P(high risk) = {result["probability"]:.1%}</div>'
                f'<div style="color:#94a3b8;">Threshold = {result["threshold"]:.3f} · '
                f'Confidence ≈ {result["confidence_pct"]:.1f}%</div></div>',
                unsafe_allow_html=True,
            )
            st.progress(min(result["probability"], 1.0))
            insight(
                "This prediction uses <strong>only customer attributes</strong> — not the rules score. "
                "Compare with a real customer via the <strong>Live demo</strong> tab for side-by-side rules vs ML."
            )
        except Exception as exc:
            st.error(f"Prediction failed: {exc}")


def render() -> None:
    page_header("ML Risk Intelligence", "XGBoost · live demo · batch alerts · SHAP · full predictor")

    _rules_vs_ml_banner()

    summary = ml_service.load_training_summary()
    model, model_info, _ = ml_service.load_model()
    if model is None:
        st.error(model_info)
        st.code("cd 4_ml\npython src/run_training.py", language="bash")
        return

    default_thr = ml_service.get_model_threshold(summary.get("best_model") if summary else None)
    threshold = _threshold_control(default_thr)

    tabs = st.tabs(["Live demo", "Batch & alerts", "Curves & SHAP", "XGBoost metrics", "Full predictor"])

    with tabs[0]:
        _render_live_demo(model, threshold)
    with tabs[1]:
        _render_batch_and_alerts(model, threshold)
    with tabs[2]:
        _render_curves_shap(model, threshold)
    with tabs[3]:
        if summary:
            _render_model_performance(summary, model_info)
        else:
            st.info(model_info)
            _render_model_performance({"best_model": "xgboost", "results": {}}, model_info)
    with tabs[4]:
        _render_full_predictor(model, threshold)
