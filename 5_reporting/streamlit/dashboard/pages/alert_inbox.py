"""Alert inbox — ML vs rules, PEP + balance, branch heatmap, DAO treemap."""
from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard import loaders, ml_service
from dashboard.charts import apply_theme, heatmap_chart, safe_size_column
from dashboard.config import RISK_COLORS, RISK_ORDER
from dashboard.i18n import lang, t
from dashboard.pdf_report import build_alert_pack_pdf
from dashboard.config import ATB_PRIMARY
from dashboard.auth import can_export
from dashboard.ui import action_bar, insight, kpi_row, page_header, plot_with_ai, section

TUNISIA_REGION_COORDS: dict[str, tuple[float, float]] = {
    "tunis": (36.8065, 10.1815),
    "ariana": (36.8665, 10.1647),
    "ben arous": (36.7533, 10.2283),
    "manouba": (36.8081, 10.0972),
    "nabeul": (36.4561, 10.7376),
    "zaghouan": (36.4029, 10.1429),
    "bizerte": (37.2744, 9.8739),
    "beja": (36.7256, 9.1817),
    "jendouba": (36.5011, 8.7803),
    "kef": (36.1822, 8.7147),
    "siliana": (36.0849, 9.3708),
    "sousse": (35.8254, 10.6360),
    "monastir": (35.7643, 10.8113),
    "mahdia": (35.5047, 11.0622),
    "sfax": (34.7406, 10.7603),
    "kairouan": (35.6781, 10.0963),
    "kasserine": (35.1676, 8.8365),
    "sidi bouzid": (35.0382, 9.4849),
    "gabes": (33.8815, 10.0982),
    "medenine": (33.3549, 10.5055),
    "tataouine": (32.9297, 10.4518),
    "gafsa": (34.4250, 8.7842),
    "tozeur": (33.9197, 8.1335),
    "kebili": (33.7044, 8.9690),
}


def _match_coords(area: str) -> tuple[float | None, float | None]:
    key = str(area).strip().lower()
    for name, coords in TUNISIA_REGION_COORDS.items():
        if name in key or key in name:
            return coords
    return None, None


def _render_tunisia_map(geo: "pd.DataFrame") -> go.Figure | None:
    import pandas as pd

    mapped = geo.copy()
    mapped[["lat", "lon"]] = mapped["dao_area"].apply(
        lambda a: pd.Series(_match_coords(a))
    )
    mapped = mapped.dropna(subset=["lat", "lon"])
    if mapped.empty:
        return None

    fig = px.scatter_geo(
        mapped,
        lat="lat",
        lon="lon",
        size="customer_count",
        color="avg_risk_score",
        hover_name="dao_area",
        color_continuous_scale="RdYlGn_r",
        title="Tunisia branch areas — avg risk (DAO proxy map)",
        scope="africa",
        projection="mercator",
    )
    fig.update_geos(
        center=dict(lat=34.0, lon=9.5),
        lataxis_range=[30.0, 37.5],
        lonaxis_range=[7.0, 12.0],
        showcountries=True,
        countrycolor="#334155",
        bgcolor="rgba(0,0,0,0)",
    )
    return apply_theme(fig, 480)


def _render_model_card(model_card: dict, ml_summary: dict) -> None:
    section(t("alert_model_card"))
    if not model_card.get("available"):
        st.info("Train the model to populate the MLflow card: `cd 4_ml/src && python run_training.py`")
        return

    m = model_card.get("metrics") or {}
    cols = st.columns(6)
    pills = [
        ("Registry", model_card.get("model_name", "—")),
        ("Version", f"v{model_card.get('version', '?')}"),
        ("Stage", model_card.get("stage", "Production")),
        ("Algorithm", str(model_card.get("best_algorithm", "")).upper()),
        ("ROC AUC", f"{m.get('roc_auc', 0):.4f}" if m.get("roc_auc") else "—"),
        ("F1", f"{m.get('f1', 0):.4f}" if m.get("f1") else "—"),
    ]
    for col, (label, val) in zip(cols, pills):
        with col:
            st.markdown(
                f'<div class="model-card" style="padding:0.6rem;text-align:center;">'
                f'<div style="font-size:0.65rem;color:#94a3b8;text-transform:uppercase;">{label}</div>'
                f'<div style="font-size:0.95rem;font-weight:700;color:{ATB_PRIMARY};">{val}</div></div>',
                unsafe_allow_html=True,
            )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.caption(f"**Trained:** {model_card.get('trained_at', '—')}")
    with c2:
        thr = model_card.get("threshold")
        st.caption(f"**Threshold:** {float(thr):.3f}" if thr is not None else "—")
    with c3:
        st.caption(f"**Last scored:** {str(ml_summary.get('last_scored', '—'))[:19]}")
    with c4:
        st.caption(f"**Run ID:** `{str(model_card.get('run_id', '—'))[:12]}…`")


def _pep_balance_chart(pep_bal: "pd.DataFrame") -> go.Figure:
    import pandas as pd

    chart_df = pep_bal.head(15).copy()
    chart_df["total_working_balance"] = pd.to_numeric(
        chart_df["total_working_balance"], errors="coerce"
    ).fillna(0)
    chart_df["customer_label"] = chart_df["customer_id"].astype(str)
    chart_df = chart_df.sort_values("total_working_balance", ascending=True)

    fig = px.bar(
        chart_df,
        x="total_working_balance",
        y="customer_label",
        orientation="h",
        color="risk_tier",
        color_discrete_map=RISK_COLORS,
        title="Top PEP customers by balance (DT)",
        labels={
            "total_working_balance": "Balance (DT)",
            "customer_label": "Customer ID",
            "risk_tier": "Risk tier",
        },
        text=chart_df["total_working_balance"].apply(lambda v: f"{v:,.0f}"),
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(yaxis=dict(type="category"))
    return apply_theme(fig, 420)


def render() -> None:
    page_header(
        t("alert_inbox_title"),
        t("alert_inbox_sub"),
        table="PFE_ML.ml_customer_predictions + PFE_DWH",
    )

    ml_ready = loaders.ml_predictions_available()
    ml_summary = loaders.load_ml_predictions_summary() if ml_ready else {}
    model_card = ml_service.load_model_registry_card()

    kpi_row([
        ("ML predictions in DWH", f"{ml_summary.get('total_predictions', 0):,}" if ml_ready else "—", "#38bdf8"),
        ("ML ≠ rules alerts", f"{ml_summary.get('disagreements', 0):,}" if ml_ready else "—", "#ef4444"),
        ("ML high-risk", f"{ml_summary.get('ml_high_risk', 0):,}" if ml_ready else "—", "#f59e0b"),
        (
            "Model",
            f"{ml_summary.get('model_name', model_card.get('best_algorithm', '—'))} "
            f"v{ml_summary.get('model_version', model_card.get('version', '?'))}"
            if ml_ready or model_card.get("available")
            else "Not loaded",
            None,
        ),
    ])

    _render_model_card(model_card, ml_summary)

    alerts = None
    triple = None

    # ── Compliance action bar (PDF + triple-flag preview) ─────────────────────
    action_bar("Compliance actions — PDF export · triple-flag · MLflow model")
    if ml_ready:
        try:
            alerts = loaders.load_ml_disagreement_alerts(300)
            triple = loaders.load_triple_flag_alerts(100)
            ac1, ac2, ac3 = st.columns([2, 1, 1])
            with ac1:
                if alerts is not None and not alerts.empty and can_export():
                    pdf_bytes = build_alert_pack_pdf(
                        alerts.to_dict("records"),
                        triple.to_dict("records") if triple is not None and not triple.empty else [],
                        ml_summary=ml_summary,
                        model_card=model_card,
                        lang=lang(),
                    )
                    st.download_button(
                        t("export_alert_pdf"),
                        pdf_bytes,
                        file_name=f"atb_alert_pack_{ml_summary.get('disagreements', 0)}_cases.pdf",
                        mime="application/pdf",
                        type="primary",
                        use_container_width=True,
                    )
                elif not can_export():
                    st.caption(t("read_only_badge") + " — PDF export disabled.")
                else:
                    st.caption("No disagreement alerts to export.")
            with ac2:
                st.metric(
                    "Triple-flag",
                    f"{len(triple) if triple is not None else 0:,}",
                    help="PEP + high balance + ML high-risk",
                )
            with ac3:
                st.metric(
                    "ML ≠ rules",
                    f"{ml_summary.get('disagreements', 0):,}",
                    help="ML high but rules tier LOW/MEDIUM",
                )
        except Exception as exc:
            st.warning(f"Action bar: {exc}")
    else:
        insight(
            "Run the ML pipeline: "
            "<code>cd 4_ml/src && python run_training.py && python run_predictions.py</code>"
        )

    # ── 1. ML ≠ rules ─────────────────────────────────────────────────────────
    section(t("alert_ml_rules"))
    if ml_ready:
        try:
            if alerts is None:
                alerts = loaders.load_ml_disagreement_alerts(300)
            insight(
                f"<strong>{len(alerts):,}</strong> customers where ML flags high risk "
                "but the rules engine tier is still LOW/MEDIUM."
            )
            if alerts.empty:
                st.success("No ML vs rules disagreements in warehouse.")
            else:
                plot_df = alerts.copy()
                plot_df["bubble_size"] = safe_size_column(plot_df, "total_working_balance")
                fig = px.scatter(
                    plot_df,
                    x="rules_global_score",
                    y="risk_probability",
                    color="rules_risk_tier",
                    size="bubble_size",
                    size_max=36,
                    hover_data=["customer_id", "dao_area", "total_working_balance"],
                    color_discrete_map=RISK_COLORS,
                    title="ML probability vs rules score (disagreement alerts)",
                )
                plot_with_ai(
                    apply_theme(fig, 400),
                    "inbox_ml_rules",
                    "ML vs rules scatter",
                    "Points above threshold with low rules score need compliance review.",
                    plot_df.head(200),
                )
                st.dataframe(alerts, use_container_width=True, height=320)
                st.download_button(
                    t("download_csv"),
                    alerts.to_csv(index=False).encode("utf-8"),
                    "ml_disagreement_alerts.csv",
                )
        except Exception as exc:
            st.warning(f"Could not load ML alerts: {exc}")
    else:
        st.info("ML predictions table not found — train and score first.")

    # ── 2. Triple-flag ────────────────────────────────────────────────────────
    section(t("alert_triple_flag"))
    if ml_ready:
        try:
            if triple is None:
                triple = loaders.load_triple_flag_alerts(100)
            triple_relaxed = False
            if triple.empty:
                triple = loaders.load_triple_flag_demo_relaxed(12)
                triple_relaxed = not triple.empty
            insight(
                "Highest regulatory priority: <strong>PEP</strong> + "
                "<strong>balance ≥ P75</strong> + <strong>ML high-risk</strong>."
                + (
                    f"<br/><em>{t('triple_flag_relaxed')}</em>" if triple_relaxed else ""
                )
            )
            kpi_row([
                ("Triple-flag cases", f"{len(triple):,}", "#dc2626"),
                ("Avg ML probability", f"{triple['risk_probability'].mean():.1%}" if not triple.empty else "—", "#f59e0b"),
                ("With disagreement", f"{triple['disagreement_flag'].sum():,}" if not triple.empty else "0", "#a855f7"),
            ])
            if triple.empty:
                st.success(t("no_triple_flag"))
                st.caption(t("triple_flag_seed_hint"))
            else:
                st.dataframe(triple, use_container_width=True, height=280)
                if can_export():
                    st.download_button(
                        "Download triple-flag CSV",
                        triple.to_csv(index=False).encode("utf-8"),
                        "triple_flag_alerts.csv",
                    )
        except Exception as exc:
            st.warning(str(exc))
    else:
        st.info("Requires PFE_ML.ml_customer_predictions.")

    # ── 3. PEP + high balance ─────────────────────────────────────────────────
    section(t("alert_pep_balance"))
    try:
        pep_bal = loaders.load_pep_high_balance_alerts(200)
        insight(
            "PEP customers with balance at or above the 75th percentile — "
            "elevated exposure for enhanced due diligence."
        )
        kpi_row([
            ("PEP + high balance", f"{len(pep_bal):,}", "#a855f7"),
            (
                "Avg balance (alert set)",
                f"{pep_bal['total_working_balance'].mean():,.0f} DT" if not pep_bal.empty else "—",
                None,
            ),
            (
                "HIGH+ tier",
                f"{pep_bal['risk_tier'].isin(['HIGH', 'VERY_HIGH']).sum():,}" if not pep_bal.empty else "0",
                "#ef4444",
            ),
        ])
        if pep_bal.empty:
            st.success("No PEP + high-balance alerts.")
        else:
            plot_with_ai(
                _pep_balance_chart(pep_bal),
                "inbox_pep_bal",
                "PEP high balance",
                "PEP with large balances warrant enhanced monitoring.",
                pep_bal.head(50),
            )
            st.dataframe(pep_bal, use_container_width=True, height=300)
    except Exception as exc:
        st.error(str(exc))

    # ── 4. Branch heatmap ─────────────────────────────────────────────────────
    section(t("alert_branch_heatmap"))
    try:
        heat = loaders.load_branch_risk_heatmap(14)
        plot_with_ai(
            heatmap_chart(
                heat,
                x="risk_tier",
                y="dao_area",
                z="avg_risk_score",
                title="Branch area × risk tier (avg global score)",
            ),
            "inbox_branch_heat",
            "Branch risk heatmap",
            "Which branch areas concentrate HIGH/VERY_HIGH customers.",
            heat,
        )
        tier_order = [tier for tier in RISK_ORDER if tier in heat["risk_tier"].unique()]
        pivot = heat.pivot_table(
            index="dao_area", columns="risk_tier", values="customer_count", aggfunc="sum", fill_value=0
        )
        if tier_order:
            pivot = pivot.reindex(columns=[c for c in tier_order if c in pivot.columns], fill_value=0)
        fig2 = px.imshow(
            pivot,
            labels=dict(x="Risk tier", y="Branch area", color="Customers"),
            color_continuous_scale="Reds",
            title="Customer count by branch area and risk tier",
            aspect="auto",
        )
        plot_with_ai(
            apply_theme(fig2, 420),
            "inbox_branch_count",
            "Branch volume heatmap",
            "Volume heatmap complements average-score view.",
            pivot.reset_index(),
        )
    except Exception as exc:
        st.error(str(exc))

    # ── 5. Tunisia map / DAO treemap ──────────────────────────────────────────
    section(t("alert_dao_map"))
    c1, c2 = st.columns(2)
    try:
        treemap_df = loaders.load_dao_risk_treemap()
        with c1:
            fig_tree = px.treemap(
                treemap_df,
                path=["dao_area", "dao_name"],
                values="customer_count",
                color="avg_risk_score",
                color_continuous_scale="RdYlGn_r",
                title="Branch treemap by DAO (area → branch)",
            )
            plot_with_ai(
                apply_theme(fig_tree, 440),
                "inbox_dao_treemap",
                "DAO treemap",
                "Hierarchy of branch areas and individual DAOs sized by customers.",
                treemap_df.head(100),
            )
        with c2:
            geo = loaders.load_dao_geo_risk()
            fig_map = _render_tunisia_map(geo)
            if fig_map is not None:
                plot_with_ai(
                    fig_map,
                    "inbox_tunisia_map",
                    "Tunisia region map",
                    "Bubble map using dao_area centroids as geographic proxy.",
                    geo,
                )
            else:
                st.info("Using bar chart — dao_area names did not match Tunisia governorates.")
                fig_bar = px.bar(
                    geo.sort_values("avg_risk_score", ascending=False).head(12),
                    x="dao_area",
                    y="avg_risk_score",
                    color="high_risk_count",
                    title="Avg risk by branch area",
                    color_continuous_scale="Reds",
                )
                plot_with_ai(
                    apply_theme(fig_bar, 440),
                    "inbox_dao_bar",
                    "DAO area risk bar",
                    "Fallback bar chart when geo mapping is unavailable.",
                    geo,
                )
    except Exception as exc:
        st.error(str(exc))
