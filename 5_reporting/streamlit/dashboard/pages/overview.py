"""Executive overview — cross-warehouse KPIs, PDF export, drill-down."""
from __future__ import annotations

import plotly.express as px
import pandas as pd
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, risk_pie
from dashboard.i18n import lang, t
from dashboard.pdf_report import build_executive_pdf
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def _render_drilldown(risk_df: pd.DataFrame, sector_df: pd.DataFrame) -> None:
    section(t("drill_down"))
    st.caption(t("drill_hint"))

    tiers = [t("all_tiers")] + sorted(risk_df["risk_tier"].dropna().unique().tolist())
    sectors = [t("all_sectors")] + sector_df["sector_description"].head(30).tolist()

    c1, c2 = st.columns(2)
    with c1:
        tier_pick = st.selectbox(t("select_risk_tier"), tiers, key="drill_tier")
    with c2:
        sector_pick = st.selectbox(t("select_sector"), sectors, key="drill_sector")

    tier_filter = None if tier_pick == t("all_tiers") else tier_pick
    sector_filter = None if sector_pick == t("all_sectors") else sector_pick

    # Interactive chart — click slice to set tier filter
    fig = risk_pie(risk_df, "risk_tier", "customer_count")
    fig.update_layout(title="Click a slice to filter by risk tier")
    event = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        selection_mode="points",
        key="overview_drill_chart",
    )
    if event and getattr(event, "selection", None) and event.selection.points:
        for pt in event.selection.points:
            label = pt.get("label") or pt.get("customdata")
            if label:
                tier_filter = str(label)
                st.session_state["drill_tier"] = tier_filter
                break
    if st.session_state.get("drill_tier"):
        tier_filter = st.session_state["drill_tier"]

    customers = loaders.load_drilldown_customers(tier_filter, sector_filter, 200)
    st.markdown(f"**{len(customers):,}** {t('customers_found')}")
    st.dataframe(customers, use_container_width=True, height=360)
    st.download_button(
        t("download_csv"),
        customers.to_csv(index=False).encode("utf-8"),
        file_name="drilldown_customers.csv",
        mime="text/csv",
    )


def render() -> None:
    page_header(t("overview_title"), t("overview_sub"))

    try:
        kpis = loaders.load_executive_kpis()
        overview = loaders.load_warehouse_overview()
        freshness = loaders.load_etl_freshness()
        risk = loaders.load_risk_tier_summary()
        sector = loaders.load_accounts_by_sector(15)
    except Exception as exc:
        st.error(f"Could not load overview data: {exc}")
        return

    col_pdf, _ = st.columns([1, 3])
    with col_pdf:
        if st.button(t("export_pdf"), type="primary", use_container_width=True):
            risk_rows = risk.to_dict("records")
            fresh_display = {
                t("last_load"): freshness.get("last_account_load", "—"),
                t("scoring_date"): freshness.get("last_scoring_date", "—"),
            }
            st.session_state["exec_pdf"] = build_executive_pdf(
                kpis, overview, risk_rows, fresh_display, lang()
            )
        if st.session_state.get("exec_pdf"):
            st.download_button(
                "Save PDF" if lang() == "en" else "Enregistrer PDF",
                st.session_state["exec_pdf"],
                file_name=f"ATB_Executive_{pd.Timestamp.now():strftime('%Y%m%d')}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

    kpi_row([
        (t("customers"), f"{kpis['customers']:,.0f}", None),
        (t("accounts"), f"{kpis['accounts']:,.0f}", None),
        (t("total_balance"), f"{kpis['total_balance']:,.0f}", "#34d399"),
        (t("avg_risk"), f"{kpis['avg_risk']:.1f}", "#f59e0b"),
        (t("high_risk_pct"), f"{kpis['high_risk_pct']:.1f}%", "#ef4444"),
        (t("overdraft_pct"), f"{kpis['overdraft_pct']:.1f}%", "#f472b6"),
    ])

    insight(
        "Live SQL Server · PFE_DWH · Use <strong>Drill-down</strong> below to explore customers. "
        "Switch language in sidebar (FR/EN)."
        if lang() == "en"
        else "SQL Server en direct · PFE_DWH · Utilisez l'<strong>exploration</strong> ci-dessous. "
        "Changez la langue dans la barre latérale."
    )

    section("Risk snapshot")
    c3, c4 = st.columns(2)
    with c3:
        plot_with_ai(risk_pie(risk, "risk_tier", "customer_count"),
                     "overview_risk_pie", "Risk mix", "Portfolio risk tiers.", risk)
    with c4:
        fig = px.bar(risk, x="risk_tier",
                     y=["avg_compliance", "avg_fragility", "avg_behavioral"],
                     barmode="group", title="Three-pillar scores by tier")
        plot_with_ai(apply_theme(fig, 400), "overview_pillars", "Pillars", "Compliance, fragility, behavioral.", risk)

    section("Account snapshot")
    currency = loaders.load_accounts_by_currency()
    c5, c6 = st.columns(2)
    with c5:
        fig = px.treemap(currency, path=["currency_name"], values="total_balance",
                         color="account_count", color_continuous_scale="Blues", title="Balance by currency")
        plot_with_ai(apply_theme(fig, 400), "overview_fx", "FX treemap", "Currency exposure.", currency)
    with c6:
        plot_with_ai(bar_chart(sector, x="sector_description", y="total_balance",
                               title="Top sectors by balance", top_n=10),
                     "overview_sector", "Top sectors", "Largest sector balances.", sector)

    _render_drilldown(risk, sector)
