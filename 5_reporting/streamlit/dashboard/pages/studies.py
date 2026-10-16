"""Cross-dimensional BI studies — joins across facts and dimensions."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, heatmap_chart, horizontal_bar, line_chart
from dashboard.config import RISK_COLORS
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header(
        "BI Studies & Cross-Analytics",
        "Multi-table analysis joining fact_customer_risk, fact_account, and dimension tables — "
        "designed for portfolio insights and PFE presentation.",
    )

    insight(
        "<strong>How to read this page:</strong> Each study joins two or more warehouse tables. "
        "Use 🤖 AI panels for quick explanations during your presentation."
    )

    section("Study 1 — Compliance profile vs risk score")
    try:
        compliance = loaders.load_compliance_flags_vs_risk()
        kyc = loaders.load_kyc_vs_risk()
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(
                compliance, x="compliance_category", y="avg_risk_score", color="high_risk_count",
                title="Average risk score by compliance category", color_continuous_scale="Reds",
            )
            plot_with_ai(
                apply_theme(fig, 400), "study1_compliance",
                "Compliance category vs risk",
                "PEP, compliance flags, and KYC gaps vs average global risk score.",
                compliance,
            )
        with c2:
            fig = px.bar(
                kyc, x="risk_tier", y="customer_count", color="kyc_status", barmode="group",
                title="Risk tier distribution by KYC status",
                color_discrete_sequence=["#22c55e", "#ef4444"],
            )
            plot_with_ai(
                apply_theme(fig, 400), "study1_kyc",
                "KYC status vs risk tier",
                "How KYC-complete vs incomplete customers distribute across risk tiers.",
                kyc,
            )
    except Exception as exc:
        st.warning(f"Compliance study unavailable: {exc}")

    section("Study 2 — Income level vs financial risk")
    try:
        salary = loaders.load_salary_vs_risk_sample(5000)
        fig = px.scatter(
            salary, x="monthly_salary", y="global_risk_score", color="risk_tier",
            color_discrete_map=RISK_COLORS, opacity=0.45,
            title="Monthly salary vs global risk score (sample)",
        )
        plot_with_ai(
            apply_theme(fig, 420), "study2_salary",
            "Salary vs risk scatter",
            "Financial fragility pillar links low/missing salary to higher scores.",
            salary.head(800),
        )
    except Exception as exc:
        st.warning(f"Salary study unavailable: {exc}")

    section("Study 3 — Sector × branch area risk heatmap")
    try:
        heat = loaders.load_risk_heatmap_sector_dao(8, 8)
        plot_with_ai(
            heatmap_chart(heat, x="dao_area", y="sector_description", z="avg_risk_score",
                          title="Average global risk score (top sectors × top areas)"),
            "study3_heatmap",
            "Sector × area heatmap",
            "Cross-tab of sector and branch area average risk — red = hot spots.",
            heat,
        )
    except Exception as exc:
        st.warning(f"Heatmap unavailable: {exc}")

    section("Study 4 — Account portfolio growth (opening year)")
    try:
        openings = loaders.load_accounts_by_opening_year()
        c3, c4 = st.columns(2)
        with c3:
            plot_with_ai(
                line_chart(openings, x="opening_year", y="account_count", title="New accounts by opening year"),
                "study4_openings",
                "Accounts by opening year",
                "Portfolio growth curve from dim_date + fact_account.",
                openings,
            )
        with c4:
            fig = px.bar(
                openings.tail(15), x="opening_year", y="overdraft_count",
                title="Overdraft accounts by opening year", color="overdraft_count",
                color_continuous_scale="Oranges",
            )
            plot_with_ai(
                apply_theme(fig, 380), "study4_overdraft_year",
                "Overdraft by vintage year",
                "Do newer account cohorts have more overdrafts?",
                openings.tail(15),
            )
    except Exception as exc:
        st.warning(f"Opening year study unavailable: {exc}")

    section("Study 5 — Industry and target segment risk ranking")
    try:
        industry = loaders.load_industry_risk(15)
        target = loaders.load_target_risk()
        c5, c6 = st.columns(2)
        with c5:
            plot_with_ai(
                horizontal_bar(industry, y="industry_description", x="avg_risk_score",
                               color="high_risk_count", title="Highest-risk industries", top_n=12),
                "study5_industry",
                "Industry risk ranking",
                "Industries with highest average risk (min 50 customers).",
                industry,
            )
        with c6:
            fig = px.scatter(
                target, x="avg_balance", y="avg_risk_score", size="customer_count",
                color="target_description", title="Target segment: balance vs risk",
            )
            plot_with_ai(
                apply_theme(fig, 420), "study5_target",
                "Target segment scatter",
                "Customer segments positioned by wallet size and risk.",
                target,
            )
    except Exception as exc:
        st.warning(f"Segment study unavailable: {exc}")

    section("Study 6 — Demographics and sector portfolio")
    try:
        gender = loaders.load_risk_by_gender()
        sector = loaders.load_sector_account_risk_join(12)
        c7, c8 = st.columns(2)
        with c7:
            fig = px.bar(gender, x="gender", y="avg_risk_score", color="high_risk_count",
                         title="Average risk score by gender")
            plot_with_ai(apply_theme(fig, 380), "study6_gender", "Gender vs risk", "Demographic risk comparison.", gender)
        with c8:
            fig = px.scatter(
                sector, x="total_balance", y="avg_risk_score", size="accounts",
                hover_name="sector_description", color="avg_risk_score",
                color_continuous_scale="RdYlGn_r", title="Sector: balance vs risk",
            )
            plot_with_ai(
                apply_theme(fig, 400), "study6_sector",
                "Sector balance vs risk",
                "Joint view of sector exposure and risk from facts + dimensions.",
                sector,
            )
    except Exception as exc:
        st.warning(f"Demographics study unavailable: {exc}")

    section("Study 7 — Multi-currency exposure")
    try:
        fx = loaders.load_currency_exposure()
        total_bal = fx["total_balance"].sum()
        kpi_row([
            ("Currencies active", f"{len(fx)}", None),
            ("Total balance (all FX)", f"{total_bal:,.0f} DT", "#34d399"),
            ("Highest overdraft %", f"{fx['overdraft_pct'].max():.1f}%", "#ef4444"),
        ])
        fig = px.treemap(
            fx, path=["currency_name"], values="total_balance", color="overdraft_pct",
            color_continuous_scale="RdYlGn_r", title="Balance treemap by currency",
        )
        plot_with_ai(
            apply_theme(fig, 440), "study7_fx",
            "Currency treemap",
            "FX exposure and overdraft rate per currency.",
            fx,
        )
    except Exception as exc:
        st.warning(f"Currency study unavailable: {exc}")

    section("Study 8 — Employment & overdraft joint analysis")
    try:
        emp = loaders.load_risk_by_employment(10)
        od = loaders.load_overdraft_risk_cross()
        c9, c10 = st.columns(2)
        with c9:
            fig = px.bar(emp, x="employment_status", y="customer_count", color="avg_risk_score",
                         title="Customers by employment (color = avg risk)")
            plot_with_ai(
                apply_theme(fig, 380), "study8_employment",
                "Employment volume vs risk",
                "Which employment types dominate the portfolio and their risk level.",
                emp,
            )
        with c10:
            fig = px.area(
                od, x="risk_tier", y="customer_count", color="overdraft_status",
                title="Risk tier area chart by overdraft status",
            )
            plot_with_ai(
                apply_theme(fig, 380), "study8_overdraft_area",
                "Overdraft area chart",
                "Stacked view of overdraft vs clean accounts across tiers.",
                od,
            )
    except Exception as exc:
        st.warning(f"Study 8 unavailable: {exc}")
