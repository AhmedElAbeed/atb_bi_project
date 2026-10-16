"""fact_customer_risk analytics."""
from __future__ import annotations

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, risk_pie
from dashboard.config import RISK_COLORS, RISK_ORDER
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header(
        "Customer Risk Analytics",
        "Three-pillar scoring model — compliance, financial fragility, and behavioral risk.",
        table="fact_customer_risk",
    )

    try:
        summary = loaders.load_risk_tier_summary()
        total = int(summary["customer_count"].sum())
        avg = float(summary["avg_score"].mean())
        high_pct = float(
            summary.loc[summary["risk_tier"].isin(["HIGH", "VERY_HIGH"]), "customer_count"].sum()
            / max(total, 1)
            * 100
        )
    except Exception as exc:
        st.error(f"Could not load risk data: {exc}")
        return

    kpi_row([
        ("Scored customers", f"{total:,}", None),
        ("Avg global score", f"{avg:.2f}", "#f59e0b"),
        ("High + very high", f"{high_pct:.1f}%", "#ef4444"),
        ("Risk tiers", f"{len(summary)}", None),
    ])

    insight(
        "Each row = one customer at one scoring date. The <strong>global risk score</strong> "
        "combines compliance (40%), financial fragility (35%), and behavioral (25%) pillars. "
        "Click 🤖 beside charts for AI explanations."
    )

    section("Risk distribution")
    c1, c2 = st.columns(2)
    with c1:
        plot_with_ai(
            risk_pie(summary, "risk_tier", "customer_count"),
            "risk_tier_pie",
            "Risk tier distribution",
            "Share of customers in LOW, MEDIUM, HIGH, VERY_HIGH tiers from fact_customer_risk.",
            summary,
        )
    with c2:
        bins = loaders.load_risk_score_bins()
        plot_with_ai(
            bar_chart(bins, x="score_bin", y="customer_count", title="Global risk score histogram (bins of 5)"),
            "risk_histogram",
            "Risk score histogram",
            "Distribution of global_risk_score in 5-point bins. Shows where most customers cluster.",
            bins,
        )

    section("Three-pillar risk model")
    melted = summary.melt(
        id_vars="risk_tier",
        value_vars=["avg_compliance", "avg_fragility", "avg_behavioral"],
        var_name="pillar",
        value_name="score",
    )
    melted["pillar"] = melted["pillar"].map({
        "avg_compliance": "Compliance",
        "avg_fragility": "Financial fragility",
        "avg_behavioral": "Behavioral",
    })
    fig = px.bar(
        melted, x="risk_tier", y="score", color="pillar", barmode="group",
        title="Average pillar scores by risk tier",
    )
    plot_with_ai(
        apply_theme(fig, 400),
        "risk_pillars",
        "Three-pillar scores by tier",
        "Compliance (KYC/PEP), financial fragility (salary/overdraft), behavioral (tenure/KYC staleness) averaged per tier.",
        melted,
    )

    section("Segmentation analysis")
    c3, c4 = st.columns(2)
    with c3:
        sector = loaders.load_risk_by_sector(12)
        fig = px.scatter(
            sector, x="avg_risk_score", y="customer_count", size="high_risk_count",
            color="avg_risk_score", hover_name="sector_description",
            color_continuous_scale="RdYlGn_r", title="Risk vs volume by sector",
        )
        plot_with_ai(
            apply_theme(fig, 400), "risk_sector_scatter",
            "Risk vs volume by sector",
            "Which economic sectors have high average risk but also large customer counts.",
            sector,
        )
    with c4:
        area = loaders.load_risk_by_dao_area(12)
        plot_with_ai(
            bar_chart(area, x="dao_area", y="avg_risk_score", title="Average risk by branch area", top_n=12),
            "risk_dao_area",
            "Risk by branch area",
            "Geographic risk concentration across DAO/branch areas.",
            area,
        )

    section("Risk vs financial exposure")
    balance = loaders.load_balance_vs_risk()
    fig = px.scatter(
        balance, x="total_balance", y="global_risk_score", color="risk_tier",
        color_discrete_map=RISK_COLORS, opacity=0.5,
        title="Total balance vs global risk score (sample)",
    )
    plot_with_ai(
        apply_theme(fig, 420), "risk_balance_scatter",
        "Balance vs risk scatter",
        "Tests whether high-balance customers are lower risk — important for portfolio monitoring.",
        balance.head(500),
    )

    section("Pillar correlation (sample)")
    sample = loaders.load_risk_pillar_corr()
    fig = px.scatter_matrix(
        sample, dimensions=["compliance", "fragility", "behavioral", "global_score"],
        color="risk_tier", color_discrete_map=RISK_COLORS, title="Risk pillar correlation matrix",
    )
    fig.update_layout(height=600)
    plot_with_ai(
        apply_theme(fig, 600), "risk_corr_matrix",
        "Pillar correlation matrix",
        "How compliance, fragility, and behavioral scores relate to each other and to global score.",
        sample.head(300),
    )

    # ── NEW ANALYSES (added, existing kept above) ─────────────────────────────
    section("Employment & tenure drivers")
    c5, c6 = st.columns(2)
    with c5:
        emp = loaders.load_risk_by_employment(10)
        fig = px.bar(
            emp, x="employment_status", y="avg_risk_score", color="high_risk_count",
            title="Average risk by employment status", color_continuous_scale="Reds",
        )
        plot_with_ai(
            apply_theme(fig, 380), "risk_employment",
            "Risk by employment",
            "Unemployed or fragile employment statuses often raise financial fragility pillar.",
            emp,
        )
    with c6:
        tenure = loaders.load_tenure_risk_bins()
        order = ["< 1 year", "1-5 years", "5-10 years", "10+ years"]
        tenure["tenure_band"] = tenure["tenure_band"].astype(str)
        fig = px.line(
            tenure, x="tenure_band", y="avg_risk_score", markers=True,
            title="Average risk by customer tenure",
        )
        plot_with_ai(
            apply_theme(fig, 380), "risk_tenure",
            "Risk by tenure",
            "New customers (<1 year) often score higher on behavioral risk.",
            tenure,
        )

    section("Overdraft & KYC compliance impact")
    c7, c8 = st.columns(2)
    with c7:
        od = loaders.load_overdraft_risk_cross()
        fig = px.bar(
            od, x="risk_tier", y="customer_count", color="overdraft_status",
            barmode="group", title="Risk tier by overdraft status",
            category_orders={"risk_tier": RISK_ORDER},
        )
        plot_with_ai(
            apply_theme(fig, 380), "risk_overdraft",
            "Overdraft vs risk tier",
            "Customers with negative balances (overdraft) should appear more in HIGH/VERY_HIGH tiers.",
            od,
        )
    with c8:
        kyc = loaders.load_kyc_pep_matrix()
        fig = px.imshow(
            kyc.pivot(index="kyc_status", columns="pep_status", values="avg_risk_score"),
            text_auto=".1f", color_continuous_scale="Reds",
            title="Avg risk score: KYC × PEP matrix",
        )
        plot_with_ai(
            apply_theme(fig, 380), "risk_kyc_pep",
            "KYC × PEP heatmap",
            "PEP and incomplete KYC combinations drive compliance pillar — expect higher scores top-right.",
            kyc,
        )

    section("Pillar radar by risk tier")
    radar = loaders.load_risk_pillar_radar()
    fig = go.Figure()
    for _, row in radar.iterrows():
        fig.add_trace(go.Scatterpolar(
            r=[row["compliance"], row["fragility"], row["behavioral"], row["compliance"]],
            theta=["Compliance", "Fragility", "Behavioral", "Compliance"],
            name=str(row["risk_tier"]),
        ))
    fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 80])), title="Pillar profile by tier")
    plot_with_ai(
        apply_theme(fig, 420), "risk_radar",
        "Risk pillar radar",
        "Radar curves show which pillar dominates each risk tier — compliance vs financial vs behavioral.",
        radar,
    )

    section("High-risk watchlist")
    watch = loaders.load_high_risk_watchlist(50)
    st.dataframe(watch, use_container_width=True, height=320)
    insight("Top 50 HIGH/VERY_HIGH customers sorted by global_risk_score — use for compliance review demos.")

    with st.expander("Detailed risk records (top scores)"):
        st.dataframe(loaders.load_risk_sample(500), use_container_width=True, height=400)
