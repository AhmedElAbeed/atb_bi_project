"""dim_target analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, pie_chart
from dashboard.ui import kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header("Target Segment Dimension", "Customer segments — mix, accounts, balances, risk.", table="dim_target")

    try:
        target = loaders.load_dim_target()
        counts = loaders.load_target_customer_counts()
        risk = loaders.load_target_risk()
        acct = loaders.load_target_accounts()
    except Exception as exc:
        st.error(f"Could not load target data: {exc}")
        return

    kpi_row([
        ("Segments", f"{len(target):,}", None),
        ("Largest segment", counts.iloc[0]["target_description"], "#38bdf8"),
        ("Highest risk", risk.iloc[0]["target_description"], "#ef4444"),
        ("Top balance segment", acct.iloc[0]["target_description"] if len(acct) else "—", "#34d399"),
    ])

    section("Segment mix")
    c1, c2 = st.columns(2)
    with c1:
        plot_with_ai(pie_chart(counts, "target_description", "customer_count", "Customers per segment"),
                     "dimtarget_pie", "Segment mix", "Customer share per target segment.", counts)
    with c2:
        fig = px.bar(risk, x="target_description", y="avg_risk_score", color="high_risk_count",
                     title="Avg risk by segment", color_continuous_scale="Reds")
        plot_with_ai(apply_theme(fig, 400), "dimtarget_risk", "Risk by segment", "Average risk per segment.", risk)

    section("Account exposure (fact_account)")
    c3, c4 = st.columns(2)
    with c3:
        plot_with_ai(bar_chart(acct, x="target_description", y="total_balance", title="Balance by segment"),
                     "dimtarget_bal", "Segment balance", "Total balance per target segment.", acct)
    with c4:
        plot_with_ai(bar_chart(acct, x="target_description", y="account_count", title="Accounts by segment"),
                     "dimtarget_acct", "Segment accounts", "Account count per segment.", acct)

    section("Balance vs risk positioning")
    fig = px.scatter(risk, x="avg_balance", y="avg_risk_score", size="customer_count",
                     color="target_description", title="Segment: balance vs risk")
    plot_with_ai(apply_theme(fig, 400), "dimtarget_scatter",
                 "Segment map", "Wallet size vs risk per segment.", risk)

    with st.expander("Target reference table"):
        st.dataframe(target, use_container_width=True, height=320)
