"""dim_industry analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, horizontal_bar
from dashboard.ui import kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header("Industry Dimension", "664 industry codes — customers, accounts, risk.", table="dim_industry")

    try:
        industry_all = loaders.load_dim_industry_all()
        top_customers = loaders.load_dim_industry(25)
        risk = loaders.load_industry_risk(20)
        acct = loaders.load_industry_accounts(15)
    except Exception as exc:
        st.error(f"Could not load industry data: {exc}")
        return

    kpi_row([
        ("Industries", f"{len(industry_all):,}", None),
        ("With customers", f"{(top_customers['customer_count'] > 0).sum():,}", "#38bdf8"),
        ("Top by balance", str(acct.iloc[0]["industry_description"])[:22] if len(acct) else "—", "#34d399"),
        ("Highest risk", str(risk.iloc[0]["industry_description"])[:22] if len(risk) else "—", "#ef4444"),
    ])

    section("Customer concentration")
    c1, c2 = st.columns(2)
    with c1:
        plot_with_ai(horizontal_bar(top_customers, y="industry_description", x="customer_count",
                                  title="Top industries by customers", top_n=15),
                     "dimind_cust", "Customer volume", "Industries with most customers.", top_customers)
    with c2:
        plot_with_ai(horizontal_bar(risk, y="industry_description", x="avg_risk_score",
                                  title="Highest avg risk", top_n=12),
                     "dimind_risk", "Risk ranking", "Riskiest industries (min 50 customers).", risk)

    section("Account exposure (fact_account)")
    c3, c4 = st.columns(2)
    with c3:
        plot_with_ai(horizontal_bar(acct, y="industry_description", x="total_balance",
                                  title="Balance by industry", top_n=12),
                     "dimind_bal", "Industry balance", "Total balance per industry.", acct)
    with c4:
        plot_with_ai(horizontal_bar(acct, y="industry_description", x="account_count",
                                  title="Accounts by industry", top_n=12),
                     "dimind_acct", "Account count", "Number of accounts per industry.", acct)

    section("Risk vs volume bubble chart")
    fig = px.scatter(risk, x="customer_count", y="avg_risk_score", size="high_risk_count",
                     hover_name="industry_description", color="avg_risk_score",
                     color_continuous_scale="RdYlGn_r", title="Industry risk vs volume")
    plot_with_ai(apply_theme(fig, 420), "dimind_scatter",
                 "Industry bubble chart", "Niche high-risk industries stand out.", risk)

    with st.expander("Full industry catalog"):
        st.dataframe(industry_all, use_container_width=True, height=450)
