"""dim_sector analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, horizontal_bar
from dashboard.ui import kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header("Sector Dimension", "Economic sectors — accounts, balances, risk, overdraft.", table="dim_sector")

    try:
        sector = loaders.load_dim_sector()
        risk = loaders.load_risk_by_sector(20)
        accounts = loaders.load_accounts_by_sector(20)
        portfolio = loaders.load_sector_account_risk_join(15)
        overdraft = loaders.load_sector_overdraft_rate(12)
    except Exception as exc:
        st.error(f"Could not load sector data: {exc}")
        return

    kpi_row([
        ("Sectors", f"{len(sector):,}", None),
        ("Top by accounts", str(accounts.iloc[0]["sector_description"])[:22] if len(accounts) else "—", "#38bdf8"),
        ("Highest risk", f"{risk.iloc[0]['avg_risk_score']:.1f}" if len(risk) else "—", "#ef4444"),
        ("Max overdraft %", f"{overdraft.iloc[0]['overdraft_pct']:.1f}%" if len(overdraft) else "—", "#f472b6"),
    ])

    section("Sector catalog & risk")
    c1, c2 = st.columns(2)
    with c1:
        plot_with_ai(bar_chart(sector.head(20), x="sector_description", y="sector_code", title="Sector catalog"),
                     "dimsector_cat", "Sector list", "All sectors in warehouse.", sector.head(20))
    with c2:
        plot_with_ai(horizontal_bar(risk, y="sector_description", x="avg_risk_score", title="Avg risk score", top_n=12),
                     "dimsector_risk", "Risk ranking", "Sectors by average global risk.", risk)

    section("Accounts, balance & overdraft")
    c3, c4 = st.columns(2)
    with c3:
        plot_with_ai(horizontal_bar(accounts, y="sector_description", x="total_balance", title="Total balance", top_n=12),
                     "dimsector_bal", "Balance by sector", "Total working balance per sector.", accounts)
    with c4:
        fig = px.bar(overdraft, x="overdraft_pct", y="sector_description", orientation="h",
                     color="account_count", title="Overdraft rate by sector")
        plot_with_ai(apply_theme(fig, 400), "dimsector_od",
                     "Overdraft rate", "Sectors with highest overdraft percentage.", overdraft)

    section("Portfolio positioning")
    fig = px.scatter(portfolio, x="accounts", y="avg_risk_score", size="total_balance",
                     hover_name="sector_description", color="avg_risk_score",
                     color_continuous_scale="RdYlGn_r", title="Accounts vs risk (size = balance)")
    plot_with_ai(apply_theme(fig, 420), "dimsector_scatter",
                 "Sector portfolio map", "Joint view of volume, risk, and balance.", portfolio)

    section("High-risk customer count")
    plot_with_ai(horizontal_bar(risk, y="sector_description", x="high_risk_count", title="High-risk customers", top_n=12),
                 "dimsector_high", "High-risk count", "HIGH+VERY_HIGH customers per sector.", risk)
