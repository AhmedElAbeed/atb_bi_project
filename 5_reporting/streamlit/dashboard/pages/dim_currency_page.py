"""dim_currency analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, pie_chart
from dashboard.ui import kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header("Currency Dimension", "Multi-currency reference, exposure, and risk.", table="dim_currency")

    try:
        currency = loaders.load_dim_currency()
        exposure = loaders.load_currency_exposure()
        by_curr = loaders.load_accounts_by_currency()
        risk_fx = loaders.load_currency_risk_exposure()
    except Exception as exc:
        st.error(f"Could not load currency data: {exc}")
        return

    active = currency[currency["currency_code"] != "UNK"]
    total_bal = exposure["total_balance"].sum() if len(exposure) else 0

    kpi_row([
        ("Currencies", f"{len(active):,}", None),
        ("With accounts", f"{len(exposure):,}", "#38bdf8"),
        ("Total balance", f"{total_bal:,.0f} DT", "#34d399"),
        ("Max overdraft %", f"{exposure['overdraft_pct'].max():.1f}%" if len(exposure) else "—", "#ef4444"),
    ])

    section("Currency reference")
    fig = px.bar(active, x="currency_code", y="no_of_decimals", color="currency_name",
                 title="Decimal precision by currency")
    plot_with_ai(apply_theme(fig, 380), "dimcurr_ref", "Currency codes", "ISO reference table.", active)

    section("Account exposure")
    c1, c2 = st.columns(2)
    with c1:
        plot_with_ai(pie_chart(by_curr.head(8), "currency_name", "account_count", "Account share (top 8)"),
                     "dimcurr_pie", "FX account mix", "Share of accounts per currency.", by_curr.head(8))
    with c2:
        plot_with_ai(bar_chart(by_curr.head(10), x="currency_name", y="total_balance", title="Balance by currency", top_n=10),
                     "dimcurr_bal", "FX balance", "Total balance per currency.", by_curr.head(10))

    section("Overdraft & risk by currency")
    c3, c4 = st.columns(2)
    with c3:
        fig = px.bar(exposure, x="currency_code", y="overdraft_pct", color="total_balance",
                     title="Overdraft % by currency", color_continuous_scale="Reds")
        plot_with_ai(apply_theme(fig, 400), "dimcurr_od", "Overdraft by FX", "Overdraft rate per currency.", exposure)
    with c4:
        fig = px.scatter(risk_fx, x="accounts", y="avg_risk_score", size="total_balance",
                         hover_name="currency_name", color="avg_risk_score",
                         color_continuous_scale="RdYlGn_r", title="Avg risk score by currency")
        plot_with_ai(apply_theme(fig, 400), "dimcurr_risk",
                     "FX risk scatter", "Customer count vs average risk per currency.", risk_fx)

    section("Balance treemap")
    fig = px.treemap(exposure, path=["currency_name"], values="total_balance",
                     color="overdraft_pct", color_continuous_scale="RdYlGn_r", title="FX exposure treemap")
    plot_with_ai(apply_theme(fig, 440), "dimcurr_treemap",
                 "FX treemap", "Balance and overdraft rate combined view.", exposure)

    with st.expander("Full currency table"):
        st.dataframe(currency, use_container_width=True, height=320)
