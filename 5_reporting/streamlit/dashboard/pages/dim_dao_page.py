"""dim_dao analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header("DAO / Branch Network", "Branches, areas, balances, and geographic risk.", table="dim_dao")

    try:
        dao = loaders.load_dim_dao()
        by_area = loaders.load_dao_by_area()
        acct_area = loaders.load_dao_accounts_by_area(15)
        risk_area = loaders.load_dao_risk_by_area(15)
    except Exception as exc:
        st.error(f"Could not load DAO dimension: {exc}")
        return

    kpi_row([
        ("Branches (DAOs)", f"{len(dao):,}", None),
        ("Areas", f"{dao['dao_area'].nunique():,}", "#38bdf8"),
        ("Departments", f"{dao['parent_department_code'].nunique():,}", None),
        ("Top area (balance)", acct_area.iloc[0]["dao_area"] if len(acct_area) else "—", "#34d399"),
    ])

    insight("DAO links every account and risk record to a branch area for geographic BI.")

    section("Network distribution")
    c1, c2 = st.columns(2)
    with c1:
        fig = px.treemap(by_area, path=["dao_area"], values="branch_count", title="Branches per area")
        plot_with_ai(apply_theme(fig, 400), "dimdao_treemap", "Branch treemap", "Branch hierarchy by area.", by_area)
    with c2:
        plot_with_ai(bar_chart(by_area, x="dao_area", y="branch_count", title="Branch count by area", top_n=15),
                     "dimdao_bar", "Branch counts", "Number of DAOs per area.", by_area)

    section("Accounts & balance by area (fact_account)")
    c3, c4 = st.columns(2)
    with c3:
        fig = px.bar(acct_area, x="dao_area", y="total_balance", color="account_count",
                     title="Total balance by branch area")
        plot_with_ai(apply_theme(fig, 400), "dimdao_balance",
                     "Balance by area", "Portfolio value per geographic area.", acct_area)
    with c4:
        fig = px.bar(acct_area, x="dao_area", y="overdraft_count", color="overdraft_count",
                     color_continuous_scale="Reds", title="Overdraft accounts by area")
        plot_with_ai(apply_theme(fig, 400), "dimdao_overdraft",
                     "Overdraft by area", "Geographic overdraft concentration.", acct_area)

    section("Risk by branch area (fact_customer_risk)")
    fig = px.scatter(risk_area, x="customer_count", y="avg_risk_score", size="high_risk_count",
                     hover_name="dao_area", color="avg_risk_score", color_continuous_scale="RdYlGn_r",
                     title="Risk vs customer volume by area")
    plot_with_ai(apply_theme(fig, 420), "dimdao_risk_scatter",
                 "Area risk scatter", "Areas with high volume AND high average risk need review.", risk_area)

    section("Branch directory")
    st.dataframe(dao, use_container_width=True, height=380)
