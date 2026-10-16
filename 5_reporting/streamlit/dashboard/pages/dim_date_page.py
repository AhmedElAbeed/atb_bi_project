"""dim_date analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, line_chart, pie_chart
from dashboard.ui import kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header("Date Dimension", "Calendar coverage, seasonality, and account opening trends.", table="dim_date")

    try:
        by_year = loaders.load_dim_date_by_year()
        weekend = loaders.load_dim_date_weekend()
        months = loaders.load_dim_date_months()
        quarters = loaders.load_date_quarter_counts()
        openings = loaders.load_date_account_openings()
        total = int(by_year["date_count"].sum())
    except Exception as exc:
        st.error(f"Could not load date dimension: {exc}")
        return

    kpi_row([
        ("Calendar days", f"{total:,}", None),
        ("Years covered", f"{len(by_year):,}", "#38bdf8"),
        ("Min year", f"{int(by_year['year'].min())}", None),
        ("Max year", f"{int(by_year['year'].max())}", None),
    ])

    section("Calendar structure")
    c1, c2 = st.columns(2)
    with c1:
        plot_with_ai(bar_chart(by_year, x="year", y="date_count", title="Dates per year"),
                     "dimdate_year", "Calendar years", "Date dimension coverage.", by_year)
    with c2:
        plot_with_ai(pie_chart(weekend, "day_type", "date_count", "Weekend vs weekday"),
                     "dimdate_weekend", "Weekend split", "Weekday vs weekend days.", weekend)

    section("Seasonality")
    c3, c4 = st.columns(2)
    with c3:
        fig = px.line(months, x="month_name", y="date_count", markers=True, title="Days per month")
        plot_with_ai(apply_theme(fig, 360), "dimdate_months", "Monthly seasonality", "Days per month in calendar.", months)
    with c4:
        plot_with_ai(bar_chart(quarters, x="quarter", y="date_count", title="Days per quarter"),
                     "dimdate_quarters", "Quarterly structure", "Calendar days per fiscal quarter.", quarters)

    section("Account openings linked to dim_date")
    c5, c6 = st.columns(2)
    with c5:
        plot_with_ai(line_chart(openings, x="year", y="account_count", title="Account openings by year"),
                     "dimdate_openings", "Openings by year", "New accounts per opening year.", openings)
    with c6:
        fig = px.area(openings, x="year", y="total_balance", title="Total balance by opening year")
        plot_with_ai(apply_theme(fig, 360), "dimdate_balance_year",
                     "Balance by vintage", "Portfolio balance by account opening year.", openings)

    with st.expander("Sample date records"):
        st.dataframe(loaders.load_table_sample("dim_date", 200), use_container_width=True, height=350)
