"""dim_sector, dim_industry, dim_target, dim_currency combined reference analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, pie_chart
from dashboard.ui import page_header, section


def render() -> None:
    page_header(
        "Reference Dimensions",
        "PFE_DWH.dim_sector · dim_industry · dim_target · dim_currency",
    )

    tabs = st.tabs(["Sector", "Industry", "Target segment", "Currency"])

    with tabs[0]:
        section("dim_sector")
        try:
            sector = loaders.load_dim_sector()
            st.metric("Sectors", len(sector))
            fig = px.bar(
                sector,
                x="sector_description",
                y="sector_code",
                title="Sector catalog",
                labels={"sector_description": "Sector", "sector_code": "Code"},
            )
            st.plotly_chart(apply_theme(fig, 380), use_container_width=True)
            st.dataframe(sector, use_container_width=True)
        except Exception as exc:
            st.error(str(exc))

    with tabs[1]:
        section("dim_industry")
        try:
            industry_all = loaders.load_dim_industry_all()
            top = loaders.load_dim_industry(20)
            st.metric("Industries", len(industry_all))
            st.plotly_chart(
                bar_chart(top, x="industry_description", y="customer_count", title="Top industries by linked customers", top_n=15),
                use_container_width=True,
            )
            with st.expander("Full industry catalog"):
                st.dataframe(industry_all, use_container_width=True, height=400)
        except Exception as exc:
            st.error(str(exc))

    with tabs[2]:
        section("dim_target")
        try:
            target = loaders.load_dim_target()
            counts = loaders.load_target_customer_counts()
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Target segments", len(target))
                st.plotly_chart(pie_chart(counts, "target_description", "customer_count", "Customers per segment"), use_container_width=True)
            with c2:
                st.dataframe(target, use_container_width=True)
        except Exception as exc:
            st.error(str(exc))

    with tabs[3]:
        section("dim_currency")
        try:
            currency = loaders.load_dim_currency()
            st.metric("Currencies", len(currency))
            fig = px.bar(
                currency[currency["currency_code"] != "UNK"],
                x="currency_code",
                y="no_of_decimals",
                color="currency_name",
                title="Currency reference (decimal precision)",
            )
            st.plotly_chart(apply_theme(fig, 360), use_container_width=True)
            st.dataframe(currency, use_container_width=True)
        except Exception as exc:
            st.error(str(exc))
