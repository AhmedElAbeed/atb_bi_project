"""Interactive table explorer."""
from __future__ import annotations

import streamlit as st

from dashboard import loaders
from dashboard.ui import page_header, section


TABLES = [
    "dim_currency",
    "dim_customer",
    "dim_date",
    "dim_dao",
    "dim_industry",
    "dim_sector",
    "dim_target",
    "fact_account",
    "fact_customer_risk",
]


def render() -> None:
    page_header(
        "Data Explorer",
        "Browse and export raw samples from every PFE_DWH table in the dashboard",
    )

    try:
        overview = loaders.load_warehouse_overview()
    except Exception as exc:
        st.error(f"Could not load table list: {exc}")
        return

    section("Table inventory")
    for name in TABLES:
        rows = overview.get(name, 0)
        st.markdown(f"**`PFE_DWH.{name}`** — {rows:,} rows")

    section("Sample viewer")
    table = st.selectbox("Select table", TABLES)
    limit = st.slider("Rows to preview", 50, 1000, 200, step=50)

    if st.button("Load sample", type="primary"):
        with st.spinner(f"Loading {table}..."):
            try:
                df = loaders.load_table_sample(table, limit)
                st.dataframe(df, use_container_width=True, height=480)
                st.download_button(
                    "Download CSV",
                    df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{table}_sample.csv",
                    mime="text/csv",
                )
            except Exception as exc:
                st.error(str(exc))
