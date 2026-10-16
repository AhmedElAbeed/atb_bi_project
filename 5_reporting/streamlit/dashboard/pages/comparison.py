"""Side-by-side comparison of sectors or branch areas."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme
from dashboard.i18n import t
from dashboard.ui import page_header, plot_with_ai, section


def _comparison_table(df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    if len(df) < 2:
        return pd.DataFrame()
    a, b = df.iloc[0], df.iloc[1]
    metrics = [
        ("customers", t("customers")),
        ("accounts", t("accounts")),
        ("total_balance", t("total_balance")),
        ("avg_risk", t("avg_risk")),
        ("high_risk_pct", t("high_risk_pct")),
    ]
    if "overdraft_pct" in df.columns:
        metrics.append(("overdraft_pct", t("overdraft_pct")))
    rows = []
    for col, label in metrics:
        va = float(a.get(col, 0) or 0)
        vb = float(b.get(col, 0) or 0)
        rows.append({
            t("metric"): label,
            t("value_a"): f"{va:,.1f}",
            t("value_b"): f"{vb:,.1f}",
            t("delta"): f"{vb - va:+,.1f}",
        })
    return pd.DataFrame(rows)


def render() -> None:
    page_header(t("comparison_title"), t("comparison_sub"))

    mode = st.radio(t("compare_type"), [t("sector"), t("dao_area")], horizontal=True)

    if mode == t("sector"):
        options = loaders.load_sector_names()
        c1, c2 = st.columns(2)
        with c1:
            entity_a = st.selectbox(t("entity_a"), options, index=0, key="cmp_sector_a")
        with c2:
            entity_b = st.selectbox(t("entity_b"), options, index=min(1, len(options) - 1), key="cmp_sector_b")
        if entity_a == entity_b:
            st.warning("Choose two different sectors.")
            return
        df = loaders.load_sector_comparison(entity_a, entity_b)
        name_col = "sector_description"
    else:
        options = loaders.load_dao_area_names()
        c1, c2 = st.columns(2)
        with c1:
            entity_a = st.selectbox(t("entity_a"), options, index=0, key="cmp_dao_a")
        with c2:
            entity_b = st.selectbox(t("entity_b"), options, index=min(1, len(options) - 1), key="cmp_dao_b")
        if entity_a == entity_b:
            st.warning("Choose two different areas.")
            return
        df = loaders.load_dao_comparison(entity_a, entity_b)
        name_col = "dao_area"

    if df.empty or len(df) < 2:
        st.info("Not enough data for comparison.")
        return

    section(f"{entity_a}  vs  {entity_b}")
    comp = _comparison_table(df, name_col)
    st.dataframe(comp, use_container_width=True, hide_index=True)

    section("Visual comparison")
    melted = df.melt(id_vars=[name_col], var_name="metric", value_name="value")
    fig = px.bar(
        melted, x="metric", y="value", color=name_col, barmode="group",
        title=f"Side-by-side metrics",
    )
    plot_with_ai(
        apply_theme(fig, 420), "comparison_bar",
        "Comparison chart", f"Compare {entity_a} vs {entity_b}.", melted,
    )

    c3, c4 = st.columns(2)
    with c3:
        fig_a = px.pie(df, names=name_col, values="customers", title=t("customers"))
        st.plotly_chart(apply_theme(fig_a, 340), use_container_width=True)
    with c4:
        fig_b = px.bar(df, x=name_col, y="avg_risk", color="avg_risk",
                       color_continuous_scale="RdYlGn_r", title=t("avg_risk"))
        st.plotly_chart(apply_theme(fig_b, 340), use_container_width=True)
