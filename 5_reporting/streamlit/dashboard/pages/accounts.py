"""fact_account analytics."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, line_chart, pie_chart
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header(
        "Account Portfolio",
        "Working balances, currency mix, sector distribution, and account aging analysis.",
        table="fact_account",
    )

    try:
        kpis = loaders.load_account_kpis()
    except Exception as exc:
        st.error(f"Could not load account data: {exc}")
        return

    kpi_row([
        ("Accounts", f"{kpis['accounts']:,.0f}", None),
        ("Total balance", f"{kpis['total_balance']:,.0f} DT", "#34d399"),
        ("Average balance", f"{kpis['avg_balance']:,.0f} DT", None),
        ("Avg account age", f"{kpis['avg_age_days']:,.0f} days", None),
        ("Overdraft rate", f"{kpis['overdraft_pct']:.1f}%", "#ef4444"),
    ])

    insight(
        "Grain = one row per account. Joins to dimensions for segmentation. "
        "🤖 AI panel explains each chart in student-friendly language."
    )

    section("Currency & sector mix")
    c1, c2 = st.columns(2)
    with c1:
        currency = loaders.load_accounts_by_currency()
        fig = px.bar(
            currency.head(12), x="currency_name", y="total_balance",
            color="account_count", title="Total balance by currency",
        )
        plot_with_ai(
            apply_theme(fig, 400), "acct_currency",
            "Balance by currency",
            "Multi-currency portfolio exposure from fact_account joined to dim_currency.",
            currency,
        )
    with c2:
        sector = loaders.load_accounts_by_sector(12)
        plot_with_ai(
            pie_chart(sector, names="sector_description", values="account_count", title="Account count by sector"),
            "acct_sector_pie",
            "Accounts by sector",
            "How accounts are distributed across economic sectors.",
            sector,
        )

    section("Geography & maturity")
    c3, c4 = st.columns(2)
    with c3:
        area = loaders.load_accounts_by_dao_area(12)
        plot_with_ai(
            bar_chart(area, x="dao_area", y="account_count", title="Accounts by branch area", top_n=12),
            "acct_dao",
            "Accounts by branch area",
            "Branch network concentration — which DAO areas hold the most accounts.",
            area,
        )
    with c4:
        age = loaders.load_account_age_distribution()
        order = ["< 1 year", "1-5 years", "5-10 years", "10+ years"]
        age["age_bucket"] = pd.Categorical(age["age_bucket"], categories=order, ordered=True)
        age = age.sort_values("age_bucket")
        plot_with_ai(
            pie_chart(age, names="age_bucket", values="account_count", title="Account age distribution"),
            "acct_age",
            "Account age distribution",
            "Maturity of the portfolio — young vs legacy accounts.",
            age,
        )

    section("Balance concentration")
    sample = loaders.load_account_sample(3000)
    fig = px.histogram(
        sample, x="working_balance", nbins=50,
        title="Working balance distribution (top accounts sample)",
    )
    plot_with_ai(
        apply_theme(fig, 380), "acct_balance_hist",
        "Balance histogram",
        "Distribution shape — long tail means few accounts hold most balance (concentration risk).",
        sample[["working_balance"]].head(500),
    )

    # ── NEW ANALYSES ───────────────────────────────────────────────────────────
    section("Overdraft rate by sector")
    od_sector = loaders.load_overdraft_by_sector(12)
    fig = px.bar(
        od_sector, x="overdraft_pct", y="sector_description", orientation="h",
        color="total_balance", title="Overdraft % by sector (color = balance)",
        color_continuous_scale="Blues",
    )
    plot_with_ai(
        apply_theme(fig, 420), "acct_overdraft_sector",
        "Overdraft by sector",
        "Sectors with highest overdraft % may need tighter credit monitoring.",
        od_sector,
    )

    section("Account openings trend (monthly)")
    monthly = loaders.load_accounts_opening_monthly()
    monthly["year_month"] = monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)
    plot_with_ai(
        line_chart(monthly.tail(60), x="year_month", y="account_count", title="Monthly account openings since 2015"),
        "acct_openings_monthly",
        "Monthly openings curve",
        "Time series of new accounts — shows growth or seasonality in acquisition.",
        monthly.tail(24),
    )

    section("Balance percentiles")
    try:
        pct = loaders.load_balance_percentiles()
        if not pct.empty:
            row = pct.iloc[0]
            pct_df = pd.DataFrame({
                "percentile": ["P10", "P25", "P50", "P75", "P90", "P99"],
                "balance": [row["p10"], row["p25"], row["p50"], row["p75"], row["p90"], row["p99"]],
            })
            fig = px.line(
                pct_df, x="percentile", y="balance", markers=True,
                title="Working balance percentiles (full portfolio)",
            )
            plot_with_ai(
                apply_theme(fig, 360), "acct_percentiles",
                "Balance percentiles",
                "P50 = median balance. Large gap between P90 and P99 indicates wealth concentration.",
                pct_df,
            )
    except Exception as exc:
        st.caption(f"Percentiles unavailable: {exc}")

    with st.expander("Account detail table"):
        st.dataframe(sample, use_container_width=True, height=400)
