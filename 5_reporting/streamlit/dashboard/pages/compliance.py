"""Compliance dashboard — PEP, KYC overdue, watchlist."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart
from dashboard.i18n import lang, t
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header(t("compliance_title"), t("compliance_sub"), table="dim_customer + fact_customer_risk")

    try:
        pep = loaders.load_pep_customers(500)
        kyc = loaders.load_kyc_overdue_customers(500)
        watch = loaders.load_compliance_watchlist(200)
    except Exception as exc:
        st.error(str(exc))
        return

    kpi_row([
        ("PEP flagged", f"{len(pep):,}", "#f59e0b"),
        ("KYC overdue / incomplete", f"{len(kyc):,}", "#ef4444"),
        ("Watchlist total", f"{len(watch):,}", "#a855f7"),
        ("HIGH+ on watchlist", f"{watch['risk_tier'].isin(['HIGH','VERY_HIGH']).sum():,}", "#ef4444"),
    ])

    insight(
        "Vue réglementaire : PEP, retards KYC et liste de surveillance pour les agents conformité."
        if lang() == "fr"
        else "Regulatory view: PEP persons, KYC gaps, and combined watchlist for compliance review."
    )

    section(t("pep_list"))
    if pep.empty:
        st.success("No PEP customers in warehouse.")
    else:
        c1, c2 = st.columns([1.2, 1])
        with c1:
            pep_risk = pep.groupby("risk_tier", dropna=False).size().reset_index(name="count")
            plot_with_ai(
                bar_chart(pep_risk, x="risk_tier", y="count", title="PEP by risk tier"),
                "comp_pep_tier", "PEP risk tiers", "Risk distribution among PEP customers.", pep_risk,
            )
        with c2:
            st.metric("Avg risk score (PEP)", f"{pep['global_risk_score'].mean():.1f}")
            st.metric("KYC incomplete (PEP)", f"{(pep['is_kyc_complete'] == 0).sum():,}")
        st.dataframe(pep, use_container_width=True, height=320)
        st.download_button(t("download_csv"), pep.to_csv(index=False).encode("utf-8"), "pep_customers.csv")

    section(t("kyc_overdue"))
    if kyc.empty:
        st.success("No KYC overdue customers.")
    else:
        kyc_bins = kyc.copy()
        kyc_bins["kyc_status"] = kyc_bins["is_kyc_complete"].map({1: "Complete", 0: "Incomplete"})
        fig = px.histogram(kyc_bins, x="days_since_kyc", color="kyc_status", nbins=30,
                           title="Days since last KYC review")
        plot_with_ai(
            apply_theme(fig, 380), "comp_kyc_hist",
            "KYC staleness", "How long since last KYC review for overdue customers.", kyc_bins.head(200),
        )
        st.dataframe(kyc, use_container_width=True, height=320)
        st.download_button(t("download_csv"), kyc.to_csv(index=False).encode("utf-8"), "kyc_overdue.csv")

    section(t("watchlist"))
    flag_counts = watch.groupby("risk_tier").size().reset_index(name="count")
    plot_with_ai(
        bar_chart(flag_counts, x="risk_tier", y="count", title="Watchlist by risk tier"),
        "comp_watch", "Watchlist tiers", "Combined high-risk, PEP, compliance, KYC gaps.", flag_counts,
    )
    st.dataframe(watch, use_container_width=True, height=360)
    st.download_button(t("download_csv"), watch.to_csv(index=False).encode("utf-8"), "compliance_watchlist.csv")
