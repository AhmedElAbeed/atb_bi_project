"""dim_customer analytics."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from dashboard import loaders
from dashboard.charts import apply_theme, bar_chart, pie_chart
from dashboard.ui import insight, kpi_row, page_header, plot_with_ai, section


def render() -> None:
    page_header(
        "Customer Dimension",
        "Demographics, KYC compliance, salary bands, and segmentation.",
        table="dim_customer",
    )

    try:
        stats = loaders.load_dim_customer_stats()
    except Exception as exc:
        st.error(f"Could not load customer dimension: {exc}")
        return

    kpi_row([
        ("Customers", f"{stats['total']:,.0f}", None),
        ("Avg monthly salary", f"{stats['avg_salary']:,.0f} DT", "#34d399"),
        ("Avg tenure", f"{stats['avg_tenure']:,.0f} days", None),
        ("KYC complete", f"{stats['kyc_pct']:.1f}%", "#38bdf8"),
        ("PEP flagged", f"{stats['pep_pct']:.2f}%", "#f59e0b"),
        ("Compliance flagged", f"{stats['compliance_pct']:.2f}%", "#ef4444"),
    ])

    insight("136K+ customer profiles — KYC, salary, tenure feed both fact tables and the ML model.")

    section("Demographics")
    c1, c2 = st.columns(2)
    with c1:
        gender = loaders.load_customer_gender()
        plot_with_ai(pie_chart(gender, "gender", "customer_count", "Gender distribution"),
                     "dimcust_gender", "Gender split", "Male vs female portfolio mix.", gender)
    with c2:
        emp = loaders.load_customer_employment(8)
        plot_with_ai(bar_chart(emp, x="employment_status", y="customer_count", title="Employment status"),
                     "dimcust_emp", "Employment", "Job status distribution.", emp)

    section("Compliance & KYC flags")
    c3, c4 = st.columns(2)
    with c3:
        flags = loaders.load_customer_compliance_flags()
        plot_with_ai(pie_chart(flags, "flag_type", "customer_count", "Compliance flag breakdown"),
                     "dimcust_flags", "Compliance flags", "PEP, KYC gaps, compliance alerts.", flags)
    with c4:
        kyc_risk = loaders.load_kyc_vs_risk()
        fig = px.bar(kyc_risk, x="risk_tier", y="customer_count", color="kyc_status", barmode="group",
                     title="Risk tier by KYC status")
        plot_with_ai(apply_theme(fig, 380), "dimcust_kyc_risk",
                     "KYC vs risk tier", "KYC incomplete customers in higher tiers.", kyc_risk)

    section("Income, tenure & residence")
    c5, c6 = st.columns(2)
    with c5:
        salary = loaders.load_customer_salary_bins()
        plot_with_ai(bar_chart(salary, x="salary_band", y="customer_count", title="Monthly salary bands"),
                     "dimcust_salary", "Salary bands", "Income distribution.", salary)
    with c6:
        tenure = loaders.load_customer_tenure_bins()
        fig = px.bar(tenure, x="tenure_band", y="customer_count", color="avg_salary",
                     title="Tenure bands (color = avg salary)")
        plot_with_ai(apply_theme(fig, 380), "dimcust_tenure",
                     "Customer tenure", "Relationship length vs average salary.", tenure)

    section("Marital status & sector segmentation")
    c7, c8 = st.columns(2)
    with c7:
        marital = loaders.load_customer_marital()
        plot_with_ai(pie_chart(marital, "marital_status", "customer_count", "Marital status"),
                     "dimcust_marital", "Marital status", "Demographic segmentation.", marital)
    with c8:
        sector = loaders.load_customer_by_sector(12)
        fig = px.bar(sector, x="customer_count", y="sector_description", orientation="h",
                     color="avg_salary", title="Customers by sector")
        plot_with_ai(apply_theme(fig, 400), "dimcust_sector",
                     "Sector segmentation", "Customer count and salary by sector.", sector)

    section("Residence status")
    residence = loaders.load_customer_residence()
    plot_with_ai(bar_chart(residence, x="residence_status", y="customer_count", title="Residence status"),
                 "dimcust_residence", "Residence", "Resident vs non-resident customers.", residence)

    with st.expander("Sample customer records"):
        st.dataframe(loaders.load_table_sample("dim_customer", 300), use_container_width=True, height=400)
