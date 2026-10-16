"""
ATB BI Dashboard — Streamlit front-end for PFE_DWH (SQL Server / SSMS).

Run:  streamlit run app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(APP_DIR / ".env")
except ImportError:
    pass

from dashboard.auth import filter_nav_groups, logout_user, require_login
from dashboard.db import check_connection
from dashboard.i18n import language_selector, t
from dashboard.pages import (
    accounts,
    dg_console,
    alert_inbox,
    comparison,
    compliance,
    dim_currency_page,
    dim_customer_page,
    dim_date_page,
    dim_dao_page,
    dim_industry_page,
    dim_sector_page,
    dim_target_page,
    explorer,
    ml_guide,
    ml_page,
    mlflow_page,
    overview,
    risk,
    studies,
)
from dashboard.ui import (
    ai_sidebar_settings,
    inject_styles,
    resolve_logo,
    sidebar_brand,
    sidebar_connection_card,
    sidebar_footer_meta,
    sidebar_freshness_card,
    sidebar_user_card,
)

_icon = resolve_logo("atb_logo.jpg")
_page_icon = str(_icon) if _icon else "🏦"

st.set_page_config(
    page_title="ATB BI Dashboard | Arab Tunisian Bank",
    page_icon=_page_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_styles()

user = require_login()

NAV_GROUPS: dict[str, dict[str, object]] = {
    "group_executive": {
        "page_overview": overview.render,
        "page_studies": studies.render,
        "page_compliance": compliance.render,
        "page_alert_inbox": alert_inbox.render,
        "page_comparison": comparison.render,
    },
    "group_facts": {
        "page_risk": risk.render,
        "page_accounts": accounts.render,
    },
    "group_dims": {
        "page_customers": dim_customer_page.render,
        "page_dao": dim_dao_page.render,
        "page_calendar": dim_date_page.render,
        "page_sectors": dim_sector_page.render,
        "page_industries": dim_industry_page.render,
        "page_targets": dim_target_page.render,
        "page_currencies": dim_currency_page.render,
    },
}

if user.get("role_code") == "DG_CENTRAL":
    NAV_GROUPS["group_admin"] = {"page_dg_console": dg_console.render}

NAV_GROUPS["group_tools"] = {
    "page_mlflow": mlflow_page.render,
    "page_ml": ml_page.render,
    "page_ml_guide": ml_guide.render,
    "page_explorer": explorer.render,
}

FILTERED_NAV = filter_nav_groups(NAV_GROUPS)

PAGE_LABELS: list[str] = []
PAGE_RENDERERS: dict[str, object] = {}
for group_key, pages in FILTERED_NAV.items():
    group_name = t(group_key)
    for page_key, fn in pages.items():
        label = f"{group_name}  ›  {t(page_key)}"
        PAGE_LABELS.append(label)
        PAGE_RENDERERS[label] = fn

with st.sidebar:
    sidebar_brand()
    sidebar_user_card(user)
    st.divider()

    st.markdown(f"**{t('nav_label')}**")
    choice = st.selectbox(
        "Page",
        PAGE_LABELS,
        label_visibility="collapsed",
        key="main_nav_select",
    )

    st.divider()
    language_selector()

    st.divider()
    sidebar_connection_card()
    sidebar_freshness_card()

    if st.button(t("refresh_data"), use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    st.divider()
    with st.expander(t("ai_explanations"), expanded=False):
        ai_sidebar_settings()

    if st.button(t("logout"), use_container_width=True):
        logout_user()
        st.rerun()

    sidebar_footer_meta()

if not check_connection():
    st.error(
        "Cannot reach SQL Server. Start SSMS/SQL Server and verify ATB_SQL_* env vars."
    )
    st.stop()

PAGE_RENDERERS[choice]()
