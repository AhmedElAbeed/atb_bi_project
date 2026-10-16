"""Shared UI helpers and styling — ATB brand theme."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import streamlit as st

from dashboard.config import (
    ATB_BG_CARD,
    ATB_BG_DARK,
    ATB_BG_SIDEBAR,
    ATB_PRIMARY,
    ATB_PRIMARY_DARK,
    ATB_SILVER,
    ATB_SILVER_DIM,
)
from dashboard.i18n import language_selector, t

_ASSETS = Path(__file__).resolve().parents[1] / "assets"
_REPO_LOGO = Path(__file__).resolve().parents[3] / "logo"
_DOCKER_LOGO = Path("/app/logo")


def resolve_logo(filename: str) -> Path | None:
    for base in (_ASSETS, _REPO_LOGO, _DOCKER_LOGO):
        path = base / filename
        if path.is_file():
            return path
    return None


def resolve_asset(filename: str) -> Path | None:
    """Resolve branding / background assets (logo dir + streamlit assets)."""
    return resolve_logo(filename)


def inject_login_background() -> None:
    """Full-screen bank building photo behind the login form."""
    building = resolve_asset("atb_building.jpg")
    if not building:
        return
    # Streamlit serves local media via app static path when possible; use base64 for reliability.
    import base64

    b64 = base64.b64encode(building.read_bytes()).decode("ascii")
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: linear-gradient(rgba(8, 4, 6, 0.72), rgba(8, 4, 6, 0.88)),
                url("data:image/jpeg;base64,{b64}") center center / cover no-repeat fixed !important;
        }}
        [data-testid="stHeader"] {{ background: rgba(8, 4, 6, 0.55) !important; }}
        .login-panel {{
            background: rgba(18, 8, 10, 0.82);
            border: 1px solid rgba(158, 27, 50, 0.45);
            border-radius: 16px;
            padding: 1.5rem 1.75rem 1.25rem 1.75rem;
            box-shadow: 0 24px 48px rgba(0, 0, 0, 0.55);
            backdrop-filter: blur(10px);
        }}
        .login-panel .stImage {{ margin-bottom: 0.5rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def inject_styles() -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
        :root {{
            --atb-primary: {ATB_PRIMARY};
            --atb-primary-dark: {ATB_PRIMARY_DARK};
            --atb-silver: {ATB_SILVER};
            --atb-bg: {ATB_BG_DARK};
            --atb-card: {ATB_BG_CARD};
        }}
        html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
        .stApp {{ background: {ATB_BG_DARK}; }}
        .block-container {{
            padding-top: 5.25rem !important;
            max-width: 1480px;
        }}
        .page-header-wrap {{
            margin-top: 0.35rem;
            padding-top: 0.5rem;
            margin-bottom: 0.25rem;
        }}
        .dash-title {{
            font-size: 2.15rem; font-weight: 800; letter-spacing: -0.03em;
            background: linear-gradient(90deg, {ATB_PRIMARY} 0%, #C42D48 45%, {ATB_SILVER} 100%);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            margin-bottom: 0.15rem;
        }}
        .dash-subtitle {{ color: {ATB_SILVER_DIM}; font-size: 0.98rem; margin-bottom: 1.1rem; line-height: 1.5; }}
        .section-title {{
            font-size: 1.12rem; font-weight: 700; color: {ATB_SILVER};
            border-left: 4px solid {ATB_PRIMARY};
            padding-left: 0.7rem;
            margin: 1.4rem 0 0.85rem 0;
        }}
        .kpi-card {{
            background: linear-gradient(145deg, rgba(30,16,18,0.95), rgba(10,10,10,0.98));
            border: 1px solid rgba(158,27,50,0.35);
            border-radius: 14px; padding: 1rem 1rem; text-align: center;
            box-shadow: 0 10px 28px rgba(0,0,0,0.35);
            min-height: 88px;
        }}
        .kpi-value {{ font-size: 1.65rem; font-weight: 800; color: {ATB_PRIMARY}; line-height: 1.15; }}
        .kpi-label {{
            font-size: 0.68rem; color: {ATB_SILVER_DIM};
            text-transform: uppercase; letter-spacing: 0.07em; margin-top: 0.4rem;
        }}
        .insight-box {{
            background: rgba(158,27,50,0.1);
            border: 1px solid rgba(158,27,50,0.35);
            border-radius: 12px; padding: 0.9rem 1.1rem;
            color: #e8e8e8; font-size: 0.92rem; line-height: 1.55;
            margin: 0.5rem 0 1rem 0;
        }}
        .insight-box strong {{ color: {ATB_SILVER}; }}
        .action-bar {{
            background: linear-gradient(90deg, rgba(158,27,50,0.2), rgba(192,192,192,0.08));
            border: 1px solid rgba(158,27,50,0.45);
            border-radius: 12px; padding: 0.85rem 1rem; margin: 0.75rem 0 1.25rem 0;
        }}
        .action-bar-title {{
            font-weight: 700; color: {ATB_SILVER}; font-size: 0.9rem; margin-bottom: 0.35rem;
        }}
        .table-badge {{
            display: inline-block; background: rgba(158,27,50,0.2);
            border: 1px solid rgba(192,192,192,0.35); border-radius: 6px;
            padding: 0.15rem 0.55rem; font-size: 0.78rem; color: {ATB_SILVER};
            font-family: monospace; margin-right: 0.35rem;
        }}
        .sidebar-logo-wrap {{
            border-radius: 10px; overflow: hidden;
            margin-bottom: 0.5rem;
            border: 1px solid rgba(158,27,50,0.4);
        }}
        .sidebar-brand-sub {{
            font-size: 0.78rem; color: {ATB_SILVER_DIM};
            margin-bottom: 0.5rem; text-align: center;
        }}
        .sidebar-card {{
            background: rgba(26,16,18,0.85);
            border: 1px solid rgba(158,27,50,0.3);
            border-radius: 10px;
            padding: 0.65rem 0.75rem;
            margin: 0.35rem 0;
        }}
        .sidebar-card-title {{
            font-size: 0.68rem; font-weight: 700; color: {ATB_SILVER};
            text-transform: uppercase; letter-spacing: 0.08em;
            margin-bottom: 0.35rem;
        }}
        .sidebar-kv {{ font-size: 0.8rem; color: #e8e8e8; line-height: 1.55; }}
        .sidebar-kv span {{ color: {ATB_SILVER_DIM}; }}
        .lang-label {{
            font-size: 0.72rem; font-weight: 700; color: {ATB_SILVER_DIM};
            text-transform: uppercase; letter-spacing: 0.08em;
            margin-bottom: 0.25rem;
        }}
        .lang-btn-wrap img {{
            vertical-align: middle; margin-right: 4px;
            border-radius: 2px; box-shadow: 0 1px 2px rgba(0,0,0,0.4);
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {ATB_BG_SIDEBAR} 0%, {ATB_BG_DARK} 100%);
            border-right: 1px solid rgba(158,27,50,0.25);
        }}
        [data-testid="stSidebar"] .stSelectbox label {{ font-weight: 600; color: {ATB_SILVER}; }}
        div.stButton > button[kind="primary"] {{
            background: {ATB_PRIMARY} !important;
            border-color: {ATB_PRIMARY_DARK} !important;
        }}
        div.stButton > button[kind="primary"]:hover {{
            background: {ATB_PRIMARY_DARK} !important;
        }}
        #MainMenu {{ visibility: hidden; }}
        footer {{ visibility: hidden; }}
        header[data-testid="stHeader"] {{
            background: rgba(18,8,10,0.95);
            backdrop-filter: blur(8px);
            border-bottom: 1px solid rgba(158,27,50,0.3);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def page_header(title: str, subtitle: str, table: str | None = None) -> None:
    badge_html = ""
    if table:
        badge = table if table.startswith("PFE_") or " + " in table else f"PFE_DWH.{table}"
        badge_html = f'<span class="table-badge">{badge}</span>'
    st.markdown(
        f'<div class="page-header-wrap">'
        f'<div class="dash-title">{title}</div>{badge_html}'
        f'<div class="dash-subtitle">{subtitle}</div></div>',
        unsafe_allow_html=True,
    )


def section(title: str) -> None:
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


def insight(text: str) -> None:
    st.markdown(f'<div class="insight-box">{text}</div>', unsafe_allow_html=True)


def action_bar(title: str) -> None:
    st.markdown(
        f'<div class="action-bar"><div class="action-bar-title">{title}</div></div>',
        unsafe_allow_html=True,
    )


def kpi_row(items: list[tuple[str, str, str | None]]) -> None:
    cols = st.columns(len(items))
    for col, (label, value, color) in zip(cols, items):
        color_style = f' style="color:{color};"' if color else ""
        with col:
            st.markdown(
                f"""
                <div class="kpi-card">
                    <div class="kpi-value"{color_style}>{value}</div>
                    <div class="kpi-label">{label}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def plot_with_ai(
    fig,
    chart_key: str,
    title: str,
    business_context: str,
    df=None,
) -> None:
    from dashboard.ai_explainer import panels_enabled, render_ai_panel

    if panels_enabled():
        c_chart, c_ai = st.columns([3.4, 1.6])
        with c_chart:
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{chart_key}")
        with c_ai:
            render_ai_panel(chart_key, title, business_context, df)
    else:
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{chart_key}")


def ai_sidebar_settings() -> None:
    default_key = os.getenv("GEMINI_API_KEY", os.getenv("ATB_GEMINI_API_KEY", ""))
    key = st.text_input(
        "Gemini API key",
        value=st.session_state.get("gemini_api_key", default_key),
        type="password",
        help="Google AI Studio key. Set GEMINI_API_KEY in .env to persist.",
    )
    if key:
        st.session_state["gemini_api_key"] = key
    st.session_state["ai_explain_enabled"] = st.toggle(
        t("show_ai"),
        value=st.session_state.get("ai_explain_enabled", True),
    )
    st.session_state["ai_auto_explain"] = st.toggle(
        t("auto_explain"),
        value=st.session_state.get("ai_auto_explain", False),
    )


def sidebar_user_card(user: dict) -> None:
    scope = ""
    if user.get("dao_scoped") and user.get("dao_area"):
        scope = f"<br/><span>{t('branch_scope')}:</span> <b>{user['dao_area']}</b>"
    ro = f" · {t('read_only_badge')}" if user.get("read_only") else ""
    st.markdown(
        f'<div class="sidebar-card">'
        f'<div class="sidebar-card-title">{t("logged_in_as")}</div>'
        f'<div class="sidebar-kv">'
        f'<b>{user.get("display_name", user.get("username"))}</b><br/>'
        f'<span>{t("role")}:</span> {user.get("role_label", user.get("role_code"))}{ro}'
        f'{scope}</div></div>',
        unsafe_allow_html=True,
    )


def sidebar_brand() -> None:
    bg_logo = resolve_logo("atb_background_logo.png")
    if bg_logo:
        st.markdown('<div class="sidebar-logo-wrap">', unsafe_allow_html=True)
        st.image(str(bg_logo), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown(f'<div class="sidebar-brand-sub">{t("app_caption")}</div>', unsafe_allow_html=True)


def sidebar_connection_card() -> None:
    server = os.getenv("ATB_SQL_SERVER", "DESKTOP-B0PDEI7")
    database = os.getenv("ATB_SQL_DATABASE", "ATB_BI")
    if check_connection_cached():
        st.markdown(
            f'<div class="sidebar-card">'
            f'<div class="sidebar-card-title">● {t("connected")}</div>'
            f'<div class="sidebar-kv"><span>{t("server")}:</span> {server}<br/>'
            f'<span>{t("database")}:</span> {database}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        st.error(t("connection_failed"))
        st.caption("Set ATB_SQL_* in .env or DATABASE_URL")


def sidebar_freshness_card() -> None:
    if not check_connection_cached():
        return
    try:
        from dashboard import loaders
        fresh = loaders.load_etl_freshness()
        st.markdown(
            f'<div class="sidebar-card">'
            f'<div class="sidebar-card-title">{t("freshness")}</div>'
            f'<div class="sidebar-kv">'
            f'<span>{t("last_load")}:</span> {fresh.get("last_account_load", "—")}<br/>'
            f'<span>{t("scoring_date")}:</span> {fresh.get("last_scoring_date", "—")}'
            f'</div></div>',
            unsafe_allow_html=True,
        )
    except Exception:
        pass


def sidebar_footer_meta() -> None:
    st.caption(f"{t('schema')}: **PFE_DWH** · **PFE_ML**")
    st.caption(f"{t('updated')}: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


@st.cache_data(ttl=60, show_spinner=False)
def check_connection_cached() -> bool:
    from dashboard.db import check_connection
    return check_connection()
