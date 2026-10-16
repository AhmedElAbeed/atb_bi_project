"""Login and RBAC — users stored in PFE_AUTH on SQL Server (SSMS)."""
from __future__ import annotations

import hashlib
import os

import streamlit as st

from dashboard.db import sql_query
from dashboard.i18n import lang, t
from dashboard.roles import DEMO_USERS, ROLE_META, allowed_pages, is_dao_scoped, is_read_only
from dashboard.ui import inject_login_background, inject_styles, resolve_logo

AUTH_SCHEMA = os.getenv("ATB_AUTH_SCHEMA", "PFE_AUTH")
PASSWORD_SALT = os.getenv("ATB_AUTH_SALT", "ATB_BI_2026")


def hash_password(password: str) -> str:
    payload = f"{PASSWORD_SALT}:{password}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _auth_tables_exist() -> bool:
    try:
        df = sql_query(
            f"SELECT TOP 1 user_id FROM {AUTH_SCHEMA}.dashboard_users WHERE is_active = 1"
        )
        return True
    except Exception:
        return False


def _authenticate_sql(username: str, password: str) -> dict | None:
    safe_user = username.replace("'", "''")
    pw_hash = hash_password(password)
    q = f"""
    SELECT u.username, u.display_name, u.role_code, u.dao_area,
           r.role_label_fr, r.role_label_en, r.dao_scoped, r.is_read_only
    FROM {AUTH_SCHEMA}.dashboard_users u
    JOIN {AUTH_SCHEMA}.dashboard_roles r ON u.role_code = r.role_code
    WHERE u.username = N'{safe_user}'
      AND u.password_hash = N'{pw_hash}'
      AND u.is_active = 1
    """
    df = sql_query(q)
    if df.empty:
        return None
    row = df.iloc[0]
    pages_df = sql_query(
        f"SELECT page_key FROM {AUTH_SCHEMA}.dashboard_role_pages "
        f"WHERE role_code = N'{row['role_code'].replace(chr(39), chr(39)+chr(39))}'"
    )
    page_set = set(pages_df["page_key"].tolist()) if not pages_df.empty else allowed_pages(row["role_code"])
    if row["role_code"] == "DG_CENTRAL":
        page_set = None  # full access including admin console pages
    return {
        "username": row["username"],
        "display_name": row["display_name"],
        "role_code": row["role_code"],
        "role_label": row["role_label_fr"] if lang() == "fr" else row["role_label_en"],
        "dao_area": row["dao_area"] if row["dao_area"] and str(row["dao_area"]) != "nan" else None,
        "dao_scoped": bool(row["dao_scoped"]),
        "read_only": bool(row["is_read_only"]),
        "allowed_pages": page_set,
    }


def _authenticate_demo(username: str, password: str) -> dict | None:
    entry = DEMO_USERS.get(username.strip().lower())
    if not entry or entry["password"] != password:
        return None
    role = entry["role_code"]
    meta = ROLE_META.get(role, {})
    pages = allowed_pages(role)
    return {
        "username": username.strip().lower(),
        "display_name": entry["display_name"],
        "role_code": role,
        "role_label": meta.get("label_fr" if lang() == "fr" else "label_en", role),
        "dao_area": entry.get("dao_area"),
        "dao_scoped": is_dao_scoped(role),
        "read_only": is_read_only(role),
        "allowed_pages": pages,
    }


def authenticate(username: str, password: str) -> dict | None:
    username = username.strip().lower()
    if _auth_tables_exist():
        user = _authenticate_sql(username, password)
        if user:
            return user
    return _authenticate_demo(username, password)


def login_user(user: dict) -> None:
    st.session_state["auth_user"] = user
    st.session_state["auth_dao_scope"] = user.get("dao_area") if user.get("dao_scoped") else None


def logout_user() -> None:
    for key in ("auth_user", "auth_dao_scope", "main_nav_select"):
        st.session_state.pop(key, None)


def current_user() -> dict | None:
    return st.session_state.get("auth_user")


def is_dg_central() -> bool:
    user = current_user()
    return bool(user and user.get("role_code") == "DG_CENTRAL")


def require_dg_central() -> dict:
    """Gate admin-only pages — DG Direction Générale only."""
    user = current_user()
    if not user:
        st.warning(t("admin_login_required"))
        st.stop()
    if user.get("role_code") != "DG_CENTRAL":
        st.error(t("admin_forbidden"))
        st.stop()
    return user


def require_login() -> dict:
    user = current_user()
    if user:
        return user

    inject_styles()
    inject_login_background()
    logo = resolve_logo("atb_background_logo.png")
    col_l, col_m, col_r = st.columns([1, 1.2, 1])
    with col_m:
        st.markdown('<div class="login-panel">', unsafe_allow_html=True)
        if logo:
            st.image(str(logo), use_container_width=True)
        st.markdown(f"### {t('login_title')}")
        st.caption(t("login_sub"))

        with st.form("login_form"):
            username = st.text_input(t("login_username"), placeholder="dg.central")
            password = st.text_input(t("login_password"), type="password")
            submitted = st.form_submit_button(t("login_submit"), type="primary", use_container_width=True)

        if submitted:
            if not username or not password:
                st.error(t("login_missing"))
            else:
                user = authenticate(username, password)
                if user:
                    login_user(user)
                    st.rerun()
                else:
                    st.error(t("login_failed"))

        with st.expander(t("login_demo_accounts")):
            st.markdown(t("login_demo_help"))
            for uname, info in DEMO_USERS.items():
                st.code(f"{uname}  /  {info['password']}  →  {info['role_code']}", language=None)

        st.caption(t("login_sql_hint"))
        st.markdown("</div>", unsafe_allow_html=True)
    st.stop()


def page_allowed(page_key: str) -> bool:
    user = current_user()
    if not user:
        return False
    allowed = user.get("allowed_pages")
    if allowed is None:
        return True
    return page_key in allowed


# DG-only pages — always visible for Direction Générale even if SQL seed is stale
DG_ONLY_PAGES = frozenset({"page_dg_console", "page_admin", "page_airbyte"})


def filter_nav_groups(nav_groups: dict) -> dict:
    user = current_user()
    if not user:
        return {}
    allowed = user.get("allowed_pages")
    is_dg = user.get("role_code") == "DG_CENTRAL"
    filtered: dict = {}
    for group_key, pages in nav_groups.items():
        sub = {}
        for page_key, fn in pages.items():
            if is_dg and page_key in DG_ONLY_PAGES:
                sub[page_key] = fn
            elif allowed is None or page_key in allowed:
                sub[page_key] = fn
        if sub:
            filtered[group_key] = sub
    return filtered


def dao_filter_sql(dao_alias: str = "d") -> str:
    """SQL AND clause for branch-scoped roles."""
    scope = st.session_state.get("auth_dao_scope")
    if not scope:
        return ""
    safe = str(scope).replace("'", "''")
    return f" AND {dao_alias}.dao_area = N'{safe}'"


def dao_filter_ml_sql() -> str:
    scope = st.session_state.get("auth_dao_scope")
    if not scope:
        return ""
    safe = str(scope).replace("'", "''")
    return f" AND p.dao_area = N'{safe}'"


def can_export() -> bool:
    user = current_user()
    return not bool(user and user.get("read_only"))
