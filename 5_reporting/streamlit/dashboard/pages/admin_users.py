"""DG-only user administration — create accounts, assign roles, deactivate."""
from __future__ import annotations

import streamlit as st

from dashboard import admin_service, loaders
from dashboard.auth import current_user, require_dg_central
from dashboard.i18n import t
from dashboard.roles import ROLE_META
from dashboard.ui import insight, page_header, section


def render() -> None:
    require_dg_central()
    actor = current_user()

    page_header(
        t("admin_title"),
        t("admin_sub"),
        table="PFE_AUTH",
    )
    insight(t("admin_explainer"))

    try:
        users = admin_service.list_users()
    except Exception as exc:
        st.error(f"{t('admin_db_error')}: {exc}")
        st.info(t("admin_seed_hint"))
        return

    # ── Create account ────────────────────────────────────────────────────────
    section(t("admin_create"))
    with st.form("create_user_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            username = st.text_input(t("admin_username"), placeholder="prenom.nom")
            display_name = st.text_input(t("admin_display_name"), placeholder="M. Ahmed Ben Ali")
            password = st.text_input(t("admin_password"), type="password")
        with c2:
            role_code = st.selectbox(
                t("admin_role"),
                options=list(ROLE_META.keys()),
                format_func=lambda c: f"{c} — {ROLE_META[c]['label_en']}",
            )
            dao_area = None
            if ROLE_META[role_code].get("dao_scoped"):
                try:
                    dao_df = loaders.load_dim_dao()
                    areas = sorted(dao_df["dao_area"].dropna().unique().tolist()) if not dao_df.empty else []
                except Exception:
                    areas = ["TUNIS CENTRE", "SFAX", "SOUSSE", "SFAX SUD"]
                dao_area = st.selectbox(t("admin_branch"), options=areas)
        submitted = st.form_submit_button(t("admin_create_btn"), type="primary")
        if submitted:
            if not all([username, display_name, password]):
                st.error(t("admin_missing_fields"))
            elif admin_service.username_exists(username):
                st.error(t("admin_username_taken"))
            else:
                try:
                    admin_service.create_user(
                        username, password, display_name, role_code, dao_area
                    )
                    st.success(t("admin_created").format(user=username.strip().lower()))
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    # ── Manage existing users ───────────────────────────────────────────────────
    section(t("admin_manage"))
    if users.empty:
        st.info(t("admin_no_users"))
        return

    show_inactive = st.checkbox(t("admin_show_inactive"), value=True)
    view = users if show_inactive else users[users["is_active"] == 1]

    st.dataframe(
        view.rename(columns={
            "username": t("admin_username"),
            "display_name": t("admin_display_name"),
            "role_code": t("admin_role"),
            "dao_area": t("admin_branch"),
            "is_active": t("admin_active"),
        }),
        use_container_width=True,
        hide_index=True,
    )

    active_users = users[users["is_active"] == 1]
    if active_users.empty:
        return

    pick = st.selectbox(
        t("admin_select_user"),
        options=active_users["user_id"].tolist(),
        format_func=lambda uid: active_users.loc[
            active_users["user_id"] == uid, "username"
        ].iloc[0],
    )
    row = users.loc[users["user_id"] == pick].iloc[0]

    if row["username"] == actor.get("username"):
        st.caption(t("admin_self_note"))

    tab_role, tab_pw, tab_deact = st.tabs([
        t("admin_tab_role"), t("admin_tab_password"), t("admin_tab_deactivate"),
    ])

    with tab_role:
        new_role = st.selectbox(
            t("admin_new_role"),
            options=list(ROLE_META.keys()),
            index=list(ROLE_META.keys()).index(row["role_code"]),
            format_func=lambda c: f"{c} — {ROLE_META[c]['label_en']}",
            key="admin_role_pick",
        )
        new_dao = row["dao_area"]
        if ROLE_META[new_role].get("dao_scoped"):
            try:
                dao_df = loaders.load_dim_dao()
                areas = sorted(dao_df["dao_area"].dropna().unique().tolist()) if not dao_df.empty else []
            except Exception:
                areas = ["TUNIS CENTRE", "SFAX"]
            default_idx = areas.index(new_dao) if new_dao in areas else 0
            new_dao = st.selectbox(t("admin_branch"), options=areas, index=default_idx, key="admin_dao_pick")
        if st.button(t("admin_save_role"), type="primary"):
            try:
                admin_service.update_user_role(int(pick), new_role, new_dao)
                st.success(t("admin_role_saved"))
                st.cache_data.clear()
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    with tab_pw:
        new_pw = st.text_input(t("admin_new_password"), type="password", key="admin_new_pw")
        if st.button(t("admin_reset_pw")):
            if len(new_pw or "") < 8:
                st.error(t("admin_pw_short"))
            else:
                admin_service.reset_password(int(pick), new_pw)
                st.success(t("admin_pw_reset"))

    with tab_deact:
        st.warning(t("admin_deact_warn").format(user=row["username"]))
        if row["username"] == actor.get("username"):
            st.error(t("admin_cannot_deact_self"))
        elif st.button(t("admin_deactivate_btn"), type="primary"):
            admin_service.set_user_active(int(pick), False)
            st.success(t("admin_deactivated"))
            st.cache_data.clear()
            st.rerun()
