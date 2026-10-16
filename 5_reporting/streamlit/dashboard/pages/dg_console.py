"""DG Control Center — users, Docker stacks, Airflow DAGs, Airbyte (DG_CENTRAL only)."""
from __future__ import annotations

import os

import streamlit as st

from dashboard import admin_service, airbyte_service, docker_ops_service, loaders
from dashboard.auth import current_user, require_dg_central
from dashboard.i18n import t
from dashboard.roles import ROLE_META
from dashboard.ui import insight, kpi_row, page_header, section


def _dao_areas() -> list[str]:
    try:
        dao_df = loaders.load_dim_dao()
        if not dao_df.empty:
            return sorted(dao_df["dao_area"].dropna().unique().tolist())
    except Exception:
        pass
    return ["TUNIS CENTRE", "SFAX", "SOUSSE", "NABEUL", "BIZERTE"]


def _tab_users() -> None:
    actor = current_user()
    try:
        users = admin_service.list_users()
    except Exception as exc:
        st.error(f"{t('admin_db_error')}: {exc}")
        st.info(t("admin_seed_hint"))
        return

    section(t("admin_create"))
    with st.form("dg_create_user", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            username = st.text_input(t("admin_username"), placeholder="prenom.nom")
            display_name = st.text_input(t("admin_display_name"))
            password = st.text_input(t("admin_password"), type="password")
        with c2:
            role_code = st.selectbox(
                t("admin_role"),
                options=list(ROLE_META.keys()),
                format_func=lambda c: f"{c} — {ROLE_META[c]['label_en']}",
            )
            dao_area = None
            if ROLE_META[role_code].get("dao_scoped"):
                dao_area = st.selectbox(t("admin_branch"), options=_dao_areas())
        if st.form_submit_button(t("admin_create_btn"), type="primary"):
            if not all([username, display_name, password]):
                st.error(t("admin_missing_fields"))
            elif admin_service.username_exists(username):
                st.error(t("admin_username_taken"))
            else:
                try:
                    admin_service.create_user(username, password, display_name, role_code, dao_area)
                    st.success(t("admin_created").format(user=username.strip().lower()))
                    st.cache_data.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    section(t("admin_manage"))
    if users.empty:
        st.info(t("admin_no_users"))
        return

    st.dataframe(
        users[["username", "display_name", "role_code", "dao_area", "is_active", "created_at"]],
        use_container_width=True,
        hide_index=True,
    )

    pick = st.selectbox(
        t("admin_select_user"),
        options=users["user_id"].tolist(),
        format_func=lambda uid: f"{users.loc[users['user_id'] == uid, 'username'].iloc[0]}"
        f" ({'active' if users.loc[users['user_id'] == uid, 'is_active'].iloc[0] else 'inactive'})",
    )
    row = users.loc[users["user_id"] == pick].iloc[0]
    is_self = row["username"] == actor.get("username")

    t1, t2, t3, t4, t5 = st.tabs([
        t("admin_tab_profile"), t("admin_tab_role"), t("admin_tab_password"),
        t("admin_tab_status"), t("admin_tab_delete"),
    ])

    with t1:
        new_name = st.text_input(t("admin_display_name"), value=row["display_name"])
        if st.button(t("admin_save_profile"), type="primary"):
            admin_service.update_display_name(int(pick), new_name)
            st.success(t("admin_profile_saved"))
            st.cache_data.clear()
            st.rerun()

    with t2:
        new_role = st.selectbox(
            t("admin_new_role"),
            options=list(ROLE_META.keys()),
            index=list(ROLE_META.keys()).index(row["role_code"]),
            format_func=lambda c: f"{c} — {ROLE_META[c]['label_en']}",
        )
        new_dao = row["dao_area"]
        if ROLE_META[new_role].get("dao_scoped"):
            areas = _dao_areas()
            idx = areas.index(new_dao) if new_dao in areas else 0
            new_dao = st.selectbox(t("admin_branch"), options=areas, index=idx)
        if st.button(t("admin_save_role"), type="primary"):
            admin_service.update_user_role(int(pick), new_role, new_dao)
            st.success(t("admin_role_saved"))
            st.cache_data.clear()
            st.rerun()

    with t3:
        new_pw = st.text_input(t("admin_new_password"), type="password")
        if st.button(t("admin_reset_pw")):
            if len(new_pw or "") < 8:
                st.error(t("admin_pw_short"))
            else:
                admin_service.reset_password(int(pick), new_pw)
                st.success(t("admin_pw_reset"))

    with t4:
        active = bool(row["is_active"])
        st.metric(t("admin_active"), t("admin_yes") if active else t("admin_no"))
        if is_self:
            st.warning(t("admin_self_note"))
        elif active:
            if st.button(t("admin_deactivate_btn"), type="primary"):
                admin_service.set_user_active(int(pick), False)
                st.success(t("admin_deactivated"))
                st.cache_data.clear()
                st.rerun()
        else:
            if st.button(t("admin_activate_btn"), type="primary"):
                admin_service.set_user_active(int(pick), True)
                st.success(t("admin_activated"))
                st.cache_data.clear()
                st.rerun()

    with t5:
        st.error(t("admin_delete_warn").format(user=row["username"]))
        if is_self:
            st.warning(t("admin_cannot_delete_self"))
        elif st.button(t("admin_delete_btn"), type="primary"):
            admin_service.delete_user(int(pick))
            st.success(t("admin_deleted"))
            st.cache_data.clear()
            st.rerun()


def _tab_platform() -> None:
    section(t("ops_self_health"))
    self_h = docker_ops_service.streamlit_self_health()
    kpi_row([
        (t("ops_host"), self_h["hostname"], None),
        (t("ops_docker_cli"), t("ops_ok") if self_h["docker_ok"] else t("ops_no"), None),
        ("MLflow", (self_h.get("mlflow_uri") or "—")[-40:], None),
    ])
    if not self_h["docker_ok"]:
        st.warning(t("ops_docker_sock_hint"))

    section(t("ops_containers"))
    if st.button(t("ops_refresh"), key="refresh_containers"):
        st.rerun()
    containers = docker_ops_service.list_atb_containers()
    if not containers:
        st.info(t("ops_no_containers"))
    else:
        st.dataframe(containers, use_container_width=True, hide_index=True)
        pick = st.selectbox(t("ops_pick_container"), [c["name"] for c in containers])
        c1, c2, c3 = st.columns(3)
        for col, action in zip((c1, c2, c3), ("start", "stop", "restart")):
            with col:
                if st.button(action.capitalize(), key=f"docker_{action}_{pick}"):
                    ok, msg = docker_ops_service.container_action(pick, action)
                    (st.success if ok else st.error)(msg)
                    st.rerun()

    section(t("ops_stacks"))
    for key, meta in docker_ops_service.STACKS.items():
        st.markdown(f"**{meta['label']}**")
        ui = docker_ops_service.stack_browser_url(key)
        st.caption(f"UI: {ui} · compose: `{meta['compose']}`")
        b1, b2, b3, b4 = st.columns(4)
        with b1:
            if st.button(t("ops_stack_up"), key=f"up_{key}"):
                ok, msg = docker_ops_service.compose_action(key, "up")
                (st.success if ok else st.error)(msg[:500])
        with b2:
            if st.button(t("ops_stack_down"), key=f"down_{key}"):
                ok, msg = docker_ops_service.compose_action(key, "down")
                (st.success if ok else st.error)(msg[:500])
        with b3:
            if st.button(t("ops_stack_restart"), key=f"rst_{key}"):
                ok, msg = docker_ops_service.compose_action(key, "restart")
                (st.success if ok else st.error)(msg[:500])
        with b4:
            st.link_button(t("ops_open_ui"), ui, key=f"link_{key}")

    section(t("ops_images"))
    imgs = docker_ops_service.list_docker_images()
    if imgs:
        st.dataframe(
            [{"image": img} for img in imgs],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption(t("ops_no_images"))


def _tab_airflow() -> None:
    ui = docker_ops_service.airflow_ui_url()
    st.link_button(t("ops_open_airflow"), ui)

    health = docker_ops_service.airflow_health()
    kpi_row([
        (t("ops_airflow_api"), t("ops_ok") if health.get("reachable") else t("ops_down"),
         "#22c55e" if health.get("reachable") else "#ef4444"),
        ("URL", health.get("url", "—").split("//")[-1][:30], None),
    ])
    if health.get("error"):
        st.caption(health["error"])
    if not health.get("reachable"):
        st.warning(t("ops_airflow_down_hint"))
        return

    section(t("ops_airflow_dags"))
    try:
        dags = docker_ops_service.airflow_list_dags()
    except Exception as exc:
        st.error(str(exc))
        return

    if not dags:
        st.info(t("ops_no_dags"))
        return

    st.dataframe(dags, use_container_width=True, hide_index=True)
    dag_ids = [d["dag_id"] for d in dags if d.get("dag_id")]
    pick = st.selectbox(t("ops_pick_dag"), dag_ids)

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button(t("ops_trigger_dag"), type="primary"):
            try:
                run = docker_ops_service.airflow_trigger_dag(pick)
                st.success(t("ops_dag_triggered").format(id=run.get("dag_run_id", "—")))
            except Exception as exc:
                st.error(str(exc))
    with c2:
        if st.button(t("ops_pause_dag")):
            docker_ops_service.airflow_pause_dag(pick, True)
            st.rerun()
    with c3:
        if st.button(t("ops_unpause_dag")):
            docker_ops_service.airflow_pause_dag(pick, False)
            st.rerun()

    section(t("ops_dag_runs"))
    try:
        runs = docker_ops_service.airflow_list_runs(pick)
        st.dataframe(runs, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.caption(str(exc))


def _tab_airbyte() -> None:
    ui = airbyte_service.airbyte_ui_url()
    st.link_button(t("airbyte_open_ui"), ui)

    with st.expander(t("airbyte_api_creds"), expanded=False):
        st.caption(t("airbyte_api_creds_help"))
        ab_client_id = st.text_input("Client ID", value=os.getenv("ATB_AIRBYTE_CLIENT_ID", ""), type="default")
        ab_client_secret = st.text_input("Client Secret", value=os.getenv("ATB_AIRBYTE_CLIENT_SECRET", ""), type="password")

    health = airbyte_service.check_health()
    kpi_row([
        (t("airbyte_api"), t("airbyte_ok") if health.get("reachable") else t("airbyte_down"), None),
        ("API", airbyte_service.airbyte_base_url().split("//")[-1][:28], None),
    ])
    if not health.get("reachable"):
        st.warning(t("airbyte_unreachable"))
        return

    try:
        connections = airbyte_service.list_connections(
            client_id=ab_client_id or None,
            client_secret=ab_client_secret or None,
        )
    except Exception as exc:
        st.error(str(exc))
        configured = airbyte_service.configured_connection_ids()
        if configured:
            st.caption(t("airbyte_configured_ids").format(n=len(configured)))
        return
    if not connections:
        st.info(t("airbyte_no_connections"))
        return
    st.dataframe(
        [{"name": c.get("name"), "id": c.get("connectionId", "")[:16], "status": c.get("status")}
         for c in connections],
        use_container_width=True,
        hide_index=True,
    )
    conn_map = {c["connectionId"]: c.get("name", c["connectionId"]) for c in connections}
    conn_id = st.selectbox(t("airbyte_pick_connection"), list(conn_map.keys()), format_func=lambda x: conn_map[x])
    if st.button(t("airbyte_trigger_sync"), type="primary"):
        try:
            job = airbyte_service.trigger_sync(
                conn_id,
                client_id=ab_client_id or None,
                client_secret=ab_client_secret or None,
            )
            st.success(t("airbyte_sync_started").format(job=job))
        except Exception as exc:
            st.error(str(exc))


def render() -> None:
    require_dg_central()
    page_header(t("dg_console_title"), t("dg_console_sub"), table="PFE_AUTH + Docker")
    insight(t("dg_console_explainer"))

    tab_users, tab_platform, tab_airflow, tab_airbyte = st.tabs([
        t("dg_tab_users"),
        t("dg_tab_platform"),
        t("dg_tab_airflow"),
        t("dg_tab_airbyte"),
    ])
    with tab_users:
        _tab_users()
    with tab_platform:
        _tab_platform()
    with tab_airflow:
        _tab_airflow()
    with tab_airbyte:
        _tab_airbyte()
