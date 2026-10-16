"""DG-only Airbyte ingestion console — syncs, health, Docker images."""
from __future__ import annotations

import streamlit as st

from dashboard import airbyte_service, loaders
from dashboard.auth import require_dg_central
from dashboard.i18n import t
from dashboard.ui import insight, kpi_row, page_header, section


def render() -> None:
    require_dg_central()

    page_header(
        t("airbyte_title"),
        t("airbyte_sub"),
        table="PFE_ODS",
    )
    insight(t("airbyte_explainer"))

    ui_url = airbyte_service.airbyte_ui_url()
    api_url = airbyte_service.airbyte_base_url()
    st.link_button(t("airbyte_open_ui"), ui_url, use_container_width=False)

    # ── Health & Docker ─────────────────────────────────────────────────────────
    section(t("airbyte_health"))
    health = airbyte_service.check_health()
    kpi_row([
        (t("airbyte_api"), t("airbyte_ok") if health.get("reachable") else t("airbyte_down"),
         "#22c55e" if health.get("reachable") else "#ef4444"),
        (t("airbyte_api_url"), api_url.split("//")[-1][:28], None),
        (t("airbyte_ui_url"), ui_url.split("//")[-1][:28], None),
    ])
    if health.get("error"):
        st.caption(f"API: {health['error']}")

    section(t("airbyte_docker"))
    images = airbyte_service.list_docker_airbyte_images()
    if images:
        st.code("\n".join(images), language=None)
    else:
        st.caption(t("airbyte_no_docker"))

    st.markdown(t("airbyte_docker_hint"))

    # ── Connections & sync ────────────────────────────────────────────────────────
    section(t("airbyte_connections"))
    configured = airbyte_service.configured_connection_ids()
    if configured:
        st.caption(t("airbyte_configured_ids").format(n=len(configured)))

    if not health.get("reachable"):
        st.warning(t("airbyte_unreachable"))
        return

    try:
        connections = airbyte_service.list_connections()
    except Exception as exc:
        st.error(f"{t('airbyte_list_fail')}: {exc}")
        return

    if not connections:
        st.info(t("airbyte_no_connections"))
        return

    rows = []
    for conn in connections:
        rows.append({
            "name": conn.get("name", "—"),
            "connectionId": conn.get("connectionId", "")[:12] + "…",
            "status": conn.get("status", "—"),
            "schedule": (conn.get("schedule") or {}).get("scheduleType", "manual"),
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)

    conn_names = {c.get("connectionId"): c.get("name", c.get("connectionId")) for c in connections}
    pick_id = st.selectbox(
        t("airbyte_pick_connection"),
        options=list(conn_names.keys()),
        format_func=lambda cid: conn_names.get(cid, cid),
    )

    if st.button(t("airbyte_trigger_sync"), type="primary"):
        try:
            job_id = airbyte_service.trigger_sync(pick_id)
            st.session_state["airbyte_last_job"] = job_id
            st.success(t("airbyte_sync_started").format(job=job_id))
        except Exception as exc:
            st.error(str(exc))

    last_job = st.session_state.get("airbyte_last_job")
    if last_job:
        try:
            job = airbyte_service.get_job(str(last_job))
            status = (job.get("job") or {}).get("status", "unknown")
            st.metric(t("airbyte_last_job"), str(last_job), delta=status)
        except Exception:
            pass

    # ── Warehouse freshness after ingestion ─────────────────────────────────────
    section(t("airbyte_freshness"))
    try:
        fresh = loaders.load_etl_freshness()
        kpi_row([
            (t("last_load"), str(fresh.get("last_account_load", "—"))[:19], None),
            (t("scoring_date"), str(fresh.get("last_scoring_date", "—"))[:19], None),
        ])
    except Exception as exc:
        st.caption(f"Freshness: {exc}")

    st.markdown(t("airbyte_pipeline_note"))
