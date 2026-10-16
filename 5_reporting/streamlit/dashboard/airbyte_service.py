"""Airbyte API helpers — Airbyte 2.x (port 8000, public API + bearer token)."""
from __future__ import annotations

import os
import time
from typing import Any

import requests

_TOKEN_CACHE: dict[str, Any] = {"token": "", "expires_at": 0.0}


def _internal_base() -> str:
    return os.getenv("ATB_AIRBYTE_URL", "http://host.docker.internal:8000").rstrip("/")


def airbyte_base_url() -> str:
    return _internal_base()


def airbyte_ui_url() -> str:
    default = "http://localhost:8000/workspaces/524f3fec-4d3d-4ed4-b4a3-93243c05cbc4/connections"
    return os.getenv("ATB_AIRBYTE_UI_URL", default).replace("host.docker.internal", "localhost")


def airbyte_workspace_id() -> str:
    return os.getenv("ATB_AIRBYTE_WORKSPACE_ID", "524f3fec-4d3d-4ed4-b4a3-93243c05cbc4")


def _public_api(path: str) -> str:
    return f"{_internal_base()}/api/public/v1/{path.lstrip('/')}"


def _legacy_api(path: str) -> str:
    return f"{_internal_base()}/api/v1/{path.lstrip('/')}"


def _get_token(client_id: str | None = None, client_secret: str | None = None) -> str | None:
    cid = (client_id or os.getenv("ATB_AIRBYTE_CLIENT_ID", "")).strip()
    secret = (client_secret or os.getenv("ATB_AIRBYTE_CLIENT_SECRET", "")).strip()
    if not cid or not secret:
        return None

    now = time.time()
    if _TOKEN_CACHE["token"] and _TOKEN_CACHE["expires_at"] > now + 30:
        return _TOKEN_CACHE["token"]

    try:
        r = requests.post(
            _public_api("applications/token"),
            json={"client_id": cid, "client_secret": secret, "grant-type": "client_credentials"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token", "")
        ttl = int(data.get("expires_in", 900))
        _TOKEN_CACHE["token"] = token
        _TOKEN_CACHE["expires_at"] = now + ttl
        return token or None
    except requests.RequestException:
        return None


def _auth_headers(client_id: str | None = None, client_secret: str | None = None) -> dict[str, str]:
    token = _get_token(client_id, client_secret)
    if token:
        return {"Authorization": f"Bearer {token}", "accept": "application/json"}
    return {"accept": "application/json"}


def check_health() -> dict[str, Any]:
    base = _internal_base()
    for path in ("/api/v1/health", "/health"):
        try:
            r = requests.get(f"{base}{path}", timeout=5)
            if r.status_code == 200:
                payload = r.json() if r.content else {}
                return {"reachable": True, "url": base, "health": payload}
        except requests.RequestException:
            continue
    return {"reachable": False, "url": base, "error": "Health check failed on port 8000"}


def list_connections(client_id: str | None = None, client_secret: str | None = None) -> list[dict[str, Any]]:
    ws = airbyte_workspace_id()
    headers = _auth_headers(client_id, client_secret)

    # Airbyte 2.x public API
    try:
        r = requests.get(
            _public_api("connections"),
            params={"workspaceIds": ws, "limit": 100},
            headers=headers,
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            items = data.get("data") or data.get("connections") or []
            return [
                {
                    "name": c.get("name", "—"),
                    "connectionId": c.get("connectionId") or c.get("id"),
                    "status": c.get("status", "—"),
                }
                for c in items
            ]
    except requests.RequestException:
        pass

    # Legacy OSS API (no auth on some installs)
    try:
        r = requests.get(_legacy_api("connections"), headers=headers, timeout=15)
        r.raise_for_status()
        return r.json().get("connections", [])
    except requests.RequestException as exc:
        raise RuntimeError(
            f"{exc}. Set ATB_AIRBYTE_CLIENT_ID and ATB_AIRBYTE_CLIENT_SECRET in .env "
            "(Airbyte UI → Settings → Applications)."
        ) from exc


def trigger_sync(connection_id: str, client_id: str | None = None, client_secret: str | None = None) -> str:
    headers = {**_auth_headers(client_id, client_secret), "Content-Type": "application/json"}

    # Public API job trigger
    try:
        r = requests.post(
            _public_api("jobs"),
            json={"jobType": "sync", "connectionId": connection_id},
            headers=headers,
            timeout=30,
        )
        if r.status_code in (200, 201):
            data = r.json()
            return str(data.get("jobId") or data.get("job", {}).get("id", "started"))
    except requests.RequestException:
        pass

    # Legacy
    r = requests.post(
        _legacy_api("connections/sync"),
        json={"connectionId": connection_id},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    return str(r.json().get("job", {}).get("id", ""))


def get_job(job_id: str, client_id: str | None = None, client_secret: str | None = None) -> dict[str, Any]:
    headers = _auth_headers(client_id, client_secret)
    r = requests.get(_legacy_api(f"jobs/{job_id}"), headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


def configured_connection_ids() -> list[str]:
    raw = os.getenv("ATB_AIRBYTE_CONNECTION_IDS", "")
    return [x.strip() for x in raw.split(",") if x.strip()]
