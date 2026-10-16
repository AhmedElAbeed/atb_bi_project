"""Docker, Airflow, Airbyte, and Streamlit platform operations for DG console."""
from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path
from typing import Any

import requests

APP_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = APP_DIR.parents[1]

STACKS: dict[str, dict[str, str]] = {
    "airflow": {
        "label": "Airflow (orchestration)",
        "compose": str(PROJECT_ROOT / "2_orchestration" / "docker-compose.yml"),
        "ui_url_env": "ATB_AIRFLOW_UI_URL",
        "default_ui": "http://localhost:8080/home",
    },
    "streamlit": {
        "label": "Streamlit dashboard",
        "compose": str(APP_DIR / "docker-compose.yml"),
        "ui_url_env": "ATB_STREAMLIT_UI_URL",
        "default_ui": "http://localhost:8501",
    },
}

# Auto-detect platform containers / images (no manual name entry)
PLATFORM_KEYWORDS = (
    "atb", "airbyte", "airflow", "superset", "streamlit", "temporal", "postgres", "redis", "kindest",
)


def _public_url(url: str) -> str:
    """Links opened in the user's browser must use localhost, not host.docker.internal."""
    return url.replace("host.docker.internal", "localhost")


def stack_browser_url(stack_key: str) -> str:
    meta = STACKS[stack_key]
    return _public_url(os.getenv(meta["ui_url_env"], meta["default_ui"]))


def airflow_base_url() -> str:
    return os.getenv("ATB_AIRFLOW_URL", "http://host.docker.internal:8080").rstrip("/")


def airflow_ui_url() -> str:
    return _public_url(os.getenv("ATB_AIRFLOW_UI_URL", "http://localhost:8080/home"))


def airflow_auth() -> tuple[str, str]:
    return (
        os.getenv("ATB_AIRFLOW_USER", "admin"),
        os.getenv("ATB_AIRFLOW_PASSWORD", "admin"),
    )


def _docker_client():
    try:
        import docker
        return docker.from_env()
    except Exception:
        return None


def docker_available() -> bool:
    client = _docker_client()
    if client is None:
        return False
    try:
        client.ping()
        return True
    except Exception:
        return False


def _matches_platform(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in PLATFORM_KEYWORDS)


def list_atb_containers() -> list[dict[str, Any]]:
    client = _docker_client()
    if client is None:
        return []
    rows: list[dict[str, Any]] = []
    try:
        for c in client.containers.list(all=True):
            name = c.name
            tags = c.image.tags
            image = tags[0] if tags else (c.image.short_id or "—")
            blob = f"{name} {image}"
            if not _matches_platform(blob):
                continue
            status = c.status
            health = (c.attrs.get("State") or {}).get("Health") or {}
            health_status = health.get("Status", "")
            rows.append({
                "name": name,
                "image": image,
                "status": c.attrs.get("State", {}).get("Status", status),
                "state": status,
                "healthy": status == "running" and health_status in ("healthy", ""),
            })
    except Exception:
        return []
    return sorted(rows, key=lambda r: r["name"])


def container_action(name: str, action: str) -> tuple[bool, str]:
    if action not in {"start", "stop", "restart"}:
        return False, f"Unknown action: {action}"
    client = _docker_client()
    if client is None:
        return False, "Docker SDK unavailable — ensure /var/run/docker.sock is mounted."
    try:
        c = client.containers.get(name)
        getattr(c, action)()
        return True, f"{action} {name} OK"
    except Exception as exc:
        return False, str(exc)


def compose_action(stack_key: str, action: str) -> tuple[bool, str]:
    stack = STACKS.get(stack_key)
    if not stack:
        return False, f"Unknown stack: {stack_key}"
    compose_path = Path(stack["compose"])
    if not compose_path.is_file():
        return False, f"Compose file not found: {compose_path}"

    docker_bin = _find_docker_bin()
    if not docker_bin:
        return False, "docker CLI not in container — rebuild image or run compose from host."

    cmd = [docker_bin, "compose", "-f", str(compose_path)]
    if action == "up":
        cmd += ["up", "-d", "--remove-orphans"]
    elif action == "down":
        cmd += ["down"]
    elif action == "restart":
        cmd += ["restart"]
    else:
        return False, f"Unknown compose action: {action}"

    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300, check=False,
            cwd=str(compose_path.parent),
        )
        ok = proc.returncode == 0
        msg = (proc.stdout or proc.stderr or "").strip()[:2000]
        return ok, msg or f"compose {action} exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        return False, "Compose command timed out (5 min)."


def _find_docker_bin() -> str | None:
    for candidate in ("/usr/bin/docker", "/usr/local/bin/docker", "docker"):
        try:
            proc = subprocess.run(
                [candidate, "version"], capture_output=True, timeout=5, check=False,
            )
            if proc.returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            continue
    return None


def list_docker_images() -> list[str]:
    """All local images related to ATB platform stacks."""
    client = _docker_client()
    if client is None:
        return []
    found: list[str] = []
    try:
        for img in client.images.list():
            tags = img.tags or [img.short_id]
            for tag in tags:
                if _matches_platform(tag) and tag not in found:
                    found.append(tag)
    except Exception:
        return []
    return sorted(found)


def streamlit_self_health() -> dict[str, Any]:
    hostname = socket.gethostname()
    started = os.getenv("HOSTNAME", hostname)
    return {
        "hostname": hostname,
        "container_id": started[:12] if started else "—",
        "app_path": str(APP_DIR),
        "mlflow_uri": os.getenv("MLFLOW_TRACKING_URI", "—"),
        "docker_ok": docker_available(),
    }


def airflow_health() -> dict[str, Any]:
    url = airflow_base_url()
    try:
        r = requests.get(f"{url}/health", timeout=6)
        if r.status_code == 200:
            return {"reachable": True, "url": url, "payload": r.json()}
        return {"reachable": False, "url": url, "error": f"HTTP {r.status_code}"}
    except requests.RequestException as exc:
        return {"reachable": False, "url": url, "error": str(exc)}


def _airflow_request(method: str, path: str, **kwargs) -> Any:
    url = f"{airflow_base_url()}/api/v1{path}"
    auth = airflow_auth()
    r = requests.request(method, url, auth=auth, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()


def airflow_list_dags() -> list[dict[str, Any]]:
    try:
        data = _airflow_request("GET", "/dags", params={"limit": 100, "only_active": False})
        dags = data.get("dags") or []
        return [
            {
                "dag_id": d.get("dag_id"),
                "is_paused": d.get("is_paused"),
                "tags": ", ".join(t.get("name", "") for t in (d.get("tags") or [])),
                "description": (d.get("description") or "")[:80],
            }
            for d in dags
        ]
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def airflow_trigger_dag(dag_id: str, conf: dict | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if conf:
        body["conf"] = conf
    return _airflow_request("POST", f"/dags/{dag_id}/dagRuns", json=body)


def airflow_list_runs(dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    data = _airflow_request("GET", f"/dags/{dag_id}/dagRuns", params={"limit": limit, "order_by": "-start_date"})
    runs = data.get("dag_runs") or []
    return [
        {
            "run_id": r.get("dag_run_id"),
            "state": r.get("state"),
            "start": r.get("start_date"),
            "end": r.get("end_date"),
        }
        for r in runs
    ]


def airflow_pause_dag(dag_id: str, paused: bool) -> None:
    _airflow_request("PATCH", f"/dags/{dag_id}", json={"is_paused": paused})
