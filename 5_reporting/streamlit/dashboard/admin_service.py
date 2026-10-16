"""User administration for DG_CENTRAL — PFE_AUTH CRUD."""
from __future__ import annotations

import os

import pandas as pd

from dashboard.auth import AUTH_SCHEMA, hash_password
from dashboard.db import sql_execute, sql_query
from dashboard.roles import ROLE_META


def list_users(include_inactive: bool = True) -> pd.DataFrame:
    where = "" if include_inactive else "WHERE u.is_active = 1"
    q = f"""
    SELECT u.user_id, u.username, u.display_name, u.role_code,
           r.role_label_fr, r.role_label_en, u.dao_area, u.is_active, u.created_at
    FROM {AUTH_SCHEMA}.dashboard_users u
    JOIN {AUTH_SCHEMA}.dashboard_roles r ON u.role_code = r.role_code
    {where}
    ORDER BY u.is_active DESC, u.username
    """
    return sql_query(q)


def username_exists(username: str) -> bool:
    safe = username.strip().lower().replace("'", "''")
    df = sql_query(
        f"SELECT 1 AS ok FROM {AUTH_SCHEMA}.dashboard_users WHERE username = N'{safe}'"
    )
    return not df.empty


def create_user(
    username: str,
    password: str,
    display_name: str,
    role_code: str,
    dao_area: str | None = None,
) -> None:
    if role_code not in ROLE_META:
        raise ValueError(f"Unknown role: {role_code}")
    if ROLE_META[role_code].get("dao_scoped") and not dao_area:
        raise ValueError("Branch (DAO area) required for branch managers.")
    sql_execute(
        f"""
        INSERT INTO {AUTH_SCHEMA}.dashboard_users
            (username, password_hash, display_name, role_code, dao_area, is_active)
        VALUES (:username, :pw_hash, :display_name, :role_code, :dao_area, 1)
        """,
        {
            "username": username.strip().lower(),
            "pw_hash": hash_password(password),
            "display_name": display_name.strip(),
            "role_code": role_code,
            "dao_area": dao_area.strip() if dao_area else None,
        },
    )


def update_user_role(user_id: int, role_code: str, dao_area: str | None) -> None:
    if role_code not in ROLE_META:
        raise ValueError(f"Unknown role: {role_code}")
    if ROLE_META[role_code].get("dao_scoped") and not dao_area:
        raise ValueError("Branch required for CHEF_AGENCE.")
    if not ROLE_META[role_code].get("dao_scoped"):
        dao_area = None
    sql_execute(
        f"""
        UPDATE {AUTH_SCHEMA}.dashboard_users
        SET role_code = :role_code, dao_area = :dao_area
        WHERE user_id = :user_id
        """,
        {"role_code": role_code, "dao_area": dao_area, "user_id": user_id},
    )


def set_user_active(user_id: int, is_active: bool) -> None:
    sql_execute(
        f"UPDATE {AUTH_SCHEMA}.dashboard_users SET is_active = :active WHERE user_id = :uid",
        {"active": 1 if is_active else 0, "uid": user_id},
    )


def reset_password(user_id: int, new_password: str) -> None:
    sql_execute(
        f"UPDATE {AUTH_SCHEMA}.dashboard_users SET password_hash = :pw WHERE user_id = :uid",
        {"pw": hash_password(new_password), "uid": user_id},
    )


def update_display_name(user_id: int, display_name: str) -> None:
    name = display_name.strip()
    if not name:
        raise ValueError("Display name is required.")
    sql_execute(
        f"UPDATE {AUTH_SCHEMA}.dashboard_users SET display_name = :dn WHERE user_id = :uid",
        {"dn": name, "uid": user_id},
    )


def delete_user(user_id: int) -> None:
    sql_execute(
        f"DELETE FROM {AUTH_SCHEMA}.dashboard_users WHERE user_id = :uid",
        {"uid": user_id},
    )


def role_options() -> list[tuple[str, str]]:
    return [(code, meta["label_en"]) for code, meta in ROLE_META.items()]
