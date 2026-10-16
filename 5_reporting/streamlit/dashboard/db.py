"""SQL Server connection helpers."""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

__all__ = ["build_db_url", "get_engine", "sql_query", "sql_execute", "check_connection"]


def build_db_url() -> str | None:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        return db_url

    server = os.getenv("ATB_SQL_SERVER", "DESKTOP-B0PDEI7").strip()
    database = os.getenv("ATB_SQL_DATABASE", "ATB_BI").strip()
    username = os.getenv("ATB_SQL_USERNAME", "airbyte_user").strip()
    password = os.getenv("ATB_SQL_PASSWORD", "AZERTY123").strip()
    driver = os.getenv("ATB_SQL_DRIVER", "ODBC Driver 17 for SQL Server").strip().replace(" ", "+")
    port = os.getenv("ATB_SQL_PORT", "1434").strip()

    if not (server and username and password):
        return None

    return (
        f"mssql+pyodbc://{username}:{password}@{server}:{port}/{database}"
        f"?driver={driver}&TrustServerCertificate=yes&Encrypt=no"
    )


@st.cache_resource
def get_engine():
    db_url = build_db_url()
    if not db_url:
        return None
    return create_engine(db_url, pool_pre_ping=True, pool_size=5, max_overflow=10)


@st.cache_data(ttl=180, show_spinner=False)
def sql_query(query: str) -> pd.DataFrame:
    engine = get_engine()
    if engine is None:
        raise RuntimeError("Database connection is not configured.")
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


def check_connection() -> bool:
    try:
        sql_query("SELECT 1 AS ok")
        return True
    except Exception:
        return False


def sql_execute(query: str, params: dict | None = None) -> int:
    """Run INSERT/UPDATE/DELETE; returns rowcount."""
    engine = get_engine()
    if engine is None:
        raise RuntimeError("Database connection is not configured.")
    with engine.begin() as conn:
        result = conn.execute(text(query), params or {})
        return result.rowcount
