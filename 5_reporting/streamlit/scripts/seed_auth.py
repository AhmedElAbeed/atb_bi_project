"""Create PFE_AUTH tables and demo users in SQL Server. Run once from repo root."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
STREAMLIT = ROOT / "5_reporting" / "streamlit"
sys.path.insert(0, str(STREAMLIT))

from dotenv import load_dotenv
load_dotenv(STREAMLIT / ".env")

from sqlalchemy import text

from dashboard.auth import AUTH_SCHEMA, hash_password
from dashboard.db import get_engine
from dashboard.roles import DEMO_USERS, ROLE_META

ALL_PAGE_KEYS = [
    "page_overview", "page_studies", "page_compliance", "page_alert_inbox",
    "page_comparison", "page_risk", "page_accounts", "page_customers",
    "page_dao", "page_calendar", "page_sectors", "page_industries",
    "page_targets", "page_currencies", "page_mlflow", "page_ml",
    "page_ml_guide", "page_explorer", "page_dg_console",
]


def main() -> None:
    engine = get_engine()
    if engine is None:
        raise SystemExit("SQL connection not configured — check .env")

    s = AUTH_SCHEMA
    with engine.begin() as conn:
        conn.execute(text(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{s}')
            EXEC('CREATE SCHEMA [{s}]')
        """))

        conn.execute(text(f"""
        IF OBJECT_ID(N'{s}.dashboard_roles', N'U') IS NULL
        CREATE TABLE {s}.dashboard_roles (
            role_code NVARCHAR(40) NOT NULL PRIMARY KEY,
            role_label_fr NVARCHAR(120) NOT NULL,
            role_label_en NVARCHAR(120) NOT NULL,
            description_fr NVARCHAR(400) NULL,
            dao_scoped BIT NOT NULL DEFAULT 0,
            is_read_only BIT NOT NULL DEFAULT 0
        )
        """))

        conn.execute(text(f"""
        IF OBJECT_ID(N'{s}.dashboard_role_pages', N'U') IS NULL
        CREATE TABLE {s}.dashboard_role_pages (
            role_code NVARCHAR(40) NOT NULL,
            page_key NVARCHAR(60) NOT NULL,
            PRIMARY KEY (role_code, page_key)
        )
        """))

        conn.execute(text(f"""
        IF OBJECT_ID(N'{s}.dashboard_users', N'U') IS NULL
        CREATE TABLE {s}.dashboard_users (
            user_id INT IDENTITY(1,1) PRIMARY KEY,
            username NVARCHAR(80) NOT NULL UNIQUE,
            password_hash NVARCHAR(128) NOT NULL,
            display_name NVARCHAR(120) NOT NULL,
            role_code NVARCHAR(40) NOT NULL,
            dao_area NVARCHAR(100) NULL,
            is_active BIT NOT NULL DEFAULT 1,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )
        """))

        for role_code, meta in ROLE_META.items():
            conn.execute(text(f"""
            MERGE {s}.dashboard_roles AS t
            USING (SELECT :rc AS role_code) AS src ON t.role_code = src.role_code
            WHEN MATCHED THEN UPDATE SET
                role_label_fr = :fr, role_label_en = :en, description_fr = :desc,
                dao_scoped = :dao, is_read_only = :ro
            WHEN NOT MATCHED THEN INSERT
                (role_code, role_label_fr, role_label_en, description_fr, dao_scoped, is_read_only)
            VALUES (:rc, :fr, :en, :desc, :dao, :ro);
            """), {
                "rc": role_code,
                "fr": meta["label_fr"],
                "en": meta["label_en"],
                "desc": meta["label_fr"],
                "dao": int(meta["dao_scoped"]),
                "ro": int(meta["read_only"]),
            })

        conn.execute(text(f"DELETE FROM {s}.dashboard_role_pages"))
        for role_code, meta in ROLE_META.items():
            pages = meta.get("pages")
            if pages == "ALL":
                page_list = ALL_PAGE_KEYS
            else:
                page_list = list(pages or [])
            for pk in page_list:
                conn.execute(text(f"""
                INSERT INTO {s}.dashboard_role_pages (role_code, page_key) VALUES (:rc, :pk)
                """), {"rc": role_code, "pk": pk})

        for username, info in DEMO_USERS.items():
            h = hash_password(info["password"])
            conn.execute(text(f"""
            MERGE {s}.dashboard_users AS t
            USING (SELECT :u AS username) AS src ON t.username = src.username
            WHEN MATCHED THEN UPDATE SET
                password_hash = :h, display_name = :dn, role_code = :rc, dao_area = :da, is_active = 1
            WHEN NOT MATCHED THEN INSERT
                (username, password_hash, display_name, role_code, dao_area)
            VALUES (:u, :h, :dn, :rc, :da);
            """), {
                "u": username,
                "h": h,
                "dn": info["display_name"],
                "rc": info["role_code"],
                "da": info.get("dao_area"),
            })

    print(f"Seeded {len(DEMO_USERS)} users into {s}.dashboard_users")


if __name__ == "__main__":
    main()
