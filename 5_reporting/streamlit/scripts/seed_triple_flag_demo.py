"""Mark PEP+high-balance customers as ML high-risk for triple-flag demo."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
STREAMLIT = ROOT / "5_reporting" / "streamlit"
sys.path.insert(0, str(STREAMLIT))

from dotenv import load_dotenv
load_dotenv(STREAMLIT / ".env")

from sqlalchemy import text
from dashboard.db import get_engine

SQL = (STREAMLIT / "sql" / "seed_demo_triple_flag.sql").read_text(encoding="utf-8")


def main() -> None:
    engine = get_engine()
    if engine is None:
        raise SystemExit("SQL connection not configured")
    with engine.begin() as conn:
        conn.execute(text(SQL))
    print("Triple-flag demo seed applied — refresh Alert Inbox.")


if __name__ == "__main__":
    main()
