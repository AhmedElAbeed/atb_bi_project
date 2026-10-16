"""Dashboard constants and theme."""
from __future__ import annotations

import os

SCHEMA = "PFE_DWH"
ML_SCHEMA = "PFE_ML"

RISK_ORDER = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH", "UNKNOWN", "CRITICAL"]
RISK_COLORS = {
    "LOW": "#22c55e",
    "MEDIUM": "#f59e0b",
    "HIGH": "#ef4444",
    "VERY_HIGH": "#a855f7",
    "CRITICAL": "#7f1d1d",
    "UNKNOWN": "#64748b",
}

# ATB Arab Tunisian Bank brand palette (from official logo)
ATB_PRIMARY = "#9E1B32"
ATB_PRIMARY_DARK = "#6B1222"
ATB_PRIMARY_LIGHT = "#C42D48"
ATB_SILVER = "#C0C0C0"
ATB_SILVER_DIM = "#8A8A8A"
ATB_GOLD = "#D4AF37"
ATB_BG_DARK = "#0a0a0a"
ATB_BG_CARD = "#1a1012"
ATB_BG_SIDEBAR = "#12080a"

CHART_TEMPLATE = "plotly_dark"
CHART_COLORS = [ATB_PRIMARY, ATB_SILVER, ATB_GOLD, ATB_PRIMARY_LIGHT, "#E8B4B8", "#6B1222"]

DEFAULT_TOP_N = int(os.getenv("ATB_DASHBOARD_TOP_N", "15000"))

# Gemini — loaded from .env (set GEMINI_API_KEY in your .env file)
DEFAULT_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

TABLES = {
    "dim_currency": f"{SCHEMA}.dim_currency",
    "dim_customer": f"{SCHEMA}.dim_customer",
    "dim_date": f"{SCHEMA}.dim_date",
    "dim_dao": f"{SCHEMA}.dim_dao",
    "dim_industry": f"{SCHEMA}.dim_industry",
    "dim_sector": f"{SCHEMA}.dim_sector",
    "dim_target": f"{SCHEMA}.dim_target",
    "fact_account": f"{SCHEMA}.fact_account",
    "fact_customer_risk": f"{SCHEMA}.fact_customer_risk",
}
